package com.github.ecyshor.akka.lease.consul

import java.time.Instant
import java.util.Date

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling._
import com.github.ecyshor.akka.lease.consul.ConsulClient.Session.jsonFormat
import com.github.ecyshor.akka.lease.consul.ConsulClient.KeyResponse.keyResponseJsonFormat
import com.github.ecyshor.akka.lease.consul.ConsulClient._
import com.github.ecyshor.akka.lease.consul.ConsulLease.ConsulSessionConfig
import com.typesafe.config.Config
import spray.json.DefaultJsonProtocol._

import scala.concurrent.duration.{Duration, FiniteDuration, _}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

class ConsulClient(clientConfig: ConsulClientConfig)(implicit actorSystem: ActorSystem) extends SprayJsonSupport with TimeoutSupport {

  import actorSystem.dispatcher

  def createSession(config: ConsulSessionConfig): Future[Either[ConsulFailure, Session]] = {
    doConsulCall[Session](HttpRequest(
      method = HttpMethods.PUT,
      uri = Uri(s"$connectionUri/session/create"),
      entity = HttpEntity.apply(ContentTypes.`application/json`,
        s"""
           |{
           |            "LockDelay" : "${config.lockDelay.toSeconds}s",
           |            "Name": "${config.name}",
           |            "TTL": "${config.ttl.toSeconds}s"
           |          }
           |""".stripMargin)
    ))
  }


  def renewSession(session: Session): Future[Either[ConsulFailure, SessionStatus]] = {
    doConsulCall[Seq[Session]](HttpRequest(
      method = HttpMethods.PUT,
      uri = Uri(s"$connectionUri/session/renew/${session.id}")
    )).map {
      case Left(value) =>
        value match {
          case ConsulResponseFailure(_, code) if code == 404 =>
            Right(SessionInvalidated)
          case other => Left(other)
        }
      case Right(_) => Right(SessionRenewed)
    }
  }

  def acquireLock(session: Session, key: String, owner: String): Future[Either[ConsulFailure, Boolean]] = {
    doConsulCall[String](HttpRequest(
      method = HttpMethods.PUT,
      uri = Uri(s"$connectionUri/kv/$key").withQuery(Uri.Query("acquire" -> session.id)),
      entity = HttpEntity.apply(ContentTypes.`application/json`,
        s"""
           |{
           |    "owner" : "$owner",
           |    "locked_at" : "${Instant.now()}"
           | }
           |""".stripMargin)
    )).map(_.map(_.trim.toBoolean))
  }

  def checkLock(key: String): Future[Either[ConsulFailure, Option[SessionId]]] = {
    doConsulCall[Seq[KeyResponse]](HttpRequest(
      method = HttpMethods.GET,
      uri = Uri(s"$connectionUri/kv/$key"))
    ).map(_.flatMap { response =>
      if (response.length == 1) {
        Right(response.head)
      } else Left(ConsulResponseFailure(s"Consul responded with incorrect number of keys $response", 200))
    }).map(_.map(_.session)).map {
      case Left(ConsulResponseFailure(_, 404)) =>
        Right(None)
      case other => other
    }
  }

  def releaseLock(session: Session, key: String): Future[Either[ConsulFailure, Boolean]] =
    doConsulCall[String](HttpRequest(
      method = HttpMethods.PUT,
      uri = Uri(s"$connectionUri/kv/$key").withQuery(Uri.Query("release" -> session.id))
    )).map(_.map(_.trim.toBoolean))

  private def doConsulCall[T](request: HttpRequest)(implicit unmarshaller: Unmarshaller[ResponseEntity, T]) = {
    runOperationWithTimeout(clientConfig.timeout) {
      Http().singleRequest(
        request
      ).flatMap(response => {
        if (response.status.isSuccess()) {
          Unmarshal(response.entity).to[T].map(Right(_))
        } else {
          Unmarshal(response.entity).to[String].map(responseMessage => {
            Left(ConsulResponseFailure(responseMessage, response.status.intValue()))
          })
        }
      }).recover {
        case NonFatal(e) => Left(ConsulCallFailure(e))
      }
    }
  }

  private lazy val connectionUri = {
    s"${clientConfig.scheme}://${clientConfig.host}${clientConfig.port.map(port => s":$port").getOrElse("")}/v1"
  }


}

object ConsulClient {

  import spray.json._

  type SessionId = String

  case class ConsulClientConfig(scheme: String, host: String, port: Option[Int], timeout: FiniteDuration)

  object ConsulClientConfig {
    def apply(config: Config): ConsulClientConfig = ConsulClientConfig(config.getString("consul.scheme"), config.getString("consul.host"), {
      if (config.hasPath("consul.port")) Some(config.getInt("consul.port")) else None
    }, config.getDuration("consul.timeout").getSeconds.seconds)
  }

  case class Session(id: SessionId, lastRenew: Date) {
    def withinTtl(ttl: Duration): Boolean = lastRenew.toInstant.plusSeconds(ttl.toSeconds).isAfter(Instant.now)

    def withinTtlAfterTimeout(ttl: Duration, timeout: Duration): Boolean = lastRenew.toInstant.plusSeconds(ttl.toSeconds).isAfter(Instant.now.plusSeconds(timeout.toSeconds))
  }

  case class KeyResponse(session: Option[SessionId])

  object KeyResponse {
    implicit val keyResponseJsonFormat: RootJsonFormat[KeyResponse] = new RootJsonFormat[KeyResponse] {
      override def read(json: JsValue): KeyResponse = {
        KeyResponse(json.asJsObject.fields.get("Session").map(_.convertTo[String]))
      }

      override def write(obj: KeyResponse): JsValue = ??? // hack around marshallers requiring a format instead of a reader
    }
  }

  object Session extends DefaultJsonProtocol {
    implicit val jsonFormat: RootJsonFormat[Session] = new RootJsonFormat[Session] {
      override def write(obj: Session): JsValue = ??? // hack around marshallers requiring a format instead of a reader

      override def read(json: JsValue): Session = Session(json.asJsObject.fields("ID").convertTo[String], Date.from(Instant.now()))
    }
  }

  sealed trait ConsulFailure

  case class ConsulTimeoutFailure(timeout: FiniteDuration) extends ConsulFailure

  case class ConsulCallFailure(failure: Throwable) extends ConsulFailure

  case class ConsulResponseFailure(message: String, code: Int) extends ConsulFailure

  sealed trait SessionStatus

  case object SessionRenewed extends SessionStatus

  case object SessionInvalidated extends SessionStatus

}
