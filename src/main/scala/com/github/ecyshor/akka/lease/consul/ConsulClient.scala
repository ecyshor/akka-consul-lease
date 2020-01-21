package com.github.ecyshor.akka.lease.consul

import java.time.Instant
import java.util.Date

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling._
import com.github.ecyshor.akka.lease.consul.ConsulClient.Session.jsonFormat
import com.github.ecyshor.akka.lease.consul.ConsulClient._
import com.github.ecyshor.akka.lease.consul.ConsulLease.ConsulSessionConfig
import com.typesafe.config.Config
import spray.json.RootJsonReader

import scala.concurrent.duration.{Duration, FiniteDuration, _}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal
import spray.json.DefaultJsonProtocol._

class ConsulClient(clientConfig: ConsulClientConfig)(implicit actorSystem: ActorSystem) extends SprayJsonSupport {

  def createSession(config: ConsulSessionConfig)(implicit ec: ExecutionContext): Future[Either[ConsulFailure, Session]] = {
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


  def renewSession(session: Session)(implicit ec: ExecutionContext): Future[Either[ConsulFailure, SessionStatus]] = {
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

  def acquireLock(session: Session, key: String, owner: String): Future[Either[ConsulFailure, Boolean]] = ???

  def checkLock(key: String): Future[Either[ConsulFailure, Option[SessionId]]] = ???

  def releaseLock(session: Session): Future[Either[ConsulFailure, Unit]] = ???

  private def doConsulCall[T: RootJsonReader](request: HttpRequest)(implicit ec: ExecutionContext) = {
    runOperationWithTimeout {
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

  private def runOperationWithTimeout[T](op: Future[Either[ConsulFailure, T]])(implicit ec: ExecutionContext) = {
    Future.firstCompletedOf(Seq(
      akka.pattern.after(clientConfig.timeout, using = actorSystem.scheduler)(Future.successful(Left(ConsulTimeoutFailure(clientConfig.timeout)))),
      op
    ))
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

  object Session extends DefaultJsonProtocol {
    implicit val jsonFormat: RootJsonFormat[Session] = new RootJsonFormat[Session] {
      override def write(obj: Session): JsValue = ???

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
