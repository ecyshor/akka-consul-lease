package com.github.ecyshor.akka.lease.consul

import akka.actor.{ActorNotFound, ActorRef, ActorSystem, InvalidActorNameException, Props}
import akka.pattern.{ask, retry}
import akka.util.Timeout
import com.github.ecyshor.akka.lease.consul.ConsulClient.{ConsulFailure, Session}
import com.github.ecyshor.akka.lease.consul.ConsulLease.ConsulSessionConfig
import com.github.ecyshor.akka.lease.consul.ConsulSessionActor.GetSession
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

class ConsulSessionClient(sessionActor: Future[ActorRef]) {

  def getCurrentSession()(implicit timeout: Timeout, ec: ExecutionContext): Future[Either[ConsulFailure, Session]] = {
    sessionActor.flatMap(ask(_, GetSession).mapTo[Session]).map(Right(_))
  }

}

object ConsulSessionClient extends LazyLogging {

  def apply(system: ActorSystem, consulClient: ConsulClient, consulSessionConfig: ConsulSessionConfig, actorName: String): ConsulSessionClient = {
    val props = ConsulSessionActor.props(consulClient, consulSessionConfig)
    new ConsulSessionClient((managerActor(system, props, actorName)).get)
  }

  private def managerActor(system: ActorSystem, props: Props, actorName: String): Try[Future[ActorRef]] = {
    Try {
      system.actorOf(props, actorName)
    }.map(Future.successful) recover {
      case _: InvalidActorNameException =>
        retry(() => resolveManager(system, actorName), 3, 1.second)(system.dispatcher, system.scheduler).recover { //retry to ensure handle some race conditions
          case ex: ActorNotFound =>
            logger.error("Failed to reuse the consul session actor. A new instance will be created and a new consul session. Please report this issue if needed.", ex)
            system.actorOf(props)
        }(system.dispatcher)
    }
  }

  private def resolveManager(system: ActorSystem, actorName: String) = {
    system.actorSelection(s"/user/$actorName").resolveOne(5.seconds)
  }
}
