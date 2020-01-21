package com.github.ecyshor.akka.lease.consul

import java.util.UUID

import akka.actor.{ActorNotFound, ActorRef, ActorSystem, InvalidActorNameException}
import akka.pattern.ask
import akka.util.Timeout
import com.github.ecyshor.akka.lease.consul.ConsulClient.{ConsulFailure, Session}
import com.github.ecyshor.akka.lease.consul.ConsulLease.ConsulSessionConfig
import com.github.ecyshor.akka.lease.consul.ConsulSessionActor.GetSession
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try
import scala.concurrent.duration._

class ConsulSessionClient(sessionActor: Future[ActorRef]) {

  def getCurrentSession()(implicit timeout: Timeout, ec: ExecutionContext): Future[Either[ConsulFailure, Session]] = {
    sessionActor.flatMap(ask(_, GetSession).mapTo[Session]).map(Right(_))
  }

}

object ConsulSessionClient extends LazyLogging {

  //ensure no name clashes will happen
  val ConsulSessionActorName = s"akka-lease-consul-session-actor-${UUID.randomUUID()}"

  def apply(system: ActorSystem, consulClient: ConsulClient, consulSessionConfig: ConsulSessionConfig): ConsulSessionClient = {
    val props = ConsulSessionActor.props(consulClient, consulSessionConfig)
    new ConsulSessionClient((Try {
      system.actorOf(props, ConsulSessionActorName)
    }.map(Future.successful) recover {
      case _: InvalidActorNameException =>
        system.actorSelection(ConsulSessionActorName).resolveOne(5.seconds).recover {
          case ex: ActorNotFound =>
            logger.error("Failed to reuse the consul session actor. A new instance will be created and a new consul session. Please report this issue if needed.", ex)
            system.actorOf(props)
        }(system.dispatcher)
    }).get)
  }
}
