package dev.nicu.akka.lease.consul

import java.time.Instant
import java.util.Date

import akka.actor.{Actor, ActorLogging, Props, Stash, Timers}
import akka.event.LoggingReceive
import akka.pattern.pipe
import dev.nicu.akka.lease.consul.ConsulClient.{ConsulFailure, Session, SessionInvalidated, SessionRenewed}
import dev.nicu.akka.lease.consul.ConsulLease.ConsulSessionConfig
import dev.nicu.akka.lease.consul.ConsulSessionActor.{CreateSession, GetSession, RenewSession}

import scala.concurrent.duration._

class ConsulSessionActor(consulClient: ConsulClient, consulSessionConfig: ConsulSessionConfig) extends Actor with ActorLogging with Timers with Stash {

  import context.dispatcher

  private val sessionRenewTimerName = "session-renew"
  private val sessionRetryTimerName = "session-retry"

  log.info("Starting consul session actor to manage a consul session for locks")

  timers.startSingleTimer(sessionRetryTimerName, CreateSession, 100.milli)

  override def receive: Receive = LoggingReceive {
    case Right(session: Session) =>
      log.info("Started new session with id {}", session.id)
      context.become(hasSession(session), discardOld = true)
      timers.startSingleTimer(sessionRenewTimerName, RenewSession, consulSessionConfig.renewDuration)
      unstashAll()
    case Left(failure: ConsulFailure) =>
      log.warning(s"Failed to create consul session for locks. Will retry again in 10 seconds $failure")
      timers.startSingleTimer(sessionRetryTimerName, CreateSession, 10.seconds)
    case CreateSession =>
      log.info("Creating new session")
      pipe(consulClient.createSession(consulSessionConfig)) to self
    case GetSession =>
      stash()
    case other =>
      log.warning(s"Received unknown message, should not happen $other")
  }

  def hasSession(session: Session, retries: Long = 0): Receive = {
    case RenewSession =>
      log.debug("Renewing session {}", session)
      context.become(hasSession(session, retries + 1))
      consulClient.renewSession(session) pipeTo self
    case Right(SessionRenewed) =>
      log.debug("Session {} renewed", session)
      context.become(hasSession(session.copy(lastRenew = Date.from(Instant.now()))))
      timers.startSingleTimer(sessionRenewTimerName, RenewSession, consulSessionConfig.renewDuration)
    case Right(SessionInvalidated) =>
      log.warning("Session {} was invalidated and all locks will be released. Creating new session and any lease will have to be reacquired", session)
      context.become(receive)
      self ! CreateSession
    case Left(failure: ConsulFailure) =>
      if (session.withinTtl(consulSessionConfig.ttl)) {
        log.warning(s"Failed to renew consul session for locks, will try again on next renew interval $failure")
        timers.startSingleTimer(sessionRenewTimerName, RenewSession, consulSessionConfig.renewDuration)
      } else {
        log.error(s"Failed to renew consul session for locks and no retries left, creating new session $failure")
        context.become(receive)
        timers.startSingleTimer(sessionRetryTimerName, CreateSession, consulSessionConfig.renewDuration)
      }
    case GetSession =>
      if (session.withinTtl(consulSessionConfig.ttl)) {
        sender() ! session
      }
      else {
        log.warning("Session has expired ttl, creating new one")
        stash()
        context.become(receive)
        timers.cancel(sessionRenewTimerName)
        timers.startSingleTimer(sessionRetryTimerName, CreateSession, 100.milli)
      }
    case other =>
      log.warning(s"Received unknown message with session, should not happen $other")
  }


  override def aroundPreRestart(reason: Throwable, message: Option[Any]): Unit = {
    super.aroundPreRestart(reason, message)
    log.warning(s"Session actor restarting on message $message and reason $reason")
  }
}

object ConsulSessionActor {

  private[consul] case object RenewSession

  private[consul] case object CreateSession

  private[consul] case object GetSession

  def props(consulClient: ConsulClient, consulSessionConfig: ConsulSessionConfig) = Props(new ConsulSessionActor(consulClient, consulSessionConfig))
}
