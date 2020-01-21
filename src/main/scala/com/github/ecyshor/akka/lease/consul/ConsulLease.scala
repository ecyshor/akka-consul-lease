package com.github.ecyshor.akka.lease.consul

import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean

import akka.actor.{ActorSystem, ExtendedActorSystem}
import akka.coordination.lease.scaladsl.Lease
import akka.coordination.lease.{LeaseException, LeaseSettings, LeaseTimeoutException}
import akka.stream.scaladsl.{BroadcastHub, Keep, Sink, Source}
import akka.stream.{ActorAttributes, Supervision}
import akka.util.Timeout
import com.github.ecyshor.akka.lease.consul.ConsulClient._
import com.github.ecyshor.akka.lease.consul.ConsulLease._
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.Future
import scala.concurrent.duration.{FiniteDuration, _}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

class ConsulLease(settings: LeaseSettings, actorSystem: ExtendedActorSystem, consulClient: ConsulClient, consulSessionClient: ConsulSessionClient) extends Lease(settings) with LazyLogging {

  import actorSystem.dispatcher

  def this(settings: LeaseSettings, actorSystem: ExtendedActorSystem, consulClient: ConsulClient) {
    this(settings, actorSystem, consulClient, ConsulSessionClient(actorSystem, consulClient, ConsulSessionConfig.fromLeaseSettings(settings)))
  }

  def this(settings: LeaseSettings, actorSystem: ExtendedActorSystem) {
    this(settings, actorSystem, new ConsulClient(ConsulClientConfig(settings.leaseConfig))(actorSystem))
  }

  private implicit val system: ActorSystem = actorSystem

  private val consulLockConfig = ConsulLockConfig(s"akka/leases/${settings.leaseName}", settings.timeoutSettings.heartbeatInterval)

  private val lockHeld: AtomicBoolean = new AtomicBoolean(false)
  private implicit val timeout: Timeout = Timeout(settings.timeoutSettings.operationTimeout)

  private lazy val lockCheckStream = Source.tick(0.seconds, settings.timeoutSettings.heartbeatInterval, {})
    .mapAsync(1)(_ => {
      val consulCheck = consulClient.checkLock(consulLockConfig.lockPath) flatMap {
        case Right(value) =>
          value match {
            case Some(sessionWhichHoldsTheLease) =>
              consulSessionClient.getCurrentSession().map(currentSession => {
                currentSession.map(sessionWhichHoldsTheLease == _.id)
              })
            case None => Future.successful(Right(false))
          }
        case Left(value) =>
          Future.successful(Left(value))
      }
      consulCheck andThen updateLockAsReleasedIfRequired
    })
    .addAttributes(ActorAttributes.supervisionStrategy {
      case NonFatal(ex) =>
        logger.warn(s"Failed to check lock for config $consulLockConfig", ex)
        Supervision.Resume
    })
    .toMat(BroadcastHub.sink(bufferSize = 1))(Keep.right).run()

  private lazy val runningStream = lockCheckStream.runWith(Sink.ignore) //used to always have at least once consumer for the broadcast

  override def acquire(): Future[Boolean] = {
    unpack {
      consulSessionClient.getCurrentSession()
    }.flatMap {
      session =>
        unpack {
          consulClient.acquireLock(session, consulLockConfig.lockPath, settings.ownerName)
        }.map(acquired => {
          lockHeld.set(acquired)
          //reference the check stream to ensure is started
          runningStream
          acquired
        })
    }
  }


  override def acquire(leaseLostCallback: Option[Throwable] => Unit): Future[Boolean] = {
    acquire().andThen {
      case Success(true) =>
        lockCheckStream.filterNot {
          case Right(value) => value
          case Left(_) => false
        }.runWith(Sink.head).foreach {
          case Left(value) =>
            leaseLostCallback(Some(new LeaseException(s"Lease lost $value")))
          case Right(_) =>
            leaseLostCallback(None)
        }
    }
  }

  override def release(): Future[Boolean] = {
    unpack {
      consulSessionClient.getCurrentSession()
    }.flatMap(session => {
      unpack(consulClient.releaseLock(session).andThen {
        case Success(_) => lockHeld.set(false)
      })
    }).map(_ => true)
  }

  override def checkLease(): Boolean = lockHeld.get()

  private def updateLockAsReleasedIfRequired: PartialFunction[Try[Either[ConsulFailure, Boolean]], Unit] = {
    case Success(value) =>
      value match {
        case Left(failure) =>
          logger.warn(s"Marking lease ${settings.leaseName} for owner ${settings.ownerName} as released because of failure from consul $failure")
          lockHeld.set(false)
        case Right(isLockOwnedByUs) =>
          if (!isLockOwnedByUs) {
            logger.warn(s"Marking lease ${settings.leaseName} for owner ${settings.ownerName} as released because another session helds the lock")
            lockHeld.set(false)
          }
      }
    case Failure(failure) =>
      logger.warn(s"Marking lease ${settings.leaseName} for owner ${settings.ownerName} as released because of failure", failure)
      lockHeld.set(false)
  }

  private def unpack[T](future: Future[Either[ConsulFailure, T]]): Future[T] = {
    future.map {
      case Left(value) =>
        value match {
          case ConsulCallFailure(failure) =>
            throw new LeaseException(s"Failre in communicating with consul $failure")
          case ConsulResponseFailure(message, code) =>
            throw new LeaseException(s"Consul responded with code $code and $message")
          case ConsulTimeoutFailure(timeout) =>
            throw new LeaseTimeoutException(s"Operation timed out after $timeout")
        }
      case Right(value) => value
    }
  }

}

object ConsulLease {

  case class ConsulSessionConfig(lockDelay: FiniteDuration, name: String, ttl: FiniteDuration, renewDuration: FiniteDuration) {
    require(ttl > renewDuration)
    require(name.nonEmpty)
    require(lockDelay.toSeconds >= 0 && lockDelay.toSeconds <= 60)
    val maxSessionRenewRetries: Long = ttl.toSeconds / renewDuration.toSeconds
  }

  object ConsulSessionConfig {
    def fromLeaseSettings(settings: LeaseSettings) = ConsulSessionConfig(settings.timeoutSettings.heartbeatInterval.plus(settings.timeoutSettings.operationTimeout), s"${settings.ownerName}-akka-lease-${settings.leaseName}", settings.timeoutSettings.heartbeatTimeout, settings.timeoutSettings.heartbeatInterval)
  }

  case class ConsulLockConfig(lockPath: String, lockCheckInterval: FiniteDuration) {
    require(lockPath.nonEmpty)
    require(lockCheckInterval.toSeconds > 0)
  }

}
