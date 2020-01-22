package dev.nicu.akka.lease.consul

import java.util.concurrent.atomic.AtomicBoolean

import akka.actor.{ActorSystem, ExtendedActorSystem}
import akka.coordination.lease.scaladsl.Lease
import akka.coordination.lease.{LeaseException, LeaseSettings, LeaseTimeoutException}
import akka.stream.scaladsl.{BroadcastHub, Keep, Sink, Source}
import akka.stream.{ActorAttributes, Supervision}
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging
import dev.nicu.akka.lease.consul.ConsulClient._
import dev.nicu.akka.lease.consul.ConsulLease._

import scala.concurrent.Future
import scala.concurrent.duration.{FiniteDuration, _}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

class ConsulLease(settings: LeaseSettings, actorSystem: ExtendedActorSystem, consulClient: ConsulClient, consulSessionClient: ConsulSessionClient) extends Lease(settings) with LazyLogging with TimeoutSupport {

  import actorSystem.dispatcher

  def this(settings: LeaseSettings, actorSystem: ExtendedActorSystem, consulClient: ConsulClient) {
    this(settings, actorSystem, consulClient, ConsulSessionClient(actorSystem, consulClient, ConsulSessionConfig.fromLeaseSettings(settings), settings.leaseConfig.withFallback(ConfigFactory.parseString(
      """
        |session-actor-name = "akka-lease-consul-session-actor"
        |""".stripMargin)).getString("session-actor-name")))
  }

  def this(settings: LeaseSettings, actorSystem: ExtendedActorSystem) {
    this(settings, actorSystem, new ConsulClient(ConsulClientConfig(settings.leaseConfig))(actorSystem))
  }

  private implicit val system: ActorSystem = actorSystem

  private val consulLockConfig = ConsulLockConfig(settings)

  private val lockHeld: AtomicBoolean = new AtomicBoolean(false)
  private implicit val timeout: Timeout = Timeout(settings.timeoutSettings.operationTimeout)

  //keep the stream alive to  account for external interventions as well
  private lazy val (streamStop, lockCheckStream) = Source.tick(0.seconds, consulLockConfig.lockCheckInterval, {})
    .mapAsync(1)(_ => {
      logger.debug(s"Checking lock for $consulLockConfig")
      val consulCheck = runLeaseOpWithTimeout(settings.timeoutSettings.operationTimeout) {
        consulClient.checkLock(consulLockConfig.lockPath) flatMap {
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
      }
      consulCheck andThen updateLockIfRequired()
    })
    .addAttributes(ActorAttributes.supervisionStrategy {
      case NonFatal(ex) =>
        logger.warn(s"Failed to check lock for config $consulLockConfig", ex)
        Supervision.Resume
    })
    .toMat(BroadcastHub.sink(bufferSize = 1))(Keep.both).run()

  private lazy val runningStream = lockCheckStream.runWith(Sink.ignore).andThen {
    case Failure(exception) =>
      logger.error("Running check stream failed, marking as lost least", exception)
    case Success(_) =>
      logger.debug(s"Checked lock successfully $consulLockConfig")
  } //used to always have at least once consumer for the broadcast

  override def acquire(): Future[Boolean] = {
    runLeaseOpWithTimeout(settings.timeoutSettings.operationTimeout) {
      unpack {
        consulSessionClient.getCurrentSession()
      }.flatMap {
        session =>
          unpack {
            logger.info(s"Acquiring lock $consulLockConfig")
            consulClient.acquireLock(session, consulLockConfig.lockPath, settings.ownerName)
          }.map(acquired => {
            logger.info(s"Acquired lock[$acquired] $consulLockConfig")
            lockHeld.set(acquired)
            //reference the check stream to ensure is started
            runningStream
            acquired
          })
      }
    }
  }


  override def acquire(leaseLostCallback: Option[Throwable] => Unit): Future[Boolean] = runLeaseOpWithTimeout(settings.timeoutSettings.operationTimeout) {
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

  override def release(): Future[Boolean] = runLeaseOpWithTimeout(settings.timeoutSettings.operationTimeout) {
    unpack {
      consulSessionClient.getCurrentSession()
    }.flatMap(session => {
      logger.info("Releasing lock")
      unpack(consulClient.releaseLock(session, consulLockConfig.lockPath)).andThen {
        case Success(true) =>
          logger.info(s"Lock released $consulLockConfig")
          lockHeld.set(false)
      }
    })
  }

  override def checkLease(): Boolean = lockHeld.get()

  private def updateLockIfRequired(): PartialFunction[Try[Either[ConsulFailure, Boolean]], Unit] = {
    case Success(value) =>
      value match {
        case Left(failure) =>
          logger.warn(s"Marking lease ${settings.leaseName} for owner ${settings.ownerName} as released because of failure from consul $failure")
          lockHeld.set(false)
        case Right(isLockOwnedByUs) =>
          if (!isLockOwnedByUs) {
            if (lockHeld.get())
              logger.warn(s"Marking lease ${settings.leaseName} for owner ${settings.ownerName} as released because another session holds the lock")
            lockHeld.set(false)
          } else {
            lockHeld.set(true)
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
            throw new LeaseException(s"Failure in communicating with consul $failure")
          case ConsulResponseFailure(message, code) =>
            throw new LeaseException(s"Consul responded with code $code and $message")
          case ConsulTimeoutFailure(timeout) =>
            throw new LeaseTimeoutException(s"Operation timed out after $timeout")
        }
      case Right(value) => value
    }
  }

  private[consul] def cancelChecks() = {
    streamStop.cancel()
  }

}

object ConsulLease {

  case class ConsulSessionConfig(lockDelay: FiniteDuration, name: String, ttl: FiniteDuration, renewDuration: FiniteDuration) {
    require(ttl > renewDuration)
    require(name.nonEmpty)
    require(lockDelay.toSeconds >= 0 && lockDelay.toSeconds <= 60, "The lock delay is (heartbeat-interval + heartbeat-timeout) by default if not set explicitly using `lock-delay` and should not exceed the maximum of 60 seconds")
    val maxSessionRenewRetries: Long = ttl.toMillis / renewDuration.toMillis
  }

  object ConsulSessionConfig {
    def fromLeaseSettings(settings: LeaseSettings): ConsulSessionConfig = {
      val config = settings.leaseConfig
      ConsulSessionConfig(
        extractDuration(settings.timeoutSettings.heartbeatInterval * 2, "lock-delay", config),
        s"${settings.ownerName}-akka-lease-${settings.leaseName}",
        extractDuration(settings.timeoutSettings.heartbeatTimeout, "session-ttl", config),
        extractDuration(settings.timeoutSettings.heartbeatTimeout / 3, "session-renew-interval", config)
      )
    }

    private def extractDuration(lockDelay: FiniteDuration, key: SessionId, config: Config) = {
      if (config.hasPath(key)) {
        config.getDuration(key).getSeconds.seconds
      } else {
        lockDelay
      }
    }
  }

  case class ConsulLockConfig(lockPath: String, lockCheckInterval: FiniteDuration) {
    require(lockPath.nonEmpty)
    require(lockCheckInterval.toSeconds > 0)
  }

  object ConsulLockConfig {
    def apply(settings: LeaseSettings): ConsulLockConfig = {
      val config = settings.leaseConfig.withFallback(ConfigFactory.parseString(
        """
          |kv-prefix = akka/leases
          |""".stripMargin))
      new ConsulLockConfig(s"${config.getString("kv-prefix").stripSuffix("/")}/${settings.leaseName}", settings.timeoutSettings.heartbeatInterval)
    }
  }

}
