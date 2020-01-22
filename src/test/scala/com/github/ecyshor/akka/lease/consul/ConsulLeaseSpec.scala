package com.github.ecyshor.akka.lease.consul

import java.time.Instant
import java.util.Date

import akka.actor.{ActorSystem, ExtendedActorSystem}
import akka.coordination.lease.{LeaseSettings, LeaseTimeoutException}
import akka.util.Timeout
import com.github.ecyshor.akka.lease.consul.ConsulClient.{ConsulCallFailure, Session}
import com.typesafe.config.ConfigFactory
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.Assertion
import org.scalatest.concurrent.Eventually._
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}

import scala.concurrent.{ExecutionContext, Future}

class ConsulLeaseSpec extends AsyncFlatSpec with Matchers with AsyncMockFactory {

  private val system = ActorSystem().asInstanceOf[ExtendedActorSystem]

  private val sessionId = "id"

  private val session = Session(sessionId, Date.from(Instant.now()))
  implicit val patienceConfig = PatienceConfig(timeout = scaled(Span(30, Seconds)), interval = scaled(Span(50, Millis)))

  "Consul lease" should "acquire lock" in {
    val client = mock[ConsulClient]
    val sessionClient = mock[ConsulSessionClient]
    (sessionClient.getCurrentSession()(_: Timeout, _: ExecutionContext)).expects(*, *).returning(Future.successful(Right(session))).once()
    (client.acquireLock _).expects(*, *, *).returning(Future.successful(Right(true))).once()
    withLease(client, sessionClient) { lease =>
      lease.acquire().map(_ shouldBe true)
    }
  }

  it should "keep lock if has the same session" in {
    val client = stub[ConsulClient]
    val sessionClient = stub[ConsulSessionClient]
    (sessionClient.getCurrentSession()(_: Timeout, _: ExecutionContext)).when(*, *).returning(Future.successful(Right(session)))
    (client.acquireLock _).when(*, *, *).returning(Future.successful(Right(true)))
    (client.checkLock _).when(*).returning(Future.successful(Right(Some(sessionId))))
    withLease(client, sessionClient) { lease =>
      lease.acquire().flatMap(res => {
        res shouldBe true
        Thread.sleep(100)
        (client.checkLock _).verify(*)
        lease.checkLease() shouldBe true
      })
    }
  }

  it should "release lock when another session holds it" in {
    val client = mock[ConsulClient]
    val sessionClient = mock[ConsulSessionClient]
    (sessionClient.getCurrentSession()(_: Timeout, _: ExecutionContext)).expects(*, *).returning(Future.successful(Right(session))).once()
    (client.acquireLock _).expects(*, *, *).returning(Future.successful(Right(true))).once()
    (client.checkLock _).expects(*).returning(Future.successful(Right(Some("otherId")))).once()
    withLease(client, sessionClient) { lease =>
      lease.acquire().map(acquired => {
        lockWasAcquiredAndEventuallyIsLost(lease, acquired)
      })
    }
  }

  it should "release lock when no session holds it" in {
    val client = mock[ConsulClient]
    val sessionClient = mock[ConsulSessionClient]
    (sessionClient.getCurrentSession()(_: Timeout, _: ExecutionContext)).expects(*, *).returning(Future.successful(Right(session))).once()
    (client.acquireLock _).expects(*, *, *).returning(Future.successful(Right(true))).once()
    (client.checkLock _).expects(*).returning(Future.successful(Right(None))).once()
    withLease(client, sessionClient) { lease =>
      lease.acquire().map(acquired => {
        lockWasAcquiredAndEventuallyIsLost(lease, acquired)
      })
    }
  }


  it should "release lock on check failure" in {
    val client = mock[ConsulClient]
    val sessionClient = mock[ConsulSessionClient]
    (sessionClient.getCurrentSession()(_: Timeout, _: ExecutionContext)).expects(*, *).returning(Future.successful(Right(session))).once()
    (client.acquireLock _).expects(*, *, *).returning(Future.successful(Right(true))).once()
    (client.checkLock _).expects(*).returning(Future.successful(Right(None))).once()
    withLease(client, sessionClient) { lease =>
      lease.acquire().map(acquired => {
        lockWasAcquiredAndEventuallyIsLost(lease, acquired)
      })
    }
  }

  it should "release lock on session failure" in {
    val client = mock[ConsulClient]
    val sessionClient = mock[ConsulSessionClient]
    (sessionClient.getCurrentSession()(_: Timeout, _: ExecutionContext)).expects(*, *).returning(Future.successful(Right(session))).once()
    (sessionClient.getCurrentSession()(_: Timeout, _: ExecutionContext)).expects(*, *).returning(Future.successful(Left(ConsulCallFailure(new RuntimeException)))).once()
    (client.acquireLock _).expects(*, *, *).returning(Future.successful(Right(true))).once()
    (client.checkLock _).expects(*).returning(Future.successful(Right(Some("maybe")))).once()
    withLease(client, sessionClient) { lease =>
      lease.acquire().map(acquired => {
        lockWasAcquiredAndEventuallyIsLost(lease, acquired)
      })
    }
  }

  it should "reset lock after failure when check returns the same session lock" in {
    val client = mock[ConsulClient]
    val sessionClient = mock[ConsulSessionClient]
    (sessionClient.getCurrentSession()(_: Timeout, _: ExecutionContext)).expects(*, *).returning(Future.successful(Right(session))).repeat(3)
    (client.acquireLock _).expects(*, *, *).returning(Future.successful(Right(true))).once()
    (client.checkLock _).expects(*).returning(Future.successful(Right(Some(sessionId)))).once()
    (client.checkLock _).expects(*).returning(Future.successful(Right(None))).once()
    (client.checkLock _).expects(*).returning(Future.successful(Right(Some(sessionId)))).once()
    withLease(client, sessionClient) { lease =>
      lease.acquire().map(acquired => {
        lockWasAcquiredAndEventuallyIsLost(lease, acquired)
        eventually {
          lease.checkLease() shouldBe true
        }
      })
    }
  }

  "releasing the lock" should "release it" in {
    val client = mock[ConsulClient]
    val sessionClient = mock[ConsulSessionClient]
    (sessionClient.getCurrentSession()(_: Timeout, _: ExecutionContext)).expects(*, *).returning(Future.successful(Right(session))).anyNumberOfTimes()
    (client.acquireLock _).expects(*, *, *).returning(Future.successful(Right(true))).once()
    (client.releaseLock _).expects(session, *).returning(Future.successful(Right {
      true
    })).once()
    withLease(client, sessionClient) { lease =>
      lease.acquire().flatMap(acquired => {
        acquired should be(true)
        lease.release().map(released => {
          released shouldBe true
          lease.checkLease() shouldBe false
        })
      })
    }

  }

  it should "timeout according to the config" in {
    val client = mock[ConsulClient]
    val sessionClient = mock[ConsulSessionClient]
    (sessionClient.getCurrentSession()(_: Timeout, _: ExecutionContext)).expects(*, *).returning(Future.never)
    withLease(client, sessionClient) { lease =>
      recoverToSucceededIf[LeaseTimeoutException] {
        lease.acquire()
      }
    }
  }


  private def lockWasAcquiredAndEventuallyIsLost(lease: ConsulLease, acquired: Boolean) = {
    acquired shouldBe true
    eventually {
      lease.checkLease() shouldBe false
    }
  }

  private def withLease(client: ConsulClient, sessionClient: ConsulSessionClient)(test: ConsulLease => Future[Assertion]) = {
    val lease = new ConsulLease(LeaseSettings(
      ConfigFactory.parseString(
        """
          |heartbeat-timeout = 30 seconds
          |heartbeat-interval = 10 seconds
          |lease-operation-timeout = 1 seconds
          |""".stripMargin), "test-lease", "test-owner"
    ), system, client, sessionClient)
    test(lease).andThen(_ => lease.cancelChecks())
  }
}
