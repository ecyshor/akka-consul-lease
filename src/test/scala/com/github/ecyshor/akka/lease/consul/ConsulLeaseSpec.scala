package com.github.ecyshor.akka.lease.consul

import java.time.Instant
import java.util.Date

import akka.actor.{ActorSystem, ExtendedActorSystem}
import akka.coordination.lease.LeaseSettings
import akka.util.Timeout
import com.github.ecyshor.akka.lease.consul.ConsulClient.{ConsulCallFailure, Session}
import com.typesafe.config.ConfigFactory
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.concurrent.Eventually._
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.{ExecutionContext, Future}

class ConsulLeaseSpec extends AsyncFlatSpec with Matchers with AsyncMockFactory {

  val system = ActorSystem().asInstanceOf[ExtendedActorSystem]

  private val sessionId = "id"

  private val session = Session(sessionId, Date.from(Instant.now()))

  "Consul lease" should "acquire lock" in {
    val client = mock[ConsulClient]
    val sessionClient = mock[ConsulSessionClient]
    (sessionClient.getCurrentSession()(_: Timeout, _: ExecutionContext)).expects(*, *).returning(Future.successful(Right(session))).once()
    (client.acquireLock _).expects(*, *, *).returning(Future.successful(Right(true))).once()
    val lease = buildLease(client, sessionClient)
    lease.acquire().map(_ shouldBe true)
  }

  it should "keep lock if has the same session" in {
    val client = stub[ConsulClient]
    val sessionClient = stub[ConsulSessionClient]
    (sessionClient.getCurrentSession()(_: Timeout, _: ExecutionContext)).when(*, *).returning(Future.successful(Right(session)))
    (client.acquireLock _).when(*, *, *).returning(Future.successful(Right(true)))
    (client.checkLock _).when(*).returning(Future.successful(Right(Some(sessionId))))
    val lease = buildLease(client, sessionClient)
    lease.acquire().map(res => {
      res shouldBe true
      eventually {
        lease.checkLease() shouldBe true
      }
      (client.checkLock _).verify(*)
      succeed
    })
  }

  it should "release lock when another session holds it" in {
    val client = mock[ConsulClient]
    val sessionClient = mock[ConsulSessionClient]
    (sessionClient.getCurrentSession()(_: Timeout, _: ExecutionContext)).expects(*, *).returning(Future.successful(Right(session))).once()
    (client.acquireLock _).expects(*, *, *).returning(Future.successful(Right(true))).once()
    (client.checkLock _).expects(*).returning(Future.successful(Right(Some("otherId")))).once()
    val lease = buildLease(client, sessionClient)
    lease.acquire().map(acquired => {
      lockWasAcquiredAndEventuallyIsLost(lease, acquired)
    })
  }

  it should "release lock when no session holds it" in {
    val client = mock[ConsulClient]
    val sessionClient = mock[ConsulSessionClient]
    (sessionClient.getCurrentSession()(_: Timeout, _: ExecutionContext)).expects(*, *).returning(Future.successful(Right(session))).once()
    (client.acquireLock _).expects(*, *, *).returning(Future.successful(Right(true))).once()
    (client.checkLock _).expects(*).returning(Future.successful(Right(None))).once()
    val lease = buildLease(client, sessionClient)
    lease.acquire().map(acquired => {
      lockWasAcquiredAndEventuallyIsLost(lease, acquired)
    })
  }


  it should "release lock on check failure" in {
    val client = mock[ConsulClient]
    val sessionClient = mock[ConsulSessionClient]
    (sessionClient.getCurrentSession()(_: Timeout, _: ExecutionContext)).expects(*, *).returning(Future.successful(Right(session))).once()
    (client.acquireLock _).expects(*, *, *).returning(Future.successful(Right(true))).once()
    (client.checkLock _).expects(*).returning(Future.successful(Right(None))).once()
    val lease = buildLease(client, sessionClient)
    lease.acquire().map(acquired => {
      lockWasAcquiredAndEventuallyIsLost(lease, acquired)
    })
  }

  it should "release lock on session failure" in {
    val client = mock[ConsulClient]
    val sessionClient = mock[ConsulSessionClient]
    (sessionClient.getCurrentSession()(_: Timeout, _: ExecutionContext)).expects(*, *).returning(Future.successful(Right(session))).once()
    (sessionClient.getCurrentSession()(_: Timeout, _: ExecutionContext)).expects(*, *).returning(Future.successful(Left(ConsulCallFailure(new RuntimeException)))).once()
    (client.acquireLock _).expects(*, *, *).returning(Future.successful(Right(true))).once()
    (client.checkLock _).expects(*).returning(Future.successful(Right(Some("maybe")))).once()
    val lease = buildLease(client, sessionClient)
    lease.acquire().map(acquired => {
      lockWasAcquiredAndEventuallyIsLost(lease, acquired)
    })
  }

  it should "reset lock after failure when check returns the same session lock" in {
    val client = mock[ConsulClient]
    val sessionClient = mock[ConsulSessionClient]
    (sessionClient.getCurrentSession()(_: Timeout, _: ExecutionContext)).expects(*, *).returning(Future.successful(Right(session))).repeat(4)
    (client.acquireLock _).expects(*, *, *).returning(Future.successful(Right(true))).once()
    (client.checkLock _).expects(*).returning(Future.successful(Right(Some(sessionId)))).once()
    (client.checkLock _).expects(*).returning(Future.successful(Right(None))).once()
    (client.checkLock _).expects(*).returning(Future.successful(Right(Some(sessionId)))).once()
    val lease = buildLease(client, sessionClient)
    lease.acquire().map(acquired => {
      lockWasAcquiredAndEventuallyIsLost(lease, acquired)
      eventually {
        lease.checkLease() shouldBe true
      }
    })
  }

  "releasing the lock" should "release it" in {
    val client = mock[ConsulClient]
    val sessionClient = mock[ConsulSessionClient]
    (sessionClient.getCurrentSession()(_: Timeout, _: ExecutionContext)).expects(*, *).returning(Future.successful(Right(session))).anyNumberOfTimes()
    (client.acquireLock _).expects(*, *, *).returning(Future.successful(Right(true))).once()
    (client.releaseLock _).expects(session).returning(Future.successful(Right {
      {}
    })).once()
    val lease = buildLease(client, sessionClient)
    lease.acquire().flatMap(acquired => {
      acquired should be(true)
      lease.release().map(released => {
        released shouldBe true
        lease.checkLease() shouldBe false
      })
    })
  }

  private def lockWasAcquiredAndEventuallyIsLost(lease: ConsulLease, acquired: Boolean) = {
    acquired shouldBe true
    eventually {
      lease.checkLease() shouldBe false
    }
  }

  private def buildLease(client: ConsulClient, sessionClient: ConsulSessionClient) = {
    new ConsulLease(LeaseSettings(
      ConfigFactory.parseString(
        """
          |heartbeat-timeout = 30 seconds
          |heartbeat-interval = 10 seconds
          |lease-operation-timeout = 1 seconds
          |""".stripMargin), "test-lease", "test-owner"
    ), system, client, sessionClient)
  }
}
