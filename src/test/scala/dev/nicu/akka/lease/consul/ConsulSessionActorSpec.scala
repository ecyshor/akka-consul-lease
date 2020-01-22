package dev.nicu.akka.lease.consul

import java.time.Instant
import java.util.Date

import akka.actor.{ActorRef, ActorSystem, PoisonPill}
import akka.testkit.{ImplicitSender, TestKit}
import dev.nicu.akka.lease.consul.ConsulClient._
import dev.nicu.akka.lease.consul.ConsulLease.ConsulSessionConfig
import dev.nicu.akka.lease.consul.ConsulSessionActor.GetSession
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.Future
import scala.concurrent.duration._

class ConsulSessionActorSpec extends TestKit(ActorSystem("consul-session-actor-spec"))
  with ImplicitSender
  with AnyWordSpecLike
  with Matchers
  with MockFactory
  with BeforeAndAfterAll with Eventually {

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  val consulSessionConfig = ConsulSessionConfig(5.seconds, "test", 3.5.seconds, 1.seconds)

  "session creation" must {

    "start session on init" in withSessionActor { (actor, client) =>
      val session = Session(
        "id", Date.from(Instant.now())
      )
      (client.createSession _).expects(consulSessionConfig) returning Future.successful(Right(session))
      actor ! GetSession
      expectMsg(session)
    }

    "retry session creation" in withSessionActor { (actor, client) =>
      lazy val session = Session(
        "id", Date.from(Instant.now().plusSeconds(20))
      )
      (client.createSession _).expects(consulSessionConfig).returning(Future.successful(Left(ConsulTimeoutFailure(2.seconds)))).once()
      (client.createSession _).expects(consulSessionConfig).returning(Future.successful(Right(session))).once()
      actor ! GetSession
      expectMsg(11.seconds, session)
    }
  }

  "session renewal" must {

    "renew session as configured" in withSessionActor { (_, client) =>
      val session = Session(
        "id", Date.from(Instant.now())
      )
      (client.createSession _).expects(consulSessionConfig) returning Future.successful(Right(session))
      (client.renewSession _).expects(session) returning Future.successful(Right(SessionRenewed))
      Thread.sleep(1200) // sleep for renewal
    }


    "create new session if it was invalidated" in withSessionActor { (actor, client) =>
      val session = Session(
        "id", Date.from(Instant.now())
      )
      val secondsSession = session.copy(id = "other")
      (client.createSession _).expects(consulSessionConfig) returning Future.successful(Right(session))
      (client.createSession _).expects(consulSessionConfig) returning Future.successful(Right(secondsSession))
      (client.renewSession _).expects(session) returning Future.successful(Right(SessionInvalidated))
      actor ! GetSession
      expectMsg(session)
      eventually(timeout(2.seconds)) {
        actor ! GetSession
        expectMsg(secondsSession)
      }
    }

    "retry renewal until it exceeds the session ttl" in withSessionActor { (actor, client) =>
      val session = Session(
        "id", Date.from(Instant.now())
      )
      val secondsSession = session.copy(id = "other", lastRenew = Date.from(Instant.now().plusSeconds(20)))
      (client.createSession _).expects(consulSessionConfig) returning Future.successful(Right(session))
      (client.createSession _).expects(consulSessionConfig) returning Future.successful(Right(secondsSession))
      (client.renewSession _).expects(session) returning Future.successful {
        Left(ConsulCallFailure(new IllegalArgumentException))
      } repeated 2
      actor ! GetSession
      expectMsg(session)
      eventually(timeout(4.seconds)) {
        actor ! GetSession
        expectMsg(secondsSession)
      }
    }

    "start new session when current one has expired ttl" in withSessionActor { (actor, client) =>
      val expiredSession = Session(
        "id", Date.from(Instant.now().minusSeconds(4))
      )
      val aliveSession = Session(id = "other", lastRenew = Date.from(Instant.now().plusSeconds(20)))
      (client.createSession _).expects(consulSessionConfig) returning Future.successful(Right(expiredSession))
      (client.createSession _).expects(consulSessionConfig) returning Future.successful(Right(aliveSession))
      actor ! GetSession
      expectMsg(aliveSession)
    }
  }

  def withSessionActor(testCode: (ActorRef, ConsulClient) => Any) {
    val client = mock[ConsulClient]
    val actor = system.actorOf(ConsulSessionActor.props(client, consulSessionConfig))
    try {
      testCode(actor, client)
    }
    finally actor ! PoisonPill
  }
}
