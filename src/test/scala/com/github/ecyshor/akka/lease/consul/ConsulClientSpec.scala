package com.github.ecyshor.akka.lease.consul

import java.time.Instant
import java.util.{Date, UUID}

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.github.ecyshor.akka.lease.consul.ConsulClient.{ConsulClientConfig, Session, SessionInvalidated, SessionRenewed}
import com.github.ecyshor.akka.lease.consul.ConsulLease.ConsulSessionConfig
import org.scalatest.Assertion
import org.scalatest.flatspec.AsyncFlatSpecLike
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Future
import scala.concurrent.duration._
import akka.pattern.{after => delayed}

class ConsulClientSpec extends TestKit(ActorSystem("consul-client-spec")) with AsyncFlatSpecLike with Matchers {
  val consulClient = new ConsulClient(ConsulClientConfig(
    system.settings.config
  ))

  val sessionConfig = ConsulSessionConfig(
    30.seconds,
    "mysession",
    10.second,
    100.millis
  )

  "Creating sessions" should "create session" in {
    consulClient.createSession(sessionConfig).map {
      case Left(value) => fail(s"Found left value $value")
      case Right(value) => value.id should not be empty
    }
  }

  "Renewing sessions" should "renew session" in withSession { session =>
    consulClient.renewSession(session).map {
      case Left(value) => fail(s"Found left value $value")
      case Right(value) => value shouldBe SessionRenewed
    }
  }

  it should "keep session alive" in withSession { session =>
    val autoRenew = system.scheduler.scheduleAtFixedRate(7.seconds, 7.seconds) {
      () => consulClient.renewSession(session)
    }
    delayed(30.seconds, system.scheduler)({
      autoRenew.cancel()
      consulClient.renewSession(session)
    }).map {
      case Left(value) => fail(s"Found left value $value")
      case Right(value) => value shouldBe SessionRenewed
    }
  }

  it should "return invalidated after ttl expired" in withSession { session =>
    delayed(30.seconds, system.scheduler)(consulClient.renewSession(session)).map {
      case Left(value) => fail(s"Found left value $value")
      case Right(value) => value shouldBe SessionInvalidated
    }
  }

  it should "return invalidated if session does not exist" in {
    consulClient.renewSession(Session(UUID.randomUUID().toString, Date.from(Instant.now()))).map {
      case Left(value) => fail(s"Found left value $value")
      case Right(value) => value shouldBe SessionInvalidated
    }
  }

  private def withSession(test: (Session) => Future[Assertion]) = {
    consulClient.createSession(sessionConfig).map {
      case Left(value) => fail(s"Found left value during session $value")
      case Right(value) => value
    }.flatMap(test(_))
  }



}
