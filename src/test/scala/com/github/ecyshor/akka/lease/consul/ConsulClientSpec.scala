package com.github.ecyshor.akka.lease.consul

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.github.ecyshor.akka.lease.consul.ConsulClient.ConsulClientConfig
import com.github.ecyshor.akka.lease.consul.ConsulLease.ConsulSessionConfig
import org.scalatest.flatspec.AsyncFlatSpecLike
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._

class ConsulClientSpec extends TestKit(ActorSystem("consul-client-spec")) with AsyncFlatSpecLike with Matchers {
  val consulClient = new ConsulClient(ConsulClientConfig(
    system.settings.config
  ))

  val sessionConfig = ConsulSessionConfig(
    30.seconds,
    "mysession",
    1.minute,
    20.seconds
  )

  "The consul client" should "create session" in {
    consulClient.createSession(sessionConfig).map {
      case Left(value) => fail(s"Found left value $value")
      case Right(value) => value.id should not be empty
    }
  }

  it should "renew session" in {
    consulClient.createSession(sessionConfig).flatMap(session )
  }
}
