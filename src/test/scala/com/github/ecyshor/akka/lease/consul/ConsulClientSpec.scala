package com.github.ecyshor.akka.lease.consul

import java.time.Instant
import java.util.{Date, UUID}

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.github.ecyshor.akka.lease.consul.ConsulClient.{ConsulClientConfig, Session, SessionInvalidated, SessionRenewed}
import com.github.ecyshor.akka.lease.consul.ConsulLease.ConsulSessionConfig
import org.scalatest.{Assertion, OptionValues}
import org.scalatest.flatspec.AsyncFlatSpecLike
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Future
import scala.concurrent.duration._
import akka.pattern.{after => delayed}

class ConsulClientSpec extends TestKit(ActorSystem("consul-client-spec")) with AsyncFlatSpecLike with Matchers with OptionValues {
  val consulClient = new ConsulClient(ConsulClientConfig(
    system.settings.config
  ))

  val sessionConfig = ConsulSessionConfig(
    5.seconds,
    "mysession",
    10.second,
    100.millis
  )

  "Creating sessions" should "create session" in {
    consulClient.createSession(sessionConfig).map {
      rightCheck {
        _.id should not be empty
      }
    }
  }

  "Renewing sessions" should "renew session" in withSession { session =>
    consulClient.renewSession(session).map {
      rightCheck {
        _ shouldBe SessionRenewed
      }
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
      rightCheck {
        _ shouldBe SessionRenewed
      }
    }
  }

  it should "return invalidated after ttl expired" in withSession { session =>
    delayed(30.seconds, system.scheduler)(consulClient.renewSession(session)).map {
      rightCheck {
        _ shouldBe SessionInvalidated
      }
    }
  }

  it should "return invalidated if session does not exist" in {
    consulClient.renewSession(Session(UUID.randomUUID().toString, Date.from(Instant.now()))).map {
      rightCheck {
        _ shouldBe SessionInvalidated
      }
    }
  }

  "acquiring lock" should "successfully acquire" in withSession { session =>
    consulClient.acquireLock(session, UUID.randomUUID().toString, "owner").map {
      rightCheck {
        _ shouldBe true
      }
    }
  }

  it should "reacquire for same session" in withLockAndSession((session, lockKey) => {
    consulClient.acquireLock(session, lockKey, "owner").map {
      rightCheck {
        _ shouldBe true
      }
    }
  })

  it should "fail to acquire with different session" in withLockAndSession { (_, lockKey) =>
    withSession { newSession =>
      consulClient.acquireLock(newSession, lockKey, "owner").map {
        rightCheck {
          _ shouldBe false
        }
      }
    }
  }

  it should "reacquire for the same session after releasing the lock" in withLockAndSession((session, lockKey) => {
    consulClient.releaseLock(session, lockKey).map {
      rightCheck(_.shouldBe(true))
    }.flatMap(_ => consulClient.acquireLock(session, lockKey, "owner").map {
      rightCheck {
        _ shouldBe true
      }
    })
  })


  "checking lock" should "check lock successfully" in withLockAndSession { (session, lockKey) =>
    consulClient.checkLock(lockKey).map(rightCheck {
      _.value shouldBe session.id
    })
  }

  it should "return no owner for non existent key" in {
    consulClient.checkLock("NotThere").map(rightCheck {
      _ shouldBe None
    })
  }

  it should "return no owner for lock with expired session" in withLockAndSession { (_, lockKey) =>
    delayed(30.seconds, system.scheduler)(consulClient.checkLock(lockKey)).map(rightCheck {
      _ shouldBe None
    })
  }

  "releasing locks" should "release successfully" in withLockAndSession { (session, lockKey) =>
    consulClient.releaseLock(session, lockKey).map(rightCheck {
      _ shouldBe true
    })
  }

  it should "return response as true only when actually releasing the lock" in withLockAndSession { (session, lockKey) =>
    consulClient.releaseLock(session, lockKey).map(rightCheck {
      _ shouldBe true
    }).flatMap { _ =>
      consulClient.releaseLock(session, lockKey).map(rightCheck {
        _ shouldBe false
      })
    }
  }

  it should "fail to release when using different sessions" in withLockAndSession { (_, lockKey) =>
    withSession { session =>
      consulClient.releaseLock(session, lockKey).map {
        rightCheck {
          _ shouldBe false
        }
      }
    }
  }

  it should "consider it not released when key does not exist" in withSession { session =>
    consulClient.releaseLock(session, "NOKEYHERE").map {
      rightCheck {
        _ shouldBe false
      }
    }
  }

  it should "consider it not released when session expired" in withLockAndSession { (session, lockKey) =>
    delayed(30.seconds, system.scheduler)(consulClient.releaseLock(session, lockKey)).map(rightCheck {
      _ shouldBe false
    })
  }


  private def rightCheck[L, R](check: R => Assertion)(either: Either[L, R]) = {
    either match {
      case Left(value) => fail(s"Found left value $value")
      case Right(value) => check(value)
    }
  }

  private def withLockAndSession(test: (Session, String) => Future[Assertion]) = {
    val key = UUID.randomUUID().toString
    withSession {
      session => {
        consulClient.acquireLock(session, key, "testOwner").flatMap {
          case Right(value) if value => test(session, key)
          case other => fail(s"Found value during lock acquire $other")
        }
      }
    }
  }

  private def withSession(test: Session => Future[Assertion]) = {
    consulClient.createSession(sessionConfig).map {
      case Left(value) => fail(s"Found left value during session $value")
      case Right(value) => value
    }.flatMap(test(_))
  }


}
