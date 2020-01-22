package com.github.ecyshor.akka.lease.consul

import java.util.UUID

import akka.actor.ActorSystem
import akka.coordination.lease.scaladsl.{Lease, LeaseProvider}
import akka.testkit.TestKit
import org.scalatest.OptionValues
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AsyncFlatSpecLike
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Future

class LeaseSpec extends TestKit(ActorSystem("consul-client-spec")) with AsyncFlatSpecLike with Matchers with OptionValues with Eventually {
  // we use different config path so we can get different consul sessions for the owners

  "The lease" should "be acquired by a single owner" in {
    val leaseName = UUID.randomUUID().toString
    val lease1 = LeaseProvider(system).getLease(leaseName, "test-lease-1", "owner1")
    val lease2 = LeaseProvider(system).getLease(leaseName, "test-lease-2", "owner2")
    val lease3 = LeaseProvider(system).getLease(leaseName, "test-lease-3", "owner3")
    oneLeaseAcquiresTheLock(lease1, lease2, lease3)
  }

  private def oneLeaseAcquiresTheLock(lease1: Lease, lease2: Lease, lease3: Lease) = {
    Future.sequence(Seq(lease1.acquire(), lease2.acquire(), lease3.acquire())).map { acquired => {
      acquired should contain allElementsOf Seq(true, false, false)
      acquired should contain theSameElementsInOrderAs Seq(lease1.checkLease(), lease2.checkLease(), lease3.checkLease())
    }
    }
  }

  it should "be acquired by another owner" in {
    val leaseName = UUID.randomUUID().toString
    val lease1 = LeaseProvider(system).getLease(leaseName, "test-lease-1", "owner1")
    val lease2 = LeaseProvider(system).getLease(leaseName, "test-lease-2", "owner2")
    val lease3 = LeaseProvider(system).getLease(leaseName, "test-lease-3", "owner3")
    Future.sequence(Seq(lease1.acquire(), lease2.acquire(), lease3.acquire())).flatMap { acquired => {
      acquired should contain allElementsOf Seq(true, false, false)
      Seq(lease1, lease2, lease3).find(_.checkLease()).value.release().flatMap(released => {
        released shouldBe true
        Seq(lease1.checkLease(), lease2.checkLease(), lease3.checkLease()) should contain allElementsOf Seq(false, false, false)
        oneLeaseAcquiresTheLock(lease1, lease2, lease3)
      })
    }
    }
  }

}
