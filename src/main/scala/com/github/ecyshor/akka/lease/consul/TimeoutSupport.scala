package com.github.ecyshor.akka.lease.consul

import akka.actor.ActorSystem
import akka.coordination.lease.LeaseTimeoutException
import com.github.ecyshor.akka.lease.consul.ConsulClient.{ConsulFailure, ConsulTimeoutFailure}

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

trait TimeoutSupport {
  def runOperationWithTimeout[T](timeout: FiniteDuration)(op: Future[Either[ConsulFailure, T]])(implicit ec: ExecutionContext, actorSystem: ActorSystem): Future[Either[ConsulFailure, T]] = {
    Future.firstCompletedOf(Seq(
      akka.pattern.after(timeout, using = actorSystem.scheduler)(Future.successful(Left(ConsulTimeoutFailure(timeout)))),
      op
    ))
  }

  def runLeaseOpWithTimeout[T](timeout: FiniteDuration)(op: Future[T])(implicit ec: ExecutionContext, actorSystem: ActorSystem): Future[T] = {
    Future.firstCompletedOf(Seq(
      akka.pattern.after(timeout, using = actorSystem.scheduler)(Future.failed(new LeaseTimeoutException(s"Operation timed out after $timeout"))),
      op
    ))
  }
}
