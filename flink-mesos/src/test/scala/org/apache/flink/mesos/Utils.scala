package org.apache.flink.mesos

import java.util.concurrent.atomic.AtomicLong

import akka.actor._
import akka.testkit.{TestActorRef, TestFSMRef}
import org.mockito.ArgumentMatcher

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

object Matchers {
  def contentsMatch[T](plan: Seq[T]): java.util.Collection[T] = {
    org.mockito.Matchers.argThat(new ArgumentMatcher[java.util.Collection[T]] {
      override def matches(o: scala.Any): Boolean = o match {
        case actual: java.util.Collection[T] => actual.size() == plan.size && actual.containsAll(plan.asJava)
        case _ => false
      }
    })
  }
}

object TestFSMUtils {

  val number = new AtomicLong
  def randomName: String = {
    val l = number.getAndIncrement()
    "$" + akka.util.Helpers.base64(l)
  }

  def testFSMRef[S, D, T <: Actor: ClassTag](factory: ⇒ T, supervisor: ActorRef)(implicit ev: T <:< FSM[S, D], system: ActorSystem): TestFSMRef[S, D, T] = {
    new TestFSMRef(system, Props(factory), supervisor, TestFSMUtils.randomName)
  }
}
