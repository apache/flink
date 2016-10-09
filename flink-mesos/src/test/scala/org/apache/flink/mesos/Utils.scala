/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.mesos

import java.util.concurrent.atomic.AtomicLong

import akka.actor._
import akka.testkit.TestFSMRef
import org.mockito.ArgumentMatcher

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

object Matchers {
  def contentsMatch[T](plan: Seq[T]): java.util.Collection[T] = {
    org.mockito.Matchers.argThat(new ArgumentMatcher[java.util.Collection[T]] {
      override def matches(o: scala.Any): Boolean = o match {
        case actual: java.util.Collection[T] =>
          actual.size() == plan.size && actual.containsAll(plan.asJava)
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

  def testFSMRef[S, D, T <: Actor: ClassTag](factory: => T, supervisor: ActorRef)
      (implicit ev: T <:< FSM[S, D], system: ActorSystem): TestFSMRef[S, D, T] = {
    new TestFSMRef(system, Props(factory), supervisor, TestFSMUtils.randomName)
  }
}
