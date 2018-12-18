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

package akka.actor

import java.lang.Thread.UncaughtExceptionHandler

import org.apache.flink.runtime.akka.AkkaUtils
import org.junit.{After, Before, Test}
import org.scalatest.Matchers
import org.scalatest.junit.JUnitSuite

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, Promise}
import scala.util.Success

class RobustActorSystemTest extends JUnitSuite with Matchers {

  var robustActorSystem: RobustActorSystem = null
  var testingUncaughtExceptionHandler: TestingUncaughtExceptionHandler = null

  @Before
  def setup(): Unit = {
    testingUncaughtExceptionHandler = new TestingUncaughtExceptionHandler
    robustActorSystem = RobustActorSystem.create(
      "testSystem",
      AkkaUtils.testDispatcherConfig,
      testingUncaughtExceptionHandler)
  }

  @After
  def teardown(): Unit = {
    robustActorSystem.terminate()
    testingUncaughtExceptionHandler = null;
  }

  @Test
  def testUncaughtExceptionHandler(): Unit = {
    val error = new UnknownError("Foobar")

    Future {
      throw error
    }(robustActorSystem.dispatcher)

    val caughtException = Await.result(
      testingUncaughtExceptionHandler.exceptionPromise.future,
      Duration.Inf)

    caughtException should equal (error)
  }

  @Test
  def testUncaughtExceptionHandlerFromActor(): Unit = {
    val error = new UnknownError()
    val actor = robustActorSystem.actorOf(Props.create(classOf[UncaughtExceptionActor], error))

    actor ! Failure

    val caughtException = Await.result(
      testingUncaughtExceptionHandler.exceptionPromise.future,
      Duration.Inf)

    caughtException should equal (error)
  }
}

class TestingUncaughtExceptionHandler extends UncaughtExceptionHandler {
  val exceptionPromise: Promise[Throwable] = Promise()

  override def uncaughtException(t: Thread, e: Throwable): Unit = {
    exceptionPromise.complete(Success(e))
  }
}

class UncaughtExceptionActor(failure: Throwable) extends Actor {
  override def receive: Receive = {
    case Failure => {
      throw failure
    };
  }
}

case object Failure
