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

package org.apache.flink.runtime.akka

import java.util.UUID

import akka.actor.{Props, Kill, ActorRef, ActorSystem}
import akka.testkit.{TestActorRef, TestKit}
import grizzled.slf4j.Logger
import org.apache.flink.runtime.akka.FlinkUntypedActorTest.PlainRequiresLeaderSessionID
import org.apache.flink.runtime.messages.JobManagerMessages.LeaderSessionMessage
import org.apache.flink.runtime.{LeaderSessionMessageFilter, FlinkActor}
import org.apache.flink.runtime.testingUtils.TestingUtils
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuiteLike, Matchers, BeforeAndAfterAll}

@RunWith(classOf[JUnitRunner])
class FlinkActorTest(_system: ActorSystem)
  extends TestKit(_system)
  with FunSuiteLike
  with Matchers
  with BeforeAndAfterAll {

  def this() = this(ActorSystem("TestingActorSystem", TestingUtils.testConfig))

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  test("A Flink actor should only accept LeaderSessionMessages with a valid leader session id") {
    val leaderSessionID = UUID.randomUUID()
    val oldSessionID = UUID.randomUUID()

    val props = Props(classOf[PlainFlinkActor], Option(leaderSessionID))

    val actor = TestActorRef[PlainFlinkActor](props)

    actor ! LeaderSessionMessage(leaderSessionID, 1)
    actor ! LeaderSessionMessage(oldSessionID, 1)
    actor ! LeaderSessionMessage(leaderSessionID, 1)

    actor ! 1

    actor.underlyingActor.counter should be (3)
  }

  test("A Flink actor should throw an exception when receiving an unwrapped " +
    "RequiresLeaderSessionID message") {
    val leaderSessionID = UUID.randomUUID()

    val props = Props(classOf[PlainFlinkActor], Option(leaderSessionID))
    val actor = TestActorRef[PlainFlinkActor](props)

    actor.receive(LeaderSessionMessage(leaderSessionID, 1))
    actor.receive(1)

    try {
      actor.receive(new PlainRequiresLeaderSessionID)

      fail("Expected an exception, because an RequiresLeaderSessionID message was sent to the " +
        "FlinkActor without being wrapped in a LeaderSessionMessage.")
    } catch {
      case e: Exception =>
        e.getMessage should be("Received a message PlainRequiresLeaderSessionID without a " +
          s"leader session ID, even though the message requires a leader session ID.")
    }
  }

  def stopActor(actor: ActorRef): Unit = {
    actor ! Kill
  }

}

class PlainFlinkActor(val leaderSessionID: Option[UUID])
  extends FlinkActor
  with LeaderSessionMessageFilter {

  val log = Logger(getClass)

  var counter = 0

  /** Handle incoming messages
    *
    * @return
    */
  override def handleMessage: Receive = {
    case _ => counter += 1
  }
}
