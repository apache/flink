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

package org.apache.flink.api.scala.runtime.jobmanager

import java.util.UUID

import akka.actor.ActorSystem
import akka.actor.Status.Success
import akka.testkit.{ImplicitSender, TestKit}
import org.apache.flink.runtime.akka.AkkaUtils
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable
import org.apache.flink.runtime.jobgraph.{JobGraph, JobVertex}
import org.apache.flink.runtime.messages.JobManagerMessages.{LeaderSessionMessage, CancelJob,
JobResultSuccess, SubmitJob}
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages.{AllVerticesRunning,
WaitForAllVerticesToBeRunning}
import org.apache.flink.runtime.testingUtils.{ScalaTestingUtils, TestingUtils}
import org.junit.runner.RunWith
import org.scalatest.{FunSuiteLike, Matchers, BeforeAndAfterAll}
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class JobManagerLeaderSessionIDITSuite(_system: ActorSystem)
  extends TestKit(_system)
  with ImplicitSender
  with FunSuiteLike
  with Matchers
  with BeforeAndAfterAll
  with ScalaTestingUtils {

  val numTaskManagers = 2
  val taskManagerNumSlots = 2
  val numSlots = numTaskManagers * taskManagerNumSlots

  val cluster = TestingUtils.startTestingCluster(
    taskManagerNumSlots,
    numTaskManagers,
    TestingUtils.DEFAULT_AKKA_ASK_TIMEOUT);

  def this() = this(ActorSystem("TestingActorSystem", AkkaUtils.getDefaultAkkaConfig))

  override def afterAll(): Unit = {
    cluster.stop()
    TestKit.shutdownActorSystem(system)
  }

  test("A JobManager should not process CancelJob messages with the wrong leader session ID") {
    val sender = new JobVertex("BlockingSender");
    sender.setParallelism(numSlots)
    sender.setInvokableClass(classOf[BlockingUntilSignalNoOpInvokable])
    val jobGraph = new JobGraph("TestJob", sender)

    val oldSessionID = Option(UUID.randomUUID())

    val jmGateway = cluster.getJobManagerGateway()
    val jm = jmGateway.actor()

    within(TestingUtils.TESTING_DURATION) {
      jmGateway.tell(SubmitJob(jobGraph, false), self)

      expectMsg(Success(jobGraph.getJobID))

      jmGateway.tell(WaitForAllVerticesToBeRunning(jobGraph.getJobID), self)

      expectMsg(AllVerticesRunning(jobGraph.getJobID))

      jm ! LeaderSessionMessage(oldSessionID, CancelJob(jobGraph.getJobID))

      BlockingUntilSignalNoOpInvokable.triggerExecution()

      expectMsgType[JobResultSuccess]
    }
  }
}

class BlockingUntilSignalNoOpInvokable extends AbstractInvokable {
  override def registerInputOutput(): Unit = {

  }

  override def invoke(): Unit = {
    BlockingUntilSignalNoOpInvokable.lock.synchronized{
      BlockingUntilSignalNoOpInvokable.lock.wait()
    }
  }
}

object BlockingUntilSignalNoOpInvokable {
  val lock = new Object

  def triggerExecution(): Unit = {
    lock.synchronized{
      lock.notifyAll()
    }
  }
}
