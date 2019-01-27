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
import java.util.concurrent.CountDownLatch

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import org.apache.flink.core.testutils.OneShotLatch
import org.apache.flink.runtime.akka.{AkkaUtils, ListeningBehaviour}
import org.apache.flink.runtime.execution.Environment
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable
import org.apache.flink.runtime.jobgraph.{JobGraph, JobVertex}
import org.apache.flink.runtime.messages.JobManagerMessages._
import org.apache.flink.runtime.testingUtils.{ScalaTestingUtils, TestingUtils}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuiteLike, Matchers}

@RunWith(classOf[JUnitRunner])
class JobManagerLeaderSessionIDITCase(_system: ActorSystem)
  extends TestKit(_system)
  with ImplicitSender
  with FunSuiteLike
  with Matchers
  with BeforeAndAfterAll
  with ScalaTestingUtils {

  import BlockingUntilSignalNoOpInvokable._

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

    val oldSessionID = UUID.randomUUID()

    val jmGateway = cluster.getLeaderGateway(TestingUtils.TESTING_DURATION)
    val jm = jmGateway.actor()

    within(TestingUtils.TESTING_DURATION) {
      jmGateway.tell(SubmitJob(jobGraph, ListeningBehaviour.EXECUTION_RESULT), self)

      expectMsg(JobSubmitSuccess(jobGraph.getJobID))

      BlockingUntilSignalNoOpInvokable.countDownLatch.await()

      jm ! LeaderSessionMessage(oldSessionID, CancelJob(jobGraph.getJobID))

      BlockingUntilSignalNoOpInvokable.oneShotLatch.trigger()

      expectMsgType[JobResultSuccess]
    }
  }
}

class BlockingUntilSignalNoOpInvokable(env: Environment) extends AbstractInvokable(env) {

  override def invoke(): Unit = {
    BlockingUntilSignalNoOpInvokable.countDownLatch.countDown()
    BlockingUntilSignalNoOpInvokable.oneShotLatch.await()
  }
}

object BlockingUntilSignalNoOpInvokable {
  val numTaskManagers = 2
  val taskManagerNumSlots = 2
  val numSlots = numTaskManagers * taskManagerNumSlots

  val countDownLatch = new CountDownLatch(numSlots)

  val oneShotLatch = new OneShotLatch()
}
