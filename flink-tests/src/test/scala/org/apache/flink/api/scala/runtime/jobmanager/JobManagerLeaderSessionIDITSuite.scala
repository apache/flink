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
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.runtime.akka.{AkkaUtils, ListeningBehaviour}
import org.apache.flink.runtime.checkpoint.{CheckpointMetaData, CheckpointMetrics, CheckpointOptions}
import org.apache.flink.runtime.execution.Environment
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable
import org.apache.flink.runtime.jobgraph.{JobGraph, JobVertex}
import org.apache.flink.runtime.messages.JobManagerMessages.{CancelJob, JobResultSuccess, LeaderSessionMessage, SubmitJob}
import org.apache.flink.runtime.state.TaskStateHandles
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages.{AllVerticesRunning, WaitForAllVerticesToBeRunning}
import org.apache.flink.runtime.testingUtils.{ScalaTestingUtils, TestingUtils}
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfterAll, FunSuiteLike, Matchers}
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

    val oldSessionID = UUID.randomUUID()

    val jmGateway = cluster.getLeaderGateway(TestingUtils.TESTING_DURATION)
    val jm = jmGateway.actor()

    within(TestingUtils.TESTING_DURATION) {
      jmGateway.tell(SubmitJob(jobGraph, ListeningBehaviour.EXECUTION_RESULT), self)

      expectMsg(Success(jobGraph.getJobID))

      jmGateway.tell(WaitForAllVerticesToBeRunning(jobGraph.getJobID), self)

      expectMsg(AllVerticesRunning(jobGraph.getJobID))

      jm ! LeaderSessionMessage(oldSessionID, CancelJob(jobGraph.getJobID))

      BlockingUntilSignalNoOpInvokable.triggerExecution()

      expectMsgType[JobResultSuccess]
    }
  }
}

class BlockingUntilSignalNoOpInvokable(environment: Environment, taskStateHandles: TaskStateHandles)
  extends AbstractInvokable(environment, taskStateHandles) {

  override def invoke(): Unit = {
    BlockingUntilSignalNoOpInvokable.lock.synchronized{
      BlockingUntilSignalNoOpInvokable.lock.wait()
    }
  }

  override def triggerCheckpoint(checkpointMetaData: CheckpointMetaData,
                                  checkpointOptions: CheckpointOptions): Boolean = {
    throw new UnsupportedOperationException(
      String.format("triggerCheckpoint not supported by %s", this.getClass.getName))
  }

  override def triggerCheckpointOnBarrier(checkpointMetaData: CheckpointMetaData,
                                          checkpointOptions: CheckpointOptions,
                                          checkpointMetrics: CheckpointMetrics): Unit = {
    throw new UnsupportedOperationException(
      String.format("triggerCheckpointOnBarrier not supported by %s", this.getClass.getName))
  }

  override def abortCheckpointOnBarrier(checkpointId: Long, cause: Throwable): Unit = {
    throw new UnsupportedOperationException(
      String.format("abortCheckpointOnBarrier not supported by %s", this.getClass.getName))
  }

  override def notifyCheckpointComplete(checkpointId: Long): Unit = {
    throw new UnsupportedOperationException(
      String.format("notifyCheckpointComplete not supported by %s", this.getClass.getName))
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
