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

package org.apache.flink.runtime.jobmanager

import akka.actor.Status.Success
import akka.actor.{ActorRef, PoisonPill, ActorSystem}
import akka.testkit.{ImplicitSender, TestKit}
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.runtime.jobgraph.{JobStatus, JobGraph, DistributionPattern,AbstractJobVertex}
import org.apache.flink.runtime.jobmanager.Tasks.{BlockingOnceReceiver, FailingOnceReceiver}
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup
import org.apache.flink.runtime.messages.JobManagerMessages.{JobResultSuccess, SubmitJob}
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages._
import org.apache.flink.runtime.testingUtils.{TestingCluster, TestingUtils}
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RecoveryITCase(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with
WordSpecLike with Matchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("TestingActorSystem", TestingUtils.testConfig))

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  def startTestClusterWithHeartbeatTimeout(numSlots: Int,
                                                numTaskManagers: Int,
                                                heartbeatTimeout: String): TestingCluster = {
    val config = new Configuration()
    config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, numSlots)
    config.setInteger(ConfigConstants.LOCAL_INSTANCE_MANAGER_NUMBER_TASK_MANAGER, numTaskManagers)
    config.setString(ConfigConstants.AKKA_WATCH_HEARTBEAT_PAUSE, heartbeatTimeout)
    config.setString(ConfigConstants.DEFAULT_EXECUTION_RETRY_DELAY_KEY, heartbeatTimeout)
    new TestingCluster(config)
  }

  val NUM_TASKS = 31

  "The recovery" must {
    "recover once failing forward job" in {
      FailingOnceReceiver.failed = false

      val sender = new AbstractJobVertex("Sender")
      val receiver = new AbstractJobVertex("Receiver")

      sender.setInvokableClass(classOf[Tasks.Sender])
      receiver.setInvokableClass(classOf[Tasks.FailingOnceReceiver])

      sender.setParallelism(NUM_TASKS)
      receiver.setParallelism(NUM_TASKS)

      receiver.connectNewDataSetAsInput(sender, DistributionPattern.POINTWISE)

      val jobGraph = new JobGraph("Pointwise job", sender, receiver)
      jobGraph.setNumberOfExecutionRetries(1)

      val cluster = startTestClusterWithHeartbeatTimeout(2 * NUM_TASKS, 1, "2 s")
      val jm = cluster.getJobManager

      try {
        within(TestingUtils.TESTING_DURATION){
          jm ! SubmitJob(jobGraph, false)

          expectMsg(Success(jobGraph.getJobID))

          val result = expectMsgType[JobResultSuccess]

          result.result.getJobId() should equal(jobGraph.getJobID)
        }
      } catch {
        case t: Throwable =>
          t.printStackTrace()
          fail(t.getMessage)
      } finally{
        cluster.stop()
      }
    }

    "recover once failing forward job with slot sharing" in {
      FailingOnceReceiver.failed = false

      val sender = new AbstractJobVertex("Sender")
      val receiver = new AbstractJobVertex("Receiver")

      sender.setInvokableClass(classOf[Tasks.Sender])
      receiver.setInvokableClass(classOf[Tasks.FailingOnceReceiver])

      sender.setParallelism(NUM_TASKS)
      receiver.setParallelism(NUM_TASKS)

      receiver.connectNewDataSetAsInput(sender, DistributionPattern.POINTWISE)

      val sharingGroup = new SlotSharingGroup
      sender.setSlotSharingGroup(sharingGroup)
      receiver.setSlotSharingGroup(sharingGroup)

      val jobGraph = new JobGraph("Pointwise job", sender, receiver)
      jobGraph.setNumberOfExecutionRetries(1)

      val cluster = startTestClusterWithHeartbeatTimeout(NUM_TASKS, 1, "2 s")
      val jm = cluster.getJobManager

      try {
        within(TestingUtils.TESTING_DURATION){
          jm ! SubmitJob(jobGraph, false)

          expectMsg(Success(jobGraph.getJobID))

          val result = expectMsgType[JobResultSuccess]

          result.result.getJobId() should equal(jobGraph.getJobID)
        }
      } catch {
        case t: Throwable =>
          t.printStackTrace()
          fail(t.getMessage)
      } finally{
        cluster.stop()
      }
    }

    "recover a task manager failure" in {
      BlockingOnceReceiver.blocking = true

      val sender = new AbstractJobVertex("Sender")
      val receiver = new AbstractJobVertex("Receiver")

      sender.setInvokableClass(classOf[Tasks.Sender])
      receiver.setInvokableClass(classOf[Tasks.BlockingOnceReceiver])

      sender.setParallelism(NUM_TASKS)
      receiver.setParallelism(NUM_TASKS)

      receiver.connectNewDataSetAsInput(sender, DistributionPattern.POINTWISE)

      val sharingGroup = new SlotSharingGroup
      sender.setSlotSharingGroup(sharingGroup)
      receiver.setSlotSharingGroup(sharingGroup)

      val jobGraph = new JobGraph("Pointwise job", sender, receiver)
      jobGraph.setNumberOfExecutionRetries(1)

      val cluster = startTestClusterWithHeartbeatTimeout(NUM_TASKS, 2, "2 s")

      val jm = cluster.getJobManager

      try {
        within(TestingUtils.TESTING_DURATION){
          jm ! SubmitJob(jobGraph, false)

          expectMsg(Success(jobGraph.getJobID))

          jm ! WaitForAllVerticesToBeRunningOrFinished(jobGraph.getJobID)

          expectMsg(AllVerticesRunning(jobGraph.getJobID))

          BlockingOnceReceiver.blocking = false
          jm ! NotifyWhenJobStatus(jobGraph.getJobID, JobStatus.RESTARTING)
          jm ! RequestWorkingTaskManager(jobGraph.getJobID)

          val WorkingTaskManager(tm) = expectMsgType[WorkingTaskManager]

          tm match {
            case ActorRef.noSender => fail("There has to be at least one task manager on which" +
              "the tasks are running.")
            case t => t ! PoisonPill
          }

          expectMsg(JobStatusIs(jobGraph.getJobID, JobStatus.RESTARTING))

          val result = expectMsgType[JobResultSuccess]

          result.result.getJobId() should equal(jobGraph.getJobID)
        }
      } catch {
        case t: Throwable =>
          t.printStackTrace()
          fail(t.getMessage)
      } finally{
        cluster.stop()
      }
    }
  }
}
