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

import akka.actor.{PoisonPill, ActorSystem}
import akka.testkit.{ImplicitSender, TestKit}
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.runtime.akka.ListeningBehaviour
import org.apache.flink.runtime.jobgraph.{JobStatus, JobGraph, DistributionPattern, JobVertex}
import org.apache.flink.runtime.jobmanager.Tasks.{BlockingOnceReceiver, FailingOnceReceiver}
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup
import org.apache.flink.runtime.messages.JobManagerMessages.{JobSubmitSuccess, JobResultSuccess, SubmitJob}
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages._
import org.apache.flink.runtime.testingUtils.{ScalaTestingUtils, TestingCluster, TestingUtils}
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import org.scalatest.junit.JUnitRunner
import scala.concurrent.duration._

import language.postfixOps

@RunWith(classOf[JUnitRunner])
class RecoveryITCase(_system: ActorSystem)
  extends TestKit(_system)
  with ImplicitSender
  with WordSpecLike
  with Matchers
  with BeforeAndAfterAll
  with ScalaTestingUtils {

  def this() = this(ActorSystem("TestingActorSystem", TestingUtils.testConfig))

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  def createTestClusterWithHeartbeatTimeout(
      numSlots: Int,
      numTaskManagers: Int,
      heartbeatTimeout: String)
    : TestingCluster = {
    val config = new Configuration()
    config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, numSlots)
    config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, numTaskManagers)
    config.setString(ConfigConstants.AKKA_WATCH_HEARTBEAT_PAUSE, heartbeatTimeout)
    config.setString(ConfigConstants.RESTART_STRATEGY, "fixeddelay")
    config.setInteger(ConfigConstants.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 1)
    config.setString(ConfigConstants.RESTART_STRATEGY_FIXED_DELAY_DELAY, heartbeatTimeout)
    new TestingCluster(config)
  }

  val NUM_TASKS = 31

  "The recovery" must {
    "recover once failing forward job" in {
      FailingOnceReceiver.failed = false

      val sender = new JobVertex("Sender")
      val receiver = new JobVertex("Receiver")

      sender.setInvokableClass(classOf[Tasks.Sender])
      receiver.setInvokableClass(classOf[Tasks.FailingOnceReceiver])

      sender.setParallelism(NUM_TASKS)
      receiver.setParallelism(NUM_TASKS)

      receiver.connectNewDataSetAsInput(sender, DistributionPattern.POINTWISE)

      val jobGraph = new JobGraph("Pointwise job", sender, receiver)

      val cluster = createTestClusterWithHeartbeatTimeout(2 * NUM_TASKS, 1, "2 s")
      cluster.start()

      val jmGateway = cluster.getLeaderGateway(1 seconds)

      try {
        within(TestingUtils.TESTING_DURATION){
          jmGateway.tell(SubmitJob(jobGraph, ListeningBehaviour.EXECUTION_RESULT), self)

          expectMsg(JobSubmitSuccess(jobGraph.getJobID))

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

      val sender = new JobVertex("Sender")
      val receiver = new JobVertex("Receiver")

      sender.setInvokableClass(classOf[Tasks.Sender])
      receiver.setInvokableClass(classOf[Tasks.FailingOnceReceiver])

      sender.setParallelism(NUM_TASKS)
      receiver.setParallelism(NUM_TASKS)

      receiver.connectNewDataSetAsInput(sender, DistributionPattern.POINTWISE)

      val sharingGroup = new SlotSharingGroup
      sender.setSlotSharingGroup(sharingGroup)
      receiver.setSlotSharingGroup(sharingGroup)

      val jobGraph = new JobGraph("Pointwise job", sender, receiver)

      val cluster = createTestClusterWithHeartbeatTimeout(NUM_TASKS, 1, "2 s")
      cluster.start()

      val jmGateway = cluster.getLeaderGateway(1 seconds)

      try {
        within(TestingUtils.TESTING_DURATION){
          jmGateway.tell(SubmitJob(jobGraph, ListeningBehaviour.EXECUTION_RESULT), self)

          expectMsg(JobSubmitSuccess(jobGraph.getJobID))

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

      val sender = new JobVertex("Sender")
      val receiver = new JobVertex("Receiver")

      sender.setInvokableClass(classOf[Tasks.Sender])
      receiver.setInvokableClass(classOf[Tasks.BlockingOnceReceiver])

      sender.setParallelism(NUM_TASKS)
      receiver.setParallelism(NUM_TASKS)

      receiver.connectNewDataSetAsInput(sender, DistributionPattern.POINTWISE)

      val sharingGroup = new SlotSharingGroup
      sender.setSlotSharingGroup(sharingGroup)
      receiver.setSlotSharingGroup(sharingGroup)

      val jobGraph = new JobGraph("Pointwise job", sender, receiver)

      val cluster = createTestClusterWithHeartbeatTimeout(NUM_TASKS, 2, "2 s")
      cluster.start()

      val jmGateway = cluster.getLeaderGateway(1 seconds)

      try {
        within(TestingUtils.TESTING_DURATION){
          jmGateway.tell(SubmitJob(jobGraph, ListeningBehaviour.EXECUTION_RESULT), self)

          expectMsg(JobSubmitSuccess(jobGraph.getJobID))

          jmGateway.tell(WaitForAllVerticesToBeRunningOrFinished(jobGraph.getJobID), self)

          expectMsg(AllVerticesRunning(jobGraph.getJobID))

          BlockingOnceReceiver.blocking = false
          jmGateway.tell(NotifyWhenJobStatus(jobGraph.getJobID, JobStatus.RESTARTING), self)
          jmGateway.tell(RequestWorkingTaskManager(jobGraph.getJobID), self)

          val WorkingTaskManager(gatewayOption) = expectMsgType[WorkingTaskManager]

          gatewayOption match {
            case None => fail("There has to be at least one task manager on which" +
              "the tasks are running.")
            case Some(gateway) => gateway.tell(PoisonPill)
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
