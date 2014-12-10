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
import org.apache.flink.runtime.jobgraph.{JobGraph, DistributionPattern, AbstractJobVertex}
import org.apache.flink.runtime.jobmanager.Tasks.{BlockingOnceReceiver, FailingOnceReceiver}
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup
import org.apache.flink.runtime.messages.JobManagerMessages.{JobResultSuccess, SubmissionSuccess,
SubmitJob}
import org.apache.flink.runtime.testingUtils.TestingUtils
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RecoveryITCase(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with
WordSpecLike with Matchers with BeforeAndAfterAll {
  def this() = this(ActorSystem("TestingActorSystem", TestingUtils.testConfig))

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  val NUM_TASKS = 31

  "The recovery" must {
    "recover once failing forward job" in {
      FailingOnceReceiver.failed = false

      val sender = new AbstractJobVertex("Sender");
      val receiver = new AbstractJobVertex("Receiver");

      sender.setInvokableClass(classOf[Tasks.Sender])
      receiver.setInvokableClass(classOf[Tasks.FailingOnceReceiver])

      sender.setParallelism(NUM_TASKS)
      receiver.setParallelism(NUM_TASKS)

      receiver.connectNewDataSetAsInput(sender, DistributionPattern.POINTWISE)

      val jobGraph = new JobGraph("Pointwise job", sender, receiver)
      jobGraph.setNumberOfExecutionRetries(1)

      val cluster = TestingUtils.startTestingCluster(2 * NUM_TASKS)
      val jm = cluster.getJobManager

      try {
        within(TestingUtils.TESTING_DURATION){
          jm ! SubmitJob(jobGraph)

          expectMsg(SubmissionSuccess(jobGraph.getJobID))

          val result = expectMsgType[JobResultSuccess]

          result.jobID should equal(jobGraph.getJobID)
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

      val sender = new AbstractJobVertex("Sender");
      val receiver = new AbstractJobVertex("Receiver");

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

      val cluster = TestingUtils.startTestingCluster(NUM_TASKS)
      val jm = cluster.getJobManager

      try {
        within(TestingUtils.TESTING_DURATION){
          jm ! SubmitJob(jobGraph)

          expectMsg(SubmissionSuccess(jobGraph.getJobID))

          val result = expectMsgType[JobResultSuccess]

          result.jobID should equal(jobGraph.getJobID)
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

      val sender = new AbstractJobVertex("Sender");
      val receiver = new AbstractJobVertex("Receiver");

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

      val cluster = TestingUtils.startTestingCluster(NUM_TASKS, 2)

      val jm = cluster.getJobManager
      val taskManagers = cluster.getTaskManagers

      try {
        within(TestingUtils.TESTING_DURATION){
          jm ! SubmitJob(jobGraph)

          expectMsg(SubmissionSuccess(jobGraph.getJobID))

          Thread.sleep(500)
          BlockingOnceReceiver.blocking = false
          taskManagers(0) ! PoisonPill

          val result = expectMsgType[JobResultSuccess]

          result.jobID should equal(jobGraph.getJobID)
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
