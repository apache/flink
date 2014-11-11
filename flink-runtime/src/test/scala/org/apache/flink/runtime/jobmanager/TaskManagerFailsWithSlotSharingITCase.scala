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
import org.apache.flink.runtime.jobgraph.{JobGraph, DistributionPattern,
AbstractJobVertex}
import org.apache.flink.runtime.jobmanager.Tasks.{Sender, BlockingReceiver}
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup
import org.apache.flink.runtime.messages.JobManagerMessages.{JobResultFailed, SubmissionSuccess,
SubmitJob}
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages.{AllVerticesRunning,
WaitForAllVerticesToBeRunning}
import org.apache.flink.runtime.testingUtils.TestingUtils
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, WordSpecLike, Matchers}

import scala.concurrent.duration._

@RunWith(classOf[JUnitRunner])
class TaskManagerFailsWithSlotSharingITCase(_system: ActorSystem) extends TestKit(_system) with
ImplicitSender with WordSpecLike with Matchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("TestingActorSystem", TestingUtils.testConfig))

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "The JobManager" should {
    "handle task manager failures with slot sharing" in {
      val num_tasks = 20

      val sender = new AbstractJobVertex("Sender")
      val receiver = new AbstractJobVertex("Receiver")

      sender.setInvokableClass(classOf[Sender])
      receiver.setInvokableClass(classOf[BlockingReceiver])

      sender.setParallelism(num_tasks)
      receiver.setParallelism(num_tasks)
      receiver.connectNewDataSetAsInput(sender, DistributionPattern.POINTWISE)

      val sharingGroup = new SlotSharingGroup()
      sender.setSlotSharingGroup(sharingGroup)
      receiver.setSlotSharingGroup(sharingGroup)

      val jobGraph = new JobGraph("Pointwise Job", sender, receiver)
      val jobID = jobGraph.getJobID

      val cluster = TestingUtils.startTestingCluster(num_tasks/2, 2)
      val jm = cluster.getJobManager
      val taskManagers = cluster.getTaskManagers

      try{
        within(TestingUtils.TESTING_DURATION) {
          jm ! SubmitJob(jobGraph)
          expectMsg(SubmissionSuccess(jobGraph.getJobID))

          jm ! WaitForAllVerticesToBeRunning(jobID)
          expectMsg(AllVerticesRunning(jobID))

          //kill task manager
          taskManagers(0) ! PoisonPill

          expectMsgType[JobResultFailed]
        }
      }finally{
        cluster.stop()
      }
    }
  }

}
