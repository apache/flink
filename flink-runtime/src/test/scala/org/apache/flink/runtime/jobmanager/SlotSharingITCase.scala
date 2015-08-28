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

import akka.actor.ActorSystem
import akka.actor.Status.Success
import akka.testkit.{ImplicitSender, TestKit}
import org.apache.flink.runtime.jobgraph.{JobVertex, DistributionPattern, JobGraph}
import org.apache.flink.runtime.jobmanager.Tasks.{Sender, AgnosticBinaryReceiver, Receiver}
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup
import org.apache.flink.runtime.messages.JobManagerMessages.{JobResultSuccess, SubmitJob}
import org.apache.flink.runtime.testingUtils.{ScalaTestingUtils, TestingUtils}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

@RunWith(classOf[JUnitRunner])
class SlotSharingITCase(_system: ActorSystem)
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

  "The JobManager actor" must {
    "support slot sharing for forward job" in {
      val num_tasks = 31

      val sender = new JobVertex("Sender")
      val receiver = new JobVertex("Receiver")

      sender.setInvokableClass(classOf[Sender])
      receiver.setInvokableClass(classOf[Receiver])

      sender.setParallelism(num_tasks)
      receiver.setParallelism(num_tasks)

      receiver.connectNewDataSetAsInput(sender, DistributionPattern.POINTWISE)

      val sharingGroup = new SlotSharingGroup(sender.getID, receiver.getID)
      sender.setSlotSharingGroup(sharingGroup)
      receiver.setSlotSharingGroup(sharingGroup)

      val jobGraph = new JobGraph("Pointwise Job", sender, receiver)

      val cluster = TestingUtils.startTestingCluster(num_tasks)
      val jmGateway = cluster.getJobManagerGateway

      try {
        within(TestingUtils.TESTING_DURATION) {
          jmGateway.tell(SubmitJob(jobGraph, false), self)
          expectMsg(Success(jobGraph.getJobID))
          expectMsgType[JobResultSuccess]

        }
      } finally {
        cluster.stop()
      }
    }

    /**
     * This job runs in N slots with 2 * N senders and N receivers. Unless slot sharing is used,
     * it cannot complete.
     */
    "support jobs with two inputs and slot sharing" in {
      val num_tasks = 11

      val sender1 = new JobVertex("Sender1")
      val sender2 = new JobVertex("Sender2")
      val receiver = new JobVertex("Receiver")

      sender1.setInvokableClass(classOf[Sender])
      sender2.setInvokableClass(classOf[Sender])
      receiver.setInvokableClass(classOf[AgnosticBinaryReceiver])

      sender1.setParallelism(num_tasks)
      sender2.setParallelism(num_tasks)
      receiver.setParallelism(num_tasks)

      val sharingGroup = new SlotSharingGroup(sender1.getID, sender2.getID, receiver.getID)
      sender1.setSlotSharingGroup(sharingGroup)
      sender2.setSlotSharingGroup(sharingGroup)
      receiver.setSlotSharingGroup(sharingGroup)

      receiver.connectNewDataSetAsInput(sender1, DistributionPattern.POINTWISE)
      receiver.connectNewDataSetAsInput(sender2, DistributionPattern.ALL_TO_ALL)

      val jobGraph = new JobGraph("Bipartite job", sender1, sender2, receiver)

      val cluster = TestingUtils.startTestingCluster(num_tasks)
      val jmGateway = cluster.getJobManagerGateway

      try {
        within(TestingUtils.TESTING_DURATION) {
          jmGateway.tell(SubmitJob(jobGraph, false), self)
          expectMsg(Success(jobGraph.getJobID))
          expectMsgType[JobResultSuccess]
        }
      } finally {
        cluster.stop()
      }

    }
  }
}
