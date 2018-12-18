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

import akka.actor.{ActorSystem, Kill, PoisonPill}
import akka.testkit.{ImplicitSender, TestKit}
import org.apache.flink.runtime.akka.ListeningBehaviour
import org.apache.flink.runtime.client.JobExecutionException
import org.apache.flink.runtime.io.network.partition.ResultPartitionType
import org.apache.flink.runtime.jobgraph.{DistributionPattern, JobGraph, JobVertex}
import org.apache.flink.runtime.jobmanager.Tasks.{BlockingReceiver, Sender}
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup
import org.apache.flink.runtime.messages.JobManagerMessages.{JobResultFailure, JobSubmitSuccess, SubmitJob}
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages._
import org.apache.flink.runtime.testingUtils.{ScalaTestingUtils, TestingUtils}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.duration._

@RunWith(classOf[JUnitRunner])
class TaskManagerFailsWithSlotSharingITCase(_system: ActorSystem)
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

  "The JobManager" should {
    "handle gracefully failing task manager with slot sharing" in {
      val num_tasks = 100

      val sender = new JobVertex("Sender")
      val receiver = new JobVertex("Receiver")

      sender.setInvokableClass(classOf[Sender])
      receiver.setInvokableClass(classOf[BlockingReceiver])

      sender.setParallelism(num_tasks)
      receiver.setParallelism(num_tasks)
      receiver.connectNewDataSetAsInput(sender, DistributionPattern.POINTWISE,
        ResultPartitionType.PIPELINED)

      val sharingGroup = new SlotSharingGroup()
      sender.setSlotSharingGroup(sharingGroup)
      receiver.setSlotSharingGroup(sharingGroup)

      val jobGraph = new JobGraph("Pointwise Job", sender, receiver)
      val jobID = jobGraph.getJobID

      val cluster = TestingUtils.startTestingCluster(num_tasks/2, 2)
      val jmGateway = cluster.getLeaderGateway(1 seconds)
      val taskManagers = cluster.getTaskManagers

      try{
        within(TestingUtils.TESTING_DURATION) {
          jmGateway.tell(SubmitJob(jobGraph, ListeningBehaviour.EXECUTION_RESULT), self)
          expectMsg(JobSubmitSuccess(jobGraph.getJobID))

          jmGateway.tell(WaitForAllVerticesToBeRunningOrFinished(jobID), self)

          expectMsg(AllVerticesRunning(jobID))

          //kill task manager
          taskManagers(0) ! PoisonPill

          val failure = expectMsgType[JobResultFailure]
          val exception = failure.cause.deserializeError(getClass.getClassLoader())
          exception match {
            case e: JobExecutionException =>
              jobGraph.getJobID should equal(e.getJobID)
            case e => fail(s"Received wrong exception $e.")
          }
        }
      } finally {
        cluster.stop()
      }
    }

    "handle hard failing task manager with slot sharing" in {
      val num_tasks = 20

      val sender = new JobVertex("Sender")
      val receiver = new JobVertex("Receiver")

      sender.setInvokableClass(classOf[Sender])
      receiver.setInvokableClass(classOf[BlockingReceiver])

      sender.setParallelism(num_tasks)
      receiver.setParallelism(num_tasks)
      receiver.connectNewDataSetAsInput(sender, DistributionPattern.POINTWISE,
        ResultPartitionType.PIPELINED)

      val sharingGroup = new SlotSharingGroup()
      sender.setSlotSharingGroup(sharingGroup)
      receiver.setSlotSharingGroup(sharingGroup)

      val jobGraph = new JobGraph("Pointwise Job", sender, receiver)
      val jobID = jobGraph.getJobID

      val cluster = TestingUtils.startTestingCluster(num_tasks/2, 2)
      val jmGateway = cluster.getLeaderGateway(1 seconds)
      val taskManagers = cluster.getTaskManagers

      try{
        within(TestingUtils.TESTING_DURATION) {
          jmGateway.tell(SubmitJob(jobGraph, ListeningBehaviour.EXECUTION_RESULT), self)
          expectMsg(JobSubmitSuccess(jobGraph.getJobID))

          jmGateway.tell(WaitForAllVerticesToBeRunningOrFinished(jobID), self)
          expectMsg(AllVerticesRunning(jobID))

          //kill task manager
          taskManagers(0) ! Kill

          val failure = expectMsgType[JobResultFailure]
          val exception = failure.cause.deserializeError(getClass.getClassLoader())
          exception match {
            case e: JobExecutionException =>
              jobGraph.getJobID should equal(e.getJobID)

            case e => fail(s"Received wrong exception $e.")
          }
        }
      }finally{
        cluster.stop()
      }
    }
  }

}
