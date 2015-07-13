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

package org.apache.flink.api.scala.runtime.taskmanager

import akka.actor.Status.{Failure, Success}
import akka.actor.{ActorSystem, Kill, PoisonPill}
import akka.testkit.{ImplicitSender, TestKit}

import org.apache.flink.configuration.ConfigConstants
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.akka.AkkaUtils
import org.apache.flink.runtime.client.JobExecutionException
import org.apache.flink.runtime.jobgraph.{JobVertex, DistributionPattern, JobGraph}
import org.apache.flink.runtime.jobmanager.Tasks.{NoOpInvokable, BlockingNoOpInvokable, BlockingReceiver, Sender}
import org.apache.flink.runtime.messages.JobManagerMessages.{JobResultSuccess, RequestNumberRegisteredTaskManager, SubmitJob}
import org.apache.flink.runtime.messages.TaskManagerMessages.{RegisteredAtJobManager, NotifyWhenRegisteredAtJobManager}
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages._
import org.apache.flink.runtime.testingUtils.TestingMessages.DisableDisconnect
import org.apache.flink.runtime.testingUtils.TestingUtils
import org.apache.flink.test.util.ForkableFlinkMiniCluster

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

@RunWith(classOf[JUnitRunner])
class TaskManagerFailsITCase(_system: ActorSystem) extends TestKit(_system) with ImplicitSender
with WordSpecLike with Matchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("TestingActorSystem", AkkaUtils.getDefaultAkkaConfig))

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "The JobManager" should {

    "detect a failing task manager" in {

      val num_slots = 11
      val cluster = startDeathwatchCluster(num_slots, 2)

      val taskManagers = cluster.getTaskManagers
      val jm = cluster.getJobManager

      jm ! DisableDisconnect

      try{
        within(TestingUtils.TESTING_DURATION){
          jm ! RequestNumberRegisteredTaskManager
          expectMsg(2)

          jm ! NotifyWhenTaskManagerTerminated(taskManagers(0))

          taskManagers(0) ! PoisonPill

          val TaskManagerTerminated(tm) = expectMsgClass(classOf[TaskManagerTerminated])

          jm ! RequestNumberRegisteredTaskManager
          expectMsg(1)
        }
      }
      finally {
        cluster.stop()
      }
    }

    "handle gracefully failing task manager" in {

      val num_tasks = 31
      val sender = new JobVertex("Sender")
      val receiver = new JobVertex("Receiver")
      sender.setInvokableClass(classOf[Sender])
      receiver.setInvokableClass(classOf[BlockingReceiver])
      sender.setParallelism(num_tasks)
      receiver.setParallelism(num_tasks)
      receiver.connectNewDataSetAsInput(sender, DistributionPattern.POINTWISE)

      val jobGraph = new JobGraph("Pointwise Job", sender, receiver)
      val jobID = jobGraph.getJobID

      val cluster = ForkableFlinkMiniCluster.startCluster(num_tasks, 2)

      val jm = cluster.getJobManager

      try {
        within(TestingUtils.TESTING_DURATION) {
          jm ! SubmitJob(jobGraph, false)
          expectMsg(Success(jobGraph.getJobID))

          jm ! WaitForAllVerticesToBeRunningOrFinished(jobID)

          expectMsg(AllVerticesRunning(jobID))

          jm ! RequestWorkingTaskManager(jobID)

          val gatewayOption = expectMsgType[WorkingTaskManager].gatewayOption

          gatewayOption match {
            case Some(gateway) =>
              // kill one task manager
              gateway.tell(PoisonPill)

            case None => fail("Could not retrieve a working task manager.")
          }

          val failure = expectMsgType[Failure]

          failure.cause match {
            case e: JobExecutionException =>
              jobGraph.getJobID should equal(e.getJobID)

            case e => fail(s"Received wrong exception $e.")
          }
        }
      } finally {
        cluster.stop()
      }
    }

    "handle hard failing task manager" in {
      val num_tasks = 31
      val sender = new JobVertex("Sender")
      val receiver = new JobVertex("Receiver")
      sender.setInvokableClass(classOf[Sender])
      receiver.setInvokableClass(classOf[BlockingReceiver])
      sender.setParallelism(num_tasks)
      receiver.setParallelism(num_tasks)
      receiver.connectNewDataSetAsInput(sender, DistributionPattern.POINTWISE)

      val jobGraph = new JobGraph("Pointwise Job", sender, receiver)
      val jobID = jobGraph.getJobID

      val cluster = ForkableFlinkMiniCluster.startCluster(num_tasks, 2)

      val taskManagers = cluster.getTaskManagers
      val jm = cluster.getJobManager

      try {
        within(TestingUtils.TESTING_DURATION) {
          jm ! SubmitJob(jobGraph, false)
          expectMsg(Success(jobGraph.getJobID))

          jm ! WaitForAllVerticesToBeRunningOrFinished(jobID)
          expectMsg(AllVerticesRunning(jobID))

          // kill one task manager
          taskManagers(0) ! Kill

          val failure = expectMsgType[Failure]

          failure.cause match {
            case e: JobExecutionException =>
              jobGraph.getJobID should equal(e.getJobID)

            case e => fail(s"Received wrong exception $e.")
          }
        }
      } finally {
        cluster.stop()
      }
    }
    
    "go into a clean state in case of a TaskManager failure" in {
      val num_slots = 20      

      val sender = new JobVertex("BlockingSender")
      sender.setParallelism(num_slots)
      sender.setInvokableClass(classOf[BlockingNoOpInvokable])
      val jobGraph = new JobGraph("Blocking Testjob", sender)

      val noOp = new JobVertex("NoOpInvokable")
      noOp.setParallelism(num_slots)
      noOp.setInvokableClass(classOf[NoOpInvokable])
      val jobGraph2 = new JobGraph("NoOp Testjob", noOp)

      val cluster = startDeathwatchCluster(num_slots/2, 2)

      var tm = cluster.getTaskManagers(0)
      val jm = cluster.getJobManager

      try{
        within(TestingUtils.TESTING_DURATION){
          jm ! SubmitJob(jobGraph, false)
          expectMsg(Success(jobGraph.getJobID))

          tm ! PoisonPill

          val failure = expectMsgType[Failure]

          failure.cause match {
            case e: JobExecutionException =>
              jobGraph.getJobID should equal(e.getJobID)

            case e => fail(s"Received wrong exception $e.")
          }

          cluster.restartTaskManager(0)

          tm = cluster.getTaskManagers(0)

          tm ! NotifyWhenRegisteredAtJobManager

          expectMsg(RegisteredAtJobManager)

          jm ! SubmitJob(jobGraph2, false)

          expectMsgType[Success]

          val result = expectMsgType[JobResultSuccess]
          result.result.getJobId() should equal(jobGraph2.getJobID)
        }
      } finally {
        cluster.stop()
      }
    }
  }

  def startDeathwatchCluster(numSlots: Int, numTaskmanagers: Int): ForkableFlinkMiniCluster = {
    val config = new Configuration()
    config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, numSlots)
    config.setInteger(ConfigConstants.LOCAL_INSTANCE_MANAGER_NUMBER_TASK_MANAGER, numTaskmanagers)

    new ForkableFlinkMiniCluster(config, singleActorSystem = false)
  }
}
