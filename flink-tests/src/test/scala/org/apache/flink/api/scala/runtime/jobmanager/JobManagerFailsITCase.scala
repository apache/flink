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

import akka.actor.Status.Success
import akka.actor.{ActorSystem, PoisonPill}
import akka.testkit.{ImplicitSender, TestKit}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.runtime.akka.AkkaUtils
import org.apache.flink.runtime.jobgraph.{AbstractJobVertex, JobGraph}
import org.apache.flink.runtime.jobmanager.Tasks.{BlockingNoOpInvokable, NoOpInvokable}
import org.apache.flink.runtime.messages.JobManagerMessages._
import org.apache.flink.runtime.testingUtils.TestingMessages.DisableDisconnect
import org.apache.flink.runtime.testingUtils.TestingTaskManagerMessages.{JobManagerTerminated, NotifyWhenJobManagerTerminated}
import org.apache.flink.runtime.testingUtils.TestingUtils
import org.apache.flink.test.util.ForkableFlinkMiniCluster

@RunWith(classOf[JUnitRunner])
class JobManagerFailsITCase(_system: ActorSystem) extends TestKit(_system) with ImplicitSender
with WordSpecLike with Matchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("TestingActorSystem", AkkaUtils.getDefaultAkkaConfig))

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "A TaskManager" should {
    "detect a lost connection to the JobManager and try to reconnect to it" in {

      val num_slots = 13
      val cluster = startDeathwatchCluster(num_slots, 1)

      val tm = cluster.getTaskManagers(0)
      val jm = cluster.getJobManager

      // disable disconnect message to test death watch
      tm ! DisableDisconnect

      try{
        jm ! RequestNumberRegisteredTaskManager
        expectMsg(1)

        tm ! NotifyWhenJobManagerTerminated(jm)

        jm ! PoisonPill

        expectMsgClass(classOf[JobManagerTerminated])

        cluster.restartJobManager()

        cluster.waitForTaskManagersToBeRegistered()

        cluster.getJobManager ! RequestNumberRegisteredTaskManager

        expectMsg(1)
      } finally {
        cluster.stop()
      }
    }

    "go into a clean state in case of a JobManager failure" in {
      val num_slots = 36

      val sender = new AbstractJobVertex("BlockingSender")
      sender.setParallelism(num_slots)
      sender.setInvokableClass(classOf[BlockingNoOpInvokable])
      val jobGraph = new JobGraph("Blocking Testjob", sender)

      val noOp = new AbstractJobVertex("NoOpInvokable")
      noOp.setParallelism(num_slots)
      noOp.setInvokableClass(classOf[NoOpInvokable])
      val jobGraph2 = new JobGraph("NoOp Testjob", noOp)

      val cluster = startDeathwatchCluster(num_slots / 2, 2)

      var jm = cluster.getJobManager
      val tm = cluster.getTaskManagers(0)

      try {
        within(TestingUtils.TESTING_DURATION) {
          jm ! SubmitJob(jobGraph, false)
          expectMsg(Success(jobGraph.getJobID))

          tm ! NotifyWhenJobManagerTerminated(jm)

          jm ! PoisonPill

          expectMsgClass(classOf[JobManagerTerminated])

          cluster.restartJobManager()

          jm = cluster.getJobManager

          cluster.waitForTaskManagersToBeRegistered()

          jm ! SubmitJob(jobGraph2, false)

          val failure = expectMsgType[Success]

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
