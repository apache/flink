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

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import org.apache.flink.configuration.{ConfigConstants, Configuration, JobManagerOptions, TaskManagerOptions}
import org.apache.flink.runtime.akka.{AkkaUtils, ListeningBehaviour}
import org.apache.flink.runtime.jobgraph.{JobGraph, JobVertex}
import org.apache.flink.runtime.messages.Acknowledge
import org.apache.flink.runtime.messages.JobManagerMessages._
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages.NotifyWhenAtLeastNumTaskManagerAreRegistered
import org.apache.flink.runtime.testingUtils.TestingMessages.DisableDisconnect
import org.apache.flink.runtime.testingUtils.{ScalaTestingUtils, TestingCluster, TestingUtils}
import org.apache.flink.runtime.testtasks.{BlockingNoOpInvokable, NoOpInvokable}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

@RunWith(classOf[JUnitRunner])
class JobManagerFailsITCase(_system: ActorSystem)
  extends TestKit(_system)
  with ImplicitSender
  with WordSpecLike
  with Matchers
  with BeforeAndAfterAll
  with ScalaTestingUtils {

  def this() = this(ActorSystem("TestingActorSystem", AkkaUtils.getDefaultAkkaConfig))

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "A TaskManager" should {
    "detect a lost connection to the JobManager and try to reconnect to it" in {

      val num_slots = 4
      val cluster = startDeathwatchCluster(num_slots, 1)

      try {
        val tm = cluster.getTaskManagers(0)
        val jmGateway = cluster.getLeaderGateway(TestingUtils.TESTING_DURATION)

        // disable disconnect message to test death watch
        tm ! DisableDisconnect

        within(TestingUtils.TESTING_DURATION) {
          jmGateway.tell(RequestNumberRegisteredTaskManager, self)
          expectMsg(1)

          // stop the current leader and make sure that he is gone
          TestingUtils.stopActorGracefully(jmGateway)

          cluster.restartLeadingJobManager()

          cluster.waitForTaskManagersToBeRegistered()

          cluster.getLeaderGateway(TestingUtils.TESTING_DURATION)
            .tell(RequestNumberRegisteredTaskManager, self)

          expectMsg(1)
        }
      } finally {
        cluster.stop()
      }
    }

    "go into a clean state in case of a JobManager failure" in {
      val num_slots = 4

      val sender = new JobVertex("BlockingSender")
      sender.setParallelism(num_slots)
      sender.setInvokableClass(classOf[BlockingNoOpInvokable])
      val jobGraph = new JobGraph("Blocking Testjob", sender)

      val noOp = new JobVertex("NoOpInvokable")
      noOp.setParallelism(num_slots)
      noOp.setInvokableClass(classOf[NoOpInvokable])
      val jobGraph2 = new JobGraph("NoOp Testjob", noOp)

      val cluster = startDeathwatchCluster(num_slots / 2, 2)

      try {
        var jmGateway = cluster.getLeaderGateway(TestingUtils.TESTING_DURATION)
        val tm = cluster.getTaskManagers(0)

        within(TestingUtils.TESTING_DURATION) {
          jmGateway.tell(SubmitJob(jobGraph, ListeningBehaviour.DETACHED), self)
          expectMsg(JobSubmitSuccess(jobGraph.getJobID))

          // stop the current leader and make sure that he is gone
          TestingUtils.stopActorGracefully(jmGateway)

          cluster.restartLeadingJobManager()

          jmGateway = cluster.getLeaderGateway(TestingUtils.TESTING_DURATION)

          // Ask the job manager for the TMs. Don't ask the TMs, because they
          // can still have state associated with the old job manager.
          jmGateway.tell(NotifyWhenAtLeastNumTaskManagerAreRegistered(2), self)
          expectMsg(Acknowledge.get())

          jmGateway.tell(SubmitJob(jobGraph2, ListeningBehaviour.EXECUTION_RESULT), self)

          expectMsg(JobSubmitSuccess(jobGraph2.getJobID()))

          val result = expectMsgType[JobResultSuccess]

          result.result.getJobId() should equal(jobGraph2.getJobID)
        }
      } finally {
        cluster.stop()
      }
    }
  }

  def startDeathwatchCluster(numSlots: Int, numTaskmanagers: Int): TestingCluster = {
    val config = new Configuration()
    config.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, numSlots)
    config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, numTaskmanagers)
    config.setInteger(JobManagerOptions.PORT, 0)
    config.setString(TaskManagerOptions.INITIAL_REGISTRATION_BACKOFF, "50 ms")
    config.setString(TaskManagerOptions.REGISTRATION_MAX_BACKOFF, "100 ms")

    val cluster = new TestingCluster(config, singleActorSystem = false)

    cluster.start()

    cluster
  }
}
