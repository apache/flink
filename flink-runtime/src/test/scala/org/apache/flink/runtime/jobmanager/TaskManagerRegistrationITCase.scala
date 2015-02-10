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

import java.net.InetAddress

import akka.actor._
import akka.testkit.{TestKit, ImplicitSender}
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.runtime.instance.{InstanceID, HardwareDescription, InstanceConnectionInfo}
import org.apache.flink.runtime.messages.RegistrationMessages.{AlreadyRegistered,
RefuseRegistration, AcknowledgeRegistration, RegisterTaskManager}
import org.apache.flink.runtime.messages.TaskManagerMessages.Heartbeat
import org.apache.flink.runtime.testingUtils.TestingUtils
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import scala.concurrent.duration._

import scala.language.postfixOps

class TaskManagerRegistrationITCase(_system: ActorSystem) extends TestKit(_system) with
ImplicitSender with WordSpecLike with Matchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("TestingActorSystem", TestingUtils.testConfig))

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "The JobManager" should {
    "notify already registered TaskManagers" in {

      val jm = TestingUtils.startTestingJobManager

      val connectionInfo = new InstanceConnectionInfo(InetAddress.getLocalHost,1)
      val hardwareDescription = HardwareDescription.extractFromSystem(10)

      try {
        within(TestingUtils.TESTING_DURATION) {
          jm ! RegisterTaskManager(connectionInfo, hardwareDescription, 1)
          jm ! RegisterTaskManager(connectionInfo, hardwareDescription, 1)

          expectMsgType[AcknowledgeRegistration]
          expectMsgType[AlreadyRegistered]
        }
      } finally {
        jm ! Kill
      }
    }
  }

  "The TaskManager" should {
    "shutdown if its registration is refused by the JobManager" in {

      val tm = TestingUtils.startTestingTaskManager(self)

      watch(tm)

      try{
        within(TestingUtils.TESTING_DURATION) {
          expectMsgType[RegisterTaskManager]
          tm ! RefuseRegistration("Testing connection refusal")

          expectTerminated(tm)
        }
      }
    }

    "ignore RefuseRegistration messages after it has been successfully registered" in {

      val tm = TestingUtils.startTestingTaskManager(self)

      try {
        ignoreMsg{
          case _: Heartbeat => true
        }
        within(TestingUtils.TESTING_DURATION) {
          expectMsgType[RegisterTaskManager]

          tm ! AcknowledgeRegistration(new InstanceID(), 42, None)

          tm ! RefuseRegistration("Should be ignored")

          // Check if the TaskManager is still alive
          tm ! Identify(1)

          expectMsgType[ActorIdentity]

        }
      } finally {
        tm ! Kill
      }
    }

    "shutdown after the maximum registration duration has been exceeded" in {

      val config = new Configuration()
      config.setString(ConfigConstants.TASK_MANAGER_MAX_REGISTRATION_DURATION, "1 second")

      val tm = TestingUtils.startTestingTaskManagerWithConfiguration("LOCALHOST",
        self.path.toString, config)

      watch(tm)

      try {
        ignoreMsg{
          case _: RegisterTaskManager => true
        }
        within(2 seconds) {
          expectTerminated(tm)
        }
      } finally {
        tm ! Kill
      }
    }
  }
}
