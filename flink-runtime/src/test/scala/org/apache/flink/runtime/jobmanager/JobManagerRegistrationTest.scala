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
import akka.testkit.{ImplicitSender, TestKit}
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.StreamingMode
import org.apache.flink.runtime.akka.AkkaUtils
import org.apache.flink.runtime.instance.{HardwareDescription, InstanceConnectionInfo, InstanceID}
import org.apache.flink.runtime.messages.RegistrationMessages.{AcknowledgeRegistration, AlreadyRegistered, RegisterTaskManager}
import org.junit.Assert.{assertNotEquals, assertNotNull}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.duration._
import scala.language.postfixOps

/**
 * Tests for the JobManager's behavior when a TaskManager solicits registration.
 * It also tests the JobManager's response to heartbeats from TaskManagers it does
 * not know.
 */
class JobManagerRegistrationTest(_system: ActorSystem) extends TestKit(_system) with
ImplicitSender with WordSpecLike with Matchers with BeforeAndAfterAll {

  def this() = this(AkkaUtils.createLocalActorSystem(new Configuration()))

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "The JobManager" should {

    "assign a TaskManager a unique instance ID" in {
      val jm = startTestingJobManager(_system)

      val tmDummy1 = _system.actorOf(Props(classOf[JobManagerRegistrationTest.DummyActor]))
      val tmDummy2 = _system.actorOf(Props(classOf[JobManagerRegistrationTest.DummyActor]))

      try {
        val connectionInfo1 = new InstanceConnectionInfo(InetAddress.getLocalHost, 10000)
        val connectionInfo2 = new InstanceConnectionInfo(InetAddress.getLocalHost, 10001)

        val hardwareDescription = HardwareDescription.extractFromSystem(10)

        var id1: InstanceID = null
        var id2: InstanceID = null

        // task manager 1
        within(1 second) {
          jm ! RegisterTaskManager(tmDummy1, connectionInfo1, hardwareDescription, 1)

          val response = receiveOne(1 second)
          response match {
            case AcknowledgeRegistration(_, id, _) => id1 = id
            case _ => fail("Wrong response message: " + response)
          }
        }

        // task manager 2
        within(1 second) {
          jm ! RegisterTaskManager(tmDummy2, connectionInfo2, hardwareDescription, 1)

          val response = receiveOne(1 second)
          response match {
            case AcknowledgeRegistration(_, id, _) => id2 = id
            case _ => fail("Wrong response message: " + response)
          }
        }

        assertNotNull(id1)
        assertNotNull(id2)
        assertNotEquals(id1, id2)
      }
      finally {
        tmDummy1 ! Kill
        tmDummy2 ! Kill
        jm ! Kill
      }
    }

    "handle repeated registration calls" in {

      val jm = startTestingJobManager(_system)
      val tmDummy = _system.actorOf(Props(classOf[JobManagerRegistrationTest.DummyActor]))

      try {
        val connectionInfo = new InstanceConnectionInfo(InetAddress.getLocalHost,1)
        val hardwareDescription = HardwareDescription.extractFromSystem(10)
        
        within(1 second) {
          jm ! RegisterTaskManager(tmDummy, connectionInfo, hardwareDescription, 1)
          jm ! RegisterTaskManager(tmDummy, connectionInfo, hardwareDescription, 1)
          jm ! RegisterTaskManager(tmDummy, connectionInfo, hardwareDescription, 1)

          expectMsgType[AcknowledgeRegistration]
          expectMsgType[AlreadyRegistered]
          expectMsgType[AlreadyRegistered]
        }
      } finally {
        tmDummy ! Kill
        jm ! Kill
      }
    }
  }

  private def startTestingJobManager(system: ActorSystem): ActorRef = {
    val (jm: ActorRef, _) = JobManager.startJobManagerActors(new Configuration(), _system,
                                                             None, None,
                                                             StreamingMode.BATCH_ONLY)
    jm
  }
}

object JobManagerRegistrationTest {

  /** Simply dummy actor that swallows all messages */
  class DummyActor extends Actor {
    override def receive: Receive = {
      case _ =>
    }
  }
}
