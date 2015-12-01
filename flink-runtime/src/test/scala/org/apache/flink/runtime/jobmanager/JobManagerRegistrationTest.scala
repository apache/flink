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
import org.apache.flink.runtime.akka.AkkaUtils
import org.apache.flink.runtime.instance._
import org.apache.flink.runtime.jobmanager.JobManagerRegistrationTest.PlainForwardingActor
import org.apache.flink.runtime.messages.JobManagerMessages.LeaderSessionMessage
import org.apache.flink.runtime.messages.RegistrationMessages.{AcknowledgeRegistration, AlreadyRegistered, RegisterTaskManager}

import org.junit.Assert.{assertNotEquals, assertNotNull}
import org.junit.runner.RunWith

import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.duration._
import scala.language.postfixOps

/**
 * Tests for the JobManager's behavior when a TaskManager solicits registration.
 * It also tests the JobManager's response to heartbeats from TaskManagers it does
 * not know.
 */
@RunWith(classOf[JUnitRunner])
class JobManagerRegistrationTest(_system: ActorSystem) extends TestKit(_system) with
ImplicitSender with WordSpecLike with Matchers with BeforeAndAfterAll {

  def this() = this(AkkaUtils.createLocalActorSystem(new Configuration()))

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "The JobManager" should {

    "assign a TaskManager a unique instance ID" in {
      val jm = startTestingJobManager(_system)
      
      val tm1 = _system.actorOf(Props(new PlainForwardingActor(testActor)))
      val tm2 = _system.actorOf(Props(new PlainForwardingActor(testActor)))
      
      val connectionInfo1 = new InstanceConnectionInfo(InetAddress.getLocalHost, 10000)
      val connectionInfo2 = new InstanceConnectionInfo(InetAddress.getLocalHost, 10001)

      val hardwareDescription = HardwareDescription.extractFromSystem(10)
      
      var id1: InstanceID = null
      var id2: InstanceID = null

      // task manager 1
      within(1 second) {
        jm.tell(
          RegisterTaskManager(
            connectionInfo1,
            hardwareDescription,
            1),
          new AkkaActorGateway(tm1, null))

        val response = expectMsgType[LeaderSessionMessage]
        response match {
          case LeaderSessionMessage(_, AcknowledgeRegistration(id, _)) => id1 = id
          case _ => fail("Wrong response message: " + response)
        }
      }

      // task manager 2
      within(1 second) {
        jm.tell(
          RegisterTaskManager(
            connectionInfo2,
            hardwareDescription,
            1),
          new AkkaActorGateway(tm2, null))

        val response = expectMsgType[LeaderSessionMessage]
        response match {
          case LeaderSessionMessage(leaderSessionID, AcknowledgeRegistration(id, _)) => id2 = id
          case _ => fail("Wrong response message: " + response)
        }
      }

      assertNotNull(id1)
      assertNotNull(id2)
      assertNotEquals(id1, id2)
    }

    "handle repeated registration calls" in {

      val jm = startTestingJobManager(_system)
      val selfGateway = new AkkaActorGateway(testActor, null)
      
      val connectionInfo = new InstanceConnectionInfo(InetAddress.getLocalHost,1)
      val hardwareDescription = HardwareDescription.extractFromSystem(10)

      within(5 second) {
        jm.tell(
          RegisterTaskManager(
            connectionInfo,
            hardwareDescription,
            1),
          selfGateway)

        jm.tell(
          RegisterTaskManager(
            connectionInfo,
            hardwareDescription,
            1),
          selfGateway)

        jm.tell(
          RegisterTaskManager(
            connectionInfo,
            hardwareDescription,
            1),
          selfGateway)

        expectMsgType[LeaderSessionMessage] match {
          case LeaderSessionMessage(null, AcknowledgeRegistration(_, _)) =>
          case m => fail("Wrong message type: " + m)
        }

        expectMsgType[LeaderSessionMessage] match {
          case LeaderSessionMessage(null, AlreadyRegistered(_, _)) =>
          case m => fail("Wrong message type: " + m)
        }

        expectMsgType[LeaderSessionMessage] match {
          case LeaderSessionMessage(null, AlreadyRegistered(_, _)) =>
          case m => fail("Wrong message type: " + m)
        }
      }
    }
  }

  private def startTestingJobManager(system: ActorSystem): ActorGateway = {
    val (jm: ActorRef, _) = JobManager.startJobManagerActors(
      new Configuration(),
      _system,
      None,
      None,
      classOf[JobManager],
      classOf[MemoryArchivist])
    new AkkaActorGateway(jm, null)
  }
}

object JobManagerRegistrationTest {
  
  class PlainForwardingActor(private val target: ActorRef) extends Actor {
    override def receive: Receive = {
      case message => target.forward(message)
    }
  }
}
