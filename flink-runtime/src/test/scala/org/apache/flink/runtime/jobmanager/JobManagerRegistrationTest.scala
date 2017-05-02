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
import java.util.concurrent.{Executors, ScheduledExecutorService}

import akka.actor._
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.akka.AkkaUtils
import org.apache.flink.runtime.clusterframework.FlinkResourceManager
import org.apache.flink.runtime.clusterframework.types.ResourceID
import org.apache.flink.runtime.highavailability.HighAvailabilityServices
import org.apache.flink.runtime.instance._
import org.apache.flink.runtime.jobmanager.JobManagerRegistrationTest.PlainForwardingActor
import org.apache.flink.runtime.messages.JobManagerMessages.LeaderSessionMessage
import org.apache.flink.runtime.messages.RegistrationMessages.{AcknowledgeRegistration, AlreadyRegistered, RegisterTaskManager}
import org.apache.flink.runtime.taskmanager.TaskManagerLocation
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages.NotifyWhenLeader
import org.apache.flink.runtime.testingUtils.{TestingJobManager, TestingUtils}
import org.apache.flink.runtime.testutils.TestingResourceManager
import org.apache.flink.runtime.util.LeaderRetrievalUtils
import org.junit.Assert.{assertNotEquals, assertNotNull}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

/**
 * Tests for the JobManager's behavior when a TaskManager solicits registration.
 */
@RunWith(classOf[JUnitRunner])
class JobManagerRegistrationTest(_system: ActorSystem) extends TestKit(_system) with
ImplicitSender with WordSpecLike with Matchers with BeforeAndAfterAll {

  def this() = this(AkkaUtils.createLocalActorSystem(new Configuration()))

  val executor: ScheduledExecutorService = Executors.newScheduledThreadPool(2)
  
  override def afterAll(): Unit = {
    executor.shutdownNow()
    TestKit.shutdownActorSystem(system)
  }

  "The JobManager" should {

    "assign a TaskManager a unique instance ID" in {

      var jmOption: Option[ActorGateway] = None
      var rmOption: Option[ActorGateway] = None
      var tm1Option: Option[ActorRef] = None
      var tm2Option: Option[ActorRef] = None

      try {
        val jm = startTestingJobManager(_system)
        jmOption = Some(jm)

        val rm = startTestingResourceManager(_system, jm.actor())
        rmOption = Some(rm)

        val probe = TestProbe()
        val tm1 = _system.actorOf(Props(new PlainForwardingActor(probe.ref)))
        tm1Option = Some(tm1)

        val tm2 = _system.actorOf(Props(new PlainForwardingActor(probe.ref)))
        tm2Option = Some(tm2)

        val resourceId1 = ResourceID.generate()
        val resourceId2 = ResourceID.generate()

        val connectionInfo1 = new TaskManagerLocation(resourceId1, InetAddress.getLocalHost, 10000)
        val connectionInfo2 = new TaskManagerLocation(resourceId2, InetAddress.getLocalHost, 10001)

        val hardwareDescription = HardwareDescription.extractFromSystem(10)

        var id1: InstanceID = null
        var id2: InstanceID = null

        // wait until the JobManager becomes the leader, otherwise the RegisterTaskManager messages
        // are dropped
        val leaderFuture = jm.ask(NotifyWhenLeader, TestingUtils.TESTING_TIMEOUT)
        Await.ready(leaderFuture, TestingUtils.TESTING_TIMEOUT)

        // task manager 1
        within(10 seconds) {
          jm.tell(
            RegisterTaskManager(
              resourceId1,
              connectionInfo1,
              hardwareDescription,
              1),
            new AkkaActorGateway(tm1, HighAvailabilityServices.DEFAULT_LEADER_ID))

          val response = probe.expectMsgType[LeaderSessionMessage]
          response match {
            case LeaderSessionMessage(_, AcknowledgeRegistration(id, _)) => id1 = id
            case _ => fail("Wrong response message: " + response)
          }
        }

        // task manager 2
        within(10 seconds) {
          jm.tell(
            RegisterTaskManager(
              resourceId2,
              connectionInfo2,
              hardwareDescription,
              1),
            new AkkaActorGateway(tm2, HighAvailabilityServices.DEFAULT_LEADER_ID))

          val response = probe.expectMsgType[LeaderSessionMessage]
          response match {
            case LeaderSessionMessage(leaderSessionID, AcknowledgeRegistration(id, _)) => id2 = id
            case _ => fail("Wrong response message: " + response)
          }
        }

        assertNotNull(id1)
        assertNotNull(id2)
        assertNotEquals(id1, id2)
      } finally {
        jmOption.foreach(TestingUtils.stopActorGracefully)
        rmOption.foreach(TestingUtils.stopActorGracefully)
        tm1Option.foreach(TestingUtils.stopActorGracefully)
        tm2Option.foreach(TestingUtils.stopActorGracefully)
      }
    }

    "handle repeated registration calls" in {

      var jmOption: Option[ActorGateway] = None
      var rmOption: Option[ActorGateway] = None

      try {
        val probe = TestProbe()

        val jm = startTestingJobManager(_system)
        jmOption = Some(jm)
        val rm = startTestingResourceManager(_system, jm.actor())
        rmOption = Some(rm)

        val selfGateway = new AkkaActorGateway(
          probe.ref,
          HighAvailabilityServices.DEFAULT_LEADER_ID)

        val resourceID = ResourceID.generate()
        val connectionInfo = new TaskManagerLocation(resourceID, InetAddress.getLocalHost, 1)
        val hardwareDescription = HardwareDescription.extractFromSystem(10)

        // wait until the JobManager becomes the leader, otherwise the RegisterTaskManager messages
        // are dropped
        val leaderFuture = jm.ask(NotifyWhenLeader, TestingUtils.TESTING_TIMEOUT)
        Await.ready(leaderFuture, TestingUtils.TESTING_TIMEOUT)

        within(20 seconds) {
          jm.tell(
            RegisterTaskManager(
              resourceID,
              connectionInfo,
              hardwareDescription,
              1),
            selfGateway)

          jm.tell(
            RegisterTaskManager(
              resourceID,
              connectionInfo,
              hardwareDescription,
              1),
            selfGateway)

          jm.tell(
            RegisterTaskManager(
              resourceID,
              connectionInfo,
              hardwareDescription,
              1),
            selfGateway)

          probe.expectMsgType[LeaderSessionMessage] match {
            case LeaderSessionMessage(
            HighAvailabilityServices.DEFAULT_LEADER_ID,
            AcknowledgeRegistration(_, _)) =>
            case m => fail("Wrong message type: " + m)
          }

          probe.expectMsgType[LeaderSessionMessage] match {
            case LeaderSessionMessage(
            HighAvailabilityServices.DEFAULT_LEADER_ID,
            AlreadyRegistered(_, _)) =>
            case m => fail("Wrong message type: " + m)
          }

          probe.expectMsgType[LeaderSessionMessage] match {
            case LeaderSessionMessage(
            HighAvailabilityServices.DEFAULT_LEADER_ID,
            AlreadyRegistered(_, _)) =>
            case m => fail("Wrong message type: " + m)
          }
        }
      } finally {
        jmOption.foreach(TestingUtils.stopActorGracefully)
        rmOption.foreach(TestingUtils.stopActorGracefully)
      }
    }
  }

  private def startTestingJobManager(system: ActorSystem): ActorGateway = {
    val config = new Configuration()

    val components = JobManager.createJobManagerComponents(
      config,
      executor,
      executor,
      None)

    // Start the JobManager without a MetricRegistry so that we don't start the MetricQueryService.
    // The problem of the MetricQueryService is that it starts an actor with a fixed name. Thus,
    // if there exists already one of these actors (e.g. JobManager has not been properly shutdown),
    // then this will fail the JobManager creation
    val props = JobManager.getJobManagerProps(
      classOf[TestingJobManager],
      config,
      executor,
      executor,
      components._1,
      components._2,
      components._3,
      ActorRef.noSender,
      components._4,
      components._5,
      components._8,
      components._9,
      components._10,
      components._11,
      None)

    val jm = _system.actorOf(props)

    new AkkaActorGateway(jm, HighAvailabilityServices.DEFAULT_LEADER_ID)
  }

  private def startTestingResourceManager(system: ActorSystem, jm: ActorRef): ActorGateway = {
    val config = new Configuration()
    val rm: ActorRef = FlinkResourceManager.startResourceManagerActors(
      config,
      _system,
      LeaderRetrievalUtils.createLeaderRetrievalService(config, jm),
      classOf[TestingResourceManager])
    new AkkaActorGateway(rm, HighAvailabilityServices.DEFAULT_LEADER_ID)
  }
}

object JobManagerRegistrationTest {

  class PlainForwardingActor(private val target: ActorRef) extends Actor {
    override def receive: Receive = {
      case message => target.forward(message)
    }
  }
}
