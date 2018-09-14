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
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

import akka.actor._
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.akka.AkkaUtils
import org.apache.flink.runtime.clusterframework.FlinkResourceManager
import org.apache.flink.runtime.clusterframework.types.ResourceID
import org.apache.flink.runtime.highavailability.HighAvailabilityServices
import org.apache.flink.runtime.highavailability.nonha.embedded.EmbeddedHaServices
import org.apache.flink.runtime.instance._
import org.apache.flink.runtime.jobmanager.JobManagerRegistrationTest.PlainForwardingActor
import org.apache.flink.runtime.messages.JobManagerMessages.LeaderSessionMessage
import org.apache.flink.runtime.messages.RegistrationMessages.{AcknowledgeRegistration, AlreadyRegistered, RegisterTaskManager}
import org.apache.flink.runtime.metrics.{MetricRegistryImpl, MetricRegistryConfiguration}
import org.apache.flink.runtime.taskmanager.TaskManagerLocation
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages.NotifyWhenLeader
import org.apache.flink.runtime.testingUtils.{TestingJobManager, TestingUtils}
import org.apache.flink.runtime.testutils.TestingResourceManager
import org.apache.flink.runtime.util.LeaderRetrievalUtils
import org.junit.Assert.{assertNotEquals, assertNotNull}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Matchers, WordSpecLike}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

/**
 * Tests for the JobManager's behavior when a TaskManager solicits registration.
 */
@RunWith(classOf[JUnitRunner])
class JobManagerRegistrationTest(_system: ActorSystem) extends TestKit(_system) with
ImplicitSender with WordSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

  def this() = this(AkkaUtils.createLocalActorSystem(new Configuration()))

  val executor: ScheduledExecutorService = Executors.newScheduledThreadPool(2)

  var highAvailabilityServices: HighAvailabilityServices = _

  val metricRegistry: MetricRegistryImpl = new MetricRegistryImpl(
    MetricRegistryConfiguration.fromConfiguration(new Configuration())
  )

  val timeout = FiniteDuration(30, TimeUnit.SECONDS)
  
  override def afterAll(): Unit = {
    executor.shutdownNow()
    TestKit.shutdownActorSystem(system)
  }

  override def beforeEach(): Unit = {
    highAvailabilityServices = new EmbeddedHaServices(executor)
  }

  override def afterEach(): Unit = {
    if (highAvailabilityServices != null) {
      highAvailabilityServices.closeAndCleanupAllData()
    }
  }

  "The JobManager" should {

    "assign a TaskManager a unique instance ID" in {

      var jmOption: Option[ActorGateway] = None
      var rmOption: Option[ActorRef] = None
      var tm1Option: Option[ActorRef] = None
      var tm2Option: Option[ActorRef] = None

      try {
        val jm = startTestingJobManager(_system, highAvailabilityServices, metricRegistry)
        jmOption = Some(jm)

        val rm = startTestingResourceManager(_system, highAvailabilityServices)
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
      var rmOption: Option[ActorRef] = None

      try {
        val probe = TestProbe()

        val jm = startTestingJobManager(_system, highAvailabilityServices, metricRegistry)
        jmOption = Some(jm)
        val rm = startTestingResourceManager(_system, highAvailabilityServices)
        rmOption = Some(rm)

        val leaderId = jm.leaderSessionID()

        val selfGateway = new AkkaActorGateway(probe.ref, leaderId)

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
              `leaderId`,
              AcknowledgeRegistration(_, _)) =>
            case m => fail("Wrong message type: " + m)
          }

          probe.expectMsgType[LeaderSessionMessage] match {
            case LeaderSessionMessage(
              `leaderId`,
              AlreadyRegistered(_, _)) =>
            case m => fail("Wrong message type: " + m)
          }

          probe.expectMsgType[LeaderSessionMessage] match {
            case LeaderSessionMessage(
              `leaderId`,
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

  private def startTestingJobManager(
      system: ActorSystem,
      highAvailabilityServices: HighAvailabilityServices,
      metricRegistry: MetricRegistryImpl): ActorGateway = {

    val config = new Configuration()
    
    val components = JobManager.createJobManagerComponents(
      config,
      executor,
      executor,
      highAvailabilityServices.createBlobStore(),
      metricRegistry)

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
      components._4,
      ActorRef.noSender,
      components._5,
      components._6,
      highAvailabilityServices.getJobManagerLeaderElectionService(
        HighAvailabilityServices.DEFAULT_JOB_ID),
      highAvailabilityServices.getSubmittedJobGraphStore(),
      highAvailabilityServices.getCheckpointRecoveryFactory(),
      components._9,
      components._10,
      None)

    _system.actorOf(props)

    LeaderRetrievalUtils.retrieveLeaderGateway(
      highAvailabilityServices.getJobManagerLeaderRetriever(
        HighAvailabilityServices.DEFAULT_JOB_ID),
      system,
      timeout)
  }

  private def startTestingResourceManager(
      system: ActorSystem,
      highAvailabilityServices: HighAvailabilityServices)
    : ActorRef = {
    val config = new Configuration()

    FlinkResourceManager.startResourceManagerActors(
      config,
      _system,
      highAvailabilityServices.getJobManagerLeaderRetriever(
        HighAvailabilityServices.DEFAULT_JOB_ID),
      classOf[TestingResourceManager])
  }
}

object JobManagerRegistrationTest {

  class PlainForwardingActor(private val target: ActorRef) extends Actor {
    override def receive: Receive = {
      case message => target.forward(message)
    }
  }
}
