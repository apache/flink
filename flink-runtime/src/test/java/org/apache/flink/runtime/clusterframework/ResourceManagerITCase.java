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

package org.apache.flink.runtime.clusterframework;

import akka.actor.ActorSystem;
import akka.testkit.JavaTestKit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.nonha.embedded.EmbeddedHaServices;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.instance.HardwareDescription;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.messages.RegistrationMessages;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.testutils.TestingResourceManager;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import scala.Option;


import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * It cases which test the interaction of the resource manager with job manager and task managers.
 * Runs all tests in one Actor system.
 */
public class ResourceManagerITCase extends TestLogger {

	private static ActorSystem system;

	private static Configuration config = new Configuration();

	private HighAvailabilityServices highAvailabilityServices;

	@BeforeClass
	public static void setup() {
		system = AkkaUtils.createActorSystem(AkkaUtils.getDefaultAkkaConfig());
	}

	@AfterClass
	public static void teardown() {
		JavaTestKit.shutdownActorSystem(system);
	}

	@Before
	public void setupTest() {
		highAvailabilityServices = new EmbeddedHaServices(TestingUtils.defaultExecutor());
	}

	@After
	public void tearDownTest() throws Exception {
		highAvailabilityServices.closeAndCleanupAllData();
		highAvailabilityServices = null;
	}

	/**
	 * Tests whether the resource manager connects and reconciles existing task managers.
	 */
	@Test
	public void testResourceManagerReconciliation() {

		new JavaTestKit(system){{
		new Within(duration("10 seconds")) {
		@Override
		protected void run() {

			ActorGateway jobManager = null;
			ActorGateway resourceManager = null;
			ActorGateway forwardingActor = null;

			try {
				jobManager =
					TestingUtils.createJobManager(
						system,
						TestingUtils.defaultExecutor(),
						TestingUtils.defaultExecutor(),
						config,
						highAvailabilityServices,
						"ReconciliationTest");

				forwardingActor =
					TestingUtils.createForwardingActor(
						system,
						getTestActor(),
						jobManager.leaderSessionID(),
						Option.<String>empty());

				// !! no resource manager started !!

				ResourceID resourceID = ResourceID.generate();

				TaskManagerLocation location = mock(TaskManagerLocation.class);
				when(location.getResourceID()).thenReturn(resourceID);

				HardwareDescription resourceProfile = HardwareDescription.extractFromSystem(1_000_000);

				jobManager.tell(
					new RegistrationMessages.RegisterTaskManager(resourceID, location, resourceProfile, 1),
					forwardingActor);

				expectMsgClass(RegistrationMessages.AcknowledgeRegistration.class);

				// now start the resource manager
				resourceManager =
					TestingUtils.createResourceManager(
						system,
						config,
						highAvailabilityServices);

				// register at testing job manager to receive a message once a resource manager registers
				resourceManager.tell(new TestingResourceManager.NotifyWhenResourceManagerConnected(), forwardingActor);

				// Wait for resource manager
				expectMsgEquals(Acknowledge.get());

				// check if we registered the task manager resource
				resourceManager.tell(new TestingResourceManager.GetRegisteredResources(), forwardingActor);

				TestingResourceManager.GetRegisteredResourcesReply reply =
					expectMsgClass(TestingResourceManager.GetRegisteredResourcesReply.class);

				assertEquals(1, reply.resources.size());
				assertTrue(reply.resources.contains(resourceID));
			} finally {
				TestingUtils.stopActorGatewaysGracefully(Arrays.asList(
					jobManager, resourceManager, forwardingActor));
			}

		}};
		}};
	}

	/**
	 * Tests whether the resource manager gets informed upon TaskManager registration.
	 */
	@Test
	public void testResourceManagerTaskManagerRegistration() {

		new JavaTestKit(system){{
		new Within(duration("30 seconds")) {
		@Override
		protected void run() {

			ActorGateway jobManager = null;
			ActorGateway taskManager = null;
			ActorGateway resourceManager = null;
			ActorGateway forwardingActor = null;

			try {
				jobManager =
					TestingUtils.createJobManager(
						system,
						TestingUtils.defaultExecutor(),
						TestingUtils.defaultExecutor(),
						config,
						highAvailabilityServices,
						"RegTest");

				forwardingActor =
					TestingUtils.createForwardingActor(
						system,
						getTestActor(),
						jobManager.leaderSessionID(),
						Option.<String>empty());

				// start the resource manager
				resourceManager =
					TestingUtils.createResourceManager(system, config, highAvailabilityServices);

				// notify about a resource manager registration at the job manager
				resourceManager.tell(new TestingResourceManager.NotifyWhenResourceManagerConnected(), forwardingActor);

				// Wait for resource manager
				expectMsgEquals(Acknowledge.get());

				// start task manager and wait for registration
				taskManager =
					TestingUtils.createTaskManager(system, highAvailabilityServices, config, true, true);

				// check if we registered the task manager resource
				resourceManager.tell(new TestingResourceManager.GetRegisteredResources(), forwardingActor);

				TestingResourceManager.GetRegisteredResourcesReply reply =
					expectMsgClass(TestingResourceManager.GetRegisteredResourcesReply.class);

				assertEquals(1, reply.resources.size());
			} finally {
				TestingUtils.stopActorGatewaysGracefully(Arrays.asList(
					jobManager, resourceManager, taskManager, forwardingActor));
			}

		}};
		}};
	}

}
