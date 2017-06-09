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
import org.apache.flink.runtime.clusterframework.messages.StopCluster;
import org.apache.flink.runtime.clusterframework.messages.StopClusterSuccessful;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.nonha.embedded.EmbeddedHaServices;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.testingUtils.TestingMessages;
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

/**
 * Runs tests to ensure that a cluster is shutdown properly.
 */
public class ClusterShutdownITCase extends TestLogger {

	private static ActorSystem system;

	private static Configuration config = new Configuration();

	private HighAvailabilityServices highAvailabilityServices;

	@Before
	public void setupTest() {
		highAvailabilityServices = new EmbeddedHaServices(TestingUtils.defaultExecutor());
	}

	@After
	public void tearDownTest() throws Exception {
		highAvailabilityServices.closeAndCleanupAllData();
		highAvailabilityServices = null;
	}

	@BeforeClass
	public static void setup() {
		system = AkkaUtils.createActorSystem(AkkaUtils.getDefaultAkkaConfig());
	}

	@AfterClass
	public static void teardown() {
		JavaTestKit.shutdownActorSystem(system);
	}

	/**
	 * Tests a faked cluster shutdown procedure without the ResourceManager.
	 */
	@Test
	public void testClusterShutdownWithoutResourceManager() {

		new JavaTestKit(system){{
		new Within(duration("30 seconds")) {
		@Override
		protected void run() {

			ActorGateway jobManager = null;
			ActorGateway taskManager = null;
			ActorGateway forwardingActor = null;

			try {
				// start job manager which doesn't shutdown the actor system
				jobManager =
					TestingUtils.createJobManager(
						system,
						TestingUtils.defaultExecutor(),
						TestingUtils.defaultExecutor(),
						config,
						highAvailabilityServices,
						"jobmanager1");

				forwardingActor =
					TestingUtils.createForwardingActor(
						system,
						getTestActor(),
						jobManager.leaderSessionID(),
						Option.<String>empty());

				// Tell the JobManager to inform us of shutdown actions
				jobManager.tell(TestingMessages.getNotifyOfComponentShutdown(), forwardingActor);

				// Register a TaskManager
				taskManager =
					TestingUtils.createTaskManager(system, highAvailabilityServices, config, true, true);

				// Tell the TaskManager to inform us of TaskManager shutdowns
				taskManager.tell(TestingMessages.getNotifyOfComponentShutdown(), forwardingActor);


				// No resource manager connected
				jobManager.tell(new StopCluster(ApplicationStatus.SUCCEEDED, "Shutting down."), forwardingActor);

				expectMsgAllOf(
					new TestingMessages.ComponentShutdown(taskManager.actor()),
					new TestingMessages.ComponentShutdown(jobManager.actor()),
					StopClusterSuccessful.getInstance()
				);
			} finally {
				TestingUtils.stopActorGatewaysGracefully(Arrays.asList(
					jobManager, taskManager, forwardingActor));
			}

		}};
		}};
	}

	/**
	 * Tests a faked cluster shutdown procedure with the ResourceManager.
	 */
	@Test
	public void testClusterShutdownWithResourceManager() {

		new JavaTestKit(system){{
		new Within(duration("30 seconds")) {
		@Override
		protected void run() {

			ActorGateway jobManager = null;
			ActorGateway taskManager = null;
			ActorGateway resourceManager = null;
			ActorGateway forwardingActor = null;

			try {
				// start job manager which doesn't shutdown the actor system
				jobManager =
					TestingUtils.createJobManager(
						system,
						TestingUtils.defaultExecutor(),
						TestingUtils.defaultExecutor(),
						config,
						highAvailabilityServices,
						"jobmanager2");

				forwardingActor =
					TestingUtils.createForwardingActor(
						system,
						getTestActor(),
						jobManager.leaderSessionID(),
						Option.<String>empty());

				// Tell the JobManager to inform us of shutdown actions
				jobManager.tell(TestingMessages.getNotifyOfComponentShutdown(), forwardingActor);

				// Register a TaskManager
				taskManager =
					TestingUtils.createTaskManager(system, highAvailabilityServices, config, true, true);

				// Tell the TaskManager to inform us of TaskManager shutdowns
				taskManager.tell(TestingMessages.getNotifyOfComponentShutdown(), forwardingActor);

				// Start resource manager and let it register
				resourceManager =
					TestingUtils.createResourceManager(
						system,
						config,
						highAvailabilityServices);

				// Tell the ResourceManager to inform us of ResourceManager shutdowns
				resourceManager.tell(TestingMessages.getNotifyOfComponentShutdown(), forwardingActor);

				// notify about a resource manager registration at the job manager
				resourceManager.tell(new TestingResourceManager.NotifyWhenResourceManagerConnected(), forwardingActor);

				// Wait for resource manager
				expectMsgEquals(Acknowledge.get());

				// Shutdown cluster with resource manager connected
				jobManager.tell(new StopCluster(ApplicationStatus.SUCCEEDED, "Shutting down."), forwardingActor);

				expectMsgAllOf(
					new TestingMessages.ComponentShutdown(taskManager.actor()),
					new TestingMessages.ComponentShutdown(jobManager.actor()),
					new TestingMessages.ComponentShutdown(resourceManager.actor()),
					StopClusterSuccessful.getInstance()
				);
			} finally {
				TestingUtils.stopActorGatewaysGracefully(Arrays.asList(
					jobManager, taskManager, resourceManager, forwardingActor));
			}

		}};
		}};
	}
}
