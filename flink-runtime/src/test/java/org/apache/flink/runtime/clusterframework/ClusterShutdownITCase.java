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
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.messages.StopCluster;
import org.apache.flink.runtime.clusterframework.messages.StopClusterSuccessful;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.messages.Messages;
import org.apache.flink.runtime.testingUtils.TestingMessages;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.testutils.TestingResourceManager;
import org.apache.flink.util.TestLogger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Option;


/**
 * Runs tests to ensure that a cluster is shutdown properly.
 */
public class ClusterShutdownITCase extends TestLogger {

	private static ActorSystem system;

	private static Configuration config = new Configuration();

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

			ActorGateway me =
				TestingUtils.createForwardingActor(system, getTestActor(), Option.<String>empty());

			// start job manager which doesn't shutdown the actor system
			ActorGateway jobManager =
				TestingUtils.createJobManager(system, config, "jobmanager1");

			// Tell the JobManager to inform us of shutdown actions
			jobManager.tell(TestingMessages.getNotifyOfComponentShutdown(), me);

			// Register a TaskManager
			ActorGateway taskManager =
				TestingUtils.createTaskManager(system, jobManager, config, true, true);

			// Tell the TaskManager to inform us of TaskManager shutdowns
			taskManager.tell(TestingMessages.getNotifyOfComponentShutdown(), me);


			// No resource manager connected
			jobManager.tell(new StopCluster(ApplicationStatus.SUCCEEDED, "Shutting down."), me);

			expectMsgAllOf(
				new TestingMessages.ComponentShutdown(taskManager.actor()),
				new TestingMessages.ComponentShutdown(jobManager.actor()),
				StopClusterSuccessful.getInstance()
			);

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

			ActorGateway me =
				TestingUtils.createForwardingActor(system, getTestActor(), Option.<String>empty());

			// start job manager which doesn't shutdown the actor system
			ActorGateway jobManager =
				TestingUtils.createJobManager(system, config, "jobmanager2");

			// Tell the JobManager to inform us of shutdown actions
			jobManager.tell(TestingMessages.getNotifyOfComponentShutdown(), me);

			// Register a TaskManager
			ActorGateway taskManager =
				TestingUtils.createTaskManager(system, jobManager, config, true, true);

			// Tell the TaskManager to inform us of TaskManager shutdowns
			taskManager.tell(TestingMessages.getNotifyOfComponentShutdown(), me);

			// Start resource manager and let it register
			ActorGateway resourceManager =
				TestingUtils.createResourceManager(system, jobManager.actor(), config);

			// Tell the ResourceManager to inform us of ResourceManager shutdowns
			resourceManager.tell(TestingMessages.getNotifyOfComponentShutdown(), me);

			// notify about a resource manager registration at the job manager
			resourceManager.tell(new TestingResourceManager.NotifyWhenResourceManagerConnected(), me);

			// Wait for resource manager
			expectMsgEquals(Messages.getAcknowledge());


			// Shutdown cluster with resource manager connected
			jobManager.tell(new StopCluster(ApplicationStatus.SUCCEEDED, "Shutting down."), me);

			expectMsgAllOf(
				new TestingMessages.ComponentShutdown(taskManager.actor()),
				new TestingMessages.ComponentShutdown(jobManager.actor()),
				new TestingMessages.ComponentShutdown(resourceManager.actor()),
				StopClusterSuccessful.getInstance()
			);

		}};
		}};
	}
}
