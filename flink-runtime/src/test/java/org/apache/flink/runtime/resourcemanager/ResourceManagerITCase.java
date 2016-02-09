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

package org.apache.flink.runtime.resourcemanager;

import akka.actor.ActorSystem;
import akka.testkit.JavaTestKit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.messages.StopCluster;
import org.apache.flink.runtime.clusterframework.messages.StopClusterSuccessful;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.instance.InstanceConnectionInfo;
import org.apache.flink.runtime.messages.Messages;
import org.apache.flink.runtime.messages.RegistrationMessages;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.testutils.TestingResourceManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import scala.Option;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class ResourceManagerITCase {

	private static ActorSystem system;

	private static Configuration config = new Configuration();

	@Before
	public void setup() {
		system = AkkaUtils.createActorSystem(AkkaUtils.getDefaultAkkaConfig());
	}

	@After
	public void teardown() {
		JavaTestKit.shutdownActorSystem(system);
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

			ActorGateway jobManager = TestingUtils.createJobManager(system, config);
			ActorGateway me =
				TestingUtils.createForwardingActor(system, getTestActor(), Option.<String>empty());

			// !! no resource manager started !!

			ResourceID resourceID = ResourceID.generate();

			jobManager.tell(
				new RegistrationMessages.RegisterTaskManager(
					resourceID,
					Mockito.mock(InstanceConnectionInfo.class),
					null,
					1),
				me);

			expectMsgClass(RegistrationMessages.AcknowledgeRegistration.class);

			// now start the resource manager
			ActorGateway resourceManager =
				TestingUtils.createResourceManager(system, jobManager.actor(), config);

			// register at testing job manager to receive a message once a resource manager registers
			resourceManager.tell(new TestingResourceManager.NotifyWhenResourceManagerConnected(), me);

			// Wait for resource manager
			expectMsgEquals(Messages.getAcknowledge());

			// check if we registered the task manager resource
			resourceManager.tell(new TestingResourceManager.GetRegisteredResources(), me);

			TestingResourceManager.GetRegisteredResourcesReply reply =
				expectMsgClass(TestingResourceManager.GetRegisteredResourcesReply.class);

			assertEquals(1, reply.resources.size());
			assertTrue(reply.resources.contains(resourceID));

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

			ActorGateway jobManager = TestingUtils.createJobManager(system, config);
			ActorGateway me =
				TestingUtils.createForwardingActor(system, getTestActor(), Option.<String>empty());

			// start the resource manager
			ActorGateway resourceManager =
				TestingUtils.createResourceManager(system, jobManager.actor(), config);

			// notify about a resource manager registration at the job manager
			resourceManager.tell(new TestingResourceManager.NotifyWhenResourceManagerConnected(), me);

			// Wait for resource manager
			expectMsgEquals(Messages.getAcknowledge());

			// start task manager and wait for registration
			ActorGateway taskManager =
				TestingUtils.createTaskManager(system, jobManager.actor(), config, true, true);

			// check if we registered the task manager resource
			resourceManager.tell(new TestingResourceManager.GetRegisteredResources(), me);

			TestingResourceManager.GetRegisteredResourcesReply reply =
				expectMsgClass(TestingResourceManager.GetRegisteredResourcesReply.class);

			assertEquals(1, reply.resources.size());

		}};
		}};
	}


	/**
	 * Tests cluster shutdown procedure of RM
	 */
	@Test
	public void testClusterShutdown() {
		new JavaTestKit(system){{
			new Within(duration("30 seconds")) {
				@Override
				protected void run() {

					ActorGateway me =
						TestingUtils.createForwardingActor(system, getTestActor(), Option.<String>empty());

					ActorGateway jobManager = TestingUtils.createJobManager(system, config);

					ActorGateway resourceManager =
						TestingUtils.createResourceManager(system, jobManager.actor(), config);

					// notify about a resource manager registration at the job manager
					resourceManager.tell(new TestingResourceManager.NotifyWhenResourceManagerConnected(), me);

					// Wait for resource manager
					expectMsgEquals(Messages.getAcknowledge());

					jobManager.tell(new StopCluster(ApplicationStatus.SUCCEEDED, "Shutting down."), me);

					expectMsgClass(StopClusterSuccessful.class);

					boolean isTerminated = false;
					for (int i=0; i < 10 && !isTerminated; i++) {
						isTerminated = system.isTerminated();
						try {
							Thread.sleep(1000);
						} catch (InterruptedException e) {
							// try again
						}
					}

					assertTrue(isTerminated);

				}};
		}};
	}

}
