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
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.clusterframework.messages.NotifyResourceStarted;
import org.apache.flink.runtime.clusterframework.messages.RegisterResourceManager;
import org.apache.flink.runtime.clusterframework.messages.RegisterResourceManagerSuccessful;
import org.apache.flink.runtime.clusterframework.messages.RemoveResource;
import org.apache.flink.runtime.clusterframework.messages.ResourceRemoved;
import org.apache.flink.runtime.clusterframework.messages.TriggerRegistrationAtJobManager;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.testutils.TestingResourceManager;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Option;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

/**
 * General tests for the resource manager component.
 */
public class ResourceManagerTest {

	private static ActorSystem system;

	private static ActorGateway fakeJobManager;
	private static ActorGateway resourceManager;

	private static Configuration config = new Configuration();

	@BeforeClass
	public static void setup() {
		system = AkkaUtils.createLocalActorSystem(config);
	}

	@AfterClass
	public static void teardown() {
		JavaTestKit.shutdownActorSystem(system);
	}

	/**
	 * Tests the registration and reconciliation of the ResourceManager with the JobManager
	 */
	@Test
	public void testJobManagerRegistrationAndReconciliation() {
		new JavaTestKit(system){{
		new Within(duration("10 seconds")) {
		@Override
		protected void run() {
			fakeJobManager = TestingUtils.createForwardingActor(system, getTestActor(), Option.<String>empty());
			resourceManager = TestingUtils.createResourceManager(system, fakeJobManager.actor(), config);

			expectMsgClass(RegisterResourceManager.class);

			List<ResourceID> resourceList = new ArrayList<>();
			resourceList.add(ResourceID.generate());
			resourceList.add(ResourceID.generate());
			resourceList.add(ResourceID.generate());

			resourceManager.tell(
				new RegisterResourceManagerSuccessful(fakeJobManager.actor(), resourceList),
				fakeJobManager);

			resourceManager.tell(new TestingResourceManager.GetRegisteredResources(), fakeJobManager);
			TestingResourceManager.GetRegisteredResourcesReply reply =
				expectMsgClass(TestingResourceManager.GetRegisteredResourcesReply.class);

			for (ResourceID id : resourceList) {
				if (!reply.resources.contains(id)) {
					fail("Expected to find all resources that were provided during registration.");
				}
			}
		}};
		}};
	}

	/**
	 * Tests delayed or erroneous registration of the ResourceManager with the JobManager
	 */
	@Test
	public void testDelayedJobManagerRegistration() {
		new JavaTestKit(system){{
		new Within(duration("10 seconds")) {
		@Override
		protected void run() {

			// set a short timeout for lookups
			Configuration shortTimeoutConfig = config.clone();
			shortTimeoutConfig.setString(ConfigConstants.AKKA_LOOKUP_TIMEOUT, "1 s");

			fakeJobManager = TestingUtils.createForwardingActor(system, getTestActor(), Option.<String>empty());
			resourceManager = TestingUtils.createResourceManager(system, fakeJobManager.actor(), shortTimeoutConfig);

			// wait for registration message
			RegisterResourceManager msg = expectMsgClass(RegisterResourceManager.class);
			// give wrong response
			getLastSender().tell(new JobManagerMessages.LeaderSessionMessage(null, new Object()),
				fakeJobManager.actor());

			// expect another retry and let it time out
			expectMsgClass(RegisterResourceManager.class);

			// wait for next try after timeout
			expectMsgClass(RegisterResourceManager.class);

		}};
		}};
	}

	@Test
	public void testTriggerReconnect() {
		new JavaTestKit(system){{
		new Within(duration("10 seconds")) {
		@Override
		protected void run() {

			// set a long timeout for lookups such that the test fails in case of timeouts
			Configuration shortTimeoutConfig = config.clone();
			shortTimeoutConfig.setString(ConfigConstants.AKKA_LOOKUP_TIMEOUT, "99999 s");

			fakeJobManager = TestingUtils.createForwardingActor(system, getTestActor(), Option.<String>empty());
			resourceManager = TestingUtils.createResourceManager(system, fakeJobManager.actor(), shortTimeoutConfig);

			// wait for registration message
			RegisterResourceManager msg = expectMsgClass(RegisterResourceManager.class);
			// all went well
			resourceManager.tell(
				new RegisterResourceManagerSuccessful(fakeJobManager.actor(), Collections.<ResourceID>emptyList()),
				fakeJobManager);

			// force a reconnect
			resourceManager.tell(
				new TriggerRegistrationAtJobManager(fakeJobManager.actor()),
				fakeJobManager);

			// new registration attempt should come in
			expectMsgClass(RegisterResourceManager.class);

		}};
		}};
	}

	/**
	 * Tests the registration and accounting of resources at the ResourceManager.
	 */
	@Test
	public void testTaskManagerRegistration() {
		new JavaTestKit(system){{
		new Within(duration("10 seconds")) {
		@Override
		protected void run() {

			fakeJobManager = TestingUtils.createForwardingActor(system, getTestActor(), Option.<String>empty());
			resourceManager = TestingUtils.createResourceManager(system, fakeJobManager.actor(), config);

			// register with JM
			expectMsgClass(RegisterResourceManager.class);
			resourceManager.tell(
				new RegisterResourceManagerSuccessful(fakeJobManager.actor(), Collections.<ResourceID>emptyList()),
				fakeJobManager);

			ResourceID resourceID = ResourceID.generate();

			// Send task manager registration
			resourceManager.tell(new NotifyResourceStarted(resourceID),
				fakeJobManager);

			expectMsgClass(Acknowledge.class);

			// check for number registration of registered resources
			resourceManager.tell(new TestingResourceManager.GetRegisteredResources(), fakeJobManager);
			TestingResourceManager.GetRegisteredResourcesReply reply =
				expectMsgClass(TestingResourceManager.GetRegisteredResourcesReply.class);

			assertEquals(1, reply.resources.size());

			// Send task manager registration again
			resourceManager.tell(new NotifyResourceStarted(resourceID),
				fakeJobManager);

			expectMsgClass(Acknowledge.class);

			// check for number registration of registered resources
			resourceManager.tell(new TestingResourceManager.GetRegisteredResources(), fakeJobManager);
			reply = expectMsgClass(TestingResourceManager.GetRegisteredResourcesReply.class);

			assertEquals(1, reply.resources.size());

			// Send invalid null resource id to throw an exception during resource registration
			resourceManager.tell(new NotifyResourceStarted(null),
				fakeJobManager);

			expectMsgClass(Acknowledge.class);

			// check for number registration of registered resources
			resourceManager.tell(new TestingResourceManager.GetRegisteredResources(), fakeJobManager);
			reply = expectMsgClass(TestingResourceManager.GetRegisteredResourcesReply.class);

			assertEquals(1, reply.resources.size());
		}};
		}};
	}

	@Test
	public void testResourceRemoval() {
		new JavaTestKit(system){{
		new Within(duration("10 seconds")) {
		@Override
		protected void run() {

			fakeJobManager = TestingUtils.createForwardingActor(system, getTestActor(), Option.<String>empty());
			resourceManager = TestingUtils.createResourceManager(system, fakeJobManager.actor(), config);

			// register with JM
			expectMsgClass(RegisterResourceManager.class);
			resourceManager.tell(
				new RegisterResourceManagerSuccessful(fakeJobManager.actor(), Collections.<ResourceID>emptyList()),
				fakeJobManager);

			ResourceID resourceID = ResourceID.generate();

			// remove unknown resource
			resourceManager.tell(new RemoveResource(resourceID), fakeJobManager);

			// Send task manager registration
			resourceManager.tell(new NotifyResourceStarted(resourceID),
				fakeJobManager);

			expectMsgClass(Acknowledge.class);

			// check for number registration of registered resources
			resourceManager.tell(new TestingResourceManager.GetRegisteredResources(), fakeJobManager);
			TestingResourceManager.GetRegisteredResourcesReply reply =
				expectMsgClass(TestingResourceManager.GetRegisteredResourcesReply.class);

			assertEquals(1, reply.resources.size());
			assertTrue(reply.resources.contains(resourceID));

			// remove resource
			resourceManager.tell(new RemoveResource(resourceID), fakeJobManager);

			// check for number registration of registered resources
			resourceManager.tell(new TestingResourceManager.GetRegisteredResources(), fakeJobManager);
			reply =	expectMsgClass(TestingResourceManager.GetRegisteredResourcesReply.class);

			assertEquals(0, reply.resources.size());

		}};
		}};
	}

	/**
	 * Tests notification of JobManager about a failed resource.
	 */
	@Test
	public void testResourceFailureNotification() {
		new JavaTestKit(system){{
		new Within(duration("10 seconds")) {
		@Override
		protected void run() {

			fakeJobManager = TestingUtils.createForwardingActor(system, getTestActor(), Option.<String>empty());
			resourceManager = TestingUtils.createResourceManager(system, fakeJobManager.actor(), config);

			// register with JM
			expectMsgClass(RegisterResourceManager.class);
			resourceManager.tell(
				new RegisterResourceManagerSuccessful(fakeJobManager.actor(), Collections.<ResourceID>emptyList()),
				fakeJobManager);

			ResourceID resourceID1 = ResourceID.generate();
			ResourceID resourceID2 = ResourceID.generate();

			// Send task manager registration
			resourceManager.tell(new NotifyResourceStarted(resourceID1),
				fakeJobManager);

			expectMsgClass(Acknowledge.class);

			// Send task manager registration
			resourceManager.tell(new NotifyResourceStarted(resourceID2),
				fakeJobManager);

			expectMsgClass(Acknowledge.class);

			// check for number registration of registered resources
			resourceManager.tell(new TestingResourceManager.GetRegisteredResources(), fakeJobManager);
			TestingResourceManager.GetRegisteredResourcesReply reply =
				expectMsgClass(TestingResourceManager.GetRegisteredResourcesReply.class);

			assertEquals(2, reply.resources.size());
			assertTrue(reply.resources.contains(resourceID1));
			assertTrue(reply.resources.contains(resourceID2));

			// fail resources
			resourceManager.tell(new TestingResourceManager.FailResource(resourceID1), fakeJobManager);
			resourceManager.tell(new TestingResourceManager.FailResource(resourceID2), fakeJobManager);

			ResourceRemoved answer = expectMsgClass(ResourceRemoved.class);
			ResourceRemoved answer2 = expectMsgClass(ResourceRemoved.class);

			assertEquals(resourceID1, answer.resourceId());
			assertEquals(resourceID2, answer2.resourceId());

		}};
		}};
	}
}
