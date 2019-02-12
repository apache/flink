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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.clusterframework.messages.NotifyResourceStarted;
import org.apache.flink.runtime.clusterframework.messages.RegisterResourceManager;
import org.apache.flink.runtime.clusterframework.messages.RegisterResourceManagerSuccessful;
import org.apache.flink.runtime.clusterframework.messages.RemoveResource;
import org.apache.flink.runtime.clusterframework.messages.ResourceRemoved;
import org.apache.flink.runtime.clusterframework.messages.TriggerRegistrationAtJobManager;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.ManuallyTriggeredScheduledExecutor;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.heartbeat.TestingHeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.instance.HardwareDescription;
import org.apache.flink.runtime.jobmaster.JobMasterRegistrationSuccess;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGateway;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGatewayBuilder;
import org.apache.flink.runtime.leaderelection.TestingLeaderElectionService;
import org.apache.flink.runtime.leaderretrieval.SettableLeaderRetrievalService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.metrics.NoOpMetricRegistry;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.resourcemanager.JobLeaderIdService;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.StandaloneResourceManager;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TaskExecutorRegistrationSuccess;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.testutils.TestingResourceManager;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.TriConsumer;

import akka.actor.ActorSystem;
import akka.testkit.JavaTestKit;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import scala.Option;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * General tests for the resource manager component.
 */
public class ResourceManagerTest extends TestLogger {

	private static ActorSystem system;

	private static ActorGateway fakeJobManager;
	private static ActorGateway resourceManager;

	private static Configuration config = new Configuration();

	private final Time timeout = Time.seconds(10L);

	private TestingHighAvailabilityServices highAvailabilityServices;
	private SettableLeaderRetrievalService jobManagerLeaderRetrievalService;

	@BeforeClass
	public static void setup() {
		system = AkkaUtils.createLocalActorSystem(config);
	}

	@AfterClass
	public static void teardown() {
		JavaTestKit.shutdownActorSystem(system);
	}

	@Before
	public void setupTest() {
		jobManagerLeaderRetrievalService = new SettableLeaderRetrievalService();

		highAvailabilityServices = new TestingHighAvailabilityServices();

		highAvailabilityServices.setJobMasterLeaderRetriever(
			HighAvailabilityServices.DEFAULT_JOB_ID,
			jobManagerLeaderRetrievalService);
	}

	@After
	public void teardownTest() throws Exception {
		if (jobManagerLeaderRetrievalService != null) {
			jobManagerLeaderRetrievalService.stop();

			jobManagerLeaderRetrievalService = null;
		}

		if (highAvailabilityServices != null) {
			highAvailabilityServices.closeAndCleanupAllData();

			highAvailabilityServices = null;
		}
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
			fakeJobManager = TestingUtils.createForwardingActor(
				system,
				getTestActor(),
				HighAvailabilityServices.DEFAULT_LEADER_ID,
				Option.<String>empty());

			jobManagerLeaderRetrievalService.notifyListener(
				fakeJobManager.path(),
				HighAvailabilityServices.DEFAULT_LEADER_ID);

			resourceManager = TestingUtils.createResourceManager(
				system,
				config,
				highAvailabilityServices);

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
			shortTimeoutConfig.setString(AkkaOptions.LOOKUP_TIMEOUT, "1 s");

			fakeJobManager = TestingUtils.createForwardingActor(
				system,
				getTestActor(),
				HighAvailabilityServices.DEFAULT_LEADER_ID,
				Option.<String>empty());

			jobManagerLeaderRetrievalService.notifyListener(
				fakeJobManager.path(),
				HighAvailabilityServices.DEFAULT_LEADER_ID);

			resourceManager = TestingUtils.createResourceManager(
				system,
				shortTimeoutConfig,
				highAvailabilityServices);

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
			shortTimeoutConfig.setString(AkkaOptions.LOOKUP_TIMEOUT, "99999 s");

			fakeJobManager = TestingUtils.createForwardingActor(
				system,
				getTestActor(),
				HighAvailabilityServices.DEFAULT_LEADER_ID,
				Option.<String>empty());

			jobManagerLeaderRetrievalService.notifyListener(
				fakeJobManager.path(),
				HighAvailabilityServices.DEFAULT_LEADER_ID);

			resourceManager = TestingUtils.createResourceManager(
				system,
				shortTimeoutConfig,
				highAvailabilityServices);

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

			fakeJobManager = TestingUtils.createForwardingActor(
				system,
				getTestActor(),
				HighAvailabilityServices.DEFAULT_LEADER_ID,
				Option.<String>empty());

			jobManagerLeaderRetrievalService.notifyListener(
				fakeJobManager.path(),
				HighAvailabilityServices.DEFAULT_LEADER_ID);

			resourceManager = TestingUtils.createResourceManager(
				system,
				config,
				highAvailabilityServices);

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

			fakeJobManager = TestingUtils.createForwardingActor(
				system,
				getTestActor(),
				HighAvailabilityServices.DEFAULT_LEADER_ID,
				Option.<String>empty());

			jobManagerLeaderRetrievalService.notifyListener(
				fakeJobManager.path(),
				HighAvailabilityServices.DEFAULT_LEADER_ID);

			resourceManager = TestingUtils.createResourceManager(
				system,
				config,
				highAvailabilityServices);

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

			fakeJobManager = TestingUtils.createForwardingActor(
				system,
				getTestActor(),
				HighAvailabilityServices.DEFAULT_LEADER_ID,
				Option.<String>empty());

			jobManagerLeaderRetrievalService.notifyListener(
				fakeJobManager.path(),
				HighAvailabilityServices.DEFAULT_LEADER_ID);

			resourceManager = TestingUtils.createResourceManager(
				system,
				config,
				highAvailabilityServices);

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

	@Test
	public void testHeartbeatTimeoutWithTaskExecutor() throws Exception {
		final CompletableFuture<ResourceID> heartbeatRequestFuture = new CompletableFuture<>();
		final CompletableFuture<Exception> disconnectResourceManagerFuture = new CompletableFuture<>();
		final TaskExecutorGateway taskExecutorGateway = new TestingTaskExecutorGatewayBuilder()
			.setHeartbeatResourceManagerConsumer(heartbeatRequestFuture::complete)
			.setDisconnectResourceManagerConsumer(disconnectResourceManagerFuture::complete)
			.createTestingTaskExecutorGateway();

		runHeartbeatTimeoutTest(
			(resourceManagerGateway, testingRpcService, testingHighAvailabilityServices) -> {
				testingRpcService.registerGateway(taskExecutorGateway.getAddress(), taskExecutorGateway);

				// test registration response successful and it will trigger monitor heartbeat target, schedule heartbeat request at interval time
				CompletableFuture<RegistrationResponse> successfulFuture = resourceManagerGateway.registerTaskExecutor(
					taskExecutorGateway.getAddress(),
					ResourceID.generate(),
					1234,
					new HardwareDescription(1, 2L, 3L, 4L),
					timeout);
				assertThat(successfulFuture.join(), instanceOf(TaskExecutorRegistrationSuccess.class));
			},
			resourceID -> assertThat(heartbeatRequestFuture.join(), is(resourceID)),
			ignored -> assertThat(disconnectResourceManagerFuture.join(), instanceOf(TimeoutException.class)));
	}

	@Test
	public void testHeartbeatTimeoutWithJobManager() throws Exception {
		final JobID jobId = new JobID();
		final CompletableFuture<ResourceID> heartbeatRequestFuture = new CompletableFuture<>();
		final CompletableFuture<ResourceManagerId> disconnectResourceManagerFuture = new CompletableFuture<>();
		final TestingJobMasterGateway jobMasterGateway = new TestingJobMasterGatewayBuilder()
			.setResourceManagerHeartbeatConsumer(heartbeatRequestFuture::complete)
			.setDisconnectResourceManagerConsumer(disconnectResourceManagerFuture::complete)
			.build();

		runHeartbeatTimeoutTest(
			(resourceManagerGateway, testingRpcService, testingHighAvailabilityServices) -> {
				testingRpcService.registerGateway(jobMasterGateway.getAddress(), jobMasterGateway);
				final SettableLeaderRetrievalService jmLeaderRetrievalService = new SettableLeaderRetrievalService(jobMasterGateway.getAddress(), jobMasterGateway.getFencingToken().toUUID());
				testingHighAvailabilityServices.setJobMasterLeaderRetriever(jobId, jmLeaderRetrievalService);

				// test registration response successful and it will trigger monitor heartbeat target, schedule heartbeat request at interval time
				CompletableFuture<RegistrationResponse> successfulFuture = resourceManagerGateway.registerJobManager(
					jobMasterGateway.getFencingToken(),
					ResourceID.generate(),
					jobMasterGateway.getAddress(),
					jobId,
					timeout);
				assertThat(successfulFuture.join(), instanceOf(JobMasterRegistrationSuccess.class));
			},
			resourceID -> assertThat(heartbeatRequestFuture.join(), is(resourceID)),
			resourceManagerId -> assertThat(disconnectResourceManagerFuture.join(), is(resourceManagerId)));
	}

	private void runHeartbeatTimeoutTest(
			TriConsumer<ResourceManagerGateway, TestingRpcService, TestingHighAvailabilityServices> registerComponent,
			Consumer<ResourceID> verifyHeartbeat,
			Consumer<ResourceManagerId> verifyDisconnect) throws Exception {
		final ResourceID rmResourceId = ResourceID.generate();
		final ResourceManagerId rmLeaderId = ResourceManagerId.generate();

		final TestingRpcService rpcService = new TestingRpcService();

		final TestingLeaderElectionService rmLeaderElectionService = new TestingLeaderElectionService();
		final TestingHighAvailabilityServices highAvailabilityServices = new TestingHighAvailabilityServices();
		highAvailabilityServices.setResourceManagerLeaderElectionService(rmLeaderElectionService);

		final long heartbeatInterval = 1L;
		final long heartbeatTimeout = 5L;
		final ManuallyTriggeredScheduledExecutor scheduledExecutor = new ManuallyTriggeredScheduledExecutor();
		final HeartbeatServices heartbeatServices = new TestingHeartbeatServices(heartbeatInterval, heartbeatTimeout, scheduledExecutor);

		final JobLeaderIdService jobLeaderIdService = new JobLeaderIdService(
			highAvailabilityServices,
			rpcService.getScheduledExecutor(),
			Time.minutes(5L));
		final TestingFatalErrorHandler testingFatalErrorHandler = new TestingFatalErrorHandler();
		final SlotManager slotManager = new SlotManager(
			TestingUtils.defaultScheduledExecutor(),
			TestingUtils.infiniteTime(),
			TestingUtils.infiniteTime(),
			TestingUtils.infiniteTime());

		try {
			final StandaloneResourceManager resourceManager = new StandaloneResourceManager(
				rpcService,
				FlinkResourceManager.RESOURCE_MANAGER_NAME,
				rmResourceId,
				highAvailabilityServices,
				heartbeatServices,
				slotManager,
				NoOpMetricRegistry.INSTANCE,
				jobLeaderIdService,
				new ClusterInformation("localhost", 1234),
				testingFatalErrorHandler,
				UnregisteredMetricGroups.createUnregisteredJobManagerMetricGroup());

			resourceManager.start();

			final ResourceManagerGateway rmGateway = resourceManager.getSelfGateway(ResourceManagerGateway.class);

			rmLeaderElectionService.isLeader(rmLeaderId.toUUID()).get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);

			// test registration response successful and it will trigger monitor heartbeat target, schedule heartbeat request at interval time
			registerComponent.accept(rmGateway, rpcService, highAvailabilityServices);

			assertThat(scheduledExecutor.getScheduledTasks(), hasSize(3));

			// run all the heartbeat requests for TMs and JMs
			scheduledExecutor.triggerScheduledTasks();
			scheduledExecutor.triggerScheduledTasks();
			verifyHeartbeat.accept(rmResourceId);

			// run the timeout runnable to simulate a heartbeat timeout
			scheduledExecutor.triggerScheduledTasks();
			verifyDisconnect.accept(rmLeaderId);
		} finally {
			RpcUtils.terminateRpcService(rpcService, timeout);
		}
	}
}
