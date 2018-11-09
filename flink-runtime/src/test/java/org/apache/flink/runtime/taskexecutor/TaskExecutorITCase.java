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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobCacheService;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.runtime.clusterframework.FlinkResourceManager;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.jobmaster.JMTMRegistrationSuccess;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.JobMasterRegistrationSuccess;
import org.apache.flink.runtime.leaderelection.TestingLeaderElectionService;
import org.apache.flink.runtime.leaderretrieval.SettableLeaderRetrievalService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.NoOpMetricRegistry;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.resourcemanager.JobLeaderIdService;
import org.apache.flink.runtime.resourcemanager.ResourceManager;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.SlotRequest;
import org.apache.flink.runtime.resourcemanager.StandaloneResourceManager;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.state.TaskExecutorLocalStateStoresManager;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskexecutor.slot.TaskSlotTable;
import org.apache.flink.runtime.taskexecutor.slot.TimerService;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.TestLogger;

import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.File;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;

public class TaskExecutorITCase extends TestLogger {

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	private final Time timeout = Time.seconds(10L);

	@Test
	public void testSlotAllocation() throws Exception {
		TestingFatalErrorHandler testingFatalErrorHandler = new TestingFatalErrorHandler();
		TestingHighAvailabilityServices testingHAServices = new TestingHighAvailabilityServices();
		final Configuration configuration = new Configuration();
		final ScheduledExecutorService scheduledExecutorService = new ScheduledThreadPoolExecutor(1);
		final ResourceID taskManagerResourceId = new ResourceID("foobar");
		final UUID rmLeaderId = UUID.randomUUID();
		final TestingLeaderElectionService rmLeaderElectionService = new TestingLeaderElectionService();
		final SettableLeaderRetrievalService rmLeaderRetrievalService = new SettableLeaderRetrievalService(null, null);
		final String rmAddress = "rm";
		final String jmAddress = "jm";
		final JobMasterId jobMasterId = JobMasterId.generate();
		final ResourceID rmResourceId = new ResourceID(rmAddress);
		final ResourceID jmResourceId = new ResourceID(jmAddress);
		final JobID jobId = new JobID();
		final ResourceProfile resourceProfile = new ResourceProfile(1.0, 1);

		testingHAServices.setResourceManagerLeaderElectionService(rmLeaderElectionService);
		testingHAServices.setResourceManagerLeaderRetriever(rmLeaderRetrievalService);
		testingHAServices.setJobMasterLeaderRetriever(jobId, new SettableLeaderRetrievalService(jmAddress, jobMasterId.toUUID()));

		TestingRpcService rpcService = new TestingRpcService();
		JobLeaderIdService jobLeaderIdService = new JobLeaderIdService(
			testingHAServices,
			rpcService.getScheduledExecutor(),
			Time.minutes(5L));
		MetricRegistry metricRegistry = NoOpMetricRegistry.INSTANCE;
		HeartbeatServices heartbeatServices = new HeartbeatServices(1000L, 1000L);

		final TaskManagerConfiguration taskManagerConfiguration = TaskManagerConfiguration.fromConfiguration(configuration);
		final TaskManagerLocation taskManagerLocation = new TaskManagerLocation(taskManagerResourceId, InetAddress.getLocalHost(), 1234);
		final List<ResourceProfile> resourceProfiles = Arrays.asList(resourceProfile);
		final TaskSlotTable taskSlotTable = new TaskSlotTable(resourceProfiles, new TimerService<AllocationID>(scheduledExecutorService, 100L));
		final SlotManager slotManager = new SlotManager(
			rpcService.getScheduledExecutor(),
			TestingUtils.infiniteTime(),
			TestingUtils.infiniteTime(),
			TestingUtils.infiniteTime());

		final File[] taskExecutorLocalStateRootDirs =
			new File[]{ new File(tempFolder.getRoot(),"localRecovery") };

		final TaskExecutorLocalStateStoresManager taskStateManager = new TaskExecutorLocalStateStoresManager(
			false,
			taskExecutorLocalStateRootDirs,
			rpcService.getExecutor());

		ResourceManager<ResourceID> resourceManager = new StandaloneResourceManager(
			rpcService,
			FlinkResourceManager.RESOURCE_MANAGER_NAME,
			rmResourceId,
			testingHAServices,
			heartbeatServices,
			slotManager,
			metricRegistry,
			jobLeaderIdService,
			new ClusterInformation("localhost", 1234),
			testingFatalErrorHandler,
			UnregisteredMetricGroups.createUnregisteredJobManagerMetricGroup());

		final TaskManagerServices taskManagerServices = new TaskManagerServicesBuilder()
			.setTaskManagerLocation(taskManagerLocation)
			.setTaskSlotTable(taskSlotTable)
			.setTaskStateManager(taskStateManager)
			.build();

		TaskExecutor taskExecutor = new TaskExecutor(
			rpcService,
			taskManagerConfiguration,
			testingHAServices,
			taskManagerServices,
			heartbeatServices,
			UnregisteredMetricGroups.createUnregisteredTaskManagerMetricGroup(),
			null,
			new BlobCacheService(
				configuration,
				new VoidBlobStore(),
				null),
			testingFatalErrorHandler);

		JobMasterGateway jmGateway = mock(JobMasterGateway.class);

		when(jmGateway.registerTaskManager(any(String.class), any(TaskManagerLocation.class), any(Time.class)))
			.thenReturn(CompletableFuture.completedFuture(new JMTMRegistrationSuccess(taskManagerResourceId)));
		when(jmGateway.getHostname()).thenReturn(jmAddress);
		when(jmGateway.offerSlots(
			eq(taskManagerResourceId),
			any(Collection.class),
			any(Time.class))).thenReturn(mock(CompletableFuture.class, RETURNS_MOCKS));
		when(jmGateway.getFencingToken()).thenReturn(jobMasterId);


		rpcService.registerGateway(rmAddress, resourceManager.getSelfGateway(ResourceManagerGateway.class));
		rpcService.registerGateway(jmAddress, jmGateway);
		rpcService.registerGateway(taskExecutor.getAddress(), taskExecutor.getSelfGateway(TaskExecutorGateway.class));

		final AllocationID allocationId = new AllocationID();
		final SlotRequest slotRequest = new SlotRequest(jobId, allocationId, resourceProfile, jmAddress);
		final SlotOffer slotOffer = new SlotOffer(allocationId, 0, resourceProfile);

		try {
			resourceManager.start();
			taskExecutor.start();

			final ResourceManagerGateway rmGateway = resourceManager.getSelfGateway(ResourceManagerGateway.class);

			// notify the RM that it is the leader
			CompletableFuture<UUID> isLeaderFuture = rmLeaderElectionService.isLeader(rmLeaderId);

			// wait for the completion of the leader election
			assertEquals(rmLeaderId, isLeaderFuture.get());

			// notify the TM about the new RM leader
			rmLeaderRetrievalService.notifyListener(rmAddress, rmLeaderId);

			CompletableFuture<RegistrationResponse> registrationResponseFuture = rmGateway.registerJobManager(
				jobMasterId,
				jmResourceId,
				jmAddress,
				jobId,
				timeout);

			RegistrationResponse registrationResponse = registrationResponseFuture.get();

			assertTrue(registrationResponse instanceof JobMasterRegistrationSuccess);

			CompletableFuture<Acknowledge> slotAck = rmGateway.requestSlot(jobMasterId, slotRequest, timeout);

			slotAck.get();

			verify(jmGateway, Mockito.timeout(timeout.toMilliseconds())).offerSlots(
				eq(taskManagerResourceId),
				(Collection<SlotOffer>)argThat(Matchers.contains(slotOffer)),
				any(Time.class));
		} finally {
			if (testingFatalErrorHandler.hasExceptionOccurred()) {
				testingFatalErrorHandler.rethrowError();
			}

			RpcUtils.terminateRpcService(rpcService, timeout);
		}


	}
}
