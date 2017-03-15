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
import org.apache.flink.runtime.broadcast.BroadcastVariableManager;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.Future;
import org.apache.flink.runtime.concurrent.impl.FlinkCompletableFuture;
import org.apache.flink.runtime.filecache.FileCache;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.NetworkEnvironment;
import org.apache.flink.runtime.jobmaster.JMTMRegistrationSuccess;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.jobmaster.JobMasterRegistrationSuccess;
import org.apache.flink.runtime.leaderelection.TestingLeaderElectionService;
import org.apache.flink.runtime.leaderelection.TestingLeaderRetrievalService;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.resourcemanager.JobLeaderIdService;
import org.apache.flink.runtime.resourcemanager.ResourceManager;
import org.apache.flink.runtime.resourcemanager.ResourceManagerConfiguration;
import org.apache.flink.runtime.resourcemanager.SlotRequest;
import org.apache.flink.runtime.resourcemanager.StandaloneResourceManager;
import org.apache.flink.runtime.resourcemanager.slotmanager.DefaultSlotManager;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManagerFactory;
import org.apache.flink.runtime.rpc.TestingSerialRpcService;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskexecutor.slot.TaskSlotTable;
import org.apache.flink.runtime.taskexecutor.slot.TimerService;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TaskExecutorITCase {

	@Test
	public void testSlotAllocation() throws Exception {
		TestingFatalErrorHandler testingFatalErrorHandler = new TestingFatalErrorHandler();
		TestingHighAvailabilityServices testingHAServices = new TestingHighAvailabilityServices();
		final Configuration configuration = new Configuration();
		final ScheduledExecutorService scheduledExecutorService = new ScheduledThreadPoolExecutor(1);
		final ResourceID taskManagerResourceId = new ResourceID("foobar");
		final UUID rmLeaderId = UUID.randomUUID();
		final TestingLeaderElectionService rmLeaderElectionService = new TestingLeaderElectionService();
		final TestingLeaderRetrievalService rmLeaderRetrievalService = new TestingLeaderRetrievalService();
		final String rmAddress = "rm";
		final String jmAddress = "jm";
		final UUID jmLeaderId = UUID.randomUUID();
		final JobID jobId = new JobID();
		final ResourceProfile resourceProfile = new ResourceProfile(1.0, 1);

		testingHAServices.setResourceManagerLeaderElectionService(rmLeaderElectionService);
		testingHAServices.setResourceManagerLeaderRetriever(rmLeaderRetrievalService);
		testingHAServices.setJobMasterLeaderRetriever(jobId, new TestingLeaderRetrievalService(jmAddress, jmLeaderId));

		TestingSerialRpcService rpcService = new TestingSerialRpcService();
		ResourceManagerConfiguration resourceManagerConfiguration = new ResourceManagerConfiguration(
			Time.milliseconds(500L),
			Time.milliseconds(500L),
			Time.minutes(5L));
		SlotManagerFactory slotManagerFactory = new DefaultSlotManager.Factory();
		JobLeaderIdService jobLeaderIdService = new JobLeaderIdService(
			testingHAServices,
			rpcService.getScheduledExecutor(),
			resourceManagerConfiguration.getJobTimeout());
		MetricRegistry metricRegistry = mock(MetricRegistry.class);

		final TaskManagerConfiguration taskManagerConfiguration = TaskManagerConfiguration.fromConfiguration(configuration);
		final TaskManagerLocation taskManagerLocation = new TaskManagerLocation(taskManagerResourceId, InetAddress.getLocalHost(), 1234);
		final MemoryManager memoryManager = mock(MemoryManager.class);
		final IOManager ioManager = mock(IOManager.class);
		final NetworkEnvironment networkEnvironment = mock(NetworkEnvironment.class);
		final TaskManagerMetricGroup taskManagerMetricGroup = mock(TaskManagerMetricGroup.class);
		final BroadcastVariableManager broadcastVariableManager = mock(BroadcastVariableManager.class);
		final FileCache fileCache = mock(FileCache.class);
		final TaskSlotTable taskSlotTable = new TaskSlotTable(Arrays.asList(resourceProfile), new TimerService<AllocationID>(scheduledExecutorService, 100L));
		final JobManagerTable jobManagerTable = new JobManagerTable();
		final JobLeaderService jobLeaderService = new JobLeaderService(taskManagerLocation);

		ResourceManager<ResourceID> resourceManager = new StandaloneResourceManager(
			rpcService,
			resourceManagerConfiguration,
			testingHAServices,
			slotManagerFactory,
			metricRegistry,
			jobLeaderIdService,
			testingFatalErrorHandler);

		TaskExecutor taskExecutor = new TaskExecutor(
			taskManagerConfiguration,
			taskManagerLocation,
			rpcService,
			memoryManager,
			ioManager,
			networkEnvironment,
			testingHAServices,
			metricRegistry,
			taskManagerMetricGroup,
			broadcastVariableManager,
			fileCache,
			taskSlotTable,
			jobManagerTable,
			jobLeaderService,
			testingFatalErrorHandler);

		JobMasterGateway jmGateway = mock(JobMasterGateway.class);

		when(jmGateway.registerTaskManager(any(String.class), any(TaskManagerLocation.class), eq(jmLeaderId), any(Time.class)))
			.thenReturn(FlinkCompletableFuture.<RegistrationResponse>completed(new JMTMRegistrationSuccess(taskManagerResourceId, 1234)));
		when(jmGateway.getAddress()).thenReturn(jmAddress);


		rpcService.registerGateway(rmAddress, resourceManager.getSelf());
		rpcService.registerGateway(jmAddress, jmGateway);

		final AllocationID allocationId = new AllocationID();
		final SlotRequest slotRequest = new SlotRequest(jobId, allocationId, resourceProfile);
		final SlotOffer slotOffer = new SlotOffer(allocationId, 0, resourceProfile);

		try {
			resourceManager.start();
			taskExecutor.start();

			// notify the RM that it is the leader
			rmLeaderElectionService.isLeader(rmLeaderId);

			// notify the TM about the new RM leader
			rmLeaderRetrievalService.notifyListener(rmAddress, rmLeaderId);

			Future<RegistrationResponse> registrationResponseFuture = resourceManager.registerJobManager(rmLeaderId, jmLeaderId, jmAddress, jobId);

			RegistrationResponse registrationResponse = registrationResponseFuture.get();

			assertTrue(registrationResponse instanceof JobMasterRegistrationSuccess);

			resourceManager.requestSlot(jmLeaderId, rmLeaderId, slotRequest);

			verify(jmGateway).offerSlots(
				eq(taskManagerResourceId),
				(Iterable<SlotOffer>)argThat(Matchers.contains(slotOffer)),
				eq(jmLeaderId), any(Time.class));
		} finally {
			if (testingFatalErrorHandler.hasExceptionOccurred()) {
				testingFatalErrorHandler.rethrowError();
			}
		}


	}
}
