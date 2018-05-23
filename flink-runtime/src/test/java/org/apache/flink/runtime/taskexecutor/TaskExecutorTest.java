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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.blob.BlobCacheService;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.JobInformation;
import org.apache.flink.runtime.executiongraph.TaskInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatListener;
import org.apache.flink.runtime.heartbeat.HeartbeatManager;
import org.apache.flink.runtime.heartbeat.HeartbeatManagerImpl;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.heartbeat.HeartbeatTarget;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.highavailability.nonha.standalone.StandaloneHaServices;
import org.apache.flink.runtime.instance.HardwareDescription;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.io.network.NetworkEnvironment;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.netty.PartitionProducerStateChecker;
import org.apache.flink.runtime.io.network.partition.ResultPartitionConsumableNotifier;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobmaster.JMTMRegistrationSuccess;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGateway;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.leaderretrieval.SettableLeaderRetrievalService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.state.TaskExecutorLocalStateStoresManager;
import org.apache.flink.runtime.taskexecutor.exceptions.RegistrationTimeoutException;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskexecutor.slot.TaskSlotTable;
import org.apache.flink.runtime.taskexecutor.slot.TimerService;
import org.apache.flink.runtime.taskmanager.CheckpointResponder;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.taskmanager.TaskManagerActions;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TaskExecutorTest extends TestLogger {

	@Rule
	public final TemporaryFolder tmp = new TemporaryFolder();

	private static final Time timeout = Time.milliseconds(10000L);

	private TestingRpcService rpc;

	private BlobCacheService dummyBlobCacheService;

	private TimerService<AllocationID> timerService;

	private Configuration configuration;

	private TaskManagerConfiguration taskManagerConfiguration;

	private TaskManagerLocation taskManagerLocation;

	private JobID jobId;

	private TestingFatalErrorHandler testingFatalErrorHandler;

	private TestingHighAvailabilityServices haServices;

	private SettableLeaderRetrievalService resourceManagerLeaderRetriever;

	private SettableLeaderRetrievalService jobManagerLeaderRetriever;

	@Before
	public void setup() throws IOException {
		rpc = new TestingRpcService();
		timerService = new TimerService<>(TestingUtils.defaultExecutor(), timeout.toMilliseconds());

		dummyBlobCacheService = new BlobCacheService(
			new Configuration(),
			new VoidBlobStore(),
			null);

		configuration = new Configuration();
		taskManagerConfiguration = TaskManagerConfiguration.fromConfiguration(configuration);

		taskManagerLocation = new LocalTaskManagerLocation();
		jobId = new JobID();

		testingFatalErrorHandler = new TestingFatalErrorHandler();

		haServices = new TestingHighAvailabilityServices();
		resourceManagerLeaderRetriever = new SettableLeaderRetrievalService();
		jobManagerLeaderRetriever = new SettableLeaderRetrievalService();
		haServices.setResourceManagerLeaderRetriever(resourceManagerLeaderRetriever);
		haServices.setJobMasterLeaderRetriever(jobId, jobManagerLeaderRetriever);
	}

	@After
	public void teardown() throws Exception {
		if (rpc != null) {
			RpcUtils.terminateRpcService(rpc, timeout);
			rpc = null;
		}

		if (timerService != null) {
			timerService.stop();
			timerService = null;
		}

		if (dummyBlobCacheService != null) {
			dummyBlobCacheService.close();
			dummyBlobCacheService = null;
		}

		testingFatalErrorHandler.rethrowError();
	}

	@Rule
	public TestName name = new TestName();

	@Test
	public void testHeartbeatTimeoutWithJobManager() throws Exception {
		final TaskSlotTable taskSlotTable = new TaskSlotTable(Arrays.asList(ResourceProfile.UNKNOWN), timerService);

		final JobLeaderService jobLeaderService = new JobLeaderService(taskManagerLocation);

		final long heartbeatInterval = 1L;
		final long heartbeatTimeout = 3L;

		HeartbeatServices heartbeatServices = new HeartbeatServices(heartbeatInterval, heartbeatTimeout);

		final String jobMasterAddress = "jm";
		final UUID jmLeaderId = UUID.randomUUID();

		final ResourceID jmResourceId = ResourceID.generate();
		final SimpleJobMasterGateway jobMasterGateway = new SimpleJobMasterGateway(
			CompletableFuture.completedFuture(new JMTMRegistrationSuccess(jmResourceId)));

		TaskExecutorLocalStateStoresManager localStateStoresManager = new TaskExecutorLocalStateStoresManager(
			false,
			new File[]{tmp.newFolder()},
			Executors.directExecutor());

		final TaskManagerServices taskManagerServices = new TaskManagerServicesBuilder()
			.setTaskManagerLocation(taskManagerLocation)
			.setTaskSlotTable(taskSlotTable)
			.setJobLeaderService(jobLeaderService)
			.setTaskStateManager(localStateStoresManager)
			.build();

		final TaskExecutor taskManager = new TaskExecutor(
			rpc,
			taskManagerConfiguration,
			haServices,
			taskManagerServices,
			heartbeatServices,
			UnregisteredMetricGroups.createUnregisteredTaskManagerMetricGroup(),
			dummyBlobCacheService,
			testingFatalErrorHandler);

		try {
			taskManager.start();

			rpc.registerGateway(jobMasterAddress, jobMasterGateway);

			// we have to add the job after the TaskExecutor, because otherwise the service has not
			// been properly started.
			jobLeaderService.addJob(jobId, jobMasterAddress);

			// now inform the task manager about the new job leader
			jobManagerLeaderRetriever.notifyListener(jobMasterAddress, jmLeaderId);

			// register task manager success will trigger monitoring heartbeat target between tm and jm
			final TaskManagerLocation taskManagerLocation1 = jobMasterGateway.getRegisterTaskManagerFuture().get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);
			assertThat(taskManagerLocation1, equalTo(taskManagerLocation));

			// the timeout should trigger disconnecting from the JobManager
			final ResourceID resourceID = jobMasterGateway.getDisconnectTaskManagerFuture().get(heartbeatTimeout * 50L, TimeUnit.MILLISECONDS);
			assertThat(resourceID, equalTo(taskManagerLocation.getResourceID()));

		} finally {
			RpcUtils.terminateRpcEndpoint(taskManager, timeout);
		}
	}

	@Test
	public void testHeartbeatTimeoutWithResourceManager() throws Exception {
		final String rmAddress = "rm";
		final ResourceID rmResourceId = new ResourceID(rmAddress);

		final long heartbeatInterval = 1L;
		final long heartbeatTimeout = 3L;

		final ResourceManagerId rmLeaderId = ResourceManagerId.generate();

		TestingResourceManagerGateway rmGateway = new TestingResourceManagerGateway(
			rmLeaderId,
			rmResourceId,
			heartbeatInterval,
			rmAddress,
			rmAddress);

		final TaskExecutorRegistrationSuccess registrationResponse = new TaskExecutorRegistrationSuccess(
			new InstanceID(),
			rmResourceId,
			heartbeatInterval,
			new ClusterInformation("localhost", 1234));

		final CompletableFuture<ResourceID> taskExecutorRegistrationFuture = new CompletableFuture<>();
		final CountDownLatch registrationAttempts = new CountDownLatch(2);
		rmGateway.setRegisterTaskExecutorFunction(
			registration -> {
				taskExecutorRegistrationFuture.complete(registration.f1);
				registrationAttempts.countDown();
				return CompletableFuture.completedFuture(registrationResponse);
			});

		final CompletableFuture<ResourceID> taskExecutorDisconnectFuture = new CompletableFuture<>();
		rmGateway.setDisconnectTaskExecutorConsumer(
			disconnectInfo -> taskExecutorDisconnectFuture.complete(disconnectInfo.f0));

		rpc.registerGateway(rmAddress, rmGateway);

		final TaskSlotTable taskSlotTable = mock(TaskSlotTable.class);
		final SlotReport slotReport = new SlotReport();
		when(taskSlotTable.createSlotReport(any(ResourceID.class))).thenReturn(slotReport);

		HeartbeatServices heartbeatServices = new HeartbeatServices(heartbeatInterval, heartbeatTimeout);

		TaskExecutorLocalStateStoresManager localStateStoresManager = new TaskExecutorLocalStateStoresManager(
			false,
			new File[]{tmp.newFolder()},
			Executors.directExecutor());

		final TaskManagerServices taskManagerServices = new TaskManagerServicesBuilder()
			.setTaskManagerLocation(taskManagerLocation)
			.setTaskSlotTable(taskSlotTable)
			.setTaskStateManager(localStateStoresManager)
			.build();

		final TaskExecutor taskManager = new TaskExecutor(
			rpc,
			taskManagerConfiguration,
			haServices,
			taskManagerServices,
			heartbeatServices,
			UnregisteredMetricGroups.createUnregisteredTaskManagerMetricGroup(),
			dummyBlobCacheService,
			testingFatalErrorHandler);

		try {
			taskManager.start();

			// define a leader and see that a registration happens
			resourceManagerLeaderRetriever.notifyListener(rmAddress, rmLeaderId.toUUID());

			// register resource manager success will trigger monitoring heartbeat target between tm and rm
			assertThat(taskExecutorRegistrationFuture.get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS), equalTo(taskManagerLocation.getResourceID()));

			// heartbeat timeout should trigger disconnect TaskManager from ResourceManager
			assertThat(taskExecutorDisconnectFuture.get(heartbeatTimeout * 50L, TimeUnit.MILLISECONDS), equalTo(taskManagerLocation.getResourceID()));

			// the TaskExecutor should try to reconnect to the RM
			registrationAttempts.await();

		} finally {
			RpcUtils.terminateRpcEndpoint(taskManager, timeout);
		}
	}

	private static final class SimpleJobMasterGateway extends TestingJobMasterGateway {

		private final CompletableFuture<TaskManagerLocation> registerTaskManagerFuture = new CompletableFuture<>();

		private final CompletableFuture<ResourceID> disconnectTaskManagerFuture = new CompletableFuture<>();

		private final CompletableFuture<RegistrationResponse> registerTaskManagerResponseFuture;

		private SimpleJobMasterGateway(CompletableFuture<RegistrationResponse> registerTaskManagerResponseFuture) {
			this.registerTaskManagerResponseFuture = registerTaskManagerResponseFuture;
		}

		@Override
		public CompletableFuture<RegistrationResponse> registerTaskManager(String taskManagerRpcAddress, TaskManagerLocation taskManagerLocation, Time timeout) {
			registerTaskManagerFuture.complete(taskManagerLocation);

			return registerTaskManagerResponseFuture;
		}

		@Override
		public CompletableFuture<Acknowledge> disconnectTaskManager(ResourceID resourceID, Exception cause) {
			disconnectTaskManagerFuture.complete(resourceID);

			return CompletableFuture.completedFuture(Acknowledge.get());
		}

		CompletableFuture<TaskManagerLocation> getRegisterTaskManagerFuture() {
			return registerTaskManagerFuture;
		}

		CompletableFuture<ResourceID> getDisconnectTaskManagerFuture() {
			return disconnectTaskManagerFuture;
		}
	}

	/**
	 * Tests that the correct slot report is sent as part of the heartbeat response.
	 */
	@Test
	public void testHeartbeatSlotReporting() throws Exception {
		final long verificationTimeout = 1000L;
		final long heartbeatTimeout = 10000L;
		final String rmAddress = "rm";
		final ResourceID rmResourceId = new ResourceID(rmAddress);
		final UUID rmLeaderId = UUID.randomUUID();

		// register the mock resource manager gateway
		ResourceManagerGateway rmGateway = mock(ResourceManagerGateway.class);
		when(rmGateway.registerTaskExecutor(
			anyString(), any(ResourceID.class), any(SlotReport.class), anyInt(), any(HardwareDescription.class), any(Time.class)))
			.thenReturn(
				CompletableFuture.completedFuture(
					new TaskExecutorRegistrationSuccess(
						new InstanceID(),
						rmResourceId,
						10L,
						new ClusterInformation("localhost", 1234))));

		rpc.registerGateway(rmAddress, rmGateway);

		final TaskSlotTable taskSlotTable = mock(TaskSlotTable.class);
		final SlotID slotId = new SlotID(taskManagerLocation.getResourceID(), 0);
		final ResourceProfile resourceProfile = new ResourceProfile(1.0, 1);
		final SlotReport slotReport1 = new SlotReport(
			new SlotStatus(
				slotId,
				resourceProfile));
		final SlotReport slotReport2 = new SlotReport(
			new SlotStatus(
				slotId,
				resourceProfile,
				new JobID(),
				new AllocationID()));

		when(taskSlotTable.createSlotReport(any(ResourceID.class))).thenReturn(slotReport1, slotReport2);

		final HeartbeatServices heartbeatServices = mock(HeartbeatServices.class);

		when(heartbeatServices.createHeartbeatManager(
			eq(taskManagerLocation.getResourceID()),
			any(HeartbeatListener.class),
			any(ScheduledExecutor.class),
			any(Logger.class))).thenAnswer(
			new Answer<HeartbeatManagerImpl<SlotReport, Void>>() {
				@Override
				public HeartbeatManagerImpl<SlotReport, Void> answer(InvocationOnMock invocation) throws Throwable {
					return spy(new HeartbeatManagerImpl<>(
						heartbeatTimeout,
						taskManagerLocation.getResourceID(),
						(HeartbeatListener<SlotReport, Void>)invocation.getArguments()[1],
						(Executor)invocation.getArguments()[2],
						(ScheduledExecutor)invocation.getArguments()[2],
						(Logger)invocation.getArguments()[3]));
				}
			}
		);

		TaskExecutorLocalStateStoresManager localStateStoresManager = new TaskExecutorLocalStateStoresManager(
			false,
			new File[]{tmp.newFolder()},
			Executors.directExecutor());

		final TaskManagerServices taskManagerServices = new TaskManagerServicesBuilder()
			.setTaskManagerLocation(taskManagerLocation)
			.setTaskSlotTable(taskSlotTable)
			.setTaskStateManager(localStateStoresManager)
			.build();

		final TaskExecutor taskManager = new TaskExecutor(
			rpc,
			taskManagerConfiguration,
			haServices,
			taskManagerServices,
			heartbeatServices,
			UnregisteredMetricGroups.createUnregisteredTaskManagerMetricGroup(),
			dummyBlobCacheService,
			testingFatalErrorHandler);

		try {
			taskManager.start();

			// wait for spied heartbeat manager instance
			HeartbeatManager<Void, SlotReport> heartbeatManager = taskManager.getResourceManagerHeartbeatManager();

			// define a leader and see that a registration happens
			resourceManagerLeaderRetriever.notifyListener(rmAddress, rmLeaderId);

			// register resource manager success will trigger monitoring heartbeat target between tm and rm
			verify(rmGateway, timeout(verificationTimeout).atLeast(1)).registerTaskExecutor(
				eq(taskManager.getAddress()), eq(taskManagerLocation.getResourceID()), eq(slotReport1), anyInt(), any(HardwareDescription.class), any(Time.class));

			verify(heartbeatManager, timeout(verificationTimeout)).monitorTarget(any(ResourceID.class), any(HeartbeatTarget.class));

			TaskExecutorGateway taskExecutorGateway = taskManager.getSelfGateway(TaskExecutorGateway.class);

			// trigger the heartbeat asynchronously
			taskExecutorGateway.heartbeatFromResourceManager(rmResourceId);

			ArgumentCaptor<SlotReport> slotReportArgumentCaptor = ArgumentCaptor.forClass(SlotReport.class);

			// wait for heartbeat response
			verify(rmGateway, timeout(verificationTimeout)).heartbeatFromTaskManager(
				eq(taskManagerLocation.getResourceID()),
				slotReportArgumentCaptor.capture());

			SlotReport actualSlotReport = slotReportArgumentCaptor.getValue();

			// the new slot report should be reported
			assertEquals(slotReport2, actualSlotReport);
		} finally {
			taskManager.shutDown();
			taskManager.getTerminationFuture().get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);
		}
	}

	@Test
	public void testImmediatelyRegistersIfLeaderIsKnown() throws Exception {
		final String resourceManagerAddress = "/resource/manager/address/one";
		final ResourceID resourceManagerResourceId = new ResourceID(resourceManagerAddress);
		final String dispatcherAddress = "localhost";
		final String jobManagerAddress = "localhost";
		final String webMonitorAddress = "localhost";

		// register a mock resource manager gateway
		ResourceManagerGateway rmGateway = mock(ResourceManagerGateway.class);
		when(rmGateway.registerTaskExecutor(
					anyString(), any(ResourceID.class), any(SlotReport.class), anyInt(), any(HardwareDescription.class), any(Time.class)))
			.thenReturn(CompletableFuture.completedFuture(new TaskExecutorRegistrationSuccess(
				new InstanceID(), resourceManagerResourceId, 10L, new ClusterInformation("localhost", 1234))));

		rpc.registerGateway(resourceManagerAddress, rmGateway);

		StandaloneHaServices haServices = new StandaloneHaServices(
			resourceManagerAddress,
			dispatcherAddress,
			jobManagerAddress,
			webMonitorAddress);

		final TaskSlotTable taskSlotTable = mock(TaskSlotTable.class);
		final SlotReport slotReport = new SlotReport();
		when(taskSlotTable.createSlotReport(any(ResourceID.class))).thenReturn(slotReport);

		TaskExecutorLocalStateStoresManager localStateStoresManager = new TaskExecutorLocalStateStoresManager(
			false,
			new File[]{tmp.newFolder()},
			Executors.directExecutor());

		final TaskManagerServices taskManagerServices = new TaskManagerServicesBuilder()
			.setTaskManagerLocation(taskManagerLocation)
			.setTaskSlotTable(taskSlotTable)
			.setTaskStateManager(localStateStoresManager)
			.build();

		TaskExecutor taskManager = new TaskExecutor(
			rpc,
			taskManagerConfiguration,
			haServices,
			taskManagerServices,
			new HeartbeatServices(1000L, 1000L),
			UnregisteredMetricGroups.createUnregisteredTaskManagerMetricGroup(),
			dummyBlobCacheService,
			testingFatalErrorHandler);

		try {
			taskManager.start();
			String taskManagerAddress = taskManager.getAddress();

			verify(rmGateway, Mockito.timeout(timeout.toMilliseconds())).registerTaskExecutor(
					eq(taskManagerAddress), eq(taskManagerLocation.getResourceID()), eq(slotReport), anyInt(), any(HardwareDescription.class), any(Time.class));
		}
		finally {
			taskManager.shutDown();
			taskManager.getTerminationFuture().get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);
		}
	}

	@Test
	public void testTriggerRegistrationOnLeaderChange() throws Exception {
		final String address1 = "/resource/manager/address/one";
		final String address2 = "/resource/manager/address/two";
		final UUID leaderId1 = UUID.randomUUID();
		final UUID leaderId2 = UUID.randomUUID();
		final ResourceID rmResourceId1 = new ResourceID(address1);
		final ResourceID rmResourceId2 = new ResourceID(address2);

		// register the mock resource manager gateways
		ResourceManagerGateway rmGateway1 = mock(ResourceManagerGateway.class);
		ResourceManagerGateway rmGateway2 = mock(ResourceManagerGateway.class);

		when(rmGateway1.registerTaskExecutor(
					anyString(), any(ResourceID.class), any(SlotReport.class), anyInt(), any(HardwareDescription.class), any(Time.class)))
			.thenReturn(CompletableFuture.completedFuture(
				new TaskExecutorRegistrationSuccess(new InstanceID(), rmResourceId1, 10L, new ClusterInformation("localhost", 1234))));
		when(rmGateway2.registerTaskExecutor(
					anyString(), any(ResourceID.class), any(SlotReport.class), anyInt(), any(HardwareDescription.class), any(Time.class)))
			.thenReturn(CompletableFuture.completedFuture(
				new TaskExecutorRegistrationSuccess(new InstanceID(), rmResourceId2, 10L, new ClusterInformation("localhost", 1234))));

		rpc.registerGateway(address1, rmGateway1);
		rpc.registerGateway(address2, rmGateway2);

		final TaskSlotTable taskSlotTable = mock(TaskSlotTable.class);
		final SlotReport slotReport = new SlotReport();
		when(taskSlotTable.createSlotReport(any(ResourceID.class))).thenReturn(slotReport);

		TaskExecutorLocalStateStoresManager localStateStoresManager = new TaskExecutorLocalStateStoresManager(
			false,
			new File[]{tmp.newFolder()},
			Executors.directExecutor());

		final TaskManagerServices taskManagerServices = new TaskManagerServicesBuilder()
			.setTaskManagerLocation(taskManagerLocation)
			.setTaskSlotTable(taskSlotTable)
			.setTaskStateManager(localStateStoresManager)
			.build();

		TaskExecutor taskManager = new TaskExecutor(
			rpc,
			taskManagerConfiguration,
			haServices,
			taskManagerServices,
			new HeartbeatServices(1000L, 1000L),
			UnregisteredMetricGroups.createUnregisteredTaskManagerMetricGroup(),
			dummyBlobCacheService,
			testingFatalErrorHandler);

		try {
			taskManager.start();
			String taskManagerAddress = taskManager.getAddress();

			// no connection initially, since there is no leader
			assertNull(taskManager.getResourceManagerConnection());

			// define a leader and see that a registration happens
			resourceManagerLeaderRetriever.notifyListener(address1, leaderId1);

			verify(rmGateway1, Mockito.timeout(timeout.toMilliseconds())).registerTaskExecutor(
					eq(taskManagerAddress), eq(taskManagerLocation.getResourceID()), any(SlotReport.class), anyInt(), any(HardwareDescription.class), any(Time.class));
			assertNotNull(taskManager.getResourceManagerConnection());

			// cancel the leader 
			resourceManagerLeaderRetriever.notifyListener(null, null);

			// set a new leader, see that a registration happens 
			resourceManagerLeaderRetriever.notifyListener(address2, leaderId2);

			verify(rmGateway2, Mockito.timeout(timeout.toMilliseconds())).registerTaskExecutor(
					eq(taskManagerAddress), eq(taskManagerLocation.getResourceID()), eq(slotReport), anyInt(), any(HardwareDescription.class), any(Time.class));
			assertNotNull(taskManager.getResourceManagerConnection());
		}
		finally {
			taskManager.shutDown();
			taskManager.getTerminationFuture().get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);
		}
	}

	/**
	 * Tests that we can submit a task to the TaskManager given that we've allocated a slot there.
	 */
	@Test(timeout = 10000L)
	public void testTaskSubmission() throws Exception {
		final AllocationID allocationId = new AllocationID();
		final JobMasterId jobMasterId = JobMasterId.generate();
		final JobVertexID jobVertexId = new JobVertexID();

		JobInformation jobInformation = new JobInformation(
				jobId,
				name.getMethodName(),
				new SerializedValue<>(new ExecutionConfig()),
				new Configuration(),
				Collections.emptyList(),
				Collections.emptyList());

		TaskInformation taskInformation = new TaskInformation(
				jobVertexId,
				"test task",
				1,
				1,
				TestInvokable.class.getName(),
				new Configuration());

		SerializedValue<JobInformation> serializedJobInformation = new SerializedValue<>(jobInformation);
		SerializedValue<TaskInformation> serializedJobVertexInformation = new SerializedValue<>(taskInformation);

		final TaskDeploymentDescriptor tdd = new TaskDeploymentDescriptor(
				jobId,
				new TaskDeploymentDescriptor.NonOffloaded<>(serializedJobInformation),
				new TaskDeploymentDescriptor.NonOffloaded<>(serializedJobVertexInformation),
				new ExecutionAttemptID(),
				allocationId,
				0,
				0,
				0,
				null,
				Collections.emptyList(),
				Collections.emptyList());

		final LibraryCacheManager libraryCacheManager = mock(LibraryCacheManager.class);
		when(libraryCacheManager.getClassLoader(any(JobID.class))).thenReturn(ClassLoader.getSystemClassLoader());

		final JobMasterGateway jobMasterGateway = mock(JobMasterGateway.class);
		when(jobMasterGateway.getFencingToken()).thenReturn(jobMasterId);

		final JobManagerConnection jobManagerConnection = new JobManagerConnection(
			jobId,
			ResourceID.generate(),
			jobMasterGateway,
			mock(TaskManagerActions.class),
			mock(CheckpointResponder.class),
			libraryCacheManager,
			mock(ResultPartitionConsumableNotifier.class),
			mock(PartitionProducerStateChecker.class));

		final JobManagerTable jobManagerTable = new JobManagerTable();
		jobManagerTable.put(jobId, jobManagerConnection);

		final TaskSlotTable taskSlotTable = mock(TaskSlotTable.class);
		when(taskSlotTable.existsActiveSlot(eq(jobId), eq(allocationId))).thenReturn(true);
		when(taskSlotTable.addTask(any(Task.class))).thenReturn(true);

		TaskEventDispatcher taskEventDispatcher = new TaskEventDispatcher();
		final NetworkEnvironment networkEnvironment = mock(NetworkEnvironment.class);

		when(networkEnvironment.createKvStateTaskRegistry(eq(jobId), eq(jobVertexId))).thenReturn(mock(TaskKvStateRegistry.class));
		when(networkEnvironment.getTaskEventDispatcher()).thenReturn(taskEventDispatcher);

		TaskExecutorLocalStateStoresManager localStateStoresManager = new TaskExecutorLocalStateStoresManager(
			false,
			new File[]{tmp.newFolder()},
			Executors.directExecutor());

		final TaskManagerServices taskManagerServices = new TaskManagerServicesBuilder()
			.setNetworkEnvironment(networkEnvironment)
			.setTaskSlotTable(taskSlotTable)
			.setJobManagerTable(jobManagerTable)
			.setTaskStateManager(localStateStoresManager)
			.build();

		TaskExecutor taskManager = new TaskExecutor(
			rpc,
			taskManagerConfiguration,
			haServices,
			taskManagerServices,
			new HeartbeatServices(1000L, 1000L),
			UnregisteredMetricGroups.createUnregisteredTaskManagerMetricGroup(),
			dummyBlobCacheService,
			testingFatalErrorHandler);

		try {
			taskManager.start();

			final TaskExecutorGateway tmGateway = taskManager.getSelfGateway(TaskExecutorGateway.class);

			tmGateway.submitTask(tdd, jobMasterId, timeout);

			CompletableFuture<Boolean> completionFuture = TestInvokable.completableFuture;

			completionFuture.get();
		} finally {
			taskManager.shutDown();
			taskManager.getTerminationFuture().get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);
		}
	}

	/**
	 * Test invokable which completes the given future when executed.
	 */
	public static class TestInvokable extends AbstractInvokable {

		static final CompletableFuture<Boolean> completableFuture = new CompletableFuture<>();

		public TestInvokable(Environment environment) {
			super(environment);
		}

		@Override
		public void invoke() throws Exception {
			completableFuture.complete(true);
		}
	}

	/**
	 * Tests that a TaskManager detects a job leader for which has reserved slots. Upon detecting
	 * the job leader, it will offer all reserved slots to the JobManager.
	 */
	@Test
	public void testJobLeaderDetection() throws Exception {
		final TaskSlotTable taskSlotTable = new TaskSlotTable(Collections.singleton(ResourceProfile.UNKNOWN), timerService);
		final JobManagerTable jobManagerTable = new JobManagerTable();
		final JobLeaderService jobLeaderService = new JobLeaderService(taskManagerLocation);

		final String resourceManagerAddress = "rm";
		final ResourceManagerId resourceManagerLeaderId = ResourceManagerId.generate();
		final ResourceID resourceManagerResourceId = new ResourceID(resourceManagerAddress);

		final ResourceManagerGateway resourceManagerGateway = mock(ResourceManagerGateway.class);
		final InstanceID registrationId = new InstanceID();

		when(resourceManagerGateway.registerTaskExecutor(
			any(String.class),
			eq(taskManagerLocation.getResourceID()),
			any(SlotReport.class),
			anyInt(),
			any(HardwareDescription.class),
			any(Time.class))).thenReturn(CompletableFuture.completedFuture(new TaskExecutorRegistrationSuccess(registrationId, resourceManagerResourceId, 1000L, new ClusterInformation("localhost", 1234))));

		final String jobManagerAddress = "jm";
		final UUID jobManagerLeaderId = UUID.randomUUID();
		final ResourceID jmResourceId = new ResourceID(jobManagerAddress);
		final JobMasterGateway jobMasterGateway = mock(JobMasterGateway.class);

		when(jobMasterGateway.registerTaskManager(
				any(String.class),
				eq(taskManagerLocation),
				any(Time.class)
		)).thenReturn(CompletableFuture.completedFuture(new JMTMRegistrationSuccess(jmResourceId)));
		when(jobMasterGateway.getHostname()).thenReturn(jobManagerAddress);
		when(jobMasterGateway.offerSlots(
			any(ResourceID.class),
			any(Collection.class),
			any(Time.class))).thenReturn(mock(CompletableFuture.class, RETURNS_MOCKS));

		rpc.registerGateway(resourceManagerAddress, resourceManagerGateway);
		rpc.registerGateway(jobManagerAddress, jobMasterGateway);

		final AllocationID allocationId = new AllocationID();
		final SlotID slotId = new SlotID(taskManagerLocation.getResourceID(), 0);
		final SlotOffer slotOffer = new SlotOffer(allocationId, 0, ResourceProfile.UNKNOWN);

		TaskExecutorLocalStateStoresManager localStateStoresManager = new TaskExecutorLocalStateStoresManager(
			false,
			new File[]{tmp.newFolder()},
			Executors.directExecutor());

		final TaskManagerServices taskManagerServices = new TaskManagerServicesBuilder()
			.setTaskManagerLocation(taskManagerLocation)
			.setTaskSlotTable(taskSlotTable)
			.setJobManagerTable(jobManagerTable)
			.setJobLeaderService(jobLeaderService)
			.setTaskStateManager(localStateStoresManager)
			.build();

		TaskExecutor taskManager = new TaskExecutor(
			rpc,
			taskManagerConfiguration,
			haServices,
			taskManagerServices,
			new HeartbeatServices(1000L, 1000L),
			UnregisteredMetricGroups.createUnregisteredTaskManagerMetricGroup(),
			dummyBlobCacheService,
			testingFatalErrorHandler);

		try {
			taskManager.start();

			final TaskExecutorGateway tmGateway = taskManager.getSelfGateway(TaskExecutorGateway.class);

			// tell the task manager about the rm leader
			resourceManagerLeaderRetriever.notifyListener(resourceManagerAddress, resourceManagerLeaderId.toUUID());

			// request slots from the task manager under the given allocation id
			CompletableFuture<Acknowledge> slotRequestAck = tmGateway.requestSlot(
				slotId,
				jobId,
				allocationId,
				jobManagerAddress,
				resourceManagerLeaderId,
				timeout);

			slotRequestAck.get();

			// now inform the task manager about the new job leader
			jobManagerLeaderRetriever.notifyListener(jobManagerAddress, jobManagerLeaderId);

			// the job leader should get the allocation id offered
			verify(jobMasterGateway, Mockito.timeout(timeout.toMilliseconds())).offerSlots(
					any(ResourceID.class),
					(Collection<SlotOffer>)Matchers.argThat(contains(slotOffer)),
					any(Time.class));
		} finally {
			taskManager.shutDown();
			taskManager.getTerminationFuture().get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);
		}
	}

	/**
	 * Tests that accepted slots go into state assigned and the others are returned to the resource
	 * manager.
	 */
	@Test
	public void testSlotAcceptance() throws Exception {
		final TaskSlotTable taskSlotTable = new TaskSlotTable(Arrays.asList(mock(ResourceProfile.class), mock(ResourceProfile.class)), timerService);
		final JobManagerTable jobManagerTable = new JobManagerTable();
		final JobLeaderService jobLeaderService = new JobLeaderService(taskManagerLocation);

		final String resourceManagerAddress = "rm";
		final UUID resourceManagerLeaderId = UUID.randomUUID();
		final ResourceID resourceManagerResourceId = new ResourceID(resourceManagerAddress);

		final String jobManagerAddress = "jm";
		final UUID jobManagerLeaderId = UUID.randomUUID();

		resourceManagerLeaderRetriever.notifyListener(resourceManagerAddress, resourceManagerLeaderId);
		jobManagerLeaderRetriever.notifyListener(jobManagerAddress, jobManagerLeaderId);

		final ResourceManagerGateway resourceManagerGateway = mock(ResourceManagerGateway.class);
		final InstanceID registrationId = new InstanceID();

		when(resourceManagerGateway.registerTaskExecutor(
			any(String.class),
			eq(taskManagerLocation.getResourceID()),
			any(SlotReport.class),
			anyInt(),
			any(HardwareDescription.class),
			any(Time.class))).thenReturn(CompletableFuture.completedFuture(new TaskExecutorRegistrationSuccess(registrationId, resourceManagerResourceId, 1000L, new ClusterInformation("localhost", 1234))));

		final ResourceID jmResourceId = new ResourceID(jobManagerAddress);

		final AllocationID allocationId1 = new AllocationID();
		final AllocationID allocationId2 = new AllocationID();

		final SlotOffer offer1 = new SlotOffer(allocationId1, 0, ResourceProfile.UNKNOWN);

		final JobMasterGateway jobMasterGateway = mock(JobMasterGateway.class);

		when(jobMasterGateway.registerTaskManager(
				any(String.class),
				eq(taskManagerLocation),
				any(Time.class)
		)).thenReturn(CompletableFuture.completedFuture(new JMTMRegistrationSuccess(jmResourceId)));
		when(jobMasterGateway.getHostname()).thenReturn(jobManagerAddress);

		when(jobMasterGateway.offerSlots(
				any(ResourceID.class), any(Collection.class), any(Time.class)))
			.thenReturn(CompletableFuture.completedFuture((Collection<SlotOffer>)Collections.singleton(offer1)));

		rpc.registerGateway(resourceManagerAddress, resourceManagerGateway);
		rpc.registerGateway(jobManagerAddress, jobMasterGateway);

		TaskExecutorLocalStateStoresManager localStateStoresManager = new TaskExecutorLocalStateStoresManager(
			false,
			new File[]{tmp.newFolder()},
			Executors.directExecutor());

		final TaskManagerServices taskManagerServices = new TaskManagerServicesBuilder()
			.setTaskManagerLocation(taskManagerLocation)
			.setTaskSlotTable(taskSlotTable)
			.setJobManagerTable(jobManagerTable)
			.setJobLeaderService(jobLeaderService)
			.setTaskStateManager(localStateStoresManager)
			.build();

		TaskExecutor taskManager = new TaskExecutor(
			rpc,
			taskManagerConfiguration,
			haServices,
			taskManagerServices,
			new HeartbeatServices(1000L, 1000L),
			UnregisteredMetricGroups.createUnregisteredTaskManagerMetricGroup(),
			dummyBlobCacheService,
			testingFatalErrorHandler);

		try {
			taskManager.start();

			// wait for the registration at the ResourceManager
			verify(resourceManagerGateway, Mockito.timeout(timeout.toMilliseconds())).registerTaskExecutor(
				eq(taskManager.getAddress()),
				eq(taskManagerLocation.getResourceID()),
				any(SlotReport.class),
				anyInt(),
				any(HardwareDescription.class),
				any(Time.class));

			taskSlotTable.allocateSlot(0, jobId, allocationId1, Time.milliseconds(10000L));
			taskSlotTable.allocateSlot(1, jobId, allocationId2, Time.milliseconds(10000L));

			// we have to add the job after the TaskExecutor, because otherwise the service has not
			// been properly started.
			jobLeaderService.addJob(jobId, jobManagerAddress);

			verify(resourceManagerGateway, Mockito.timeout(timeout.toMilliseconds())).notifySlotAvailable(
				eq(registrationId),
				eq(new SlotID(taskManagerLocation.getResourceID(), 1)),
				eq(allocationId2));

			assertTrue(taskSlotTable.existsActiveSlot(jobId, allocationId1));
			assertFalse(taskSlotTable.existsActiveSlot(jobId, allocationId2));
			assertTrue(taskSlotTable.isSlotFree(1));
		} finally {
			taskManager.shutDown();
			taskManager.getTerminationFuture().get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);
		}
	}

	/**
	 * This tests task executor receive SubmitTask before OfferSlot response.
	 */
	@Test
	public void testSubmitTaskBeforeAcceptSlot() throws Exception {
		final TaskSlotTable taskSlotTable = new TaskSlotTable(Arrays.asList(mock(ResourceProfile.class), mock(ResourceProfile.class)), timerService);
		final JobManagerTable jobManagerTable = new JobManagerTable();
		final JobLeaderService jobLeaderService = new JobLeaderService(taskManagerLocation);

		final String resourceManagerAddress = "rm";
		final UUID resourceManagerLeaderId = UUID.randomUUID();
		final ResourceID resourceManagerResourceId = new ResourceID(resourceManagerAddress);

		final String jobManagerAddress = "jm";
		final JobMasterId jobMasterId = JobMasterId.generate();

		resourceManagerLeaderRetriever.notifyListener(resourceManagerAddress, resourceManagerLeaderId);
		jobManagerLeaderRetriever.notifyListener(jobManagerAddress, jobMasterId.toUUID());

		final ResourceManagerGateway resourceManagerGateway = mock(ResourceManagerGateway.class);
		final InstanceID registrationId = new InstanceID();

		when(resourceManagerGateway.registerTaskExecutor(
			any(String.class),
			eq(taskManagerLocation.getResourceID()),
			any(SlotReport.class),
			anyInt(),
			any(HardwareDescription.class),
			any(Time.class))).thenReturn(
				CompletableFuture.completedFuture(new TaskExecutorRegistrationSuccess(registrationId, resourceManagerResourceId, 1000L, new ClusterInformation("localhost", 1234))));

		final ResourceID jmResourceId = new ResourceID(jobManagerAddress);

		final AllocationID allocationId1 = new AllocationID();
		final AllocationID allocationId2 = new AllocationID();

		final SlotOffer offer1 = new SlotOffer(allocationId1, 0, ResourceProfile.UNKNOWN);

		final JobMasterGateway jobMasterGateway = mock(JobMasterGateway.class);

		when(jobMasterGateway.registerTaskManager(
			any(String.class),
			eq(taskManagerLocation),
			any(Time.class)
		)).thenReturn(CompletableFuture.completedFuture(new JMTMRegistrationSuccess(jmResourceId)));
		when(jobMasterGateway.getHostname()).thenReturn(jobManagerAddress);
		when(jobMasterGateway.updateTaskExecutionState(any(TaskExecutionState.class))).thenReturn(CompletableFuture.completedFuture(Acknowledge.get()));


		rpc.registerGateway(resourceManagerAddress, resourceManagerGateway);
		rpc.registerGateway(jobManagerAddress, jobMasterGateway);

		final LibraryCacheManager libraryCacheManager = mock(LibraryCacheManager.class);
		when(libraryCacheManager.getClassLoader(eq(jobId))).thenReturn(getClass().getClassLoader());

		final JobManagerConnection jobManagerConnection = new JobManagerConnection(
			jobId,
			jmResourceId,
			jobMasterGateway,
			mock(TaskManagerActions.class),
			mock(CheckpointResponder.class),
			libraryCacheManager,
			mock(ResultPartitionConsumableNotifier.class),
			mock(PartitionProducerStateChecker.class));

		final TaskManagerMetricGroup taskManagerMetricGroup = mock(TaskManagerMetricGroup.class);
		TaskMetricGroup taskMetricGroup = mock(TaskMetricGroup.class);
		when(taskMetricGroup.getIOMetricGroup()).thenReturn(mock(TaskIOMetricGroup.class));

		when(taskManagerMetricGroup.addTaskForJob(
			any(JobID.class), anyString(), any(JobVertexID.class), any(ExecutionAttemptID.class),
			anyString(), anyInt(), anyInt())
		).thenReturn(taskMetricGroup);

		final NetworkEnvironment networkMock = mock(NetworkEnvironment.class, Mockito.RETURNS_MOCKS);

		TaskExecutorLocalStateStoresManager localStateStoresManager = new TaskExecutorLocalStateStoresManager(
			false,
			new File[]{tmp.newFolder()},
			Executors.directExecutor());

		final TaskManagerServices taskManagerServices = new TaskManagerServicesBuilder()
			.setTaskManagerLocation(taskManagerLocation)
			.setNetworkEnvironment(networkMock)
			.setTaskSlotTable(taskSlotTable)
			.setJobLeaderService(jobLeaderService)
			.setJobManagerTable(jobManagerTable)
			.setTaskStateManager(localStateStoresManager)
			.build();

		final TaskExecutor taskManager = new TaskExecutor(
			rpc,
			taskManagerConfiguration,
			haServices,
			taskManagerServices,
			new HeartbeatServices(1000L, 1000L),
			taskManagerMetricGroup,
			dummyBlobCacheService,
			testingFatalErrorHandler);

		try {
			taskManager.start();

			final TaskExecutorGateway tmGateway = taskManager.getSelfGateway(TaskExecutorGateway.class);

			taskSlotTable.allocateSlot(0, jobId, allocationId1, Time.milliseconds(10000L));
			taskSlotTable.allocateSlot(1, jobId, allocationId2, Time.milliseconds(10000L));

			final JobVertexID jobVertexId = new JobVertexID();

			JobInformation jobInformation = new JobInformation(
				jobId,
				name.getMethodName(),
				new SerializedValue<>(new ExecutionConfig()),
				new Configuration(),
				Collections.emptyList(),
				Collections.emptyList());

			TaskInformation taskInformation = new TaskInformation(
				jobVertexId,
				"test task",
				1,
				1,
				TestInvokable.class.getName(),
				new Configuration());

			SerializedValue<JobInformation> serializedJobInformation = new SerializedValue<>(jobInformation);
			SerializedValue<TaskInformation> serializedJobVertexInformation = new SerializedValue<>(taskInformation);

			final TaskDeploymentDescriptor tdd = new TaskDeploymentDescriptor(
				jobId,
				new TaskDeploymentDescriptor.NonOffloaded<>(serializedJobInformation),
				new TaskDeploymentDescriptor.NonOffloaded<>(serializedJobVertexInformation),
				new ExecutionAttemptID(),
				allocationId1,
				0,
				0,
				0,
				null,
				Collections.<ResultPartitionDeploymentDescriptor>emptyList(),
				Collections.<InputGateDeploymentDescriptor>emptyList());

			CompletableFuture<Collection<SlotOffer>> offerResultFuture = new CompletableFuture<>();

			// submit task first and then return acceptance response
			when(
				jobMasterGateway.offerSlots(
					any(ResourceID.class),
					any(Collection.class),
					any(Time.class)))
				.thenReturn(offerResultFuture);

			// we have to add the job after the TaskExecutor, because otherwise the service has not
			// been properly started. This will also offer the slots to the job master
			jobLeaderService.addJob(jobId, jobManagerAddress);

			verify(jobMasterGateway, Mockito.timeout(timeout.toMilliseconds())).offerSlots(any(ResourceID.class), any(Collection.class), any(Time.class));

			// submit the task without having acknowledge the offered slots
			tmGateway.submitTask(tdd, jobMasterId, timeout);

			// acknowledge the offered slots
			offerResultFuture.complete(Collections.singleton(offer1));

			verify(resourceManagerGateway, Mockito.timeout(timeout.toMilliseconds())).notifySlotAvailable(
				eq(registrationId),
				eq(new SlotID(taskManagerLocation.getResourceID(), 1)),
				any(AllocationID.class));

			assertTrue(taskSlotTable.existsActiveSlot(jobId, allocationId1));
			assertFalse(taskSlotTable.existsActiveSlot(jobId, allocationId2));
			assertTrue(taskSlotTable.isSlotFree(1));
		} finally {
			taskManager.shutDown();
			taskManager.getTerminationFuture().get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);
		}
	}

	/**
	 * This tests makes sure that duplicate JobMaster gained leadership messages are filtered out
	 * by the TaskExecutor.
	 *
	 * See FLINK-7526
	 */
	@Test
	public void testFilterOutDuplicateJobMasterRegistrations() throws Exception {
		final long verificationTimeout = 500L;
		final JobLeaderService jobLeaderService = mock(JobLeaderService.class);
		final HeartbeatServices heartbeatServicesMock = mock(HeartbeatServices.class, Mockito.RETURNS_MOCKS);

		final JobMasterGateway jobMasterGateway = mock(JobMasterGateway.class);
		when(jobMasterGateway.getHostname()).thenReturn("localhost");
		final JMTMRegistrationSuccess registrationMessage = new JMTMRegistrationSuccess(ResourceID.generate());
		final JobManagerTable jobManagerTableMock = spy(new JobManagerTable());

		TaskExecutorLocalStateStoresManager localStateStoresManager = new TaskExecutorLocalStateStoresManager(
			false,
			new File[]{tmp.newFolder()},
			Executors.directExecutor());

		final TaskManagerServices taskManagerServices = new TaskManagerServicesBuilder()
			.setTaskManagerLocation(taskManagerLocation)
			.setJobManagerTable(jobManagerTableMock)
			.setJobLeaderService(jobLeaderService)
			.setTaskStateManager(localStateStoresManager)
			.build();

		final TaskExecutor taskExecutor = new TaskExecutor(
			rpc,
			taskManagerConfiguration,
			haServices,
			taskManagerServices,
			heartbeatServicesMock,
			UnregisteredMetricGroups.createUnregisteredTaskManagerMetricGroup(),
			dummyBlobCacheService,
			testingFatalErrorHandler);

		try {
			taskExecutor.start();

			ArgumentCaptor<JobLeaderListener> jobLeaderListenerArgumentCaptor = ArgumentCaptor.forClass(JobLeaderListener.class);

			verify(jobLeaderService).start(anyString(), any(RpcService.class), any(HighAvailabilityServices.class), jobLeaderListenerArgumentCaptor.capture());

			JobLeaderListener taskExecutorListener = jobLeaderListenerArgumentCaptor.getValue();

			taskExecutorListener.jobManagerGainedLeadership(jobId, jobMasterGateway, registrationMessage);

			// duplicate job manager gained leadership message
			taskExecutorListener.jobManagerGainedLeadership(jobId, jobMasterGateway, registrationMessage);

			ArgumentCaptor<JobManagerConnection> jobManagerConnectionArgumentCaptor = ArgumentCaptor.forClass(JobManagerConnection.class);

			verify(jobManagerTableMock, Mockito.timeout(verificationTimeout).times(1)).put(eq(jobId), jobManagerConnectionArgumentCaptor.capture());

			JobManagerConnection jobManagerConnection = jobManagerConnectionArgumentCaptor.getValue();

			assertEquals(jobMasterGateway, jobManagerConnection.getJobManagerGateway());
		} finally {
			taskExecutor.shutDown();
			taskExecutor.getTerminationFuture().get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);
		}
	}

	/**
	 * Tests that the heartbeat is stopped once the TaskExecutor detects that the RM is no longer leader.
	 *
	 * <p>See FLINK-8462
	 */
	@Test
	public void testRMHeartbeatStopWhenLeadershipRevoked() throws Exception {
		final long heartbeatInterval = 1L;
		final long heartbeatTimeout = 10000L;
		final long pollTimeout = 1000L;
		final RecordingHeartbeatServices heartbeatServices = new RecordingHeartbeatServices(heartbeatInterval, heartbeatTimeout);
		final ResourceID rmResourceID = ResourceID.generate();

		final TaskSlotTable taskSlotTable = new TaskSlotTable(Collections.singleton(ResourceProfile.UNKNOWN), timerService);

		final String rmAddress = "rm";
		final TestingResourceManagerGateway rmGateway = new TestingResourceManagerGateway(
			ResourceManagerId.generate(),
			rmResourceID,
			heartbeatInterval,
			rmAddress,
			rmAddress);

		rpc.registerGateway(rmAddress, rmGateway);

		TaskExecutorLocalStateStoresManager localStateStoresManager = new TaskExecutorLocalStateStoresManager(
			false,
			new File[]{tmp.newFolder()},
			Executors.directExecutor());

		final TaskManagerServices taskManagerServices = new TaskManagerServicesBuilder()
			.setTaskManagerLocation(taskManagerLocation)
			.setTaskSlotTable(taskSlotTable)
			.setTaskStateManager(localStateStoresManager)
			.build();

		final TaskExecutor taskExecutor = new TaskExecutor(
			rpc,
			taskManagerConfiguration,
			haServices,
			taskManagerServices,
			heartbeatServices,
			UnregisteredMetricGroups.createUnregisteredTaskManagerMetricGroup(),
			dummyBlobCacheService,
			testingFatalErrorHandler);

		try {
			taskExecutor.start();

			final BlockingQueue<ResourceID> unmonitoredTargets = heartbeatServices.getUnmonitoredTargets();
			final BlockingQueue<ResourceID> monitoredTargets = heartbeatServices.getMonitoredTargets();

			resourceManagerLeaderRetriever.notifyListener(rmAddress, rmGateway.getFencingToken().toUUID());

			// wait for TM registration by checking the registered heartbeat targets
			assertThat(
				monitoredTargets.poll(pollTimeout, TimeUnit.MILLISECONDS),
				equalTo(rmResourceID));

			// let RM lose leadership
			resourceManagerLeaderRetriever.notifyListener(null, null);

			// the timeout should not have triggered since it is much higher
			assertThat(unmonitoredTargets.poll(pollTimeout, TimeUnit.MILLISECONDS), equalTo(rmResourceID));
		} finally {
			RpcUtils.terminateRpcEndpoint(taskExecutor, timeout);
		}
	}

	/**
	 * Tests that a job is removed from the JobLeaderService once a TaskExecutor has
	 * no more slots assigned to this job.
	 *
	 * <p>See FLINK-8504
	 */
	@Test
	public void testRemoveJobFromJobLeaderService() throws Exception {
		final TaskSlotTable taskSlotTable = new TaskSlotTable(
			Collections.singleton(ResourceProfile.UNKNOWN),
			timerService);

		TaskExecutorLocalStateStoresManager localStateStoresManager = new TaskExecutorLocalStateStoresManager(
			false,
			new File[]{tmp.newFolder()},
			Executors.directExecutor());

		final TaskManagerServices taskManagerServices = new TaskManagerServicesBuilder()
			.setTaskManagerLocation(taskManagerLocation)
			.setTaskSlotTable(taskSlotTable)
			.setTaskStateManager(localStateStoresManager)
			.build();

		final TaskExecutor taskExecutor = new TaskExecutor(
			rpc,
			taskManagerConfiguration,
			haServices,
			taskManagerServices,
			new HeartbeatServices(1000L, 1000L),
			UnregisteredMetricGroups.createUnregisteredTaskManagerMetricGroup(),
			dummyBlobCacheService,
			testingFatalErrorHandler);

		try {
			final TestingResourceManagerGateway resourceManagerGateway = new TestingResourceManagerGateway();
			final ResourceManagerId resourceManagerId = resourceManagerGateway.getFencingToken();

			rpc.registerGateway(resourceManagerGateway.getAddress(), resourceManagerGateway);
			resourceManagerLeaderRetriever.notifyListener(resourceManagerGateway.getAddress(), resourceManagerId.toUUID());

			final CompletableFuture<LeaderRetrievalListener> startFuture = new CompletableFuture<>();
			final CompletableFuture<Void> stopFuture = new CompletableFuture<>();

			final StartStopNotifyingLeaderRetrievalService jobMasterLeaderRetriever = new StartStopNotifyingLeaderRetrievalService(
				startFuture,
				stopFuture);
			haServices.setJobMasterLeaderRetriever(jobId, jobMasterLeaderRetriever);

			taskExecutor.start();

			final TaskExecutorGateway taskExecutorGateway = taskExecutor.getSelfGateway(TaskExecutorGateway.class);

			final SlotID slotId = new SlotID(taskManagerLocation.getResourceID(), 0);
			final AllocationID allocationId = new AllocationID();

			assertThat(startFuture.isDone(), is(false));
			final JobLeaderService jobLeaderService = taskManagerServices.getJobLeaderService();
			assertThat(jobLeaderService.containsJob(jobId), is(false));

			taskExecutorGateway.requestSlot(
				slotId,
				jobId,
				allocationId,
				"foobar",
				resourceManagerId,
				timeout).get();

			// wait until the job leader retrieval service for jobId is started
			startFuture.get();
			assertThat(jobLeaderService.containsJob(jobId), is(true));

			taskExecutorGateway.freeSlot(allocationId, new FlinkException("Test exception"), timeout).get();

			// wait that the job leader retrieval service for jobId stopped becaue it should get removed
			stopFuture.get();
			assertThat(jobLeaderService.containsJob(jobId), is(false));
		} finally {
			RpcUtils.terminateRpcEndpoint(taskExecutor, timeout);
		}
	}

	@Test
	public void testMaximumRegistrationDuration() throws Exception {
		configuration.setString(TaskManagerOptions.REGISTRATION_TIMEOUT, "10 ms");

		final TaskExecutor taskExecutor = new TaskExecutor(
			rpc,
			TaskManagerConfiguration.fromConfiguration(configuration),
			haServices,
			new TaskManagerServicesBuilder().build(),
			new HeartbeatServices(1000L, 1000L),
			UnregisteredMetricGroups.createUnregisteredTaskManagerMetricGroup(),
			dummyBlobCacheService,
			testingFatalErrorHandler);

		taskExecutor.start();

		try {
			final Throwable error = testingFatalErrorHandler.getErrorFuture().get();
			assertThat(error, is(notNullValue()));
			assertThat(ExceptionUtils.stripExecutionException(error), instanceOf(RegistrationTimeoutException.class));

			testingFatalErrorHandler.clearError();
		} finally {
			RpcUtils.terminateRpcEndpoint(taskExecutor, timeout);
		}
	}

	@Test
	public void testMaximumRegistrationDurationAfterConnectionLoss() throws Exception {
		configuration.setString(TaskManagerOptions.REGISTRATION_TIMEOUT, "100 ms");
		final TaskSlotTable taskSlotTable = new TaskSlotTable(Collections.singleton(ResourceProfile.UNKNOWN), timerService);

		final long heartbeatInterval = 10L;
		final TaskManagerServices taskManagerServices = new TaskManagerServicesBuilder().setTaskSlotTable(taskSlotTable).build();
		final TaskExecutor taskExecutor = new TaskExecutor(
			rpc,
			TaskManagerConfiguration.fromConfiguration(configuration),
			haServices,
			taskManagerServices,
			new HeartbeatServices(heartbeatInterval, 10L),
			UnregisteredMetricGroups.createUnregisteredTaskManagerMetricGroup(),
			dummyBlobCacheService,
			testingFatalErrorHandler);

		taskExecutor.start();

		final CompletableFuture<ResourceID> registrationFuture = new CompletableFuture<>();
		final OneShotLatch secondRegistration = new OneShotLatch();
		try {
			final TestingResourceManagerGateway testingResourceManagerGateway = new TestingResourceManagerGateway();
			testingResourceManagerGateway.setRegisterTaskExecutorFunction(
				tuple -> {
					if (registrationFuture.complete(tuple.f1)) {
						return CompletableFuture.completedFuture(new TaskExecutorRegistrationSuccess(
							new InstanceID(),
							testingResourceManagerGateway.getOwnResourceId(),
							heartbeatInterval,
							new ClusterInformation("localhost", 1234)));
					} else {
						secondRegistration.trigger();
						return CompletableFuture.completedFuture(new RegistrationResponse.Decline("Only the first registration should succeed."));
					}
				}
			);
			rpc.registerGateway(testingResourceManagerGateway.getAddress(), testingResourceManagerGateway);

			resourceManagerLeaderRetriever.notifyListener(testingResourceManagerGateway.getAddress(), UUID.randomUUID());

			final ResourceID registrationResourceId = registrationFuture.get();

			assertThat(registrationResourceId, equalTo(taskManagerServices.getTaskManagerLocation().getResourceID()));

			secondRegistration.await();

			final Throwable error = testingFatalErrorHandler.getErrorFuture().get();
			assertThat(error, is(notNullValue()));
			assertThat(ExceptionUtils.stripExecutionException(error), instanceOf(RegistrationTimeoutException.class));

			testingFatalErrorHandler.clearError();
		} finally {
			RpcUtils.terminateRpcEndpoint(taskExecutor, timeout);
		}
	}

	private static final class StartStopNotifyingLeaderRetrievalService implements LeaderRetrievalService {
		private final CompletableFuture<LeaderRetrievalListener> startFuture;

		private final CompletableFuture<Void> stopFuture;

		private StartStopNotifyingLeaderRetrievalService(
				CompletableFuture<LeaderRetrievalListener> startFuture,
				CompletableFuture<Void> stopFuture) {
			this.startFuture = startFuture;
			this.stopFuture = stopFuture;
		}

		@Override
		public void start(LeaderRetrievalListener listener) throws Exception {
			startFuture.complete(listener);
		}

		@Override
		public void stop() throws Exception {
			stopFuture.complete(null);
		}
	}

	/**
	 * Special {@link HeartbeatServices} which creates a {@link RecordingHeartbeatManagerImpl}.
	 */
	private static final class RecordingHeartbeatServices extends HeartbeatServices {

		private final BlockingQueue<ResourceID> unmonitoredTargets;

		private final BlockingQueue<ResourceID> monitoredTargets;

		public RecordingHeartbeatServices(long heartbeatInterval, long heartbeatTimeout) {
			super(heartbeatInterval, heartbeatTimeout);

			this.unmonitoredTargets = new ArrayBlockingQueue<>(1);
			this.monitoredTargets = new ArrayBlockingQueue<>(1);
		}

		@Override
		public <I, O> HeartbeatManager<I, O> createHeartbeatManager(ResourceID resourceId, HeartbeatListener<I, O> heartbeatListener, ScheduledExecutor scheduledExecutor, Logger log) {
			return new RecordingHeartbeatManagerImpl<>(
				heartbeatTimeout,
				resourceId,
				heartbeatListener,
				scheduledExecutor,
				scheduledExecutor,
				log,
				unmonitoredTargets,
				monitoredTargets);
		}

		public BlockingQueue<ResourceID> getUnmonitoredTargets() {
			return unmonitoredTargets;
		}

		public BlockingQueue<ResourceID> getMonitoredTargets() {
			return monitoredTargets;
		}
	}

	/**
	 * {@link HeartbeatManagerImpl} which records the unmonitored targets.
	 */
	private static final class RecordingHeartbeatManagerImpl<I, O> extends HeartbeatManagerImpl<I, O> {

		private final BlockingQueue<ResourceID> unmonitoredTargets;

		private final BlockingQueue<ResourceID> monitoredTargets;

		public RecordingHeartbeatManagerImpl(
				long heartbeatTimeoutIntervalMs,
				ResourceID ownResourceID,
				HeartbeatListener<I, O> heartbeatListener,
				Executor executor,
				ScheduledExecutor scheduledExecutor,
				Logger log,
				BlockingQueue<ResourceID> unmonitoredTargets,
				BlockingQueue<ResourceID> monitoredTargets) {
			super(heartbeatTimeoutIntervalMs, ownResourceID, heartbeatListener, executor, scheduledExecutor, log);
			this.unmonitoredTargets = unmonitoredTargets;
			this.monitoredTargets = monitoredTargets;
		}

		@Override
		public void unmonitorTarget(ResourceID resourceID) {
			super.unmonitorTarget(resourceID);
			unmonitoredTargets.offer(resourceID);
		}

		@Override
		public void monitorTarget(ResourceID resourceID, HeartbeatTarget<O> heartbeatTarget) {
			super.monitorTarget(resourceID, heartbeatTarget);
			monitoredTargets.offer(resourceID);
		}
	}
}
