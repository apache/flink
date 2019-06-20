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
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
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
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
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
import org.apache.flink.runtime.io.network.partition.NoOpResultPartitionConsumableNotifier;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobmaster.AllocatedSlotInfo;
import org.apache.flink.runtime.jobmaster.AllocatedSlotReport;
import org.apache.flink.runtime.jobmaster.JMTMRegistrationSuccess;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGateway;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGatewayBuilder;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.leaderretrieval.SettableLeaderRetrievalService;
import org.apache.flink.runtime.messages.Acknowledge;
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
import org.apache.flink.runtime.taskexecutor.exceptions.TaskManagerException;
import org.apache.flink.runtime.taskexecutor.slot.SlotNotFoundException;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskexecutor.slot.TaskSlotTable;
import org.apache.flink.runtime.taskexecutor.slot.TimerService;
import org.apache.flink.runtime.taskmanager.CheckpointResponder;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.runtime.taskmanager.TaskManagerActions;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
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
import org.mockito.Mockito;
import org.mockito.hamcrest.MockitoHamcrest;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
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

/**
 * Tests for the {@link TaskExecutor}.
 */
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
		final CompletableFuture<TaskManagerLocation> taskManagerLocationFuture = new CompletableFuture<>();
		final CompletableFuture<ResourceID> disconnectTaskManagerFuture = new CompletableFuture<>();
		final TestingJobMasterGateway jobMasterGateway = new TestingJobMasterGatewayBuilder()
			.setRegisterTaskManagerFunction((s, taskManagerLocation) -> {
				taskManagerLocationFuture.complete(taskManagerLocation);
				return CompletableFuture.completedFuture(new JMTMRegistrationSuccess(jmResourceId));
			})
			.setDisconnectTaskManagerFunction(
				resourceID -> {
					disconnectTaskManagerFuture.complete(resourceID);
					return CompletableFuture.completedFuture(Acknowledge.get());
				}
			)
			.build();

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
			null,
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
			final TaskManagerLocation taskManagerLocation1 = taskManagerLocationFuture.get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);
			assertThat(taskManagerLocation1, equalTo(taskManagerLocation));

			// the timeout should trigger disconnecting from the JobManager
			final ResourceID resourceID = disconnectTaskManagerFuture.get(heartbeatTimeout * 50L, TimeUnit.MILLISECONDS);
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
			rmAddress,
			rmAddress);

		final TaskExecutorRegistrationSuccess registrationResponse = new TaskExecutorRegistrationSuccess(
			new InstanceID(),
			rmResourceId,
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
			null,
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

	/**
	 * Tests that the correct slot report is sent as part of the heartbeat response.
	 */
	@Test
	public void testHeartbeatSlotReporting() throws Exception {
		final long verificationTimeout = 1000L;
		final long heartbeatTimeout = 10000L;
		final String rmAddress = "rm";
		final UUID rmLeaderId = UUID.randomUUID();

		// register the mock resource manager gateway
		final TestingResourceManagerGateway rmGateway = new TestingResourceManagerGateway();
		final CompletableFuture<ResourceID> taskExecutorRegistrationFuture = new CompletableFuture<>();
		final ResourceID rmResourceId = rmGateway.getOwnResourceId();
		final CompletableFuture<RegistrationResponse> registrationResponse = CompletableFuture.completedFuture(
			new TaskExecutorRegistrationSuccess(
				new InstanceID(),
				rmResourceId,
				new ClusterInformation("localhost", 1234)));

		rmGateway.setRegisterTaskExecutorFunction(stringResourceIDIntegerHardwareDescriptionTuple4 -> {
			taskExecutorRegistrationFuture.complete(stringResourceIDIntegerHardwareDescriptionTuple4.f1);
			return registrationResponse;
		});

		final CompletableFuture<SlotReport> initialSlotReportFuture = new CompletableFuture<>();
		rmGateway.setSendSlotReportFunction(resourceIDInstanceIDSlotReportTuple3 -> {
			initialSlotReportFuture.complete(resourceIDInstanceIDSlotReportTuple3.f2);
			return CompletableFuture.completedFuture(Acknowledge.get());
		});

		final CompletableFuture<SlotReport> heartbeatSlotReportFuture = new CompletableFuture<>();
		rmGateway.setTaskExecutorHeartbeatConsumer((resourceID, slotReport) -> heartbeatSlotReportFuture.complete(slotReport));

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
						(HeartbeatListener<SlotReport, Void>) invocation.getArguments()[1],
						(ScheduledExecutor) invocation.getArguments()[2],
						(Logger) invocation.getArguments()[3]));
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
			null,
			dummyBlobCacheService,
			testingFatalErrorHandler);

		try {
			taskManager.start();

			// wait for spied heartbeat manager instance
			HeartbeatManager<Void, SlotReport> heartbeatManager = taskManager.getResourceManagerHeartbeatManager();

			// define a leader and see that a registration happens
			resourceManagerLeaderRetriever.notifyListener(rmAddress, rmLeaderId);

			// register resource manager success will trigger monitoring heartbeat target between tm and rm
			assertThat(taskExecutorRegistrationFuture.get(), equalTo(taskManagerLocation.getResourceID()));
			assertThat(initialSlotReportFuture.get(), equalTo(slotReport1));

			verify(heartbeatManager, timeout(verificationTimeout)).monitorTarget(any(ResourceID.class), any(HeartbeatTarget.class));

			TaskExecutorGateway taskExecutorGateway = taskManager.getSelfGateway(TaskExecutorGateway.class);

			// trigger the heartbeat asynchronously
			taskExecutorGateway.heartbeatFromResourceManager(rmResourceId);

			// wait for heartbeat response
			SlotReport actualSlotReport = heartbeatSlotReportFuture.get();

			// the new slot report should be reported
			assertEquals(slotReport2, actualSlotReport);
		} finally {
			RpcUtils.terminateRpcEndpoint(taskManager, timeout);
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
					anyString(), any(ResourceID.class), anyInt(), any(HardwareDescription.class), any(Time.class)))
			.thenReturn(CompletableFuture.completedFuture(new TaskExecutorRegistrationSuccess(
				new InstanceID(), resourceManagerResourceId, new ClusterInformation("localhost", 1234))));

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
			null,
			dummyBlobCacheService,
			testingFatalErrorHandler);

		try {
			taskManager.start();
			String taskManagerAddress = taskManager.getAddress();

			verify(rmGateway, Mockito.timeout(timeout.toMilliseconds())).registerTaskExecutor(
					eq(taskManagerAddress), eq(taskManagerLocation.getResourceID()), anyInt(), any(HardwareDescription.class), any(Time.class));
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
					anyString(), any(ResourceID.class), anyInt(), any(HardwareDescription.class), any(Time.class)))
			.thenReturn(CompletableFuture.completedFuture(
				new TaskExecutorRegistrationSuccess(new InstanceID(), rmResourceId1, new ClusterInformation("localhost", 1234))));
		when(rmGateway2.registerTaskExecutor(
					anyString(), any(ResourceID.class), anyInt(), any(HardwareDescription.class), any(Time.class)))
			.thenReturn(CompletableFuture.completedFuture(
				new TaskExecutorRegistrationSuccess(new InstanceID(), rmResourceId2, new ClusterInformation("localhost", 1234))));

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
			null,
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
					eq(taskManagerAddress), eq(taskManagerLocation.getResourceID()), anyInt(), any(HardwareDescription.class), any(Time.class));
			assertNotNull(taskManager.getResourceManagerConnection());

			// cancel the leader
			resourceManagerLeaderRetriever.notifyListener(null, null);

			// set a new leader, see that a registration happens
			resourceManagerLeaderRetriever.notifyListener(address2, leaderId2);

			verify(rmGateway2, Mockito.timeout(timeout.toMilliseconds())).registerTaskExecutor(
					eq(taskManagerAddress), eq(taskManagerLocation.getResourceID()), anyInt(), any(HardwareDescription.class), any(Time.class));
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
			new NoOpResultPartitionConsumableNotifier(),
			mock(PartitionProducerStateChecker.class));

		final JobManagerTable jobManagerTable = new JobManagerTable();
		jobManagerTable.put(jobId, jobManagerConnection);

		final TaskSlotTable taskSlotTable = mock(TaskSlotTable.class);
		when(taskSlotTable.tryMarkSlotActive(eq(jobId), eq(allocationId))).thenReturn(true);
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
			null,
			dummyBlobCacheService,
			testingFatalErrorHandler);

		try {
			taskManager.start();

			final TaskExecutorGateway tmGateway = taskManager.getSelfGateway(TaskExecutorGateway.class);

			tmGateway.submitTask(tdd, jobMasterId, timeout);

			CompletableFuture<Boolean> completionFuture = TestInvokable.COMPLETABLE_FUTURE;

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

		static final CompletableFuture<Boolean> COMPLETABLE_FUTURE = new CompletableFuture<>();

		public TestInvokable(Environment environment) {
			super(environment);
		}

		@Override
		public void invoke() throws Exception {
			COMPLETABLE_FUTURE.complete(true);
		}
	}

	/**
	 * Tests that a TaskManager detects a job leader for which it has reserved slots. Upon detecting
	 * the job leader, it will offer all reserved slots to the JobManager.
	 */
	@Test
	public void testJobLeaderDetection() throws Exception {
		final TaskSlotTable taskSlotTable = new TaskSlotTable(Collections.singleton(ResourceProfile.UNKNOWN), timerService);
		final JobManagerTable jobManagerTable = new JobManagerTable();
		final JobLeaderService jobLeaderService = new JobLeaderService(taskManagerLocation);

		final String resourceManagerAddress = "rm";
		final ResourceManagerId resourceManagerLeaderId = ResourceManagerId.generate();

		final TestingResourceManagerGateway resourceManagerGateway = new TestingResourceManagerGateway();
		CompletableFuture<Void> initialSlotReportFuture = new CompletableFuture<>();
		resourceManagerGateway.setSendSlotReportFunction(resourceIDInstanceIDSlotReportTuple3 -> {
			initialSlotReportFuture.complete(null);
			return CompletableFuture.completedFuture(Acknowledge.get());
		});

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
			null,
			dummyBlobCacheService,
			testingFatalErrorHandler);

		try {
			taskManager.start();

			final TaskExecutorGateway tmGateway = taskManager.getSelfGateway(TaskExecutorGateway.class);

			// tell the task manager about the rm leader
			resourceManagerLeaderRetriever.notifyListener(resourceManagerAddress, resourceManagerLeaderId.toUUID());

			// wait for the initial slot report
			initialSlotReportFuture.get();

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
					(Collection<SlotOffer>) MockitoHamcrest.argThat(contains(slotOffer)),
					any(Time.class));
		} finally {
			RpcUtils.terminateRpcEndpoint(taskManager, timeout);
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

		final String jobManagerAddress = "jm";
		final UUID jobManagerLeaderId = UUID.randomUUID();

		resourceManagerLeaderRetriever.notifyListener(resourceManagerAddress, resourceManagerLeaderId);
		jobManagerLeaderRetriever.notifyListener(jobManagerAddress, jobManagerLeaderId);

		final TestingResourceManagerGateway resourceManagerGateway = new TestingResourceManagerGateway();
		final ResourceID resourceManagerResourceId = resourceManagerGateway.getOwnResourceId();
		final InstanceID registrationId = new InstanceID();

		final CompletableFuture<ResourceID> registrationFuture = new CompletableFuture<>();
		resourceManagerGateway.setRegisterTaskExecutorFunction(
			stringResourceIDIntegerHardwareDescriptionTuple4 -> {
				registrationFuture.complete(stringResourceIDIntegerHardwareDescriptionTuple4.f1);
				return CompletableFuture.completedFuture(new TaskExecutorRegistrationSuccess(registrationId, resourceManagerResourceId, new ClusterInformation("localhost", 1234)));
			});

		final CompletableFuture<Tuple3<InstanceID, SlotID, AllocationID>> availableSlotFuture = new CompletableFuture<>();
		resourceManagerGateway.setNotifySlotAvailableConsumer(availableSlotFuture::complete);

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
			.thenReturn(CompletableFuture.completedFuture((Collection<SlotOffer>) Collections.singleton(offer1)));

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
			null,
			dummyBlobCacheService,
			testingFatalErrorHandler);

		try {
			taskManager.start();

			assertThat(registrationFuture.get(), equalTo(taskManagerLocation.getResourceID()));

			taskSlotTable.allocateSlot(0, jobId, allocationId1, Time.milliseconds(10000L));
			taskSlotTable.allocateSlot(1, jobId, allocationId2, Time.milliseconds(10000L));

			// we have to add the job after the TaskExecutor, because otherwise the service has not
			// been properly started.
			jobLeaderService.addJob(jobId, jobManagerAddress);

			final Tuple3<InstanceID, SlotID, AllocationID> instanceIDSlotIDAllocationIDTuple3 = availableSlotFuture.get();

			final Tuple3<InstanceID, SlotID, AllocationID> expectedResult = Tuple3.of(registrationId, new SlotID(taskManagerLocation.getResourceID(), 1), allocationId2);

			assertThat(instanceIDSlotIDAllocationIDTuple3, equalTo(expectedResult));

			assertTrue(taskSlotTable.tryMarkSlotActive(jobId, allocationId1));
			assertFalse(taskSlotTable.tryMarkSlotActive(jobId, allocationId2));
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

		final TestingResourceManagerGateway resourceManagerGateway = new TestingResourceManagerGateway();
		resourceManagerLeaderRetriever.notifyListener(resourceManagerGateway.getAddress(), resourceManagerGateway.getFencingToken().toUUID());

		final CompletableFuture<Tuple3<InstanceID, SlotID, AllocationID>> availableSlotFuture = new CompletableFuture<>();
		resourceManagerGateway.setNotifySlotAvailableConsumer(availableSlotFuture::complete);

		final AllocationID allocationId1 = new AllocationID();
		final AllocationID allocationId2 = new AllocationID();

		final SlotOffer offer1 = new SlotOffer(allocationId1, 0, ResourceProfile.UNKNOWN);

		final OneShotLatch offerSlotsLatch = new OneShotLatch();
		final OneShotLatch taskInTerminalState = new OneShotLatch();
		final CompletableFuture<Collection<SlotOffer>> offerResultFuture = new CompletableFuture<>();
		final TestingJobMasterGateway jobMasterGateway = new TestingJobMasterGatewayBuilder()
			.setOfferSlotsFunction((resourceID, slotOffers) -> {
				offerSlotsLatch.trigger();
				return offerResultFuture;
			})
			.setUpdateTaskExecutionStateFunction(taskExecutionState -> {
				if (taskExecutionState.getExecutionState().isTerminal()) {
					taskInTerminalState.trigger();
				}
				return CompletableFuture.completedFuture(Acknowledge.get());
			})
			.build();

		jobManagerLeaderRetriever.notifyListener(jobMasterGateway.getAddress(), jobMasterGateway.getFencingToken().toUUID());

		rpc.registerGateway(resourceManagerGateway.getAddress(), resourceManagerGateway);
		rpc.registerGateway(jobMasterGateway.getAddress(), jobMasterGateway);

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
			UnregisteredMetricGroups.createUnregisteredTaskManagerMetricGroup(),
			null,
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
				NoOpInvokable.class.getName(),
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
				Collections.emptyList(),
				Collections.emptyList());

			// we have to add the job after the TaskExecutor, because otherwise the service has not
			// been properly started. This will also offer the slots to the job master
			jobLeaderService.addJob(jobId, jobMasterGateway.getAddress());

			offerSlotsLatch.await();

			// submit the task without having acknowledge the offered slots
			tmGateway.submitTask(tdd, jobMasterGateway.getFencingToken(), timeout).get();

			// acknowledge the offered slots
			offerResultFuture.complete(Collections.singleton(offer1));

			final Tuple3<InstanceID, SlotID, AllocationID> instanceIDSlotIDAllocationIDTuple3 = availableSlotFuture.get();
			assertThat(instanceIDSlotIDAllocationIDTuple3.f1, equalTo(new SlotID(taskManagerLocation.getResourceID(), 1)));

			assertTrue(taskSlotTable.tryMarkSlotActive(jobId, allocationId1));
			assertFalse(taskSlotTable.tryMarkSlotActive(jobId, allocationId2));
			assertTrue(taskSlotTable.isSlotFree(1));

			// wait for the task completion
			taskInTerminalState.await();
		} finally {
			taskManager.shutDown();
			taskManager.getTerminationFuture().get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);
		}
	}

	/**
	 * This tests makes sure that duplicate JobMaster gained leadership messages are filtered out
	 * by the TaskExecutor. See FLINK-7526.
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
			null,
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
			null,
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
			null,
			dummyBlobCacheService,
			testingFatalErrorHandler);

		try {
			final TestingResourceManagerGateway resourceManagerGateway = new TestingResourceManagerGateway();
			final CompletableFuture<Void> initialSlotReport = new CompletableFuture<>();
			resourceManagerGateway.setSendSlotReportFunction(resourceIDInstanceIDSlotReportTuple3 -> {
				initialSlotReport.complete(null);
				return CompletableFuture.completedFuture(Acknowledge.get());
			});
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

			// wait for the initial slot report
			initialSlotReport.get();

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

		final TaskExecutor taskExecutor = createTaskExecutor(new TaskManagerServicesBuilder().build());

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
			null,
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

	/**
	 * Tests that we ignore slot requests if the TaskExecutor is not
	 * registered at a ResourceManager.
	 */
	@Test
	public void testIgnoringSlotRequestsIfNotRegistered() throws Exception {
		final TaskSlotTable taskSlotTable = new TaskSlotTable(Collections.singleton(ResourceProfile.UNKNOWN), timerService);
		final TaskManagerServices taskManagerServices = new TaskManagerServicesBuilder().setTaskSlotTable(taskSlotTable).build();

		final TaskExecutor taskExecutor = createTaskExecutor(taskManagerServices);

		taskExecutor.start();

		try {
			final TestingResourceManagerGateway testingResourceManagerGateway = new TestingResourceManagerGateway();

			final CompletableFuture<RegistrationResponse> registrationFuture = new CompletableFuture<>();
			final CompletableFuture<ResourceID> taskExecutorResourceIdFuture = new CompletableFuture<>();

			testingResourceManagerGateway.setRegisterTaskExecutorFunction(stringResourceIDSlotReportIntegerHardwareDescriptionTuple5 -> {
				taskExecutorResourceIdFuture.complete(stringResourceIDSlotReportIntegerHardwareDescriptionTuple5.f1);
				return registrationFuture;
			});

			rpc.registerGateway(testingResourceManagerGateway.getAddress(), testingResourceManagerGateway);
			resourceManagerLeaderRetriever.notifyListener(testingResourceManagerGateway.getAddress(), testingResourceManagerGateway.getFencingToken().toUUID());

			final TaskExecutorGateway taskExecutorGateway = taskExecutor.getSelfGateway(TaskExecutorGateway.class);

			final ResourceID resourceId = taskExecutorResourceIdFuture.get();

			final SlotID slotId = new SlotID(resourceId, 0);
			final CompletableFuture<Acknowledge> slotRequestResponse = taskExecutorGateway.requestSlot(slotId, jobId, new AllocationID(), "foobar", testingResourceManagerGateway.getFencingToken(), timeout);

			try {
				slotRequestResponse.get();
				fail("We should not be able to request slots before the TaskExecutor is registered at the ResourceManager.");
			} catch (ExecutionException ee) {
				assertThat(ExceptionUtils.stripExecutionException(ee), instanceOf(TaskManagerException.class));
			}
		} finally {
			RpcUtils.terminateRpcEndpoint(taskExecutor, timeout);
		}
	}

	/**
	 * Tests that the TaskExecutor tries to reconnect to a ResourceManager from which it
	 * was explicitly disconnected.
	 */
	@Test
	public void testReconnectionAttemptIfExplicitlyDisconnected() throws Exception {
		final long heartbeatInterval = 1000L;
		final TaskSlotTable taskSlotTable = new TaskSlotTable(Collections.singleton(ResourceProfile.UNKNOWN), timerService);
		final TaskManagerLocation taskManagerLocation = new LocalTaskManagerLocation();
		final TaskExecutor taskExecutor = new TaskExecutor(
			rpc,
			TaskManagerConfiguration.fromConfiguration(configuration),
			haServices,
			new TaskManagerServicesBuilder()
				.setTaskSlotTable(taskSlotTable)
				.setTaskManagerLocation(taskManagerLocation)
				.build(),
			new HeartbeatServices(heartbeatInterval, 1000L),
			UnregisteredMetricGroups.createUnregisteredTaskManagerMetricGroup(),
			null,
			dummyBlobCacheService,
			testingFatalErrorHandler);

		taskExecutor.start();

		try {
			final TestingResourceManagerGateway testingResourceManagerGateway = new TestingResourceManagerGateway();
			final ClusterInformation clusterInformation = new ClusterInformation("foobar", 1234);
			final CompletableFuture<RegistrationResponse> registrationResponseFuture = CompletableFuture.completedFuture(new TaskExecutorRegistrationSuccess(new InstanceID(), ResourceID.generate(), clusterInformation));
			final BlockingQueue<ResourceID> registrationQueue = new ArrayBlockingQueue<>(1);

			testingResourceManagerGateway.setRegisterTaskExecutorFunction(stringResourceIDSlotReportIntegerHardwareDescriptionTuple5 -> {
				registrationQueue.offer(stringResourceIDSlotReportIntegerHardwareDescriptionTuple5.f1);
				return registrationResponseFuture;
			});
			rpc.registerGateway(testingResourceManagerGateway.getAddress(), testingResourceManagerGateway);

			resourceManagerLeaderRetriever.notifyListener(testingResourceManagerGateway.getAddress(), testingResourceManagerGateway.getFencingToken().toUUID());

			final ResourceID firstRegistrationAttempt = registrationQueue.take();

			assertThat(firstRegistrationAttempt, equalTo(taskManagerLocation.getResourceID()));

			final TaskExecutorGateway taskExecutorGateway = taskExecutor.getSelfGateway(TaskExecutorGateway.class);

			assertThat(registrationQueue, is(empty()));

			taskExecutorGateway.disconnectResourceManager(new FlinkException("Test exception"));

			final ResourceID secondRegistrationAttempt = registrationQueue.take();

			assertThat(secondRegistrationAttempt, equalTo(taskManagerLocation.getResourceID()));

		} finally {
			RpcUtils.terminateRpcEndpoint(taskExecutor, timeout);
		}
	}

	/**
	 * Tests that the {@link TaskExecutor} sends the initial slot report after it
	 * registered at the ResourceManager.
	 */
	@Test
	public void testInitialSlotReport() throws Exception {
		final TaskSlotTable taskSlotTable = new TaskSlotTable(Collections.singleton(ResourceProfile.UNKNOWN), timerService);
		final TaskManagerLocation taskManagerLocation = new LocalTaskManagerLocation();
		final TaskManagerServices taskManagerServices = new TaskManagerServicesBuilder()
			.setTaskSlotTable(taskSlotTable)
			.setTaskManagerLocation(taskManagerLocation)
			.build();
		final TaskExecutor taskExecutor = createTaskExecutor(taskManagerServices);

		taskExecutor.start();

		try {
			final TestingResourceManagerGateway testingResourceManagerGateway = new TestingResourceManagerGateway();
			final CompletableFuture<ResourceID> initialSlotReportFuture = new CompletableFuture<>();

			testingResourceManagerGateway.setSendSlotReportFunction(
				resourceIDInstanceIDSlotReportTuple3 -> {
					initialSlotReportFuture.complete(resourceIDInstanceIDSlotReportTuple3.f0);
					return CompletableFuture.completedFuture(Acknowledge.get());
				});

			rpc.registerGateway(testingResourceManagerGateway.getAddress(), testingResourceManagerGateway);
			resourceManagerLeaderRetriever.notifyListener(testingResourceManagerGateway.getAddress(), testingResourceManagerGateway.getFencingToken().toUUID());

			assertThat(initialSlotReportFuture.get(), equalTo(taskManagerLocation.getResourceID()));
		} finally {
			RpcUtils.terminateRpcEndpoint(taskExecutor, timeout);
		}
	}

	/**
	 * Tests that the {@link TaskExecutor} tries to reconnect if the initial slot report
	 * fails.
	 */
	@Test
	public void testInitialSlotReportFailure() throws Exception {
		final TaskSlotTable taskSlotTable = new TaskSlotTable(Collections.singleton(ResourceProfile.UNKNOWN), timerService);
		final TaskManagerLocation taskManagerLocation = new LocalTaskManagerLocation();
		final TaskManagerServices taskManagerServices = new TaskManagerServicesBuilder()
			.setTaskSlotTable(taskSlotTable)
			.setTaskManagerLocation(taskManagerLocation)
			.build();
		final TaskExecutor taskExecutor = createTaskExecutor(taskManagerServices);

		taskExecutor.start();

		try {
			final TestingResourceManagerGateway testingResourceManagerGateway = new TestingResourceManagerGateway();

			final BlockingQueue<CompletableFuture<Acknowledge>> responseQueue = new ArrayBlockingQueue<>(2);
			testingResourceManagerGateway.setSendSlotReportFunction(
				resourceIDInstanceIDSlotReportTuple3 -> {
					try {
						return responseQueue.take();
					} catch (InterruptedException e) {
						return FutureUtils.completedExceptionally(e);
					}
				});

			final CompletableFuture<RegistrationResponse> registrationResponse = CompletableFuture.completedFuture(
				new TaskExecutorRegistrationSuccess(
					new InstanceID(),
					testingResourceManagerGateway.getOwnResourceId(),
					new ClusterInformation("foobar", 1234)));

			final CountDownLatch numberRegistrations = new CountDownLatch(2);

			testingResourceManagerGateway.setRegisterTaskExecutorFunction(new Function<Tuple4<String, ResourceID, Integer, HardwareDescription>, CompletableFuture<RegistrationResponse>>() {
				@Override
				public CompletableFuture<RegistrationResponse> apply(Tuple4<String, ResourceID, Integer, HardwareDescription> stringResourceIDIntegerHardwareDescriptionTuple4) {
					numberRegistrations.countDown();
					return registrationResponse;
				}
			});

			responseQueue.offer(FutureUtils.completedExceptionally(new FlinkException("Test exception")));
			responseQueue.offer(CompletableFuture.completedFuture(Acknowledge.get()));

			rpc.registerGateway(testingResourceManagerGateway.getAddress(), testingResourceManagerGateway);
			resourceManagerLeaderRetriever.notifyListener(testingResourceManagerGateway.getAddress(), testingResourceManagerGateway.getFencingToken().toUUID());

			//wait for the second registration attempt
			numberRegistrations.await();
		} finally {
			RpcUtils.terminateRpcEndpoint(taskExecutor, timeout);
		}
	}

	/**
	 * Tests that offers slots to job master timeout and retry.
	 */
	@Test
	public void testOfferSlotToJobMasterAfterTimeout() throws Exception {
		final TaskSlotTable taskSlotTable = new TaskSlotTable(
			Arrays.asList(ResourceProfile.UNKNOWN, ResourceProfile.UNKNOWN),
			timerService);
		final TaskManagerServices taskManagerServices = new TaskManagerServicesBuilder()
			.setTaskSlotTable(taskSlotTable)
			.build();

		final TaskExecutor taskExecutor = createTaskExecutor(taskManagerServices);

		final AllocationID allocationId = new AllocationID();

		final CompletableFuture<ResourceID> initialSlotReportFuture = new CompletableFuture<>();

		final TestingResourceManagerGateway testingResourceManagerGateway = new TestingResourceManagerGateway();
		testingResourceManagerGateway.setSendSlotReportFunction(resourceIDInstanceIDSlotReportTuple3 -> {
			initialSlotReportFuture.complete(null);
			return CompletableFuture.completedFuture(Acknowledge.get());

		});
		rpc.registerGateway(testingResourceManagerGateway.getAddress(), testingResourceManagerGateway);
		resourceManagerLeaderRetriever.notifyListener(testingResourceManagerGateway.getAddress(), testingResourceManagerGateway.getFencingToken().toUUID());

		final CountDownLatch slotOfferings = new CountDownLatch(3);
		final CompletableFuture<AllocationID> offeredSlotFuture = new CompletableFuture<>();
		final TestingJobMasterGateway jobMasterGateway = new TestingJobMasterGatewayBuilder()
			.setOfferSlotsFunction((resourceID, slotOffers) -> {
				assertThat(slotOffers.size(), is(1));
				slotOfferings.countDown();

				if (slotOfferings.getCount() == 0) {
					offeredSlotFuture.complete(slotOffers.iterator().next().getAllocationId());
					return CompletableFuture.completedFuture(slotOffers);
				} else {
					return FutureUtils.completedExceptionally(new TimeoutException());
				}
			})
			.build();
		final String jobManagerAddress = jobMasterGateway.getAddress();
		rpc.registerGateway(jobManagerAddress, jobMasterGateway);
		jobManagerLeaderRetriever.notifyListener(jobManagerAddress, jobMasterGateway.getFencingToken().toUUID());

		try {
			taskExecutor.start();
			final TaskExecutorGateway taskExecutorGateway = taskExecutor.getSelfGateway(TaskExecutorGateway.class);

			// wait for the connection to the ResourceManager
			initialSlotReportFuture.get();

			taskExecutorGateway.requestSlot(
				new SlotID(taskExecutor.getResourceID(), 0),
				jobId,
				allocationId,
				jobManagerAddress,
				testingResourceManagerGateway.getFencingToken(),
				timeout).get();

			slotOfferings.await();

			assertThat(offeredSlotFuture.get(), is(allocationId));
			assertTrue(taskSlotTable.isSlotFree(1));
		} finally {
			RpcUtils.terminateRpcEndpoint(taskExecutor, timeout);
		}
	}

	/**
	 * Tests that the TaskExecutor syncs its slots view with the JobMaster's view
	 * via the AllocatedSlotReport reported by the heartbeat (See FLINK-11059).
	 */
	@Test
	public void testSyncSlotsWithJobMasterByHeartbeat() throws Exception {
		final CountDownLatch activeSlots = new CountDownLatch(2);
		final TaskSlotTable taskSlotTable = new ActivateSlotNotifyingTaskSlotTable(
				Arrays.asList(ResourceProfile.UNKNOWN, ResourceProfile.UNKNOWN),
				timerService,
				activeSlots);
		final TaskManagerServices taskManagerServices = new TaskManagerServicesBuilder().setTaskSlotTable(taskSlotTable).build();

		final TaskExecutor taskExecutor = createTaskExecutor(taskManagerServices);

		final TestingResourceManagerGateway testingResourceManagerGateway = new TestingResourceManagerGateway();

		final BlockingQueue<AllocationID> allocationsNotifiedFree = new ArrayBlockingQueue<>(2);

		OneShotLatch initialSlotReporting = new OneShotLatch();
		testingResourceManagerGateway.setSendSlotReportFunction(resourceIDInstanceIDSlotReportTuple3 -> {
			initialSlotReporting.trigger();
			return CompletableFuture.completedFuture(Acknowledge.get());

		});

		testingResourceManagerGateway.setNotifySlotAvailableConsumer(instanceIDSlotIDAllocationIDTuple3 ->
				allocationsNotifiedFree.offer(instanceIDSlotIDAllocationIDTuple3.f2));

		rpc.registerGateway(testingResourceManagerGateway.getAddress(), testingResourceManagerGateway);
		resourceManagerLeaderRetriever.notifyListener(testingResourceManagerGateway.getAddress(), testingResourceManagerGateway.getFencingToken().toUUID());

		final BlockingQueue<AllocationID> failedSlotFutures = new ArrayBlockingQueue<>(2);
		final ResourceID jobManagerResourceId = ResourceID.generate();
		final TestingJobMasterGateway jobMasterGateway = new TestingJobMasterGatewayBuilder()
				.setFailSlotConsumer((resourceID, allocationID, throwable) ->
					failedSlotFutures.offer(allocationID))
				.setOfferSlotsFunction((resourceID, slotOffers) -> CompletableFuture.completedFuture(new ArrayList<>(slotOffers)))
				.setRegisterTaskManagerFunction((ignoredA, ignoredB) -> CompletableFuture.completedFuture(new JMTMRegistrationSuccess(jobManagerResourceId)))
				.build();
		final String jobManagerAddress = jobMasterGateway.getAddress();
		rpc.registerGateway(jobManagerAddress, jobMasterGateway);
		jobManagerLeaderRetriever.notifyListener(jobManagerAddress, jobMasterGateway.getFencingToken().toUUID());

		taskExecutor.start();

		try {
			final TaskExecutorGateway taskExecutorGateway = taskExecutor.getSelfGateway(TaskExecutorGateway.class);

			initialSlotReporting.await();

			final SlotID slotId1 = new SlotID(taskExecutor.getResourceID(), 0);
			final SlotID slotId2 = new SlotID(taskExecutor.getResourceID(), 1);
			final AllocationID allocationIdInBoth = new AllocationID();
			final AllocationID allocationIdOnlyInJM = new AllocationID();
			final AllocationID allocationIdOnlyInTM = new AllocationID();

			taskExecutorGateway.requestSlot(slotId1, jobId, allocationIdInBoth, "foobar", testingResourceManagerGateway.getFencingToken(), timeout);
			taskExecutorGateway.requestSlot(slotId2, jobId, allocationIdOnlyInTM, "foobar", testingResourceManagerGateway.getFencingToken(), timeout);

			activeSlots.await();

			List<AllocatedSlotInfo> allocatedSlotInfos = Arrays.asList(
					new AllocatedSlotInfo(0, allocationIdInBoth),
					new AllocatedSlotInfo(1, allocationIdOnlyInJM)
			);
			AllocatedSlotReport allocatedSlotReport = new AllocatedSlotReport(jobId, allocatedSlotInfos);
			taskExecutorGateway.heartbeatFromJobManager(jobManagerResourceId, allocatedSlotReport);

			assertThat(failedSlotFutures.take(), is(allocationIdOnlyInJM));
			assertThat(allocationsNotifiedFree.take(), is(allocationIdOnlyInTM));
			assertThat(failedSlotFutures.poll(5L, TimeUnit.MILLISECONDS), nullValue());
			assertThat(allocationsNotifiedFree.poll(5L, TimeUnit.MILLISECONDS), nullValue());
		} finally {
			RpcUtils.terminateRpcEndpoint(taskExecutor, timeout);
		}
	}

	@Nonnull
	private TaskExecutor createTaskExecutor(TaskManagerServices taskManagerServices) {
		return new TaskExecutor(
			rpc,
			TaskManagerConfiguration.fromConfiguration(configuration),
			haServices,
			taskManagerServices,
			new HeartbeatServices(1000L, 1000L),
			UnregisteredMetricGroups.createUnregisteredTaskManagerMetricGroup(),
			null,
			dummyBlobCacheService,
			testingFatalErrorHandler);
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
		public <I, O> HeartbeatManager<I, O> createHeartbeatManager(ResourceID resourceId, HeartbeatListener<I, O> heartbeatListener, ScheduledExecutor mainThreadExecutor, Logger log) {
			return new RecordingHeartbeatManagerImpl<>(
				heartbeatTimeout,
				resourceId,
				heartbeatListener,
				mainThreadExecutor,
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
				ScheduledExecutor mainThreadExecutor,
				Logger log,
				BlockingQueue<ResourceID> unmonitoredTargets,
				BlockingQueue<ResourceID> monitoredTargets) {
			super(heartbeatTimeoutIntervalMs, ownResourceID, heartbeatListener, mainThreadExecutor, log);
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

	private static final class ActivateSlotNotifyingTaskSlotTable extends TaskSlotTable {

		private final CountDownLatch slotsToActivate;

		private ActivateSlotNotifyingTaskSlotTable(Collection<ResourceProfile> resourceProfiles, TimerService<AllocationID> timerService, CountDownLatch slotsToActivate) {
			super(resourceProfiles, timerService);
			this.slotsToActivate = slotsToActivate;
		}

		@Override
		public boolean markSlotActive(AllocationID allocationId) throws SlotNotFoundException {
			final boolean result = super.markSlotActive(allocationId);

			if (result) {
				slotsToActivate.countDown();
			}

			return result;
		}
	}
}
