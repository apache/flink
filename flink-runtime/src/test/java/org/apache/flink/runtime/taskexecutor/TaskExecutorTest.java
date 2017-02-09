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
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.broadcast.BroadcastVariableManager;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.concurrent.CompletableFuture;
import org.apache.flink.runtime.concurrent.Future;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.concurrent.impl.FlinkCompletableFuture;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.JobInformation;
import org.apache.flink.runtime.executiongraph.TaskInformation;
import org.apache.flink.runtime.filecache.FileCache;
import org.apache.flink.runtime.heartbeat.HeartbeatListener;
import org.apache.flink.runtime.heartbeat.HeartbeatManagerImpl;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.NonHaServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.NetworkEnvironment;
import org.apache.flink.runtime.io.network.netty.PartitionProducerStateChecker;
import org.apache.flink.runtime.io.network.partition.ResultPartitionConsumableNotifier;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobmaster.JMTMRegistrationSuccess;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.leaderelection.TestingLeaderRetrievalService;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.rpc.TestingSerialRpcService;
import org.apache.flink.runtime.taskexecutor.exceptions.SlotAllocationException;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskexecutor.slot.TaskSlotTable;
import org.apache.flink.runtime.taskexecutor.slot.TimerService;
import org.apache.flink.runtime.taskmanager.CheckpointResponder;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.runtime.taskmanager.TaskManagerActions;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TestLogger;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.mockito.Matchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;

import java.net.InetAddress;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

public class TaskExecutorTest extends TestLogger {

	@Rule
	public TestName name = new TestName();

	@Test
	public void testHeartbeatTimeoutWithJobManager() throws Exception {
		final JobID jobId = new JobID();
		final Configuration configuration = new Configuration();
		final TaskManagerConfiguration tmConfig = TaskManagerConfiguration.fromConfiguration(configuration);
		final ResourceID tmResourceId = new ResourceID("tm");
		final TaskManagerLocation taskManagerLocation = new TaskManagerLocation(tmResourceId, InetAddress.getLoopbackAddress(), 1234);
		final TaskSlotTable taskSlotTable = new TaskSlotTable(Arrays.asList(mock(ResourceProfile.class)), mock(TimerService.class));

		final TestingSerialRpcService rpc = new TestingSerialRpcService();
		final JobLeaderService jobLeaderService = new JobLeaderService(taskManagerLocation);
		final TestingHighAvailabilityServices haServices = new TestingHighAvailabilityServices();
		final TestingLeaderRetrievalService rmLeaderRetrievalService = new TestingLeaderRetrievalService(
			null,
			null);
		final TestingLeaderRetrievalService jmLeaderRetrievalService = new TestingLeaderRetrievalService(
			null,
			null);
		haServices.setJobMasterLeaderRetriever(jobId, jmLeaderRetrievalService);
		haServices.setResourceManagerLeaderRetriever(rmLeaderRetrievalService);

		final TestingFatalErrorHandler testingFatalErrorHandler = new TestingFatalErrorHandler();

		final long heartbeatTimeout = 10L;

		HeartbeatServices heartbeatServices = mock(HeartbeatServices.class);
		when(heartbeatServices.createHeartbeatManager(
			eq(taskManagerLocation.getResourceID()),
			any(HeartbeatListener.class),
			any(ScheduledExecutor.class),
			any(Logger.class))).thenAnswer(
			new Answer<HeartbeatManagerImpl<Void, Void>>() {
				@Override
				public HeartbeatManagerImpl<Void, Void> answer(InvocationOnMock invocation) throws Throwable {
					return new HeartbeatManagerImpl<>(
						heartbeatTimeout,
						taskManagerLocation.getResourceID(),
						(HeartbeatListener<Void, Void>)invocation.getArguments()[1],
						(Executor)invocation.getArguments()[2],
						(ScheduledExecutor)invocation.getArguments()[2],
						(Logger)invocation.getArguments()[3]);
				}
			}
		);

		final String jobMasterAddress = "jm";
		final UUID jmLeaderId = UUID.randomUUID();
		final ResourceID jmResourceId = new ResourceID(jobMasterAddress);
		final JobMasterGateway jobMasterGateway = mock(JobMasterGateway.class);
		final int blobPort = 42;

		when(jobMasterGateway.registerTaskManager(
				any(String.class),
				eq(taskManagerLocation),
				eq(jmLeaderId),
				any(Time.class)
		)).thenReturn(FlinkCompletableFuture.<RegistrationResponse>completed(new JMTMRegistrationSuccess(jmResourceId, blobPort)));
		when(jobMasterGateway.getAddress()).thenReturn(jobMasterAddress);
		when(jobMasterGateway.getHostname()).thenReturn("localhost");

		try {
			final TaskExecutor taskManager = new TaskExecutor(
				rpc,
				tmConfig,
				taskManagerLocation,
				mock(MemoryManager.class),
				mock(IOManager.class),
				mock(NetworkEnvironment.class),
				haServices,
				heartbeatServices,
				mock(MetricRegistry.class),
				mock(TaskManagerMetricGroup.class),
				mock(BroadcastVariableManager.class),
				mock(FileCache.class),
				taskSlotTable,
				new JobManagerTable(),
				jobLeaderService,
				testingFatalErrorHandler);

			taskManager.start();

			rpc.registerGateway(jobMasterAddress, jobMasterGateway);

			// we have to add the job after the TaskExecutor, because otherwise the service has not
			// been properly started.
			jobLeaderService.addJob(jobId, jobMasterAddress);

			// now inform the task manager about the new job leader
			jmLeaderRetrievalService.notifyListener(jobMasterAddress, jmLeaderId);

			// register task manager success will trigger monitoring heartbeat target between tm and jm
			verify(jobMasterGateway).registerTaskManager(
					eq(taskManager.getAddress()), eq(taskManagerLocation), eq(jmLeaderId), any(Time.class));

			// the timeout should trigger disconnecting from the JobManager
			verify(jobMasterGateway, timeout(heartbeatTimeout * 50L)).disconnectTaskManager(eq(taskManagerLocation.getResourceID()), any(TimeoutException.class));

			// check if a concurrent error occurred
			testingFatalErrorHandler.rethrowError();

		} finally {
			rpc.stopService();
		}
	}

	@Test
	public void testHeartbeatTimeoutWithResourceManager() throws Exception {
		final String rmAddress = "rm";
		final String tmAddress = "tm";
		final ResourceID rmResourceId = new ResourceID(rmAddress);
		final ResourceID tmResourceId = new ResourceID(tmAddress);
		final UUID rmLeaderId = UUID.randomUUID();

		// register the mock resource manager gateway
		ResourceManagerGateway rmGateway = mock(ResourceManagerGateway.class);
		when(rmGateway.registerTaskExecutor(
			any(UUID.class), anyString(), any(ResourceID.class), any(SlotReport.class), any(Time.class)))
			.thenReturn(
				FlinkCompletableFuture.<RegistrationResponse>completed(
					new TaskExecutorRegistrationSuccess(
						new InstanceID(),
						rmResourceId,
						10L)));

		final TestingSerialRpcService rpc = new TestingSerialRpcService();
		rpc.registerGateway(rmAddress, rmGateway);

		final TestingLeaderRetrievalService testLeaderService = new TestingLeaderRetrievalService(
			null,
			null);
		final TestingHighAvailabilityServices haServices = new TestingHighAvailabilityServices();
		haServices.setResourceManagerLeaderRetriever(testLeaderService);

		final TaskManagerConfiguration taskManagerConfiguration = mock(TaskManagerConfiguration.class);
		when(taskManagerConfiguration.getNumberSlots()).thenReturn(1);

		final TaskManagerLocation taskManagerLocation = mock(TaskManagerLocation.class);
		when(taskManagerLocation.getResourceID()).thenReturn(tmResourceId);

		final TaskSlotTable taskSlotTable = mock(TaskSlotTable.class);
		final SlotReport slotReport = new SlotReport();
		when(taskSlotTable.createSlotReport(any(ResourceID.class))).thenReturn(slotReport);

		final TestingFatalErrorHandler testingFatalErrorHandler = new TestingFatalErrorHandler();

		final long heartbeatTimeout = 10L;
		HeartbeatServices heartbeatServices = mock(HeartbeatServices.class);
		when(heartbeatServices.createHeartbeatManager(
			eq(taskManagerLocation.getResourceID()),
			any(HeartbeatListener.class),
			any(ScheduledExecutor.class),
			any(Logger.class))).thenAnswer(
			new Answer<HeartbeatManagerImpl<Void, Void>>() {
				@Override
				public HeartbeatManagerImpl<Void, Void> answer(InvocationOnMock invocation) throws Throwable {
					return new HeartbeatManagerImpl<>(
						heartbeatTimeout,
						taskManagerLocation.getResourceID(),
						(HeartbeatListener<Void, Void>)invocation.getArguments()[1],
						(Executor)invocation.getArguments()[2],
						(ScheduledExecutor)invocation.getArguments()[2],
						(Logger)invocation.getArguments()[3]);
					}
				}
		);

		try {
			final TaskExecutor taskManager = new TaskExecutor(
				rpc,
				taskManagerConfiguration,
				taskManagerLocation,
				mock(MemoryManager.class),
				mock(IOManager.class),
				mock(NetworkEnvironment.class),
				haServices,
				heartbeatServices,
				mock(MetricRegistry.class),
				mock(TaskManagerMetricGroup.class),
				mock(BroadcastVariableManager.class),
				mock(FileCache.class),
				taskSlotTable,
				mock(JobManagerTable.class),
				mock(JobLeaderService.class),
				testingFatalErrorHandler);

			taskManager.start();

			// define a leader and see that a registration happens
			testLeaderService.notifyListener(rmAddress, rmLeaderId);

			// register resource manager success will trigger monitoring heartbeat target between tm and rm
			verify(rmGateway, atLeast(1)).registerTaskExecutor(
					eq(rmLeaderId), eq(taskManager.getAddress()), eq(tmResourceId), any(SlotReport.class), any(Time.class));

			// heartbeat timeout should trigger disconnect TaskManager from ResourceManager
			verify(rmGateway, timeout(heartbeatTimeout * 50L)).disconnectTaskManager(eq(taskManagerLocation.getResourceID()), any(TimeoutException.class));

			// check if a concurrent error occurred
			testingFatalErrorHandler.rethrowError();

		} finally {
			rpc.stopService();
		}
	}

	@Test
	public void testImmediatelyRegistersIfLeaderIsKnown() throws Exception {
		final ResourceID resourceID = ResourceID.generate();
		final String resourceManagerAddress = "/resource/manager/address/one";
		final ResourceID resourceManagerResourceId = new ResourceID(resourceManagerAddress);

		final TestingSerialRpcService rpc = new TestingSerialRpcService();
		try {
			// register a mock resource manager gateway
			ResourceManagerGateway rmGateway = mock(ResourceManagerGateway.class);
			when(rmGateway.registerTaskExecutor(
					any(UUID.class), anyString(), any(ResourceID.class), any(SlotReport.class), any(Time.class)))
				.thenReturn(FlinkCompletableFuture.<RegistrationResponse>completed(new TaskExecutorRegistrationSuccess(
					new InstanceID(), resourceManagerResourceId, 10L)));

			TaskManagerConfiguration taskManagerServicesConfiguration = mock(TaskManagerConfiguration.class);
			when(taskManagerServicesConfiguration.getNumberSlots()).thenReturn(1);

			rpc.registerGateway(resourceManagerAddress, rmGateway);

			TaskManagerLocation taskManagerLocation = mock(TaskManagerLocation.class);
			when(taskManagerLocation.getResourceID()).thenReturn(resourceID);

			NonHaServices haServices = new NonHaServices(resourceManagerAddress);

			final TaskSlotTable taskSlotTable = mock(TaskSlotTable.class);
			final SlotReport slotReport = new SlotReport();
			when(taskSlotTable.createSlotReport(any(ResourceID.class))).thenReturn(slotReport);

			final TestingFatalErrorHandler testingFatalErrorHandler = new TestingFatalErrorHandler();

			TaskExecutor taskManager = new TaskExecutor(
				rpc,
				taskManagerServicesConfiguration,
				taskManagerLocation,
				mock(MemoryManager.class),
				mock(IOManager.class),
				mock(NetworkEnvironment.class),
				haServices,
				mock(HeartbeatServices.class, RETURNS_MOCKS),
				mock(MetricRegistry.class),
				mock(TaskManagerMetricGroup.class),
				mock(BroadcastVariableManager.class),
				mock(FileCache.class),
				taskSlotTable,
				mock(JobManagerTable.class),
				mock(JobLeaderService.class),
				testingFatalErrorHandler);

			taskManager.start();
			String taskManagerAddress = taskManager.getAddress();

			verify(rmGateway).registerTaskExecutor(
					any(UUID.class), eq(taskManagerAddress), eq(resourceID), eq(slotReport), any(Time.class));

			// check if a concurrent error occurred
			testingFatalErrorHandler.rethrowError();
		}
		finally {
			rpc.stopService();
		}
	}

	@Test
	public void testTriggerRegistrationOnLeaderChange() throws Exception {
		final ResourceID tmResourceID = ResourceID.generate();

		final String address1 = "/resource/manager/address/one";
		final String address2 = "/resource/manager/address/two";
		final UUID leaderId1 = UUID.randomUUID();
		final UUID leaderId2 = UUID.randomUUID();
		final ResourceID rmResourceId1 = new ResourceID(address1);
		final ResourceID rmResourceId2 = new ResourceID(address2);

		final TestingSerialRpcService rpc = new TestingSerialRpcService();
		try {
			// register the mock resource manager gateways
			ResourceManagerGateway rmGateway1 = mock(ResourceManagerGateway.class);
			ResourceManagerGateway rmGateway2 = mock(ResourceManagerGateway.class);

			when(rmGateway1.registerTaskExecutor(
					any(UUID.class), anyString(), any(ResourceID.class), any(SlotReport.class), any(Time.class)))
					.thenReturn(FlinkCompletableFuture.<RegistrationResponse>completed(
						new TaskExecutorRegistrationSuccess(new InstanceID(), rmResourceId1, 10L)));
			when(rmGateway2.registerTaskExecutor(
					any(UUID.class), anyString(), any(ResourceID.class), any(SlotReport.class), any(Time.class)))
					.thenReturn(FlinkCompletableFuture.<RegistrationResponse>completed(
						new TaskExecutorRegistrationSuccess(new InstanceID(), rmResourceId2, 10L)));

			rpc.registerGateway(address1, rmGateway1);
			rpc.registerGateway(address2, rmGateway2);

			TestingLeaderRetrievalService testLeaderService = new TestingLeaderRetrievalService(
				null,
				null);

			TestingHighAvailabilityServices haServices = new TestingHighAvailabilityServices();
			haServices.setResourceManagerLeaderRetriever(testLeaderService);

			TaskManagerConfiguration taskManagerServicesConfiguration = mock(TaskManagerConfiguration.class);
			when(taskManagerServicesConfiguration.getNumberSlots()).thenReturn(1);
			when(taskManagerServicesConfiguration.getConfiguration()).thenReturn(new Configuration());
			when(taskManagerServicesConfiguration.getTmpDirectories()).thenReturn(new String[1]);

			TaskManagerLocation taskManagerLocation = mock(TaskManagerLocation.class);
			when(taskManagerLocation.getResourceID()).thenReturn(tmResourceID);
			when(taskManagerLocation.getHostname()).thenReturn("foobar");

			final TaskSlotTable taskSlotTable = mock(TaskSlotTable.class);
			final SlotReport slotReport = new SlotReport();
			when(taskSlotTable.createSlotReport(any(ResourceID.class))).thenReturn(slotReport);

			final TestingFatalErrorHandler testingFatalErrorHandler = new TestingFatalErrorHandler();

			TaskExecutor taskManager = new TaskExecutor(
				rpc,
				taskManagerServicesConfiguration,
				taskManagerLocation,
				mock(MemoryManager.class),
				mock(IOManager.class),
				mock(NetworkEnvironment.class),
				haServices,
				mock(HeartbeatServices.class, RETURNS_MOCKS),
				mock(MetricRegistry.class),
				mock(TaskManagerMetricGroup.class),
				mock(BroadcastVariableManager.class),
				mock(FileCache.class),
				taskSlotTable,
				mock(JobManagerTable.class),
				mock(JobLeaderService.class),
				testingFatalErrorHandler);

			taskManager.start();
			String taskManagerAddress = taskManager.getAddress();

			// no connection initially, since there is no leader
			assertNull(taskManager.getResourceManagerConnection());

			// define a leader and see that a registration happens
			testLeaderService.notifyListener(address1, leaderId1);

			verify(rmGateway1).registerTaskExecutor(
					eq(leaderId1), eq(taskManagerAddress), eq(tmResourceID), any(SlotReport.class), any(Time.class));
			assertNotNull(taskManager.getResourceManagerConnection());

			// cancel the leader 
			testLeaderService.notifyListener(null, null);

			// set a new leader, see that a registration happens 
			testLeaderService.notifyListener(address2, leaderId2);

			verify(rmGateway2).registerTaskExecutor(
					eq(leaderId2), eq(taskManagerAddress), eq(tmResourceID), eq(slotReport), any(Time.class));
			assertNotNull(taskManager.getResourceManagerConnection());

			// check if a concurrent error occurred
			testingFatalErrorHandler.rethrowError();
		}
		finally {
			rpc.stopService();
		}
	}

	/**
	 * Tests that we can submit a task to the TaskManager given that we've allocated a slot there.
	 */
	@Test(timeout = 1000L)
	public void testTaskSubmission() throws Exception {
		final Configuration configuration = new Configuration();

		final TestingSerialRpcService rpc = new TestingSerialRpcService();
		final TaskManagerConfiguration taskManagerConfiguration = TaskManagerConfiguration.fromConfiguration(configuration);
		final JobID jobId = new JobID();
		final AllocationID allocationId = new AllocationID();
		final UUID jobManagerLeaderId = UUID.randomUUID();
		final JobVertexID jobVertexId = new JobVertexID();

		JobInformation jobInformation = new JobInformation(
				jobId,
				name.getMethodName(),
				new SerializedValue<>(new ExecutionConfig()),
				new Configuration(),
				Collections.<BlobKey>emptyList(),
				Collections.<URL>emptyList());

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
				serializedJobInformation,
				serializedJobVertexInformation,
				new ExecutionAttemptID(),
				allocationId,
				0,
				0,
				0,
				null,
				Collections.<ResultPartitionDeploymentDescriptor>emptyList(),
				Collections.<InputGateDeploymentDescriptor>emptyList());

		final LibraryCacheManager libraryCacheManager = mock(LibraryCacheManager.class);
		when(libraryCacheManager.getClassLoader(eq(jobId))).thenReturn(getClass().getClassLoader());

		final JobManagerConnection jobManagerConnection = new JobManagerConnection(
			jobId,
			ResourceID.generate(),
			mock(JobMasterGateway.class),
			jobManagerLeaderId,
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

		final NetworkEnvironment networkEnvironment = mock(NetworkEnvironment.class);

		when(networkEnvironment.createKvStateTaskRegistry(eq(jobId), eq(jobVertexId))).thenReturn(mock(TaskKvStateRegistry.class));

		final TaskManagerMetricGroup taskManagerMetricGroup = mock(TaskManagerMetricGroup.class);

		TaskMetricGroup taskMetricGroup = mock(TaskMetricGroup.class);
		when(taskMetricGroup.getIOMetricGroup()).thenReturn(mock(TaskIOMetricGroup.class));

		when(taskManagerMetricGroup.addTaskForJob(
				any(JobID.class), anyString(), any(JobVertexID.class), any(ExecutionAttemptID.class),
				anyString(), anyInt(), anyInt())
			).thenReturn(taskMetricGroup);

		final HighAvailabilityServices haServices = mock(HighAvailabilityServices.class);
		when(haServices.getResourceManagerLeaderRetriever()).thenReturn(mock(LeaderRetrievalService.class));

		try {
			final TestingFatalErrorHandler testingFatalErrorHandler = new TestingFatalErrorHandler();

			TaskExecutor taskManager = new TaskExecutor(
				rpc,
				taskManagerConfiguration,
				mock(TaskManagerLocation.class),
				mock(MemoryManager.class),
				mock(IOManager.class),
				networkEnvironment,
				haServices,
				mock(HeartbeatServices.class, RETURNS_MOCKS),
				mock(MetricRegistry.class),
				taskManagerMetricGroup,
				mock(BroadcastVariableManager.class),
				mock(FileCache.class),
				taskSlotTable,
				jobManagerTable,
				mock(JobLeaderService.class),
				testingFatalErrorHandler);

			taskManager.start();

			taskManager.submitTask(tdd, jobManagerLeaderId);

			Future<Boolean> completionFuture = TestInvokable.completableFuture;

			completionFuture.get();

			// check if a concurrent error occurred
			testingFatalErrorHandler.rethrowError();
		} finally {
			rpc.stopService();
		}
	}

	/**
	 * Test invokable which completes the given future when executed.
	 */
	public static class TestInvokable extends AbstractInvokable {

		static final CompletableFuture<Boolean> completableFuture = new FlinkCompletableFuture<>();

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
		final JobID jobId = new JobID();

		final TestingSerialRpcService rpc = new TestingSerialRpcService();
		final Configuration configuration = new Configuration();
		final TaskManagerConfiguration taskManagerConfiguration = TaskManagerConfiguration.fromConfiguration(configuration);
		final ResourceID resourceId = new ResourceID("foobar");
		final TaskManagerLocation taskManagerLocation = new TaskManagerLocation(resourceId, InetAddress.getLoopbackAddress(), 1234);
		final TestingHighAvailabilityServices haServices = new TestingHighAvailabilityServices();
		final TimerService<AllocationID> timerService = mock(TimerService.class);
		final TaskSlotTable taskSlotTable = new TaskSlotTable(Arrays.asList(mock(ResourceProfile.class)), timerService);
		final JobManagerTable jobManagerTable = new JobManagerTable();
		final JobLeaderService jobLeaderService = new JobLeaderService(taskManagerLocation);
		final TestingFatalErrorHandler testingFatalErrorHandler = new TestingFatalErrorHandler();

		final TestingLeaderRetrievalService resourceManagerLeaderRetrievalService = new TestingLeaderRetrievalService(
			null,
			null);
		final TestingLeaderRetrievalService jobManagerLeaderRetrievalService = new TestingLeaderRetrievalService(
			null,
			null);
		haServices.setResourceManagerLeaderRetriever(resourceManagerLeaderRetrievalService);
		haServices.setJobMasterLeaderRetriever(jobId, jobManagerLeaderRetrievalService);

		final String resourceManagerAddress = "rm";
		final UUID resourceManagerLeaderId = UUID.randomUUID();
		final ResourceID resourceManagerResourceId = new ResourceID(resourceManagerAddress);

		final ResourceManagerGateway resourceManagerGateway = mock(ResourceManagerGateway.class);
		final InstanceID registrationId = new InstanceID();

		when(resourceManagerGateway.registerTaskExecutor(
			eq(resourceManagerLeaderId),
			any(String.class),
			eq(resourceId),
			any(SlotReport.class),
			any(Time.class))).thenReturn(FlinkCompletableFuture.<RegistrationResponse>completed(new TaskExecutorRegistrationSuccess(registrationId, resourceManagerResourceId, 1000L)));

		final String jobManagerAddress = "jm";
		final UUID jobManagerLeaderId = UUID.randomUUID();
		final ResourceID jmResourceId = new ResourceID(jobManagerAddress);
		final int blobPort = 42;

		final JobMasterGateway jobMasterGateway = mock(JobMasterGateway.class);

		when(jobMasterGateway.registerTaskManager(
				any(String.class),
				eq(taskManagerLocation),
				eq(jobManagerLeaderId),
				any(Time.class)
		)).thenReturn(FlinkCompletableFuture.<RegistrationResponse>completed(new JMTMRegistrationSuccess(jmResourceId, blobPort)));
		when(jobMasterGateway.getHostname()).thenReturn(jobManagerAddress);
		when(jobMasterGateway.offerSlots(
			any(ResourceID.class),
			any(Iterable.class),
			any(UUID.class),
			any(Time.class))).thenReturn(mock(Future.class, RETURNS_MOCKS));

		rpc.registerGateway(resourceManagerAddress, resourceManagerGateway);
		rpc.registerGateway(jobManagerAddress, jobMasterGateway);

		final AllocationID allocationId = new AllocationID();
		final SlotID slotId = new SlotID(resourceId, 0);
		final SlotOffer slotOffer = new SlotOffer(allocationId, 0, ResourceProfile.UNKNOWN);

		try {
			TaskExecutor taskManager = new TaskExecutor(
				rpc,
				taskManagerConfiguration,
				taskManagerLocation,
				mock(MemoryManager.class),
				mock(IOManager.class),
				mock(NetworkEnvironment.class),
				haServices,
				mock(HeartbeatServices.class, RETURNS_MOCKS),
				mock(MetricRegistry.class),
				mock(TaskManagerMetricGroup.class),
				mock(BroadcastVariableManager.class),
				mock(FileCache.class),
				taskSlotTable,
				jobManagerTable,
				jobLeaderService,
				testingFatalErrorHandler);

			taskManager.start();

			// tell the task manager about the rm leader
			resourceManagerLeaderRetrievalService.notifyListener(resourceManagerAddress, resourceManagerLeaderId);

			// request slots from the task manager under the given allocation id
			taskManager.requestSlot(slotId, jobId, allocationId, jobManagerAddress, resourceManagerLeaderId);

			// now inform the task manager about the new job leader
			jobManagerLeaderRetrievalService.notifyListener(jobManagerAddress, jobManagerLeaderId);

			// the job leader should get the allocation id offered
			verify(jobMasterGateway).offerSlots(
					any(ResourceID.class),
					(Iterable<SlotOffer>)Matchers.argThat(contains(slotOffer)),
					eq(jobManagerLeaderId),
					any(Time.class));

			// check if a concurrent error occurred
			testingFatalErrorHandler.rethrowError();
		} finally {
			rpc.stopService();
		}
	}

	/**
	 * Tests that accepted slots go into state assigned and the others are returned to the resource
	 * manager.
	 */
	@Test
	public void testSlotAcceptance() throws Exception {
		final JobID jobId = new JobID();

		final TestingSerialRpcService rpc = new TestingSerialRpcService();
		final Configuration configuration = new Configuration();
		final TaskManagerConfiguration taskManagerConfiguration = TaskManagerConfiguration.fromConfiguration(configuration);
		final ResourceID resourceId = new ResourceID("foobar");
		final TaskManagerLocation taskManagerLocation = new TaskManagerLocation(resourceId, InetAddress.getLoopbackAddress(), 1234);
		final TestingHighAvailabilityServices haServices = new TestingHighAvailabilityServices();
		final TimerService<AllocationID> timerService = mock(TimerService.class);
		final TaskSlotTable taskSlotTable = new TaskSlotTable(Arrays.asList(mock(ResourceProfile.class), mock(ResourceProfile.class)), timerService);
		final JobManagerTable jobManagerTable = new JobManagerTable();
		final JobLeaderService jobLeaderService = new JobLeaderService(taskManagerLocation);
		final TestingFatalErrorHandler testingFatalErrorHandler = new TestingFatalErrorHandler();

		final String resourceManagerAddress = "rm";
		final UUID resourceManagerLeaderId = UUID.randomUUID();
		final ResourceID resourceManagerResourceId = new ResourceID(resourceManagerAddress);

		final String jobManagerAddress = "jm";
		final UUID jobManagerLeaderId = UUID.randomUUID();

		final LeaderRetrievalService resourceManagerLeaderRetrievalService = new TestingLeaderRetrievalService(resourceManagerAddress, resourceManagerLeaderId);
		final LeaderRetrievalService jobManagerLeaderRetrievalService = new TestingLeaderRetrievalService(jobManagerAddress, jobManagerLeaderId);
		haServices.setResourceManagerLeaderRetriever(resourceManagerLeaderRetrievalService);
		haServices.setJobMasterLeaderRetriever(jobId, jobManagerLeaderRetrievalService);

		final ResourceManagerGateway resourceManagerGateway = mock(ResourceManagerGateway.class);
		final InstanceID registrationId = new InstanceID();

		when(resourceManagerGateway.registerTaskExecutor(
			eq(resourceManagerLeaderId),
			any(String.class),
			eq(resourceId),
			any(SlotReport.class),
			any(Time.class))).thenReturn(FlinkCompletableFuture.<RegistrationResponse>completed(new TaskExecutorRegistrationSuccess(registrationId, resourceManagerResourceId, 1000L)));

		final ResourceID jmResourceId = new ResourceID(jobManagerAddress);
		final int blobPort = 42;

		final AllocationID allocationId1 = new AllocationID();
		final AllocationID allocationId2 = new AllocationID();

		final SlotOffer offer1 = new SlotOffer(allocationId1, 0, ResourceProfile.UNKNOWN);

		final JobMasterGateway jobMasterGateway = mock(JobMasterGateway.class);

		when(jobMasterGateway.registerTaskManager(
				any(String.class),
				eq(taskManagerLocation),
				eq(jobManagerLeaderId),
				any(Time.class)
		)).thenReturn(FlinkCompletableFuture.<RegistrationResponse>completed(new JMTMRegistrationSuccess(jmResourceId, blobPort)));
		when(jobMasterGateway.getHostname()).thenReturn(jobManagerAddress);

		when(jobMasterGateway.offerSlots(
				any(ResourceID.class), any(Iterable.class), eq(jobManagerLeaderId), any(Time.class)))
			.thenReturn(FlinkCompletableFuture.completed((Iterable<SlotOffer>)Collections.singleton(offer1)));

		rpc.registerGateway(resourceManagerAddress, resourceManagerGateway);
		rpc.registerGateway(jobManagerAddress, jobMasterGateway);

		try {
			TaskExecutor taskManager = new TaskExecutor(
				rpc,
				taskManagerConfiguration,
				taskManagerLocation,
				mock(MemoryManager.class),
				mock(IOManager.class),
				mock(NetworkEnvironment.class),
				haServices,
				mock(HeartbeatServices.class, RETURNS_MOCKS),
				mock(MetricRegistry.class),
				mock(TaskManagerMetricGroup.class),
				mock(BroadcastVariableManager.class),
				mock(FileCache.class),
				taskSlotTable,
				jobManagerTable,
				jobLeaderService,
				testingFatalErrorHandler);

			taskManager.start();

			taskSlotTable.allocateSlot(0, jobId, allocationId1, Time.milliseconds(10000L));
			taskSlotTable.allocateSlot(1, jobId, allocationId2, Time.milliseconds(10000L));

			// we have to add the job after the TaskExecutor, because otherwise the service has not
			// been properly started.
			jobLeaderService.addJob(jobId, jobManagerAddress);

			verify(resourceManagerGateway).notifySlotAvailable(
				eq(resourceManagerLeaderId),
				eq(registrationId),
				eq(new SlotID(resourceId, 1)),
				eq(allocationId2));

			assertTrue(taskSlotTable.existsActiveSlot(jobId, allocationId1));
			assertFalse(taskSlotTable.existsActiveSlot(jobId, allocationId2));
			assertTrue(taskSlotTable.isSlotFree(1));

			// check if a concurrent error occurred
			testingFatalErrorHandler.rethrowError();
		} finally {
			rpc.stopService();
		}
	}

	/**
	 * Tests that all allocation requests for slots are ignored if the slot has been reported as
	 * free by the TaskExecutor but this report hasn't been confirmed by the ResourceManager.
	 *
	 * This is essential for the correctness of the state of the ResourceManager.
	 */
	@Ignore
	@Test
	public void testRejectAllocationRequestsForOutOfSyncSlots() throws Exception {
		final ResourceID resourceID = ResourceID.generate();

		final String address1 = "/resource/manager/address/one";
		final UUID leaderId = UUID.randomUUID();
		final JobID jobId = new JobID();
		final String jobManagerAddress = "foobar";

		final TestingSerialRpcService rpc = new TestingSerialRpcService();
		try {
			// register the mock resource manager gateways
			ResourceManagerGateway rmGateway1 = mock(ResourceManagerGateway.class);
			rpc.registerGateway(address1, rmGateway1);

			TestingLeaderRetrievalService testLeaderService = new TestingLeaderRetrievalService(
				"localhost",
				HighAvailabilityServices.DEFAULT_LEADER_ID);

			TestingHighAvailabilityServices haServices = new TestingHighAvailabilityServices();
			haServices.setResourceManagerLeaderRetriever(testLeaderService);

			TaskManagerConfiguration taskManagerServicesConfiguration = mock(TaskManagerConfiguration.class);
			when(taskManagerServicesConfiguration.getNumberSlots()).thenReturn(1);

			TaskManagerLocation taskManagerLocation = mock(TaskManagerLocation.class);
			when(taskManagerLocation.getResourceID()).thenReturn(resourceID);

			final TestingFatalErrorHandler testingFatalErrorHandler = new TestingFatalErrorHandler();

			TaskExecutor taskManager = new TaskExecutor(
				rpc,
				taskManagerServicesConfiguration,
				taskManagerLocation,
				mock(MemoryManager.class),
				mock(IOManager.class),
				mock(NetworkEnvironment.class),
				haServices,
				mock(HeartbeatServices.class, RETURNS_MOCKS),
				mock(MetricRegistry.class),
				mock(TaskManagerMetricGroup.class),
				mock(BroadcastVariableManager.class),
				mock(FileCache.class),
				mock(TaskSlotTable.class),
				mock(JobManagerTable.class),
				mock(JobLeaderService.class),
				testingFatalErrorHandler);

			taskManager.start();
			String taskManagerAddress = taskManager.getAddress();

			// no connection initially, since there is no leader
			assertNull(taskManager.getResourceManagerConnection());

			// define a leader and see that a registration happens
			testLeaderService.notifyListener(address1, leaderId);

			verify(rmGateway1).registerTaskExecutor(
				eq(leaderId), eq(taskManagerAddress), eq(resourceID), any(SlotReport.class), any(Time.class));
			assertNotNull(taskManager.getResourceManagerConnection());

			// test that allocating a slot works
			final SlotID slotID = new SlotID(resourceID, 0);
			taskManager.requestSlot(slotID, jobId, new AllocationID(), jobManagerAddress, leaderId);

			// TODO: Figure out the concrete allocation behaviour between RM and TM. Maybe we don't need the SlotID...
			// test that we can't allocate slots which are blacklisted due to pending confirmation of the RM
			final SlotID unconfirmedFreeSlotID = new SlotID(resourceID, 1);

			try {
				taskManager.requestSlot(unconfirmedFreeSlotID, jobId, new AllocationID(), jobManagerAddress, leaderId);

				fail("The slot request should have failed.");
			} catch (SlotAllocationException e) {
				// expected
			}

			// re-register
			verify(rmGateway1).registerTaskExecutor(
				eq(leaderId), eq(taskManagerAddress), eq(resourceID), any(SlotReport.class), any(Time.class));
			testLeaderService.notifyListener(address1, leaderId);

			// now we should be successful because the slots status has been synced
			// test that we can't allocate slots which are blacklisted due to pending confirmation of the RM
			taskManager.requestSlot(unconfirmedFreeSlotID, jobId, new AllocationID(), jobManagerAddress, leaderId);

			// check if a concurrent error occurred
			testingFatalErrorHandler.rethrowError();
		}
		finally {
			rpc.stopService();
		}

	}

	/**
	 * This tests task executor receive SubmitTask before OfferSlot response.
	 */
	@Test
	public void testSubmitTaskBeforeAcceptSlot() throws Exception {
		final JobID jobId = new JobID();

		final TestingSerialRpcService rpc = new TestingSerialRpcService();
		final Configuration configuration = new Configuration();
		final TaskManagerConfiguration taskManagerConfiguration = TaskManagerConfiguration.fromConfiguration(configuration);
		final ResourceID resourceId = new ResourceID("foobar");
		final TaskManagerLocation taskManagerLocation = new TaskManagerLocation(resourceId, InetAddress.getLoopbackAddress(), 1234);
		final TestingHighAvailabilityServices haServices = new TestingHighAvailabilityServices();
		final TimerService<AllocationID> timerService = mock(TimerService.class);
		final TaskSlotTable taskSlotTable = new TaskSlotTable(Arrays.asList(mock(ResourceProfile.class), mock(ResourceProfile.class)), timerService);
		final JobManagerTable jobManagerTable = new JobManagerTable();
		final JobLeaderService jobLeaderService = new JobLeaderService(taskManagerLocation);
		final TestingFatalErrorHandler testingFatalErrorHandler = new TestingFatalErrorHandler();

		final String resourceManagerAddress = "rm";
		final UUID resourceManagerLeaderId = UUID.randomUUID();
		final ResourceID resourceManagerResourceId = new ResourceID(resourceManagerAddress);

		final String jobManagerAddress = "jm";
		final UUID jobManagerLeaderId = UUID.randomUUID();

		final LeaderRetrievalService resourceManagerLeaderRetrievalService = new TestingLeaderRetrievalService(resourceManagerAddress, resourceManagerLeaderId);
		final LeaderRetrievalService jobManagerLeaderRetrievalService = new TestingLeaderRetrievalService(jobManagerAddress, jobManagerLeaderId);
		haServices.setResourceManagerLeaderRetriever(resourceManagerLeaderRetrievalService);
		haServices.setJobMasterLeaderRetriever(jobId, jobManagerLeaderRetrievalService);

		final ResourceManagerGateway resourceManagerGateway = mock(ResourceManagerGateway.class);
		final InstanceID registrationId = new InstanceID();

		when(resourceManagerGateway.registerTaskExecutor(
			eq(resourceManagerLeaderId),
			any(String.class),
			eq(resourceId),
			any(SlotReport.class),
			any(Time.class))).thenReturn(
				FlinkCompletableFuture.<RegistrationResponse>completed(new TaskExecutorRegistrationSuccess(registrationId, resourceManagerResourceId, 1000L)));

		final ResourceID jmResourceId = new ResourceID(jobManagerAddress);
		final int blobPort = 42;

		final AllocationID allocationId1 = new AllocationID();
		final AllocationID allocationId2 = new AllocationID();

		final SlotOffer offer1 = new SlotOffer(allocationId1, 0, ResourceProfile.UNKNOWN);

		final JobMasterGateway jobMasterGateway = mock(JobMasterGateway.class);

		when(jobMasterGateway.registerTaskManager(
			any(String.class),
			eq(taskManagerLocation),
			eq(jobManagerLeaderId),
			any(Time.class)
		)).thenReturn(FlinkCompletableFuture.<RegistrationResponse>completed(new JMTMRegistrationSuccess(jmResourceId, blobPort)));
		when(jobMasterGateway.getHostname()).thenReturn(jobManagerAddress);


		rpc.registerGateway(resourceManagerAddress, resourceManagerGateway);
		rpc.registerGateway(jobManagerAddress, jobMasterGateway);

		final LibraryCacheManager libraryCacheManager = mock(LibraryCacheManager.class);
		when(libraryCacheManager.getClassLoader(eq(jobId))).thenReturn(getClass().getClassLoader());

		final JobManagerConnection jobManagerConnection = new JobManagerConnection(
			jobId,
			jmResourceId,
			jobMasterGateway,
			jobManagerLeaderId,
			mock(TaskManagerActions.class),
			mock(CheckpointResponder.class),
			libraryCacheManager,
			mock(ResultPartitionConsumableNotifier.class),
			mock(PartitionProducerStateChecker.class));

		jobManagerTable.put(jobId, jobManagerConnection);

		try {
			final TaskManagerMetricGroup taskManagerMetricGroup = mock(TaskManagerMetricGroup.class);
			TaskMetricGroup taskMetricGroup = mock(TaskMetricGroup.class);
			when(taskMetricGroup.getIOMetricGroup()).thenReturn(mock(TaskIOMetricGroup.class));

			when(taskManagerMetricGroup.addTaskForJob(
				any(JobID.class), anyString(), any(JobVertexID.class), any(ExecutionAttemptID.class),
				anyString(), anyInt(), anyInt())
			).thenReturn(taskMetricGroup);

			final TaskExecutor taskManager = new TaskExecutor(
				rpc,
				taskManagerConfiguration,
				taskManagerLocation,
				mock(MemoryManager.class),
				mock(IOManager.class),
				mock(NetworkEnvironment.class),
				haServices,
				mock(HeartbeatServices.class, RETURNS_MOCKS),
				mock(MetricRegistry.class),
				taskManagerMetricGroup,
				mock(BroadcastVariableManager.class),
				mock(FileCache.class),
				taskSlotTable,
				jobManagerTable,
				jobLeaderService,
				testingFatalErrorHandler);
			taskManager.start();
			taskSlotTable.allocateSlot(0, jobId, allocationId1, Time.milliseconds(10000L));
			taskSlotTable.allocateSlot(1, jobId, allocationId2, Time.milliseconds(10000L));

			final JobVertexID jobVertexId = new JobVertexID();

			JobInformation jobInformation = new JobInformation(
				jobId,
				name.getMethodName(),
				new SerializedValue<>(new ExecutionConfig()),
				new Configuration(),
				Collections.<BlobKey>emptyList(),
				Collections.<URL>emptyList());

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
				serializedJobInformation,
				serializedJobVertexInformation,
				new ExecutionAttemptID(),
				allocationId1,
				0,
				0,
				0,
				null,
				Collections.<ResultPartitionDeploymentDescriptor>emptyList(),
				Collections.<InputGateDeploymentDescriptor>emptyList());

			CompletableFuture<Iterable<SlotOffer>> offerResultFuture = new FlinkCompletableFuture<>();

			// submit task first and then return acceptance response
			when(
				jobMasterGateway.offerSlots(
					any(ResourceID.class),
					any(Iterable.class),
					eq(jobManagerLeaderId),
					any(Time.class)))
				.thenReturn(offerResultFuture);

			// we have to add the job after the TaskExecutor, because otherwise the service has not
			// been properly started. This will also offer the slots to the job master
			jobLeaderService.addJob(jobId, jobManagerAddress);

			verify(jobMasterGateway).offerSlots(any(ResourceID.class), any(Iterable.class), eq(jobManagerLeaderId), any(Time.class));

			// submit the task without having acknowledge the offered slots
			taskManager.submitTask(tdd, jobManagerLeaderId);

			// acknowledge the offered slots
			offerResultFuture.complete(Collections.singleton(offer1));

			verify(resourceManagerGateway).notifySlotAvailable(
				eq(resourceManagerLeaderId),
				eq(registrationId),
				eq(new SlotID(resourceId, 1)),
				any(AllocationID.class));

			assertTrue(taskSlotTable.existsActiveSlot(jobId, allocationId1));
			assertFalse(taskSlotTable.existsActiveSlot(jobId, allocationId2));
			assertTrue(taskSlotTable.isSlotFree(1));

			// check if a concurrent error occurred
			testingFatalErrorHandler.rethrowError();
		} finally {
			rpc.stopService();
		}

	}
}
