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
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.broadcast.BroadcastVariableManager;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.concurrent.CompletableFuture;
import org.apache.flink.runtime.concurrent.Future;
import org.apache.flink.runtime.concurrent.impl.FlinkCompletableFuture;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.filecache.FileCache;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.NonHaServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.NetworkEnvironment;
import org.apache.flink.runtime.io.network.netty.PartitionStateChecker;
import org.apache.flink.runtime.io.network.partition.ResultPartitionConsumableNotifier;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobmaster.JMTMRegistrationSuccess;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.leaderelection.TestingLeaderRetrievalService;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.messages.taskexecutor.TMSlotRequestRegistered;
import org.apache.flink.runtime.resourcemanager.messages.taskexecutor.TMSlotRequestRejected;
import org.apache.flink.runtime.resourcemanager.messages.taskexecutor.TMSlotRequestReply;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.TestingSerialRpcService;
import org.apache.flink.runtime.taskexecutor.slot.TaskSlotTable;
import org.apache.flink.runtime.taskexecutor.exceptions.SlotAllocationException;
import org.apache.flink.runtime.taskexecutor.slot.TimerService;
import org.apache.flink.runtime.taskmanager.CheckpointResponder;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.runtime.taskmanager.TaskManagerActions;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TestLogger;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import org.junit.rules.TestName;
import org.mockito.Matchers;
import org.powermock.api.mockito.PowerMockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.hamcrest.Matchers.contains;

public class TaskExecutorTest extends TestLogger {

	@Rule
	public TestName name = new TestName();


	@Test
	public void testImmediatelyRegistersIfLeaderIsKnown() throws Exception {
		final ResourceID resourceID = ResourceID.generate();
		final String resourceManagerAddress = "/resource/manager/address/one";

		final TestingSerialRpcService rpc = new TestingSerialRpcService();
		try {
			// register a mock resource manager gateway
			ResourceManagerGateway rmGateway = mock(ResourceManagerGateway.class);
			TaskManagerConfiguration taskManagerServicesConfiguration = mock(TaskManagerConfiguration.class);
			PowerMockito.when(taskManagerServicesConfiguration.getNumberSlots()).thenReturn(1);

			rpc.registerGateway(resourceManagerAddress, rmGateway);

			TaskManagerLocation taskManagerLocation = mock(TaskManagerLocation.class);
			when(taskManagerLocation.getResourceID()).thenReturn(resourceID);

			NonHaServices haServices = new NonHaServices(resourceManagerAddress);

			TaskExecutor taskManager = new TaskExecutor(
				taskManagerServicesConfiguration,
				taskManagerLocation,
				rpc,
				mock(MemoryManager.class),
				mock(IOManager.class),
				mock(NetworkEnvironment.class),
				haServices,
				mock(MetricRegistry.class),
				mock(TaskManagerMetricGroup.class),
				mock(BroadcastVariableManager.class),
				mock(FileCache.class),
				mock(TaskSlotTable.class),
				mock(JobManagerTable.class),
				mock(JobLeaderService.class),
				mock(FatalErrorHandler.class));

			taskManager.start();
			String taskManagerAddress = taskManager.getAddress();

			verify(rmGateway).registerTaskExecutor(
					any(UUID.class), eq(taskManagerAddress), eq(resourceID), any(SlotReport.class), any(Time.class));
		}
		finally {
			rpc.stopService();
		}
	}

	@Test
	public void testTriggerRegistrationOnLeaderChange() throws Exception {
		final ResourceID resourceID = ResourceID.generate();

		final String address1 = "/resource/manager/address/one";
		final String address2 = "/resource/manager/address/two";
		final UUID leaderId1 = UUID.randomUUID();
		final UUID leaderId2 = UUID.randomUUID();

		final TestingSerialRpcService rpc = new TestingSerialRpcService();
		try {
			// register the mock resource manager gateways
			ResourceManagerGateway rmGateway1 = mock(ResourceManagerGateway.class);
			ResourceManagerGateway rmGateway2 = mock(ResourceManagerGateway.class);
			rpc.registerGateway(address1, rmGateway1);
			rpc.registerGateway(address2, rmGateway2);

			TestingLeaderRetrievalService testLeaderService = new TestingLeaderRetrievalService();

			TestingHighAvailabilityServices haServices = new TestingHighAvailabilityServices();
			haServices.setResourceManagerLeaderRetriever(testLeaderService);

			TaskManagerConfiguration taskManagerServicesConfiguration = mock(TaskManagerConfiguration.class);
			PowerMockito.when(taskManagerServicesConfiguration.getNumberSlots()).thenReturn(1);
			PowerMockito.when(taskManagerServicesConfiguration.getConfiguration()).thenReturn(new Configuration());
			PowerMockito.when(taskManagerServicesConfiguration.getTmpDirectories()).thenReturn(new String[1]);

			TaskManagerLocation taskManagerLocation = mock(TaskManagerLocation.class);
			when(taskManagerLocation.getResourceID()).thenReturn(resourceID);
			when(taskManagerLocation.getHostname()).thenReturn("foobar");

			TaskExecutor taskManager = new TaskExecutor(
				taskManagerServicesConfiguration,
				taskManagerLocation,
				rpc,
				mock(MemoryManager.class),
				mock(IOManager.class),
				mock(NetworkEnvironment.class),
				haServices,
				mock(MetricRegistry.class),
				mock(TaskManagerMetricGroup.class),
				mock(BroadcastVariableManager.class),
				mock(FileCache.class),
				mock(TaskSlotTable.class),
				mock(JobManagerTable.class),
				mock(JobLeaderService.class),
				mock(FatalErrorHandler.class));

			taskManager.start();
			String taskManagerAddress = taskManager.getAddress();

			// no connection initially, since there is no leader
			assertNull(taskManager.getResourceManagerConnection());

			// define a leader and see that a registration happens
			testLeaderService.notifyListener(address1, leaderId1);

			verify(rmGateway1).registerTaskExecutor(
					eq(leaderId1), eq(taskManagerAddress), eq(resourceID), any(SlotReport.class), any(Time.class));
			assertNotNull(taskManager.getResourceManagerConnection());

			// cancel the leader 
			testLeaderService.notifyListener(null, null);

			// set a new leader, see that a registration happens 
			testLeaderService.notifyListener(address2, leaderId2);

			verify(rmGateway2).registerTaskExecutor(
					eq(leaderId2), eq(taskManagerAddress), eq(resourceID), any(SlotReport.class), any(Time.class));
			assertNotNull(taskManager.getResourceManagerConnection());
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

		final TaskDeploymentDescriptor tdd = new TaskDeploymentDescriptor(
			jobId,
			allocationId,
			name.getMethodName(),
			jobVertexId,
			new ExecutionAttemptID(),
			new SerializedValue<>(new ExecutionConfig()),
			"test task",
			1,
			0,
			1,
			0,
			configuration,
			configuration,
			TestInvokable.class.getName(),
			Collections.<ResultPartitionDeploymentDescriptor>emptyList(),
			Collections.<InputGateDeploymentDescriptor>emptyList(),
			Collections.<BlobKey>emptyList(),
			Collections.<URL>emptyList(),
			0);

		final LibraryCacheManager libraryCacheManager = mock(LibraryCacheManager.class);
		when(libraryCacheManager.getClassLoader(eq(jobId))).thenReturn(getClass().getClassLoader());

		final JobManagerConnection jobManagerConnection = new JobManagerConnection(
			mock(JobMasterGateway.class),
			jobManagerLeaderId,
			mock(TaskManagerActions.class),
			mock(CheckpointResponder.class),
			libraryCacheManager,
			mock(ResultPartitionConsumableNotifier.class),
			mock(PartitionStateChecker.class));

		final JobManagerTable jobManagerTable = new JobManagerTable();
		jobManagerTable.put(jobId, jobManagerConnection);

		final TaskSlotTable taskSlotTable = mock(TaskSlotTable.class);
		when(taskSlotTable.existsActiveSlot(eq(jobId), eq(allocationId))).thenReturn(true);
		when(taskSlotTable.addTask(any(Task.class))).thenReturn(true);

		final NetworkEnvironment networkEnvironment = mock(NetworkEnvironment.class);

		when(networkEnvironment.createKvStateTaskRegistry(eq(jobId), eq(jobVertexId))).thenReturn(mock(TaskKvStateRegistry.class));

		final TaskManagerMetricGroup taskManagerMetricGroup = mock(TaskManagerMetricGroup.class);

		when(taskManagerMetricGroup.addTaskForJob(eq(tdd))).thenReturn(mock(TaskMetricGroup.class));

		final HighAvailabilityServices haServices = mock(HighAvailabilityServices.class);
		when(haServices.getResourceManagerLeaderRetriever()).thenReturn(mock(LeaderRetrievalService.class));

		try {

			TaskExecutor taskManager = new TaskExecutor(
				taskManagerConfiguration,
				mock(TaskManagerLocation.class),
				rpc,
				mock(MemoryManager.class),
				mock(IOManager.class),
				networkEnvironment,
				haServices,
				mock(MetricRegistry.class),
				taskManagerMetricGroup,
				mock(BroadcastVariableManager.class),
				mock(FileCache.class),
				taskSlotTable,
				jobManagerTable,
				mock(JobLeaderService.class),
				mock(FatalErrorHandler.class));

			taskManager.start();

			taskManager.submitTask(tdd, jobManagerLeaderId);

			Future<Boolean> completionFuture = TestInvokable.completableFuture;

			completionFuture.get();

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
	public void testJobLeaderDetection() throws TestingFatalErrorHandler.TestingException, SlotAllocationException {
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
		final JobLeaderService jobLeaderService = new JobLeaderService(resourceId);
		final TestingFatalErrorHandler testingFatalErrorHandler = new TestingFatalErrorHandler();

		final TestingLeaderRetrievalService resourceManagerLeaderRetrievalService = new TestingLeaderRetrievalService();
		final TestingLeaderRetrievalService jobManagerLeaderRetrievalService = new TestingLeaderRetrievalService();
		haServices.setResourceManagerLeaderRetriever(resourceManagerLeaderRetrievalService);
		haServices.setJobMasterLeaderRetriever(jobId, jobManagerLeaderRetrievalService);

		final String resourceManagerAddress = "rm";
		final UUID resourceManagerLeaderId = UUID.randomUUID();

		final ResourceManagerGateway resourceManagerGateway = mock(ResourceManagerGateway.class);
		final InstanceID registrationId = new InstanceID();

		when(resourceManagerGateway.registerTaskExecutor(
			eq(resourceManagerLeaderId),
			any(String.class),
			eq(resourceId),
			any(SlotReport.class),
			any(Time.class))).thenReturn(FlinkCompletableFuture.<RegistrationResponse>completed(new TaskExecutorRegistrationSuccess(registrationId, 1000L)));

		final String jobManagerAddress = "jm";
		final UUID jobManagerLeaderId = UUID.randomUUID();
		final ResourceID jmResourceId = new ResourceID(jobManagerAddress);
		final int blobPort = 42;

		final JobMasterGateway jobMasterGateway = mock(JobMasterGateway.class);

		when(jobMasterGateway.registerTaskManager(
			any(String.class),
			eq(resourceId),
			eq(jobManagerLeaderId),
			any(Time.class)
		)).thenReturn(FlinkCompletableFuture.<RegistrationResponse>completed(new JMTMRegistrationSuccess(jmResourceId, blobPort)));
		when(jobMasterGateway.getAddress()).thenReturn(jobManagerAddress);

		rpc.registerGateway(resourceManagerAddress, resourceManagerGateway);
		rpc.registerGateway(jobManagerAddress, jobMasterGateway);

		final AllocationID allocationId = new AllocationID();
		final SlotID slotId = new SlotID(resourceId, 0);

		try {
			TaskExecutor taskManager = new TaskExecutor(
				taskManagerConfiguration,
				taskManagerLocation,
				rpc,
				mock(MemoryManager.class),
				mock(IOManager.class),
				mock(NetworkEnvironment.class),
				haServices,
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
			TMSlotRequestReply reply = taskManager.requestSlot(slotId, jobId, allocationId, jobManagerAddress, resourceManagerLeaderId);

			// this is hopefully successful :-)
			assertTrue(reply instanceof TMSlotRequestRegistered);

			// now inform the task manager about the new job leader
			jobManagerLeaderRetrievalService.notifyListener(jobManagerAddress, jobManagerLeaderId);

			// the job leader should get the allocation id offered
			verify(jobMasterGateway).offerSlots((Iterable<AllocationID>)Matchers.argThat(contains(allocationId)), eq(jobManagerLeaderId), any(Time.class));
		} finally {
			// check if a concurrent error occurred
			testingFatalErrorHandler.rethrowException();

			rpc.stopService();
		}
	}

	/**
	 * Tests that accepted slots go into state assigned and the others are returned to the  resource
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
		final JobLeaderService jobLeaderService = new JobLeaderService(resourceId);
		final TestingFatalErrorHandler testingFatalErrorHandler = new TestingFatalErrorHandler();

		final String resourceManagerAddress = "rm";
		final UUID resourceManagerLeaderId = UUID.randomUUID();

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
			any(Time.class))).thenReturn(FlinkCompletableFuture.<RegistrationResponse>completed(new TaskExecutorRegistrationSuccess(registrationId, 1000L)));

		final ResourceID jmResourceId = new ResourceID(jobManagerAddress);
		final int blobPort = 42;

		final AllocationID allocationId1 = new AllocationID();
		final AllocationID allocationId2 = new AllocationID();

		final JobMasterGateway jobMasterGateway = mock(JobMasterGateway.class);

		when(jobMasterGateway.registerTaskManager(
			any(String.class),
			eq(resourceId),
			eq(jobManagerLeaderId),
			any(Time.class)
		)).thenReturn(FlinkCompletableFuture.<RegistrationResponse>completed(new JMTMRegistrationSuccess(jmResourceId, blobPort)));
		when(jobMasterGateway.getAddress()).thenReturn(jobManagerAddress);

		when(jobMasterGateway.offerSlots(any(Iterable.class), eq(jobManagerLeaderId), any(Time.class)))
			.thenReturn(FlinkCompletableFuture.completed((Iterable<AllocationID>)Collections.singleton(allocationId1)));

		rpc.registerGateway(resourceManagerAddress, resourceManagerGateway);
		rpc.registerGateway(jobManagerAddress, jobMasterGateway);

		try {
			TaskExecutor taskManager = new TaskExecutor(
				taskManagerConfiguration,
				taskManagerLocation,
				rpc,
				mock(MemoryManager.class),
				mock(IOManager.class),
				mock(NetworkEnvironment.class),
				haServices,
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

			verify(resourceManagerGateway).notifySlotAvailable(eq(resourceManagerLeaderId), eq(registrationId), eq(new SlotID(resourceId, 1)));

			assertTrue(taskSlotTable.existsActiveSlot(jobId, allocationId1));
			assertFalse(taskSlotTable.existsActiveSlot(jobId, allocationId2));
			assertTrue(taskSlotTable.isSlotFree(1));
		} finally {
			// check if a concurrent error occurred
			testingFatalErrorHandler.rethrowException();

			rpc.stopService();
		}
	}

	private static class TestingFatalErrorHandler implements FatalErrorHandler {
		private static final Logger LOG = LoggerFactory.getLogger(TestingFatalErrorHandler.class);
		private final AtomicReference<Throwable> atomicThrowable;

		public TestingFatalErrorHandler() {
			atomicThrowable = new AtomicReference<>(null);
		}

		public void rethrowException() throws TestingException {
			Throwable throwable = atomicThrowable.get();

			if (throwable != null) {
				throw new TestingException(throwable);
			}
		}

		public boolean hasExceptionOccurred() {
			return atomicThrowable.get() != null;
		}

		public Throwable getException() {
			return atomicThrowable.get();
		}

		@Override
		public void onFatalError(Throwable exception) {
			LOG.error("OnFatalError:", exception);
			atomicThrowable.compareAndSet(null, exception);
		}

		//------------------------------------------------------------------
		// static utility classes
		//------------------------------------------------------------------

		private static final class TestingException extends Exception {
			public TestingException(String message) {
				super(message);
			}

			public TestingException(String message, Throwable cause) {
				super(message, cause);
			}

			public TestingException(Throwable cause) {
				super(cause);
			}

			private static final long serialVersionUID = -4648195335470914498L;
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
	public void testRejectAllocationRequestsForOutOfSyncSlots() throws SlotAllocationException {
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

			TestingLeaderRetrievalService testLeaderService = new TestingLeaderRetrievalService();

			TestingHighAvailabilityServices haServices = new TestingHighAvailabilityServices();
			haServices.setResourceManagerLeaderRetriever(testLeaderService);

			TaskManagerConfiguration taskManagerServicesConfiguration = mock(TaskManagerConfiguration.class);
			PowerMockito.when(taskManagerServicesConfiguration.getNumberSlots()).thenReturn(1);

			TaskManagerLocation taskManagerLocation = mock(TaskManagerLocation.class);
			when(taskManagerLocation.getResourceID()).thenReturn(resourceID);

			TaskExecutor taskManager = new TaskExecutor(
				taskManagerServicesConfiguration,
				taskManagerLocation,
				rpc,
				mock(MemoryManager.class),
				mock(IOManager.class),
				mock(NetworkEnvironment.class),
				haServices,
				mock(MetricRegistry.class),
				mock(TaskManagerMetricGroup.class),
				mock(BroadcastVariableManager.class),
				mock(FileCache.class),
				mock(TaskSlotTable.class),
				mock(JobManagerTable.class),
				mock(JobLeaderService.class),
				mock(FatalErrorHandler.class));

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
			TMSlotRequestReply tmSlotRequestReply = taskManager.requestSlot(slotID, jobId, new AllocationID(), jobManagerAddress, leaderId);
			assertTrue(tmSlotRequestReply instanceof TMSlotRequestRegistered);

			// TODO: Figure out the concrete allocation behaviour between RM and TM. Maybe we don't need the SlotID...
			// test that we can't allocate slots which are blacklisted due to pending confirmation of the RM
			final SlotID unconfirmedFreeSlotID = new SlotID(resourceID, 1);
			TMSlotRequestReply tmSlotRequestReply2 =
				taskManager.requestSlot(unconfirmedFreeSlotID, jobId, new AllocationID(), jobManagerAddress, leaderId);
			assertTrue(tmSlotRequestReply2 instanceof TMSlotRequestRejected);

			// re-register
			verify(rmGateway1).registerTaskExecutor(
				eq(leaderId), eq(taskManagerAddress), eq(resourceID), any(SlotReport.class), any(Time.class));
			testLeaderService.notifyListener(address1, leaderId);

			// now we should be successful because the slots status has been synced
			// test that we can't allocate slots which are blacklisted due to pending confirmation of the RM
			TMSlotRequestReply tmSlotRequestReply3 =
				taskManager.requestSlot(unconfirmedFreeSlotID, jobId, new AllocationID(), jobManagerAddress, leaderId);
			assertTrue(tmSlotRequestReply3 instanceof TMSlotRequestRegistered);

		}
		finally {
			rpc.stopService();
		}

	}
}
