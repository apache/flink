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
import org.apache.flink.api.common.resources.CPUResource;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.blob.BlobCacheService;
import org.apache.flink.runtime.blob.TransientBlobKey;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptorBuilder;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.execution.librarycache.TestingClassLoaderLease;
import org.apache.flink.runtime.externalresource.ExternalResourceInfoProvider;
import org.apache.flink.runtime.heartbeat.HeartbeatListener;
import org.apache.flink.runtime.heartbeat.HeartbeatManager;
import org.apache.flink.runtime.heartbeat.HeartbeatManagerImpl;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.heartbeat.HeartbeatTarget;
import org.apache.flink.runtime.heartbeat.TestingHeartbeatServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironment;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironmentBuilder;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.TaskExecutorPartitionTracker;
import org.apache.flink.runtime.io.network.partition.TaskExecutorPartitionTrackerImpl;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobmaster.AllocatedSlotInfo;
import org.apache.flink.runtime.jobmaster.AllocatedSlotReport;
import org.apache.flink.runtime.jobmaster.JMTMRegistrationSuccess;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGateway;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGatewayBuilder;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.leaderretrieval.SettableLeaderRetrievalService;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.registration.RegistrationResponse.Decline;
import org.apache.flink.runtime.registration.RetryingRegistrationConfiguration;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.TaskExecutorRegistration;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.shuffle.ShuffleEnvironment;
import org.apache.flink.runtime.state.TaskExecutorLocalStateStoresManager;
import org.apache.flink.runtime.taskexecutor.TaskSubmissionTestEnvironment.Builder;
import org.apache.flink.runtime.taskexecutor.exceptions.RegistrationTimeoutException;
import org.apache.flink.runtime.taskexecutor.exceptions.TaskManagerException;
import org.apache.flink.runtime.taskexecutor.exceptions.TaskSubmissionException;
import org.apache.flink.runtime.taskexecutor.partition.ClusterPartitionReport;
import org.apache.flink.runtime.taskexecutor.slot.SlotNotFoundException;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskexecutor.slot.TaskSlotTable;
import org.apache.flink.runtime.taskexecutor.slot.TaskSlotTableImpl;
import org.apache.flink.runtime.taskexecutor.slot.TaskSlotUtils;
import org.apache.flink.runtime.taskexecutor.slot.TestingTaskSlotTable;
import org.apache.flink.runtime.taskmanager.LocalUnresolvedTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.runtime.taskmanager.UnresolvedTaskManagerLocation;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.NetUtils;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.TimeUtils;
import org.apache.flink.util.function.FunctionUtils;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.slf4j.Logger;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.taskexecutor.slot.TaskSlotUtils.DEFAULT_RESOURCE_PROFILE;
import static org.apache.flink.runtime.taskexecutor.slot.TaskSlotUtils.createDefaultTimerService;
import static org.apache.flink.runtime.taskexecutor.slot.TaskSlotUtils.createTotalResourceProfile;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for the {@link TaskExecutor}.
 */
public class TaskExecutorTest extends TestLogger {

	public static final HeartbeatServices HEARTBEAT_SERVICES = new HeartbeatServices(1000L, 1000L);

	private static final TaskExecutorResourceSpec TM_RESOURCE_SPEC = new TaskExecutorResourceSpec(
		new CPUResource(1.0),
		MemorySize.parse("1m"),
		MemorySize.parse("2m"),
		MemorySize.parse("3m"),
		MemorySize.parse("4m"));

	@Rule
	public final TemporaryFolder tmp = new TemporaryFolder();

	@Rule
	public final TestName testName = new TestName();

	private static final Time timeout = Time.milliseconds(10000L);

	private TestingRpcService rpc;

	private BlobCacheService dummyBlobCacheService;

	private Configuration configuration;

	private UnresolvedTaskManagerLocation unresolvedTaskManagerLocation;

	private JobID jobId;

	private TestingFatalErrorHandler testingFatalErrorHandler;

	private TestingHighAvailabilityServices haServices;

	private SettableLeaderRetrievalService resourceManagerLeaderRetriever;

	private SettableLeaderRetrievalService jobManagerLeaderRetriever;

	private NettyShuffleEnvironment nettyShuffleEnvironment;

	@Before
	public void setup() throws IOException {
		rpc = new TestingRpcService();

		dummyBlobCacheService = new BlobCacheService(
			new Configuration(),
			new VoidBlobStore(),
			null);

		configuration = new Configuration();

		unresolvedTaskManagerLocation = new LocalUnresolvedTaskManagerLocation();
		jobId = new JobID();

		testingFatalErrorHandler = new TestingFatalErrorHandler();

		haServices = new TestingHighAvailabilityServices();
		resourceManagerLeaderRetriever = new SettableLeaderRetrievalService();
		jobManagerLeaderRetriever = new SettableLeaderRetrievalService();
		haServices.setResourceManagerLeaderRetriever(resourceManagerLeaderRetriever);
		haServices.setJobMasterLeaderRetriever(jobId, jobManagerLeaderRetriever);

		nettyShuffleEnvironment = new NettyShuffleEnvironmentBuilder().build();
	}

	@After
	public void teardown() throws Exception {
		if (rpc != null) {
			RpcUtils.terminateRpcService(rpc, timeout);
			rpc = null;
		}

		if (dummyBlobCacheService != null) {
			dummyBlobCacheService.close();
			dummyBlobCacheService = null;
		}

		if (nettyShuffleEnvironment != null) {
			nettyShuffleEnvironment.close();
		}

		testingFatalErrorHandler.rethrowError();
	}

	@Test
	public void testShouldShutDownTaskManagerServicesInPostStop() throws Exception {
		final TaskSlotTableImpl<Task> taskSlotTable = TaskSlotUtils.createTaskSlotTable(1);

		final JobLeaderService jobLeaderService = new DefaultJobLeaderService(unresolvedTaskManagerLocation, RetryingRegistrationConfiguration.defaultConfiguration());

		final IOManager ioManager = new IOManagerAsync(tmp.newFolder().getAbsolutePath());

		final TaskExecutorLocalStateStoresManager localStateStoresManager = new TaskExecutorLocalStateStoresManager(
			false,
			ioManager.getSpillingDirectories(),
			Executors.directExecutor());

		nettyShuffleEnvironment.start();

		final KvStateService kvStateService = new KvStateService(new KvStateRegistry(), null, null);
		kvStateService.start();

		final TaskManagerServices taskManagerServices = new TaskManagerServicesBuilder()
			.setUnresolvedTaskManagerLocation(unresolvedTaskManagerLocation)
			.setIoManager(ioManager)
			.setShuffleEnvironment(nettyShuffleEnvironment)
			.setKvStateService(kvStateService)
			.setTaskSlotTable(taskSlotTable)
			.setJobLeaderService(jobLeaderService)
			.setTaskStateManager(localStateStoresManager)
			.build();

		final TaskExecutor taskManager = createTaskExecutor(taskManagerServices);

		try {
			taskManager.start();
		} finally {
			RpcUtils.terminateRpcEndpoint(taskManager, timeout);
		}

		assertThat(taskSlotTable.isClosed(), is(true));
		assertThat(nettyShuffleEnvironment.isClosed(), is(true));
		assertThat(kvStateService.isShutdown(), is(true));
	}

	@Test
	public void testHeartbeatTimeoutWithJobManager() throws Exception {
		final TaskSlotTable taskSlotTable = TaskSlotUtils.createTaskSlotTable(1);

		final JobLeaderService jobLeaderService = new DefaultJobLeaderService(unresolvedTaskManagerLocation, RetryingRegistrationConfiguration.defaultConfiguration());

		final TestingHeartbeatServices heartbeatServices = new TestingHeartbeatServices();

		final String jobMasterAddress = "jm";
		final UUID jmLeaderId = UUID.randomUUID();

		final ResourceID jmResourceId = ResourceID.generate();
		final CountDownLatch registrationAttempts = new CountDownLatch(2);
		final OneShotLatch slotOfferedLatch = new OneShotLatch();
		final CompletableFuture<ResourceID> disconnectTaskManagerFuture = new CompletableFuture<>();
		final TestingJobMasterGateway jobMasterGateway = new TestingJobMasterGatewayBuilder()
			.setRegisterTaskManagerFunction((s, taskManagerUnresolvedLocation) -> {
				registrationAttempts.countDown();
				return CompletableFuture.completedFuture(new JMTMRegistrationSuccess(jmResourceId));
			})
			.setDisconnectTaskManagerFunction(
				resourceID -> {
					disconnectTaskManagerFuture.complete(resourceID);
					return CompletableFuture.completedFuture(Acknowledge.get());
				})
			.setOfferSlotsFunction((resourceID, slotOffers) -> {
				slotOfferedLatch.trigger();
				return CompletableFuture.completedFuture(slotOffers);
			})
			.build();

		final TaskExecutorLocalStateStoresManager localStateStoresManager = createTaskExecutorLocalStateStoresManager();

		final TaskManagerServices taskManagerServices = new TaskManagerServicesBuilder()
			.setUnresolvedTaskManagerLocation(unresolvedTaskManagerLocation)
			.setTaskSlotTable(taskSlotTable)
			.setJobLeaderService(jobLeaderService)
			.setTaskStateManager(localStateStoresManager)
			.build();

		final TestingTaskExecutor taskManager = createTestingTaskExecutor(taskManagerServices, heartbeatServices);

		final OneShotLatch slotReportReceived = new OneShotLatch();
		final TestingResourceManagerGateway testingResourceManagerGateway = new TestingResourceManagerGateway();
		testingResourceManagerGateway.setSendSlotReportFunction(ignored -> {
			slotReportReceived.trigger();
			return CompletableFuture.completedFuture(Acknowledge.get());
		});

		final Queue<CompletableFuture<RegistrationResponse>> registrationResponses = new ArrayDeque<>();
		registrationResponses.add(CompletableFuture.completedFuture(new TaskExecutorRegistrationSuccess(new InstanceID(), testingResourceManagerGateway.getOwnResourceId(), new ClusterInformation("foobar", 1234))));
		registrationResponses.add(new CompletableFuture<>());
		testingResourceManagerGateway.setRegisterTaskExecutorFunction(taskExecutorRegistration -> registrationResponses.poll());

		rpc.registerGateway(jobMasterAddress, jobMasterGateway);
		rpc.registerGateway(testingResourceManagerGateway.getAddress(), testingResourceManagerGateway);

		try {
			taskManager.start();
			taskManager.waitUntilStarted();

			final TaskExecutorGateway taskExecutorGateway = taskManager.getSelfGateway(TaskExecutorGateway.class);

			resourceManagerLeaderRetriever.notifyListener(testingResourceManagerGateway.getAddress(), testingResourceManagerGateway.getFencingToken().toUUID());
			slotReportReceived.await();

			taskExecutorGateway.requestSlot(
				new SlotID(unresolvedTaskManagerLocation.getResourceID(), 0),
				jobId,
				new AllocationID(),
				ResourceProfile.UNKNOWN,
				jobMasterAddress,
				testingResourceManagerGateway.getFencingToken(),
				timeout).join();

			// now inform the task manager about the new job leader
			jobManagerLeaderRetriever.notifyListener(jobMasterAddress, jmLeaderId);

			// register task manager success will trigger monitoring heartbeat target between tm and jm
			slotOfferedLatch.await();

			// trigger the JM heartbeat timeout
			heartbeatServices.triggerHeartbeatTimeout(unresolvedTaskManagerLocation.getResourceID(), jmResourceId);

			// the timeout should trigger disconnecting from the JobManager
			final ResourceID resourceID = disconnectTaskManagerFuture.get();
			assertThat(resourceID, equalTo(unresolvedTaskManagerLocation.getResourceID()));

			assertTrue(
				"The TaskExecutor should try to reconnect to the JM",
				registrationAttempts.await(timeout.toMilliseconds(), TimeUnit.SECONDS));
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
				taskExecutorRegistrationFuture.complete(registration.getResourceId());
				registrationAttempts.countDown();
				return CompletableFuture.completedFuture(registrationResponse);
			});

		final CompletableFuture<ResourceID> taskExecutorDisconnectFuture = new CompletableFuture<>();
		rmGateway.setDisconnectTaskExecutorConsumer(
			disconnectInfo -> taskExecutorDisconnectFuture.complete(disconnectInfo.f0));

		rpc.registerGateway(rmAddress, rmGateway);

		final TaskSlotTable<Task> taskSlotTable = TestingTaskSlotTable
			.<Task>newBuilder()
			.createSlotReportSupplier(SlotReport::new)
			.closeAsyncReturns(CompletableFuture.completedFuture(null))
			.build();

		HeartbeatServices heartbeatServices = new HeartbeatServices(heartbeatInterval, heartbeatTimeout);

		final TaskExecutorLocalStateStoresManager localStateStoresManager = createTaskExecutorLocalStateStoresManager();

		final TaskManagerServices taskManagerServices = new TaskManagerServicesBuilder()
			.setUnresolvedTaskManagerLocation(unresolvedTaskManagerLocation)
			.setTaskSlotTable(taskSlotTable)
			.setTaskStateManager(localStateStoresManager)
			.build();

		final TaskExecutor taskManager = createTaskExecutor(taskManagerServices, heartbeatServices);

		try {
			taskManager.start();

			// define a leader and see that a registration happens
			resourceManagerLeaderRetriever.notifyListener(rmAddress, rmLeaderId.toUUID());

			// register resource manager success will trigger monitoring heartbeat target between tm and rm
			assertThat(taskExecutorRegistrationFuture.get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS), equalTo(unresolvedTaskManagerLocation.getResourceID()));

			// heartbeat timeout should trigger disconnect TaskManager from ResourceManager
			assertThat(taskExecutorDisconnectFuture.get(heartbeatTimeout * 50L, TimeUnit.MILLISECONDS), equalTo(unresolvedTaskManagerLocation.getResourceID()));

			assertTrue(
				"The TaskExecutor should try to reconnect to the RM",
				registrationAttempts.await(timeout.toMilliseconds(), TimeUnit.SECONDS));
		} finally {
			RpcUtils.terminateRpcEndpoint(taskManager, timeout);
		}
	}

	/**
	 * Tests that the correct partition/slot report is sent as part of the heartbeat response.
	 */
	@Test
	public void testHeartbeatReporting() throws Exception {
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

		rmGateway.setRegisterTaskExecutorFunction(taskExecutorRegistration -> {
			taskExecutorRegistrationFuture.complete(taskExecutorRegistration.getResourceId());
			return registrationResponse;
		});

		final CompletableFuture<SlotReport> initialSlotReportFuture = new CompletableFuture<>();
		rmGateway.setSendSlotReportFunction(resourceIDInstanceIDSlotReportTuple3 -> {
			initialSlotReportFuture.complete(resourceIDInstanceIDSlotReportTuple3.f2);
			return CompletableFuture.completedFuture(Acknowledge.get());
		});

		final CompletableFuture<TaskExecutorHeartbeatPayload> heartbeatPayloadCompletableFuture = new CompletableFuture<>();
		rmGateway.setTaskExecutorHeartbeatConsumer((resourceID, heartbeatPayload) -> heartbeatPayloadCompletableFuture.complete(heartbeatPayload));

		rpc.registerGateway(rmAddress, rmGateway);

		final SlotID slotId = new SlotID(unresolvedTaskManagerLocation.getResourceID(), 0);
		final ResourceProfile resourceProfile = ResourceProfile.fromResources(1.0, 1);
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

		final Queue<SlotReport> reports = new ArrayDeque<>(Arrays.asList(slotReport1, slotReport2));
		final TaskSlotTable<Task> taskSlotTable = TestingTaskSlotTable
			.<Task>newBuilder()
			.createSlotReportSupplier(reports::poll)
			.closeAsyncReturns(CompletableFuture.completedFuture(null))
			.build();

		final TaskExecutorLocalStateStoresManager localStateStoresManager = createTaskExecutorLocalStateStoresManager();

		final TaskManagerServices taskManagerServices = new TaskManagerServicesBuilder()
			.setUnresolvedTaskManagerLocation(unresolvedTaskManagerLocation)
			.setTaskSlotTable(taskSlotTable)
			.setTaskStateManager(localStateStoresManager)
			.build();

		final TaskExecutorPartitionTracker partitionTracker = createPartitionTrackerWithFixedPartitionReport(taskManagerServices.getShuffleEnvironment());

		final TaskExecutor taskManager = createTaskExecutor(taskManagerServices, HEARTBEAT_SERVICES, partitionTracker);

		try {
			taskManager.start();

			// define a leader and see that a registration happens
			resourceManagerLeaderRetriever.notifyListener(rmAddress, rmLeaderId);

			// register resource manager success will trigger monitoring heartbeat target between tm and rm
			assertThat(taskExecutorRegistrationFuture.get(), equalTo(unresolvedTaskManagerLocation.getResourceID()));
			assertThat(initialSlotReportFuture.get(), equalTo(slotReport1));

			TaskExecutorGateway taskExecutorGateway = taskManager.getSelfGateway(TaskExecutorGateway.class);

			// trigger the heartbeat asynchronously
			taskExecutorGateway.heartbeatFromResourceManager(rmResourceId);

			// wait for heartbeat response
			SlotReport actualSlotReport = heartbeatPayloadCompletableFuture.get().getSlotReport();

			// the new slot report should be reported
			assertEquals(slotReport2, actualSlotReport);

			ClusterPartitionReport actualClusterPartitionReport = heartbeatPayloadCompletableFuture.get().getClusterPartitionReport();
			assertEquals(partitionTracker.createClusterPartitionReport(), actualClusterPartitionReport);
		} finally {
			RpcUtils.terminateRpcEndpoint(taskManager, timeout);
		}
	}

	private static TaskExecutorPartitionTracker createPartitionTrackerWithFixedPartitionReport(ShuffleEnvironment<?, ?> shuffleEnvironment) {
		final ClusterPartitionReport.ClusterPartitionReportEntry clusterPartitionReportEntry =
			new ClusterPartitionReport.ClusterPartitionReportEntry(
				new IntermediateDataSetID(),
				Collections.singleton(new ResultPartitionID()),
				4);

		final ClusterPartitionReport clusterPartitionReport = new ClusterPartitionReport(Collections.singletonList(clusterPartitionReportEntry));

		return new TaskExecutorPartitionTrackerImpl(shuffleEnvironment) {
			@Override
			public ClusterPartitionReport createClusterPartitionReport() {
				return clusterPartitionReport;
			}
		};
	}

	@Test
	public void testImmediatelyRegistersIfLeaderIsKnown() throws Exception {
		final String resourceManagerAddress = "/resource/manager/address/one";

		final TestingResourceManagerGateway testingResourceManagerGateway = new TestingResourceManagerGateway();
		final CountDownLatch taskManagerRegisteredLatch = new CountDownLatch(1);
		testingResourceManagerGateway.setRegisterTaskExecutorFunction(FunctionUtils.uncheckedFunction(
			ignored -> {
				taskManagerRegisteredLatch.countDown();
				return CompletableFuture.completedFuture(new TaskExecutorRegistrationSuccess(
					new InstanceID(), new ResourceID(resourceManagerAddress), new ClusterInformation("localhost", 1234)));
			}
		));

		rpc.registerGateway(resourceManagerAddress, testingResourceManagerGateway);

		final TaskSlotTable taskSlotTable = TaskSlotUtils.createTaskSlotTable(1);
		final TaskExecutorLocalStateStoresManager localStateStoresManager = createTaskExecutorLocalStateStoresManager();
		final TaskManagerServices taskManagerServices = new TaskManagerServicesBuilder()
			.setUnresolvedTaskManagerLocation(unresolvedTaskManagerLocation)
			.setTaskSlotTable(taskSlotTable)
			.setTaskStateManager(localStateStoresManager)
			.build();

		final TaskExecutor taskManager = createTaskExecutor(taskManagerServices);

		try {
			taskManager.start();
			resourceManagerLeaderRetriever.notifyListener(resourceManagerAddress, UUID.randomUUID());

			assertTrue(taskManagerRegisteredLatch.await(timeout.toMilliseconds(), TimeUnit.MILLISECONDS));
		} finally {
			RpcUtils.terminateRpcEndpoint(taskManager, timeout);
		}
	}

	@Test
	public void testTriggerRegistrationOnLeaderChange() throws Exception {
		final UUID leaderId1 = UUID.randomUUID();
		final UUID leaderId2 = UUID.randomUUID();

		// register the mock resource manager gateways
		final CompletableFuture<TaskExecutorRegistration> rmGateway1TaskExecutorRegistration = new CompletableFuture<>();
		TestingResourceManagerGateway rmGateway1 = new TestingResourceManagerGateway();
		rmGateway1.setRegisterTaskExecutorFunction(
			taskExecutorRegistration -> {
				rmGateway1TaskExecutorRegistration.complete(taskExecutorRegistration);
				return createRegistrationResponse(rmGateway1);
			});

		final CompletableFuture<TaskExecutorRegistration> rmGateway2TaskExecutorRegistration = new CompletableFuture<>();
		TestingResourceManagerGateway rmGateway2 = new TestingResourceManagerGateway();
		rmGateway2.setRegisterTaskExecutorFunction(
			taskExecutorRegistration -> {
				rmGateway2TaskExecutorRegistration.complete(taskExecutorRegistration);
				return createRegistrationResponse(rmGateway2);
			});

		rpc.registerGateway(rmGateway1.getAddress(), rmGateway1);
		rpc.registerGateway(rmGateway2.getAddress(), rmGateway2);

		final TaskSlotTable<Task> taskSlotTable = TestingTaskSlotTable
			.<Task>newBuilder()
			.createSlotReportSupplier(SlotReport::new)
			.closeAsyncReturns(CompletableFuture.completedFuture(null))
			.build();

		final TaskExecutorLocalStateStoresManager localStateStoresManager = createTaskExecutorLocalStateStoresManager();

		final TaskManagerServices taskManagerServices = new TaskManagerServicesBuilder()
			.setUnresolvedTaskManagerLocation(unresolvedTaskManagerLocation)
			.setTaskSlotTable(taskSlotTable)
			.setTaskStateManager(localStateStoresManager)
			.build();

		TaskExecutor taskManager = createTaskExecutor(taskManagerServices);

		try {
			taskManager.start();
			String taskManagerAddress = taskManager.getAddress();

			// no connection initially, since there is no leader
			assertNull(taskManager.getResourceManagerConnection());

			// define a leader and see that a registration happens
			resourceManagerLeaderRetriever.notifyListener(rmGateway1.getAddress(), leaderId1);
			final TaskExecutorRegistration taskExecutorRegistration1 = rmGateway1TaskExecutorRegistration.join();
			assertThat(taskExecutorRegistration1.getTaskExecutorAddress(), is(taskManagerAddress));
			assertThat(taskExecutorRegistration1.getResourceId(), is(unresolvedTaskManagerLocation.getResourceID()));
			assertNotNull(taskManager.getResourceManagerConnection());

			// cancel the leader
			resourceManagerLeaderRetriever.notifyListener(null, null);

			// set a new leader, see that a registration happens
			resourceManagerLeaderRetriever.notifyListener(rmGateway2.getAddress(), leaderId2);

			final TaskExecutorRegistration taskExecutorRegistration2 = rmGateway2TaskExecutorRegistration.join();
			assertThat(taskExecutorRegistration2.getTaskExecutorAddress(), is(taskManagerAddress));
			assertThat(taskExecutorRegistration2.getResourceId(), is(unresolvedTaskManagerLocation.getResourceID()));
			assertNotNull(taskManager.getResourceManagerConnection());
		}
		finally {
			RpcUtils.terminateRpcEndpoint(taskManager, timeout);
		}
	}

	private CompletableFuture<RegistrationResponse> createRegistrationResponse(TestingResourceManagerGateway rmGateway1) {
		return CompletableFuture.completedFuture(
			new TaskExecutorRegistrationSuccess(
				new InstanceID(),
				rmGateway1.getOwnResourceId(),
				new ClusterInformation("localhost", 1234)));
	}

	@Test
	public void testTaskSlotTableTerminationOnShutdown() throws Exception {
		CompletableFuture<Void> taskSlotTableClosingFuture = new CompletableFuture<>();
		TaskExecutorTestingContext submissionContext = createTaskExecutorTestingContext(
			TestingTaskSlotTable.<Task>newBuilder().closeAsyncReturns(taskSlotTableClosingFuture).build());
		final CompletableFuture<Void> taskExecutorTerminationFuture;
		try {
			submissionContext.start();
		} finally {
			taskExecutorTerminationFuture = submissionContext.taskExecutor.closeAsync();
		}

		// check task executor is waiting for the task completion and has not terminated yet
		assertThat(taskExecutorTerminationFuture.isDone(), is(false));

		// check task executor has exited after task slot table termination
		taskSlotTableClosingFuture.complete(null);
		taskExecutorTerminationFuture.get();
	}

	private ResourceManagerId createAndRegisterResourceManager(
			CompletableFuture<Tuple3<ResourceID, InstanceID, SlotReport>> initialSlotReportFuture) {
		final TestingResourceManagerGateway resourceManagerGateway = new TestingResourceManagerGateway();
		resourceManagerGateway.setSendSlotReportFunction(resourceIDInstanceIDSlotReportTuple3 -> {
			initialSlotReportFuture.complete(resourceIDInstanceIDSlotReportTuple3);
			return CompletableFuture.completedFuture(Acknowledge.get());
		});
		rpc.registerGateway(resourceManagerGateway.getAddress(), resourceManagerGateway);

		// tell the task manager about the rm leader
		resourceManagerLeaderRetriever.notifyListener(
			resourceManagerGateway.getAddress(),
			resourceManagerGateway.getFencingToken().toUUID());

		return resourceManagerGateway.getFencingToken();
	}

	/**
	 * Tests that a TaskManager detects a job leader for which it has reserved slots. Upon detecting
	 * the job leader, it will offer all reserved slots to the JobManager.
	 */
	@Test
	public void testJobLeaderDetection() throws Exception {
		final TaskSlotTable<Task> taskSlotTable = TaskSlotUtils.createTaskSlotTable(1);
		final JobLeaderService jobLeaderService = new DefaultJobLeaderService(unresolvedTaskManagerLocation, RetryingRegistrationConfiguration.defaultConfiguration());

		final TestingResourceManagerGateway resourceManagerGateway = new TestingResourceManagerGateway();
		CompletableFuture<Void> initialSlotReportFuture = new CompletableFuture<>();
		resourceManagerGateway.setSendSlotReportFunction(resourceIDInstanceIDSlotReportTuple3 -> {
			initialSlotReportFuture.complete(null);
			return CompletableFuture.completedFuture(Acknowledge.get());
		});

		final CompletableFuture<Collection<SlotOffer>> offeredSlotsFuture = new CompletableFuture<>();
		final TestingJobMasterGateway jobMasterGateway = new TestingJobMasterGatewayBuilder()
			.setOfferSlotsFunction((resourceID, slotOffers) -> {

				offeredSlotsFuture.complete(new ArrayList<>(slotOffers));
				return CompletableFuture.completedFuture(slotOffers);
			})
			.build();

		rpc.registerGateway(resourceManagerGateway.getAddress(), resourceManagerGateway);
		rpc.registerGateway(jobMasterGateway.getAddress(), jobMasterGateway);

		final AllocationID allocationId = new AllocationID();
		final SlotID slotId = new SlotID(unresolvedTaskManagerLocation.getResourceID(), 0);

		final TaskExecutorLocalStateStoresManager localStateStoresManager = createTaskExecutorLocalStateStoresManager();

		final TaskManagerServices taskManagerServices = new TaskManagerServicesBuilder()
			.setUnresolvedTaskManagerLocation(unresolvedTaskManagerLocation)
			.setTaskSlotTable(taskSlotTable)
			.setJobLeaderService(jobLeaderService)
			.setTaskStateManager(localStateStoresManager)
			.build();

		TaskExecutor taskManager = createTaskExecutor(taskManagerServices);

		try {
			taskManager.start();

			final TaskExecutorGateway tmGateway = taskManager.getSelfGateway(TaskExecutorGateway.class);

			// tell the task manager about the rm leader
			resourceManagerLeaderRetriever.notifyListener(resourceManagerGateway.getAddress(), resourceManagerGateway.getFencingToken().toUUID());

			// wait for the initial slot report
			initialSlotReportFuture.get();

			// request slots from the task manager under the given allocation id
			CompletableFuture<Acknowledge> slotRequestAck = tmGateway.requestSlot(
				slotId,
				jobId,
				allocationId,
				ResourceProfile.ZERO,
				jobMasterGateway.getAddress(),
				resourceManagerGateway.getFencingToken(),
				timeout);

			slotRequestAck.get();

			// now inform the task manager about the new job leader
			jobManagerLeaderRetriever.notifyListener(jobMasterGateway.getAddress(), jobMasterGateway.getFencingToken().toUUID());

			final Collection<SlotOffer> offeredSlots = offeredSlotsFuture.get();
			final Collection<AllocationID> allocationIds = offeredSlots.stream().map(SlotOffer::getAllocationId).collect(Collectors.toList());
			assertThat(allocationIds, containsInAnyOrder(allocationId));
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
		final InstanceID registrationId = new InstanceID();
		final OneShotLatch taskExecutorIsRegistered = new OneShotLatch();
		final CompletableFuture<Tuple3<InstanceID, SlotID, AllocationID>> availableSlotFuture = new CompletableFuture<>();
		final TestingResourceManagerGateway resourceManagerGateway =
			createRmWithTmRegisterAndNotifySlotHooks(registrationId, taskExecutorIsRegistered, availableSlotFuture);

		final AllocationID allocationId1 = new AllocationID();
		final AllocationID allocationId2 = new AllocationID();

		final SlotOffer offer1 = new SlotOffer(allocationId1, 0, ResourceProfile.ANY);

		final OneShotLatch offerSlotsLatch = new OneShotLatch();
		final OneShotLatch taskInTerminalState = new OneShotLatch();
		final CompletableFuture<Collection<SlotOffer>> offerResultFuture = new CompletableFuture<>();
		final TestingJobMasterGateway jobMasterGateway =
			createJobMasterWithSlotOfferAndTaskTerminationHooks(offerSlotsLatch, taskInTerminalState, offerResultFuture);

		rpc.registerGateway(resourceManagerGateway.getAddress(), resourceManagerGateway);
		rpc.registerGateway(jobMasterGateway.getAddress(), jobMasterGateway);

		final TaskSlotTable<Task> taskSlotTable = TaskSlotUtils.createTaskSlotTable(2);
		final TaskManagerServices taskManagerServices = createTaskManagerServicesWithTaskSlotTable(taskSlotTable);
		final TestingTaskExecutor taskManager = createTestingTaskExecutor(taskManagerServices);

		try {
			taskManager.start();
			taskManager.waitUntilStarted();

			final TaskExecutorGateway tmGateway = taskManager.getSelfGateway(TaskExecutorGateway.class);

			// wait until registered at the RM
			taskExecutorIsRegistered.await();

			// request 2 slots for the given allocation ids
			requestSlots(
				tmGateway,
				Arrays.asList(allocationId1, allocationId2),
				resourceManagerGateway.getFencingToken(),
				jobMasterGateway.getAddress());

			// notify job leader to start slot offering
			jobManagerLeaderRetriever.notifyListener(jobMasterGateway.getAddress(), jobMasterGateway.getFencingToken().toUUID());

			// wait until slots have been offered
			offerSlotsLatch.await();
			offerResultFuture.complete(Collections.singletonList(offer1));

			final Tuple3<InstanceID, SlotID, AllocationID> instanceIDSlotIDAllocationIDTuple3 = availableSlotFuture.get();

			final Tuple3<InstanceID, SlotID, AllocationID> expectedResult = Tuple3.of(registrationId, new SlotID(unresolvedTaskManagerLocation.getResourceID(), 1), allocationId2);

			assertThat(instanceIDSlotIDAllocationIDTuple3, equalTo(expectedResult));
			// the slot 1 can be activate for task submission
			submitNoOpInvokableTask(allocationId1, jobMasterGateway, tmGateway);
			// wait for the task completion
			taskInTerminalState.await();
			// the slot 2 can NOT be activate for task submission
			try {
				submitNoOpInvokableTask(allocationId2, jobMasterGateway, tmGateway);
				fail("It should not be possible to submit task to acquired by JM slot with index 1 (allocationId2)");
			} catch (CompletionException e) {
				assertThat(e.getCause(), instanceOf(TaskSubmissionException.class));
			}
			// the slot 2 is free to request
			tmGateway
				.requestSlot(
					new SlotID(unresolvedTaskManagerLocation.getResourceID(), 1),
					jobId,
					allocationId2,
					ResourceProfile.UNKNOWN,
					jobMasterGateway.getAddress(),
					resourceManagerGateway.getFencingToken(),
					timeout)
				.join();
		} finally {
			RpcUtils.terminateRpcEndpoint(taskManager, timeout);
		}
	}

	/**
	 * This tests task executor receive SubmitTask before OfferSlot response.
	 */
	@Test
	public void testSubmitTaskBeforeAcceptSlot() throws Exception {
		final InstanceID registrationId = new InstanceID();
		final OneShotLatch taskExecutorIsRegistered = new OneShotLatch();
		final CompletableFuture<Tuple3<InstanceID, SlotID, AllocationID>> availableSlotFuture = new CompletableFuture<>();
		final TestingResourceManagerGateway resourceManagerGateway =
			createRmWithTmRegisterAndNotifySlotHooks(registrationId, taskExecutorIsRegistered, availableSlotFuture);

		final AllocationID allocationId1 = new AllocationID();
		final AllocationID allocationId2 = new AllocationID();

		final SlotOffer offer1 = new SlotOffer(allocationId1, 0, ResourceProfile.ANY);

		final OneShotLatch offerSlotsLatch = new OneShotLatch();
		final OneShotLatch taskInTerminalState = new OneShotLatch();
		final CompletableFuture<Collection<SlotOffer>> offerResultFuture = new CompletableFuture<>();
		final TestingJobMasterGateway jobMasterGateway =
			createJobMasterWithSlotOfferAndTaskTerminationHooks(offerSlotsLatch, taskInTerminalState, offerResultFuture);

		rpc.registerGateway(resourceManagerGateway.getAddress(), resourceManagerGateway);
		rpc.registerGateway(jobMasterGateway.getAddress(), jobMasterGateway);

		final TaskSlotTable<Task> taskSlotTable = TaskSlotUtils.createTaskSlotTable(2);
		final TaskManagerServices taskManagerServices = createTaskManagerServicesWithTaskSlotTable(taskSlotTable);
		final TestingTaskExecutor taskManager = createTestingTaskExecutor(taskManagerServices);

		try {
			taskManager.start();
			taskManager.waitUntilStarted();

			final TaskExecutorGateway tmGateway = taskManager.getSelfGateway(TaskExecutorGateway.class);

			// wait until registered at the RM
			taskExecutorIsRegistered.await();

			// request 2 slots for the given allocation ids
			requestSlots(
				tmGateway,
				Arrays.asList(allocationId1, allocationId2),
				resourceManagerGateway.getFencingToken(),
				jobMasterGateway.getAddress());

			// notify job leader to start slot offering
			jobManagerLeaderRetriever.notifyListener(jobMasterGateway.getAddress(), jobMasterGateway.getFencingToken().toUUID());

			// wait until slots have been offered
			offerSlotsLatch.await();

			submitNoOpInvokableTask(allocationId1, jobMasterGateway, tmGateway);

			// acknowledge the offered slots
			offerResultFuture.complete(Collections.singleton(offer1));

			// check that the rejected slot will be made available again
			final Tuple3<InstanceID, SlotID, AllocationID> instanceIDSlotIDAllocationIDTuple3 = availableSlotFuture.get();
			assertThat(instanceIDSlotIDAllocationIDTuple3.f2, equalTo(allocationId2));

			// wait for the task completion
			taskInTerminalState.await();
		} finally {
			RpcUtils.terminateRpcEndpoint(taskManager, timeout);
		}
	}

	private TestingResourceManagerGateway createRmWithTmRegisterAndNotifySlotHooks(
			InstanceID registrationId,
			OneShotLatch taskExecutorIsRegistered,
			CompletableFuture<Tuple3<InstanceID, SlotID, AllocationID>> availableSlotFuture) {
		final TestingResourceManagerGateway resourceManagerGateway = new TestingResourceManagerGateway();
		resourceManagerLeaderRetriever.notifyListener(
			resourceManagerGateway.getAddress(),
			resourceManagerGateway.getFencingToken().toUUID());

		resourceManagerGateway.setRegisterTaskExecutorFunction(
			taskExecutorRegistration -> CompletableFuture.completedFuture(
				new TaskExecutorRegistrationSuccess(registrationId, resourceManagerGateway.getOwnResourceId(),
					new ClusterInformation("localhost", 1234))));

		resourceManagerGateway.setNotifySlotAvailableConsumer(availableSlotFuture::complete);

		resourceManagerGateway.setSendSlotReportFunction(ignored -> {
			taskExecutorIsRegistered.trigger();
			return CompletableFuture.completedFuture(Acknowledge.get());
		});
		return resourceManagerGateway;
	}

	private TaskManagerServices createTaskManagerServicesWithTaskSlotTable(
			TaskSlotTable<Task> taskSlotTable) throws IOException {
		return new TaskManagerServicesBuilder()
			.setUnresolvedTaskManagerLocation(unresolvedTaskManagerLocation)
			.setShuffleEnvironment(nettyShuffleEnvironment)
			.setTaskSlotTable(taskSlotTable)
			.setJobLeaderService(new DefaultJobLeaderService(
				unresolvedTaskManagerLocation,
				RetryingRegistrationConfiguration.defaultConfiguration()))
			.setTaskStateManager(createTaskExecutorLocalStateStoresManager())
			.build();
	}

	private static TestingJobMasterGateway createJobMasterWithSlotOfferAndTaskTerminationHooks(
			OneShotLatch offerSlotsLatch,
			OneShotLatch taskInTerminalState,
			CompletableFuture<Collection<SlotOffer>> offerResultFuture) {
		return new TestingJobMasterGatewayBuilder()
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
	}

	private void requestSlots(TaskExecutorGateway tmGateway, Iterable<? extends AllocationID> allocationIds, ResourceManagerId resourceManagerId, String jobMasterGatewayAddress) {
		int slotIndex = 0;
		for (AllocationID allocationId : allocationIds) {
			final SlotID slotId1 = new SlotID(unresolvedTaskManagerLocation.getResourceID(), slotIndex);
			tmGateway.requestSlot(
				slotId1,
				jobId,
				allocationId,
				ResourceProfile.UNKNOWN,
				jobMasterGatewayAddress,
				resourceManagerId,
				Time.seconds(10L)).join();

			slotIndex++;
		}
	}

	private void submitNoOpInvokableTask(
			AllocationID allocationId,
			TestingJobMasterGateway jobMasterGateway,
			TaskExecutorGateway tmGateway) throws IOException {
		final TaskDeploymentDescriptor tdd = TaskDeploymentDescriptorBuilder
			.newBuilder(jobId, NoOpInvokable.class)
			.setAllocationId(allocationId)
			.build();
		tmGateway.submitTask(tdd, jobMasterGateway.getFencingToken(), timeout).join();
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

		final TaskSlotTable<Task> taskSlotTable = TaskSlotUtils.createTaskSlotTable(1);

		final String rmAddress = "rm";
		final TestingResourceManagerGateway rmGateway = new TestingResourceManagerGateway(
			ResourceManagerId.generate(),
			rmResourceID,
			rmAddress,
			rmAddress);

		rpc.registerGateway(rmAddress, rmGateway);

		final TaskExecutorLocalStateStoresManager localStateStoresManager = createTaskExecutorLocalStateStoresManager();

		final TaskManagerServices taskManagerServices = new TaskManagerServicesBuilder()
			.setUnresolvedTaskManagerLocation(unresolvedTaskManagerLocation)
			.setTaskSlotTable(taskSlotTable)
			.setTaskStateManager(localStateStoresManager)
			.build();

		final TaskExecutor taskExecutor = createTaskExecutor(taskManagerServices, heartbeatServices);

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
		final TaskSlotTable<Task> taskSlotTable = TaskSlotUtils.createTaskSlotTable(1);

		final TaskExecutorLocalStateStoresManager localStateStoresManager = createTaskExecutorLocalStateStoresManager();

		final TaskManagerServices taskManagerServices = new TaskManagerServicesBuilder()
			.setUnresolvedTaskManagerLocation(unresolvedTaskManagerLocation)
			.setTaskSlotTable(taskSlotTable)
			.setTaskStateManager(localStateStoresManager)
			.build();

		final TestingTaskExecutor taskExecutor = createTestingTaskExecutor(taskManagerServices);

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
			taskExecutor.waitUntilStarted();

			final TaskExecutorGateway taskExecutorGateway = taskExecutor.getSelfGateway(TaskExecutorGateway.class);

			final SlotID slotId = new SlotID(unresolvedTaskManagerLocation.getResourceID(), 0);
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
				ResourceProfile.ZERO,
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
		configuration.set(TaskManagerOptions.REGISTRATION_TIMEOUT, TimeUtils.parseDuration("10 ms"));

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
		configuration.set(TaskManagerOptions.REGISTRATION_TIMEOUT, TimeUtils.parseDuration("100 ms"));
		final TaskSlotTable<Task> taskSlotTable = TaskSlotUtils.createTaskSlotTable(1);

		final TaskManagerServices taskManagerServices = new TaskManagerServicesBuilder().setTaskSlotTable(taskSlotTable).build();
		final TaskExecutor taskExecutor = createTaskExecutor(taskManagerServices, new HeartbeatServices(10L, 10L));

		taskExecutor.start();

		final CompletableFuture<ResourceID> registrationFuture = new CompletableFuture<>();
		final OneShotLatch secondRegistration = new OneShotLatch();
		try {
			final TestingResourceManagerGateway testingResourceManagerGateway = new TestingResourceManagerGateway();
			testingResourceManagerGateway.setRegisterTaskExecutorFunction(
				taskExecutorRegistration -> {
					if (registrationFuture.complete(taskExecutorRegistration.getResourceId())) {
						return createRegistrationResponse(testingResourceManagerGateway);
					} else {
						secondRegistration.trigger();
						return CompletableFuture.completedFuture(new Decline("Only the first registration should succeed."));
					}
				}
			);
			rpc.registerGateway(testingResourceManagerGateway.getAddress(), testingResourceManagerGateway);

			resourceManagerLeaderRetriever.notifyListener(testingResourceManagerGateway.getAddress(), UUID.randomUUID());

			final ResourceID registrationResourceId = registrationFuture.get();

			assertThat(registrationResourceId, equalTo(taskManagerServices.getUnresolvedTaskManagerLocation().getResourceID()));

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
		final TaskExecutor taskExecutor = createTaskExecutor(1);

		taskExecutor.start();

		try {
			final TestingResourceManagerGateway testingResourceManagerGateway = new TestingResourceManagerGateway();

			final CompletableFuture<RegistrationResponse> registrationFuture = new CompletableFuture<>();
			final CompletableFuture<ResourceID> taskExecutorResourceIdFuture = new CompletableFuture<>();

			testingResourceManagerGateway.setRegisterTaskExecutorFunction(taskExecutorRegistration -> {
				taskExecutorResourceIdFuture.complete(taskExecutorRegistration.getResourceId());
				return registrationFuture;
			});

			rpc.registerGateway(testingResourceManagerGateway.getAddress(), testingResourceManagerGateway);
			resourceManagerLeaderRetriever.notifyListener(testingResourceManagerGateway.getAddress(), testingResourceManagerGateway.getFencingToken().toUUID());

			final TaskExecutorGateway taskExecutorGateway = taskExecutor.getSelfGateway(TaskExecutorGateway.class);

			final ResourceID resourceId = taskExecutorResourceIdFuture.get();

			final SlotID slotId = new SlotID(resourceId, 0);
			final CompletableFuture<Acknowledge> slotRequestResponse = taskExecutorGateway.requestSlot(slotId, jobId, new AllocationID(), ResourceProfile.ZERO, "foobar", testingResourceManagerGateway.getFencingToken(), timeout);

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
		final TaskSlotTable<Task> taskSlotTable = TaskSlotUtils.createTaskSlotTable(1);
		final UnresolvedTaskManagerLocation unresolvedTaskManagerLocation = new LocalUnresolvedTaskManagerLocation();
		final TaskExecutor taskExecutor = createTaskExecutor(new TaskManagerServicesBuilder()
			.setTaskSlotTable(taskSlotTable)
			.setUnresolvedTaskManagerLocation(unresolvedTaskManagerLocation)
			.build());

		taskExecutor.start();

		try {
			final TestingResourceManagerGateway testingResourceManagerGateway = new TestingResourceManagerGateway();
			final ClusterInformation clusterInformation = new ClusterInformation("foobar", 1234);
			final CompletableFuture<RegistrationResponse> registrationResponseFuture = CompletableFuture.completedFuture(new TaskExecutorRegistrationSuccess(new InstanceID(), ResourceID.generate(), clusterInformation));
			final BlockingQueue<ResourceID> registrationQueue = new ArrayBlockingQueue<>(1);

			testingResourceManagerGateway.setRegisterTaskExecutorFunction(taskExecutorRegistration -> {
				registrationQueue.offer(taskExecutorRegistration.getResourceId());
				return registrationResponseFuture;
			});
			rpc.registerGateway(testingResourceManagerGateway.getAddress(), testingResourceManagerGateway);

			resourceManagerLeaderRetriever.notifyListener(testingResourceManagerGateway.getAddress(), testingResourceManagerGateway.getFencingToken().toUUID());

			final ResourceID firstRegistrationAttempt = registrationQueue.take();

			assertThat(firstRegistrationAttempt, equalTo(unresolvedTaskManagerLocation.getResourceID()));

			final TaskExecutorGateway taskExecutorGateway = taskExecutor.getSelfGateway(TaskExecutorGateway.class);

			assertThat(registrationQueue, is(empty()));

			taskExecutorGateway.disconnectResourceManager(new FlinkException("Test exception"));

			final ResourceID secondRegistrationAttempt = registrationQueue.take();

			assertThat(secondRegistrationAttempt, equalTo(unresolvedTaskManagerLocation.getResourceID()));

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
		final TaskExecutor taskExecutor = createTaskExecutor(1);

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

			assertThat(initialSlotReportFuture.get(), equalTo(taskExecutor.getResourceID()));
		} finally {
			RpcUtils.terminateRpcEndpoint(taskExecutor, timeout);
		}
	}

	@Test
	public void testRegisterWithDefaultSlotResourceProfile() throws Exception {
		final int numberOfSlots = 2;
		final TaskExecutor taskExecutor = createTaskExecutor(numberOfSlots);

		taskExecutor.start();

		try {
			final TestingResourceManagerGateway testingResourceManagerGateway = new TestingResourceManagerGateway();
			final CompletableFuture<ResourceProfile> registeredDefaultSlotResourceProfileFuture = new CompletableFuture<>();

			final ResourceID ownResourceId = testingResourceManagerGateway.getOwnResourceId();
			testingResourceManagerGateway.setRegisterTaskExecutorFunction(taskExecutorRegistration -> {
				registeredDefaultSlotResourceProfileFuture.complete(taskExecutorRegistration.getDefaultSlotResourceProfile());
				return CompletableFuture.completedFuture(
					new TaskExecutorRegistrationSuccess(
						new InstanceID(),
						ownResourceId,
						new ClusterInformation("localhost", 1234)));
			});

			rpc.registerGateway(testingResourceManagerGateway.getAddress(), testingResourceManagerGateway);
			resourceManagerLeaderRetriever.notifyListener(testingResourceManagerGateway.getAddress(), testingResourceManagerGateway.getFencingToken().toUUID());

			assertThat(registeredDefaultSlotResourceProfileFuture.get(),
				equalTo(TaskExecutorResourceUtils.generateDefaultSlotResourceProfile(TM_RESOURCE_SPEC, numberOfSlots)));
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
		final TaskSlotTable<Task> taskSlotTable = TaskSlotUtils.createTaskSlotTable(1);
		final UnresolvedTaskManagerLocation unresolvedTaskManagerLocation = new LocalUnresolvedTaskManagerLocation();
		final TaskManagerServices taskManagerServices = new TaskManagerServicesBuilder()
			.setTaskSlotTable(taskSlotTable)
			.setUnresolvedTaskManagerLocation(unresolvedTaskManagerLocation)
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

			testingResourceManagerGateway.setRegisterTaskExecutorFunction(taskExecutorRegistration -> {
					numberRegistrations.countDown();
					return registrationResponse;
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
		final TaskSlotTable<Task> taskSlotTable = TaskSlotUtils.createTaskSlotTable(2);
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
				ResourceProfile.ZERO,
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
	 * Tests that the TaskExecutor disconnects from the JobMaster if a new leader
	 * is detected.
	 */
	@Test
	public void testDisconnectFromJobMasterWhenNewLeader() throws Exception {
		final TaskManagerServices taskManagerServices = new TaskManagerServicesBuilder()
			.setTaskSlotTable(TaskSlotUtils.createTaskSlotTable(1))
			.build();
		final TaskExecutor taskExecutor = createTaskExecutor(taskManagerServices);

		final CompletableFuture<Integer> offeredSlotsFuture = new CompletableFuture<>();
		final CompletableFuture<ResourceID> disconnectFuture = new CompletableFuture<>();
		final TestingJobMasterGateway jobMasterGateway = new TestingJobMasterGatewayBuilder()
			.setOfferSlotsFunction((resourceID, slotOffers) -> {
				offeredSlotsFuture.complete(slotOffers.size());
				return CompletableFuture.completedFuture(slotOffers);
			})
			.setDisconnectTaskManagerFunction(
				resourceID -> {
					disconnectFuture.complete(resourceID);
					return CompletableFuture.completedFuture(Acknowledge.get());
				}
			)
			.build();
		final TestingResourceManagerGateway resourceManagerGateway = new TestingResourceManagerGateway();

		final CompletableFuture<Void> initialSlotReportFuture = new CompletableFuture<>();
		resourceManagerGateway.setSendSlotReportFunction(resourceIDInstanceIDSlotReportTuple3 -> {
			initialSlotReportFuture.complete(null);
			return CompletableFuture.completedFuture(Acknowledge.get());

		});

		rpc.registerGateway(resourceManagerGateway.getAddress(), resourceManagerGateway);
		rpc.registerGateway(jobMasterGateway.getAddress(), jobMasterGateway);

		try {
			taskExecutor.start();

			TaskExecutorGateway taskExecutorGateway = taskExecutor.getSelfGateway(TaskExecutorGateway.class);

			resourceManagerLeaderRetriever.notifyListener(resourceManagerGateway.getAddress(), resourceManagerGateway.getFencingToken().toUUID());

			initialSlotReportFuture.get();

			ResourceID resourceID = taskManagerServices.getUnresolvedTaskManagerLocation().getResourceID();
			taskExecutorGateway.requestSlot(
				new SlotID(resourceID, 0),
				jobId,
				new AllocationID(),
				ResourceProfile.ZERO,
				"foobar",
				resourceManagerGateway.getFencingToken(),
				timeout).get();

			jobManagerLeaderRetriever.notifyListener(jobMasterGateway.getAddress(), UUID.randomUUID());

			assertThat(offeredSlotsFuture.get(), is(1));

			// notify loss of leadership
			jobManagerLeaderRetriever.notifyListener(null, null);

			assertThat(disconnectFuture.get(), is(resourceID));
		} finally {
			RpcUtils.terminateRpcEndpoint(taskExecutor, timeout);
		}
	}

	@Test(timeout = 10000L)
	public void testLogNotFoundHandling() throws Throwable {
		final int dataPort = NetUtils.getAvailablePort();
		Configuration config = new Configuration();
		config.setInteger(NettyShuffleEnvironmentOptions.DATA_PORT, dataPort);
		config.setInteger(NettyShuffleEnvironmentOptions.NETWORK_REQUEST_BACKOFF_INITIAL, 100);
		config.setInteger(NettyShuffleEnvironmentOptions.NETWORK_REQUEST_BACKOFF_MAX, 200);
		config.setString(ConfigConstants.TASK_MANAGER_LOG_PATH_KEY, "/i/dont/exist");

		try (TaskSubmissionTestEnvironment env =
			new Builder(jobId)
				.setConfiguration(config)
				.setLocalCommunication(false)
				.build()) {
			TaskExecutorGateway tmGateway = env.getTaskExecutorGateway();
			try {
				CompletableFuture<TransientBlobKey> logFuture =
					tmGateway.requestFileUploadByType(FileType.LOG, timeout);
				logFuture.get();
			} catch (Exception e) {
				assertThat(e.getMessage(), containsString("The file LOG does not exist on the TaskExecutor."));
			}
		}
	}

	@Test(timeout = 10000L)
	public void testTerminationOnFatalError() throws Throwable {
		try (TaskSubmissionTestEnvironment env = new Builder(jobId).build()) {
			String testExceptionMsg = "Test exception of fatal error.";

			env.getTaskExecutor().onFatalError(new Exception(testExceptionMsg));

			Throwable exception = env.getTestingFatalErrorHandler().getErrorFuture().get();
			env.getTestingFatalErrorHandler().clearError();

			assertThat(exception.getMessage(), startsWith(testExceptionMsg));
		}
	}

	/**
	 * Tests that the TaskExecutor syncs its slots view with the JobMaster's view
	 * via the AllocatedSlotReport reported by the heartbeat (See FLINK-11059).
	 */
	@Test
	public void testSyncSlotsWithJobMasterByHeartbeat() throws Exception {
		final CountDownLatch activeSlots = new CountDownLatch(2);
		final TaskSlotTable<Task> taskSlotTable = new ActivateSlotNotifyingTaskSlotTable(
				2,
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

			taskExecutorGateway.requestSlot(slotId1, jobId, allocationIdInBoth, ResourceProfile.ZERO, "foobar", testingResourceManagerGateway.getFencingToken(), timeout);
			taskExecutorGateway.requestSlot(slotId2, jobId, allocationIdOnlyInTM, ResourceProfile.ZERO, "foobar", testingResourceManagerGateway.getFencingToken(), timeout);

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

	/**
	 * Tests that the {@link SlotReport} sent to the RM does not contain
	 * out dated/stale information as slots are being requested from the
	 * TM.
	 *
	 * <p>This is a probabilistic test case and needs to be executed
	 * several times to produce a failure without the fix for FLINK-12865.
	 */
	@Test
	public void testSlotReportDoesNotContainStaleInformation() throws Exception {
		final OneShotLatch receivedSlotRequest = new OneShotLatch();
		final CompletableFuture<Void> verifySlotReportFuture = new CompletableFuture<>();
		final OneShotLatch terminateSlotReportVerification = new OneShotLatch();
		final TestingResourceManagerGateway testingResourceManagerGateway = new TestingResourceManagerGateway();
		// Assertions for this test
		testingResourceManagerGateway.setTaskExecutorHeartbeatConsumer((ignored, heartbeatPayload) -> {
			try {
				final ArrayList<SlotStatus> slots = Lists.newArrayList(heartbeatPayload.getSlotReport());
				assertThat(slots, hasSize(1));
				final SlotStatus slotStatus = slots.get(0);

				log.info("Received SlotStatus: {}", slotStatus);

				if (receivedSlotRequest.isTriggered()) {
					assertThat(slotStatus.getAllocationID(), is(notNullValue()));
				} else {
					assertThat(slotStatus.getAllocationID(), is(nullValue()));
				}
			} catch (AssertionError e) {
				verifySlotReportFuture.completeExceptionally(e);
			}

			if (terminateSlotReportVerification.isTriggered()) {
				verifySlotReportFuture.complete(null);
			}
		});
		final CompletableFuture<ResourceID> taskExecutorRegistrationFuture = new CompletableFuture<>();

		testingResourceManagerGateway.setSendSlotReportFunction(ignored -> {
			taskExecutorRegistrationFuture.complete(null);
			return CompletableFuture.completedFuture(Acknowledge.get());
		});

		rpc.registerGateway(testingResourceManagerGateway.getAddress(), testingResourceManagerGateway);
		resourceManagerLeaderRetriever.notifyListener(testingResourceManagerGateway.getAddress(), testingResourceManagerGateway.getFencingToken().toUUID());

		final TaskManagerServices taskManagerServices = new TaskManagerServicesBuilder()
			.setTaskSlotTable(new AllocateSlotNotifyingTaskSlotTable(receivedSlotRequest))
			.build();
		final TaskExecutor taskExecutor = createTaskExecutor(taskManagerServices);
		final ResourceID taskExecutorResourceId = taskManagerServices.getUnresolvedTaskManagerLocation().getResourceID();

		taskExecutor.start();

		final TaskExecutorGateway taskExecutorGateway = taskExecutor.getSelfGateway(TaskExecutorGateway.class);

		final ScheduledExecutorService heartbeatExecutor = java.util.concurrent.Executors.newSingleThreadScheduledExecutor();

		try {
			taskExecutorRegistrationFuture.get();

			final OneShotLatch scheduleFirstHeartbeat = new OneShotLatch();
			final ResourceID resourceManagerResourceId = testingResourceManagerGateway.getOwnResourceId();
			final long heartbeatInterval = 5L;
			heartbeatExecutor.scheduleWithFixedDelay(
				() -> {
					scheduleFirstHeartbeat.trigger();
					taskExecutorGateway.heartbeatFromResourceManager(resourceManagerResourceId);
				},
				0L,
				heartbeatInterval,
				TimeUnit.MILLISECONDS);

			scheduleFirstHeartbeat.await();

			SlotID slotId = new SlotID(taskExecutorResourceId, 0);
			final CompletableFuture<Acknowledge> requestSlotFuture = taskExecutorGateway.requestSlot(
				slotId,
				jobId,
				new AllocationID(),
				ResourceProfile.ZERO,
				"foobar",
				testingResourceManagerGateway.getFencingToken(),
				timeout);

			requestSlotFuture.get();

			terminateSlotReportVerification.trigger();

			verifySlotReportFuture.get();
		} finally {
			ExecutorUtils.gracefulShutdown(timeout.toMilliseconds(), TimeUnit.MILLISECONDS, heartbeatExecutor);
			RpcUtils.terminateRpcEndpoint(taskExecutor, timeout);
		}
	}

	@Test
	public void testDynamicSlotAllocation() throws Exception {
		final AllocationID allocationId = new AllocationID();
		try (TaskExecutorTestingContext submissionContext = createTaskExecutorTestingContext(2)) {
			submissionContext.start();
			final CompletableFuture<Tuple3<ResourceID, InstanceID, SlotReport>> initialSlotReportFuture =
				new CompletableFuture<>();
			ResourceManagerId resourceManagerId = createAndRegisterResourceManager(initialSlotReportFuture);
			initialSlotReportFuture.get();
			final ResourceProfile resourceProfile = DEFAULT_RESOURCE_PROFILE
				.merge(ResourceProfile.newBuilder().setCpuCores(0.1).build());

			submissionContext.taskExecutor
				.getSelfGateway(TaskExecutorGateway.class)
				.requestSlot(
					SlotID.generateDynamicSlotID(ResourceID.generate()),
					jobId,
					allocationId,
					resourceProfile,
					submissionContext.jobMasterGateway.getAddress(),
					resourceManagerId,
					timeout)
				.get();

			ResourceID resourceId = ResourceID.generate();
			SlotReport slotReport = submissionContext.taskSlotTable.createSlotReport(resourceId);
			assertThat(slotReport, containsInAnyOrder(
				new SlotStatus(new SlotID(resourceId, 0), DEFAULT_RESOURCE_PROFILE),
				new SlotStatus(new SlotID(resourceId, 1), DEFAULT_RESOURCE_PROFILE),
				new SlotStatus(SlotID.generateDynamicSlotID(resourceId), resourceProfile, jobId, allocationId)));
		}
	}

	@Test
	public void taskExecutorJobServicesCloseClassLoaderLeaseUponClosing() throws InterruptedException {
		final OneShotLatch leaseReleaseLatch = new OneShotLatch();
		final OneShotLatch closeHookLatch = new OneShotLatch();
		final TestingClassLoaderLease classLoaderLease = TestingClassLoaderLease.newBuilder()
			.setCloseRunnable(leaseReleaseLatch::trigger)
			.build();

		final TaskExecutor.TaskExecutorJobServices taskExecutorJobServices = TaskExecutor.TaskExecutorJobServices.create(
			classLoaderLease,
			closeHookLatch::trigger);

		taskExecutorJobServices.close();
		leaseReleaseLatch.await();
		closeHookLatch.await();
	}

	private TaskExecutorLocalStateStoresManager createTaskExecutorLocalStateStoresManager() throws IOException {
		return new TaskExecutorLocalStateStoresManager(
			false,
			new File[]{tmp.newFolder()},
			Executors.directExecutor());
	}

	private TaskExecutor createTaskExecutor(int numberOFSlots) {
		final TaskSlotTable<Task> taskSlotTable = TaskSlotUtils.createTaskSlotTable(numberOFSlots);
		final UnresolvedTaskManagerLocation unresolvedTaskManagerLocation = new LocalUnresolvedTaskManagerLocation();
		final TaskManagerServices taskManagerServices = new TaskManagerServicesBuilder()
			.setTaskSlotTable(taskSlotTable)
			.setUnresolvedTaskManagerLocation(unresolvedTaskManagerLocation)
			.build();
		configuration.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, numberOFSlots);
		return createTaskExecutor(taskManagerServices);
	}

	@Nonnull
	private TaskExecutor createTaskExecutor(TaskManagerServices taskManagerServices) {
		return createTaskExecutor(taskManagerServices, HEARTBEAT_SERVICES);
	}

	private TaskExecutor createTaskExecutor(TaskManagerServices taskManagerServices, HeartbeatServices heartbeatServices) {
		return createTaskExecutor(taskManagerServices, heartbeatServices, new TaskExecutorPartitionTrackerImpl(taskManagerServices.getShuffleEnvironment()));
	}

	private TaskExecutor createTaskExecutor(TaskManagerServices taskManagerServices, HeartbeatServices heartbeatServices, TaskExecutorPartitionTracker taskExecutorPartitionTracker) {
		return new TaskExecutor(
			rpc,
			TaskManagerConfiguration.fromConfiguration(
				configuration,
				TM_RESOURCE_SPEC,
				InetAddress.getLoopbackAddress().getHostAddress()),
			haServices,
			taskManagerServices,
			ExternalResourceInfoProvider.NO_EXTERNAL_RESOURCES,
			heartbeatServices,
			UnregisteredMetricGroups.createUnregisteredTaskManagerMetricGroup(),
			null,
			dummyBlobCacheService,
			testingFatalErrorHandler,
			taskExecutorPartitionTracker,
			TaskManagerRunner.createBackPressureSampleService(configuration, rpc.getScheduledExecutor()));
	}

	private TestingTaskExecutor createTestingTaskExecutor(TaskManagerServices taskManagerServices) {
		return createTestingTaskExecutor(taskManagerServices, HEARTBEAT_SERVICES);
	}

	private TestingTaskExecutor createTestingTaskExecutor(TaskManagerServices taskManagerServices, HeartbeatServices heartbeatServices) {
		return new TestingTaskExecutor(
			rpc,
			TaskManagerConfiguration.fromConfiguration(
				configuration,
				TM_RESOURCE_SPEC,
				InetAddress.getLoopbackAddress().getHostAddress()),
			haServices,
			taskManagerServices,
			ExternalResourceInfoProvider.NO_EXTERNAL_RESOURCES,
			heartbeatServices,
			UnregisteredMetricGroups.createUnregisteredTaskManagerMetricGroup(),
			null,
			dummyBlobCacheService,
			testingFatalErrorHandler,
			new TaskExecutorPartitionTrackerImpl(taskManagerServices.getShuffleEnvironment()),
			TaskManagerRunner.createBackPressureSampleService(configuration, rpc.getScheduledExecutor()));
	}

	private TaskExecutorTestingContext createTaskExecutorTestingContext(int numberOfSlots) throws IOException {
		return createTaskExecutorTestingContext(TaskSlotUtils.createTaskSlotTable(numberOfSlots));
	}

	private TaskExecutorTestingContext createTaskExecutorTestingContext(final TaskSlotTable<Task> taskSlotTable) throws IOException {
		final OneShotLatch offerSlotsLatch = new OneShotLatch();
		final TestingJobMasterGateway jobMasterGateway = new TestingJobMasterGatewayBuilder()
			.setOfferSlotsFunction((resourceID, slotOffers) -> {
				offerSlotsLatch.trigger();
				return CompletableFuture.completedFuture(slotOffers);
			}).build();
		rpc.registerGateway(jobMasterGateway.getAddress(), jobMasterGateway);

		final JobLeaderService jobLeaderService = new DefaultJobLeaderService(
			unresolvedTaskManagerLocation,
			RetryingRegistrationConfiguration.defaultConfiguration());

		TaskExecutorLocalStateStoresManager stateStoresManager = createTaskExecutorLocalStateStoresManager();
		final TestingTaskExecutor taskExecutor = createTestingTaskExecutor(new TaskManagerServicesBuilder()
			.setTaskSlotTable(taskSlotTable)
			.setJobLeaderService(jobLeaderService)
			.setTaskStateManager(stateStoresManager)
			.build());

		jobManagerLeaderRetriever.notifyListener(jobMasterGateway.getAddress(), jobMasterGateway.getFencingToken().toUUID());
		return new TaskExecutorTestingContext(jobMasterGateway, taskSlotTable, taskExecutor);
	}

	private class TaskExecutorTestingContext implements AutoCloseable {
		private final TestingJobMasterGateway jobMasterGateway;
		private final TaskSlotTable taskSlotTable;
		private final TestingTaskExecutor taskExecutor;

		private TaskExecutorTestingContext(
				TestingJobMasterGateway jobMasterGateway,
				TaskSlotTable taskSlotTable,
				TestingTaskExecutor taskExecutor) {
			this.jobMasterGateway = jobMasterGateway;
			this.taskSlotTable = taskSlotTable;
			this.taskExecutor = taskExecutor;
		}

		private void start() {
			taskExecutor.start();
			taskExecutor.waitUntilStarted();
		}

		@Override
		public void close() throws ExecutionException, InterruptedException, TimeoutException {
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
		public void start(LeaderRetrievalListener listener) {
			startFuture.complete(listener);
		}

		@Override
		public void stop() {
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

	private static final class AllocateSlotNotifyingTaskSlotTable extends TaskSlotTableImpl<Task> {

		private final OneShotLatch allocateSlotLatch;

		private AllocateSlotNotifyingTaskSlotTable(OneShotLatch allocateSlotLatch) {
			super(1, createTotalResourceProfile(1), DEFAULT_RESOURCE_PROFILE, MemoryManager.MIN_PAGE_SIZE, createDefaultTimerService(timeout.toMilliseconds()));
			this.allocateSlotLatch = allocateSlotLatch;
		}

		@Override
		public boolean allocateSlot(int index, JobID jobId, AllocationID allocationId, Time slotTimeout) {
			final boolean result = super.allocateSlot(index, jobId, allocationId, slotTimeout);
			allocateSlotLatch.trigger();

			return result;
		}

		@Override
		public boolean allocateSlot(int index, JobID jobId, AllocationID allocationId, ResourceProfile resourceProfile, Time slotTimeout) {
			final boolean result = super.allocateSlot(index, jobId, allocationId, resourceProfile, slotTimeout);
			allocateSlotLatch.trigger();

			return result;
		}
	}

	private static final class ActivateSlotNotifyingTaskSlotTable extends TaskSlotTableImpl<Task> {

		private final CountDownLatch slotsToActivate;

		private ActivateSlotNotifyingTaskSlotTable(int numberOfDefaultSlots, CountDownLatch slotsToActivate) {
			super(numberOfDefaultSlots, createTotalResourceProfile(numberOfDefaultSlots), DEFAULT_RESOURCE_PROFILE, MemoryManager.MIN_PAGE_SIZE, createDefaultTimerService(timeout.toMilliseconds()));
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
