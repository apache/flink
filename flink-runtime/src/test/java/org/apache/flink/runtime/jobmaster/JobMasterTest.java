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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.core.io.InputSplitSource;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.queryablestate.KvStateID;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.checkpoint.CheckpointProperties;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy;
import org.apache.flink.runtime.checkpoint.Checkpoints;
import org.apache.flink.runtime.checkpoint.CheckpointsCleaner;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.StandaloneCompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.dispatcher.SchedulerNGFactoryFactory;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.AccessExecution;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.failover.flip1.FailoverStrategyFactoryLoader;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.heartbeat.TestingHeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.instance.SimpleSlotContext;
import org.apache.flink.runtime.io.network.partition.NoOpJobMasterPartitionTracker;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.TestingJobMasterPartitionTracker;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.jobgraph.utils.JobGraphTestUtils;
import org.apache.flink.runtime.jobmanager.OnCompletionActions;
import org.apache.flink.runtime.jobmanager.PartitionProducerDisposedException;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.factories.UnregisteredJobManagerJobMetricGroupFactory;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlot;
import org.apache.flink.runtime.jobmaster.slotpool.SlotInfoWithUtilization;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPool;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPoolFactory;
import org.apache.flink.runtime.jobmaster.utils.JobMasterBuilder;
import org.apache.flink.runtime.leaderretrieval.SettableLeaderRetrievalService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;
import org.apache.flink.runtime.query.KvStateLocation;
import org.apache.flink.runtime.query.UnknownKvStateLocation;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.rpc.akka.AkkaRpcService;
import org.apache.flink.runtime.rpc.akka.AkkaRpcServiceConfiguration;
import org.apache.flink.runtime.scheduler.SchedulerNGFactory;
import org.apache.flink.runtime.shuffle.NettyShuffleMaster;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.OperatorStreamStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TaskExecutorToJobManagerHeartbeatPayload;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.runtime.taskexecutor.rpc.RpcCheckpointResponder;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.LocalUnresolvedTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.taskmanager.UnresolvedTaskManagerLocation;
import org.apache.flink.runtime.testtasks.BlockingNoOpInvokable;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.testutils.ClassLoaderUtils;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.SerializedThrowable;
import org.apache.flink.util.TestLogger;

import akka.actor.ActorSystem;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URLClassLoader;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.runtime.checkpoint.PerJobCheckpointRecoveryFactory.useSameServicesForAllJobs;
import static org.apache.flink.runtime.checkpoint.StateHandleDummyUtil.createNewInputChannelStateHandle;
import static org.apache.flink.runtime.checkpoint.StateHandleDummyUtil.createNewResultSubpartitionStateHandle;
import static org.apache.flink.runtime.checkpoint.StateObjectCollection.singleton;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for {@link JobMaster}.
 */
public class JobMasterTest extends TestLogger {

	private static final TestingInputSplit[] EMPTY_TESTING_INPUT_SPLITS = new TestingInputSplit[0];

	@ClassRule
	public static TemporaryFolder temporaryFolder = new TemporaryFolder();

	private static final Time testingTimeout = Time.seconds(10L);

	private static final long fastHeartbeatInterval = 1L;
	private static final long fastHeartbeatTimeout = 10L;

	private static final long heartbeatInterval = 1000L;
	private static final long heartbeatTimeout = 5_000_000L;

	private static final JobGraph jobGraph = new JobGraph();

	private static TestingRpcService rpcService;

	private static HeartbeatServices fastHeartbeatServices;

	private static HeartbeatServices heartbeatServices;

	private Configuration configuration;

	private ResourceID jmResourceId;

	private JobMasterId jobMasterId;

	private TestingHighAvailabilityServices haServices;

	private SettableLeaderRetrievalService rmLeaderRetrievalService;

	private TestingFatalErrorHandler testingFatalErrorHandler;

	@BeforeClass
	public static void setupClass() {
		rpcService = new TestingRpcService();

		fastHeartbeatServices = new HeartbeatServices(fastHeartbeatInterval, fastHeartbeatTimeout);
		heartbeatServices = new HeartbeatServices(heartbeatInterval, heartbeatTimeout);
	}

	@Before
	public void setup() throws IOException {
		configuration = new Configuration();
		haServices = new TestingHighAvailabilityServices();
		jobMasterId = JobMasterId.generate();
		jmResourceId = ResourceID.generate();

		testingFatalErrorHandler = new TestingFatalErrorHandler();

		haServices.setCheckpointRecoveryFactory(new StandaloneCheckpointRecoveryFactory());

		rmLeaderRetrievalService = new SettableLeaderRetrievalService(
			null,
			null);
		haServices.setResourceManagerLeaderRetriever(rmLeaderRetrievalService);

		configuration.setString(BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());
	}

	@After
	public void teardown() throws Exception {
		if (testingFatalErrorHandler != null) {
			testingFatalErrorHandler.rethrowError();
		}

		rpcService.clearGateways();
	}

	@AfterClass
	public static void teardownClass() {
		if (rpcService != null) {
			rpcService.stopService();
			rpcService = null;
		}
	}

	/**
	 * This test ensures that the bookkeeping of TaskExecutors in the JobMaster handles cases where TaskExecutors with the same
	 * ID re-register properly. FLINK-19237 was a bug where the TaskExecutors and the SlotPool got out of sync, and
	 * slot offers were rejected.
	 */
	@Test
	public void testAcceptSlotOfferAfterLeaderChange() throws Exception {

		final JobManagerSharedServices jobManagerSharedServices = new TestingJobManagerSharedServicesBuilder().build();
		final JobMasterConfiguration jobMasterConfiguration = JobMasterConfiguration.fromConfiguration(configuration);

		final SchedulerNGFactory schedulerNGFactory = SchedulerNGFactoryFactory.createSchedulerNGFactory(configuration);

		final JobMaster testingJobMaster = new JobMaster(
			rpcService,
			jobMasterConfiguration,
			jmResourceId,
			JobGraphTestUtils.createSingleVertexJobGraph(),
			haServices,
			SlotPoolFactory.fromConfiguration(configuration),
			jobManagerSharedServices,
			heartbeatServices,
			UnregisteredJobManagerJobMetricGroupFactory.INSTANCE,
			new JobMasterBuilder.TestingOnCompletionActions(),
			testingFatalErrorHandler,
			JobMasterTest.class.getClassLoader(),
			schedulerNGFactory,
			NettyShuffleMaster.INSTANCE,
			NoOpJobMasterPartitionTracker.FACTORY,
			new DefaultExecutionDeploymentTracker(),
			DefaultExecutionDeploymentReconciler::new,
			System.currentTimeMillis());

		testingJobMaster.start(jobMasterId).get();

		JobMasterGateway jobMasterGateway = testingJobMaster.getSelfGateway(JobMasterGateway.class);

		log.info("Register TaskManager");

		String testingTaskManagerAddress = "fake";
		UnresolvedTaskManagerLocation unresolvedTaskManagerLocation = new LocalUnresolvedTaskManagerLocation();
		TestingTaskExecutorGateway testingTaskExecutorGateway = new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway();
		rpcService.registerGateway(testingTaskManagerAddress, testingTaskExecutorGateway);
		Assert.assertThat(jobMasterGateway.registerTaskManager(testingTaskManagerAddress, unresolvedTaskManagerLocation, testingTimeout).get(), instanceOf(RegistrationResponse.Success.class));

		log.info("Revoke leadership & re-grant leadership");
		testingJobMaster.suspend(new FlinkException("Lost leadership")).get();

		testingJobMaster.start(JobMasterId.generate()).get();

		log.info("re-register same TaskManager");
		Assert.assertThat(jobMasterGateway.registerTaskManager(testingTaskManagerAddress, unresolvedTaskManagerLocation, testingTimeout).get(), instanceOf(RegistrationResponse.Success.class));

		log.info("Ensure JobMaster accepts slot offer");
		final SlotOffer slotOffer = new SlotOffer(new AllocationID(), 0, ResourceProfile.ANY);

		Collection<SlotOffer> acceptedSlots = jobMasterGateway.offerSlots(unresolvedTaskManagerLocation.getResourceID(), Collections.singleton(slotOffer), testingTimeout).get();
		Assert.assertThat(acceptedSlots.size(), is(1));
	}

	@Test
	public void testDeclineCheckpointInvocationWithUserException() throws Exception {
		RpcService rpcService1 = null;
		RpcService rpcService2 = null;
		try {
			final ActorSystem actorSystem1 = AkkaUtils.createDefaultActorSystem();
			final ActorSystem actorSystem2 = AkkaUtils.createDefaultActorSystem();

			AkkaRpcServiceConfiguration akkaRpcServiceConfig = AkkaRpcServiceConfiguration.fromConfiguration(configuration);
			rpcService1 = new AkkaRpcService(actorSystem1, akkaRpcServiceConfig);
			rpcService2 = new AkkaRpcService(actorSystem2, akkaRpcServiceConfig);

			final CompletableFuture<Throwable> declineCheckpointMessageFuture = new CompletableFuture<>();

			final JobManagerSharedServices jobManagerSharedServices = new TestingJobManagerSharedServicesBuilder().build();
			final JobMasterConfiguration jobMasterConfiguration = JobMasterConfiguration.fromConfiguration(configuration);

			final SchedulerNGFactory schedulerNGFactory = SchedulerNGFactoryFactory.createSchedulerNGFactory(configuration);

			final JobMaster jobMaster = new JobMaster(
				rpcService1,
				jobMasterConfiguration,
				jmResourceId,
				jobGraph,
				haServices,
				SlotPoolFactory.fromConfiguration(configuration),
				jobManagerSharedServices,
				heartbeatServices,
				UnregisteredJobManagerJobMetricGroupFactory.INSTANCE,
				new JobMasterBuilder.TestingOnCompletionActions(),
				testingFatalErrorHandler,
				JobMasterTest.class.getClassLoader(),
				schedulerNGFactory,
				NettyShuffleMaster.INSTANCE,
				NoOpJobMasterPartitionTracker.FACTORY,
				new DefaultExecutionDeploymentTracker(),
				DefaultExecutionDeploymentReconciler::new,
				System.currentTimeMillis()) {
				@Override
				public void declineCheckpoint(DeclineCheckpoint declineCheckpoint) {
					declineCheckpointMessageFuture.complete(declineCheckpoint.getReason());
				}
			};

			jobMaster.start(jobMasterId).get();

			final String className = "UserException";
			final URLClassLoader userClassLoader = ClassLoaderUtils.compileAndLoadJava(
				temporaryFolder.newFolder(),
				className + ".java",
				String.format("public class %s extends RuntimeException { public %s() {super(\"UserMessage\");} }",
					className,
					className));

			Throwable userException = (Throwable) Class.forName(className, false, userClassLoader).newInstance();

			JobMasterGateway jobMasterGateway =
				rpcService2.connect(jobMaster.getAddress(), jobMaster.getFencingToken(), JobMasterGateway.class).get();

			RpcCheckpointResponder rpcCheckpointResponder = new RpcCheckpointResponder(jobMasterGateway);
			rpcCheckpointResponder.declineCheckpoint(
				jobGraph.getJobID(),
				new ExecutionAttemptID(),
				1,
				userException
			);

			Throwable throwable = declineCheckpointMessageFuture.get(testingTimeout.toMilliseconds(),
				TimeUnit.MILLISECONDS);
			assertThat(throwable, instanceOf(SerializedThrowable.class));
			assertThat(throwable.getMessage(), equalTo(userException.getMessage()));
		} finally {
			RpcUtils.terminateRpcServices(testingTimeout, rpcService1, rpcService2);
		}
	}

	@Test
	public void testHeartbeatTimeoutWithTaskManager() throws Exception {
		final CompletableFuture<ResourceID> heartbeatResourceIdFuture = new CompletableFuture<>();
		final CompletableFuture<JobID> disconnectedJobManagerFuture = new CompletableFuture<>();
		final UnresolvedTaskManagerLocation unresolvedTaskManagerLocation = new LocalUnresolvedTaskManagerLocation();
		final TestingTaskExecutorGateway taskExecutorGateway = new TestingTaskExecutorGatewayBuilder()
			.setHeartbeatJobManagerConsumer((taskManagerId, ignored) -> heartbeatResourceIdFuture.complete(taskManagerId))
			.setDisconnectJobManagerConsumer((jobId, throwable) -> disconnectedJobManagerFuture.complete(jobId))
			.createTestingTaskExecutorGateway();

		rpcService.registerGateway(taskExecutorGateway.getAddress(), taskExecutorGateway);

		final JobManagerSharedServices jobManagerSharedServices = new TestingJobManagerSharedServicesBuilder().build();

		final JobMaster jobMaster = createJobMaster(
			configuration,
			jobGraph,
			haServices,
			jobManagerSharedServices);

		CompletableFuture<Acknowledge> startFuture = jobMaster.start(jobMasterId);

		try {
			// wait for the start to complete
			startFuture.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);

			final JobMasterGateway jobMasterGateway = jobMaster.getSelfGateway(JobMasterGateway.class);

			// register task manager will trigger monitor heartbeat target, schedule heartbeat request at interval time
			CompletableFuture<RegistrationResponse> registrationResponse = jobMasterGateway.registerTaskManager(
				taskExecutorGateway.getAddress(),
				unresolvedTaskManagerLocation,
				testingTimeout);

			// wait for the completion of the registration
			registrationResponse.get();

			final JobID disconnectedJobManager = disconnectedJobManagerFuture.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);

			assertThat(disconnectedJobManager, Matchers.equalTo(jobGraph.getJobID()));

			final ResourceID heartbeatResourceId = heartbeatResourceIdFuture.getNow(null);

			assertThat(heartbeatResourceId, anyOf(nullValue(), equalTo(jmResourceId)));
		} finally {
			jobManagerSharedServices.shutdown();
			RpcUtils.terminateRpcEndpoint(jobMaster, testingTimeout);
		}
	}

	/**
	 * Tests that the {@link AllocatedSlotReport} contains up to date information and not
	 * stale information about the allocated slots on the {@link JobMaster}.
	 *
	 * <p>This is a probabilistic test case which only fails if executed repeatedly without
	 * the fix for FLINK-12863.
	 */
	@Test
	public void testAllocatedSlotReportDoesNotContainStaleInformation() throws Exception {
		final CompletableFuture<Void> assertionFuture = new CompletableFuture<>();
		final UnresolvedTaskManagerLocation unresolvedTaskManagerLocation = new LocalUnresolvedTaskManagerLocation();
		final AtomicBoolean terminateHeartbeatVerification = new AtomicBoolean(false);
		final OneShotLatch hasReceivedSlotOffers = new OneShotLatch();
		final TestingTaskExecutorGateway taskExecutorGateway = new TestingTaskExecutorGatewayBuilder()
			.setHeartbeatJobManagerConsumer((taskManagerId, allocatedSlotReport) -> {
				try {
					if (hasReceivedSlotOffers.isTriggered()) {
						assertThat(allocatedSlotReport.getAllocatedSlotInfos(), hasSize(1));
					} else {
						assertThat(allocatedSlotReport.getAllocatedSlotInfos(), empty());
					}
				} catch (AssertionError e) {
					assertionFuture.completeExceptionally(e);
				}

				if (terminateHeartbeatVerification.get()) {
					assertionFuture.complete(null);
				}
			})
			.createTestingTaskExecutorGateway();

		rpcService.registerGateway(taskExecutorGateway.getAddress(), taskExecutorGateway);

		final JobManagerSharedServices jobManagerSharedServices = new TestingJobManagerSharedServicesBuilder().build();

		final JobMaster jobMaster = new JobMasterBuilder(JobGraphTestUtils.createSingleVertexJobGraph(), rpcService)
			.withHeartbeatServices(new HeartbeatServices(5L, 1000L))
			.withSlotPoolFactory(new TestingSlotPoolFactory(hasReceivedSlotOffers))
			.createJobMaster();

		CompletableFuture<Acknowledge> startFuture = jobMaster.start(jobMasterId);

		try {
			// wait for the start to complete
			startFuture.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);

			final JobMasterGateway jobMasterGateway = jobMaster.getSelfGateway(JobMasterGateway.class);

			// register task manager will trigger monitor heartbeat target, schedule heartbeat request at interval time
			CompletableFuture<RegistrationResponse> registrationResponse = jobMasterGateway.registerTaskManager(
				taskExecutorGateway.getAddress(),
				unresolvedTaskManagerLocation,
				testingTimeout);

			// wait for the completion of the registration
			registrationResponse.get();

			final SlotOffer slotOffer = new SlotOffer(new AllocationID(), 0, ResourceProfile.ANY);

			final CompletableFuture<Collection<SlotOffer>> slotOfferFuture = jobMasterGateway.offerSlots(unresolvedTaskManagerLocation.getResourceID(), Collections.singleton(slotOffer), testingTimeout);

			assertThat(slotOfferFuture.get(), containsInAnyOrder(slotOffer));

			terminateHeartbeatVerification.set(true);

			// make sure that no assertion has been violated
			assertionFuture.get();
		} finally {
			RpcUtils.terminateRpcEndpoint(jobMaster, testingTimeout);
			jobManagerSharedServices.shutdown();
		}
	}

	private static final class TestingSlotPoolFactory implements SlotPoolFactory {

		private final OneShotLatch hasReceivedSlotOffers;

		public TestingSlotPoolFactory(OneShotLatch hasReceivedSlotOffers) {
			this.hasReceivedSlotOffers = hasReceivedSlotOffers;
		}

		@Nonnull
		@Override
		public SlotPool createSlotPool(@Nonnull JobID jobId) {
			return new TestingSlotPool(jobId, hasReceivedSlotOffers);
		}
	}

	private static final class TestingSlotPool implements SlotPool {

		private final JobID jobId;

		private final OneShotLatch hasReceivedSlotOffers;

		private final Map<ResourceID, Collection<SlotInfo>> registeredSlots;

		private TestingSlotPool(JobID jobId, OneShotLatch hasReceivedSlotOffers) {
			this.jobId = jobId;
			this.hasReceivedSlotOffers = hasReceivedSlotOffers;
			this.registeredSlots = new HashMap<>(16);
		}

		@Override
		public void start(JobMasterId jobMasterId, String newJobManagerAddress, ComponentMainThreadExecutor jmMainThreadScheduledExecutor) {
		}

		@Override
		public void suspend() {
			clear();
		}

		@Override
		public void close() {
			clear();
		}

		private void clear() {
			registeredSlots.clear();
		}

		@Override
		public void connectToResourceManager(ResourceManagerGateway resourceManagerGateway) {
			throw new UnsupportedOperationException("TestingSlotPool does not support this operation.");
		}

		@Override
		public void disconnectResourceManager() {
			throw new UnsupportedOperationException("TestingSlotPool does not support this operation.");
		}

		@Override
		public boolean registerTaskManager(ResourceID resourceID) {
			registeredSlots.computeIfAbsent(resourceID, ignored -> new ArrayList<>(16));
			return true;
		}

		@Override
		public boolean releaseTaskManager(ResourceID resourceId, Exception cause) {
			registeredSlots.remove(resourceId);
			return true;
		}

		@Override
		public Collection<SlotOffer> offerSlots(TaskManagerLocation taskManagerLocation, TaskManagerGateway taskManagerGateway, Collection<SlotOffer> offers) {
			hasReceivedSlotOffers.trigger();
			final Collection<SlotInfo> slotInfos = Optional.ofNullable(registeredSlots.get(taskManagerLocation.getResourceID()))
				.orElseThrow(() -> new FlinkRuntimeException("TaskManager not registered."));

			int slotIndex = slotInfos.size();

			for (SlotOffer offer : offers) {
				slotInfos.add(new SimpleSlotContext(
					offer.getAllocationId(),
					taskManagerLocation,
					slotIndex,
					taskManagerGateway));
				slotIndex++;
			}

			return offers;
		}

		@Override
		public Optional<ResourceID> failAllocation(AllocationID allocationID, Exception cause) {
			throw new UnsupportedOperationException("TestingSlotPool does not support this operation.");
		}

		@Nonnull
		@Override
		public Collection<SlotInfoWithUtilization> getAvailableSlotsInformation() {
			final Collection<SlotInfoWithUtilization> allSlotInfos = registeredSlots.values().stream()
				.flatMap(Collection::stream)
				.map(slot -> SlotInfoWithUtilization.from(slot, 0))
				.collect(Collectors.toList());

			return Collections.unmodifiableCollection(allSlotInfos);
		}

		@Override
		public Collection<SlotInfo> getAllocatedSlotsInformation() {
			return Collections.emptyList();
		}

		@Override
		public Optional<PhysicalSlot> allocateAvailableSlot(@Nonnull SlotRequestId slotRequestId, @Nonnull AllocationID allocationID) {
			throw new UnsupportedOperationException("TestingSlotPool does not support this operation.");
		}

		@Nonnull
		@Override
		public CompletableFuture<PhysicalSlot> requestNewAllocatedSlot(
				@Nonnull SlotRequestId slotRequestId,
				@Nonnull ResourceProfile resourceProfile,
				@Nullable Time timeout) {
			return new CompletableFuture<>();
		}

		@Nonnull
		@Override
		public CompletableFuture<PhysicalSlot> requestNewAllocatedBatchSlot(@Nonnull SlotRequestId slotRequestId, @Nonnull ResourceProfile resourceProfile) {
			return new CompletableFuture<>();
		}

		@Override
		public void disableBatchSlotRequestTimeoutCheck() {
			// no action and no exception is expected
		}

		@Override
		public AllocatedSlotReport createAllocatedSlotReport(ResourceID taskManagerId) {
			final Collection<SlotInfo> slotInfos = registeredSlots.getOrDefault(taskManagerId, Collections.emptyList());

			final List<AllocatedSlotInfo> allocatedSlotInfos = slotInfos
				.stream()
				.map(slotInfo -> new AllocatedSlotInfo(slotInfo.getPhysicalSlotNumber(), slotInfo.getAllocationId()))
				.collect(Collectors.toList());

			return new AllocatedSlotReport(jobId, allocatedSlotInfos);
		}

		@Override
		public void releaseSlot(@Nonnull SlotRequestId slotRequestId, @Nullable Throwable cause) {
			throw new UnsupportedOperationException("TestingSlotPool does not support this operation.");
		}
	}

	@Test
	public void testHeartbeatTimeoutWithResourceManager() throws Exception {
		final String resourceManagerAddress = "rm";
		final ResourceManagerId resourceManagerId = ResourceManagerId.generate();
		final ResourceID rmResourceId = new ResourceID(resourceManagerAddress);

		final TestingResourceManagerGateway resourceManagerGateway = new TestingResourceManagerGateway(
			resourceManagerId,
			rmResourceId,
			resourceManagerAddress,
			"localhost");

		final CompletableFuture<Tuple3<JobMasterId, ResourceID, JobID>> jobManagerRegistrationFuture = new CompletableFuture<>();
		final CompletableFuture<JobID> disconnectedJobManagerFuture = new CompletableFuture<>();
		final CountDownLatch registrationAttempts = new CountDownLatch(2);

		resourceManagerGateway.setRegisterJobManagerFunction((jobMasterId, resourceID, s, jobID) -> {
			jobManagerRegistrationFuture.complete(
				Tuple3.of(
					jobMasterId,
					resourceID,
					jobID));
			registrationAttempts.countDown();

			return CompletableFuture.completedFuture(resourceManagerGateway.getJobMasterRegistrationSuccess());
		});

		resourceManagerGateway.setDisconnectJobManagerConsumer(tuple -> disconnectedJobManagerFuture.complete(tuple.f0));

		rpcService.registerGateway(resourceManagerAddress, resourceManagerGateway);

		final JobManagerSharedServices jobManagerSharedServices = new TestingJobManagerSharedServicesBuilder().build();

		final JobMaster jobMaster = createJobMaster(
			configuration,
			jobGraph,
			haServices,
			jobManagerSharedServices);

		CompletableFuture<Acknowledge> startFuture = jobMaster.start(jobMasterId);

		try {
			// wait for the start operation to complete
			startFuture.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);

			// define a leader and see that a registration happens
			rmLeaderRetrievalService.notifyListener(resourceManagerAddress, resourceManagerId.toUUID());

			// register job manager success will trigger monitor heartbeat target between jm and rm
			final Tuple3<JobMasterId, ResourceID, JobID> registrationInformation = jobManagerRegistrationFuture.get(
				testingTimeout.toMilliseconds(),
				TimeUnit.MILLISECONDS);

			assertThat(registrationInformation.f0, Matchers.equalTo(jobMasterId));
			assertThat(registrationInformation.f1, Matchers.equalTo(jmResourceId));
			assertThat(registrationInformation.f2, Matchers.equalTo(jobGraph.getJobID()));

			final JobID disconnectedJobManager = disconnectedJobManagerFuture.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);

			// heartbeat timeout should trigger disconnect JobManager from ResourceManager
			assertThat(disconnectedJobManager, Matchers.equalTo(jobGraph.getJobID()));

			// the JobMaster should try to reconnect to the RM
			registrationAttempts.await();
		} finally {
			jobManagerSharedServices.shutdown();
			RpcUtils.terminateRpcEndpoint(jobMaster, testingTimeout);
		}
	}

	/**
	 * Tests that a JobMaster will restore the given JobGraph from its savepoint upon
	 * initial submission.
	 */
	@Test
	public void testRestoringFromSavepoint() throws Exception {

		// create savepoint data
		final long savepointId = 42L;
		final File savepointFile = createSavepoint(savepointId);

		// set savepoint settings
		final SavepointRestoreSettings savepointRestoreSettings = SavepointRestoreSettings.forPath(
			savepointFile.getAbsolutePath(),
			true);
		final JobGraph jobGraph = createJobGraphWithCheckpointing(savepointRestoreSettings);

		final StandaloneCompletedCheckpointStore completedCheckpointStore = new StandaloneCompletedCheckpointStore(1);
		final CheckpointRecoveryFactory testingCheckpointRecoveryFactory = useSameServicesForAllJobs(completedCheckpointStore, new StandaloneCheckpointIDCounter());
		haServices.setCheckpointRecoveryFactory(testingCheckpointRecoveryFactory);
		final JobMaster jobMaster = createJobMaster(
			configuration,
			jobGraph,
			haServices,
			new TestingJobManagerSharedServicesBuilder().build());

		try {
			// starting the JobMaster should have read the savepoint
			final CompletedCheckpoint savepointCheckpoint = completedCheckpointStore.getLatestCheckpoint(false);

			assertThat(savepointCheckpoint, Matchers.notNullValue());

			assertThat(savepointCheckpoint.getCheckpointID(), is(savepointId));
		} finally {
			RpcUtils.terminateRpcEndpoint(jobMaster, testingTimeout);
		}
	}

	/**
	 * Tests that a JobMaster will only restore a modified JobGraph if non
	 * restored state is allowed.
	 */
	@Test
	public void testRestoringModifiedJobFromSavepoint() throws Exception {

		// create savepoint data
		final long savepointId = 42L;
		final OperatorID operatorID = new OperatorID();
		final File savepointFile = createSavepointWithOperatorState(savepointId, operatorID);

		// set savepoint settings which don't allow non restored state
		final SavepointRestoreSettings savepointRestoreSettings = SavepointRestoreSettings.forPath(
			savepointFile.getAbsolutePath(),
			false);

		// create a new operator
		final JobVertex jobVertex = new JobVertex("New operator");
		jobVertex.setInvokableClass(NoOpInvokable.class);
		final JobGraph jobGraphWithNewOperator = createJobGraphFromJobVerticesWithCheckpointing(savepointRestoreSettings, jobVertex);

		final StandaloneCompletedCheckpointStore completedCheckpointStore = new StandaloneCompletedCheckpointStore(1);
		final CheckpointRecoveryFactory testingCheckpointRecoveryFactory = useSameServicesForAllJobs(completedCheckpointStore, new StandaloneCheckpointIDCounter());
		haServices.setCheckpointRecoveryFactory(testingCheckpointRecoveryFactory);

		try {
			createJobMaster(
				configuration,
				jobGraphWithNewOperator,
				haServices,
				new TestingJobManagerSharedServicesBuilder().build());
			fail("Should fail because we cannot resume the changed JobGraph from the savepoint.");
		} catch (IllegalStateException expected) {
			// that was expected :-)
		}

		// allow for non restored state
		jobGraphWithNewOperator.setSavepointRestoreSettings(
			SavepointRestoreSettings.forPath(
				savepointFile.getAbsolutePath(),
				true));

		final JobMaster jobMaster = createJobMaster(
			configuration,
			jobGraphWithNewOperator,
			haServices,
			new TestingJobManagerSharedServicesBuilder().build());

		try {
			// starting the JobMaster should have read the savepoint
			final CompletedCheckpoint savepointCheckpoint = completedCheckpointStore.getLatestCheckpoint(false);

			assertThat(savepointCheckpoint, Matchers.notNullValue());

			assertThat(savepointCheckpoint.getCheckpointID(), is(savepointId));
		} finally {
			RpcUtils.terminateRpcEndpoint(jobMaster, testingTimeout);
		}
	}

	/**
	 * Tests that an existing checkpoint will have precedence over an savepoint.
	 */
	@Test
	public void testCheckpointPrecedesSavepointRecovery() throws Exception {

		// create savepoint data
		final long savepointId = 42L;
		final File savepointFile = createSavepoint(savepointId);

		// set savepoint settings
		final SavepointRestoreSettings savepointRestoreSettings = SavepointRestoreSettings.forPath("" +
				savepointFile.getAbsolutePath(),
			true);
		final JobGraph jobGraph = createJobGraphWithCheckpointing(savepointRestoreSettings);

		final long checkpointId = 1L;

		final CompletedCheckpoint completedCheckpoint = new CompletedCheckpoint(
			jobGraph.getJobID(),
			checkpointId,
			1L,
			1L,
			Collections.emptyMap(),
			null,
			CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
			new DummyCheckpointStorageLocation()
		);

		final StandaloneCompletedCheckpointStore completedCheckpointStore = new StandaloneCompletedCheckpointStore(1);
		completedCheckpointStore.addCheckpoint(completedCheckpoint, new CheckpointsCleaner(), () -> {
		});
		final CheckpointRecoveryFactory testingCheckpointRecoveryFactory = useSameServicesForAllJobs(completedCheckpointStore, new StandaloneCheckpointIDCounter());
		haServices.setCheckpointRecoveryFactory(testingCheckpointRecoveryFactory);

		final JobMaster jobMaster = createJobMaster(
			configuration,
			jobGraph,
			haServices,
			new TestingJobManagerSharedServicesBuilder().build());

		try {
			// starting the JobMaster should have read the savepoint
			final CompletedCheckpoint savepointCheckpoint = completedCheckpointStore.getLatestCheckpoint(false);

			assertThat(savepointCheckpoint, Matchers.notNullValue());

			assertThat(savepointCheckpoint.getCheckpointID(), is(checkpointId));
		} finally {
			RpcUtils.terminateRpcEndpoint(jobMaster, testingTimeout);
		}
	}

	/**
	 * Tests that the JobMaster retries the scheduling of a job
	 * in case of a missing slot offering from a registered TaskExecutor.
	 */
	@Test
	public void testSlotRequestTimeoutWhenNoSlotOffering() throws Exception {
		final JobGraph restartingJobGraph = createSingleVertexJobWithRestartStrategy();

		final long slotRequestTimeout = 10L;
		configuration.setLong(JobManagerOptions.SLOT_REQUEST_TIMEOUT, slotRequestTimeout);

		final JobMaster jobMaster = createJobMaster(
			configuration,
			restartingJobGraph,
			haServices,
			new TestingJobManagerSharedServicesBuilder().build(),
			heartbeatServices);

		final JobMasterGateway jobMasterGateway = jobMaster.getSelfGateway(JobMasterGateway.class);

		try {
			final long start = System.nanoTime();
			jobMaster.start(JobMasterId.generate()).get();

			final CompletableFuture<TaskDeploymentDescriptor> submittedTaskFuture = new CompletableFuture<>();
			final LocalUnresolvedTaskManagerLocation taskManagerUnresolvedLocation = new LocalUnresolvedTaskManagerLocation();
			final TestingTaskExecutorGateway taskExecutorGateway = new TestingTaskExecutorGatewayBuilder()
				.setSubmitTaskConsumer((tdd, ignored) -> {
					submittedTaskFuture.complete(tdd);
					return CompletableFuture.completedFuture(Acknowledge.get());
				})
				.createTestingTaskExecutorGateway();

			rpcService.registerGateway(taskExecutorGateway.getAddress(), taskExecutorGateway);

			jobMasterGateway.registerTaskManager(taskExecutorGateway.getAddress(), taskManagerUnresolvedLocation, testingTimeout).get();

			// wait for the job to restart, due to the slot request timing out
			CommonTestUtils.waitUntilCondition(
					() -> jobMasterGateway.requestJob(testingTimeout).join().getStatusTimestamp(JobStatus.RESTARTING) > 0,
					Deadline.fromNow(Duration.ofSeconds(5)),
					50L);
			final long end = System.nanoTime();

			// we rely on the slot request timeout to fail a stuck scheduling operation
			assertThat((end - start) / 1_000_000L, Matchers.greaterThanOrEqualTo(slotRequestTimeout));

			assertThat(submittedTaskFuture.isDone(), is(false));

			final SlotOffer slotOffer = new SlotOffer(new AllocationID(), 0, ResourceProfile.ANY);

			final CompletableFuture<Collection<SlotOffer>> acceptedSlotsFuture = jobMasterGateway.offerSlots(taskManagerUnresolvedLocation.getResourceID(), Collections.singleton(slotOffer), testingTimeout);

			assertThat(acceptedSlotsFuture.get(), hasSize(1));

			// wait for the deployed task
			submittedTaskFuture.get();
		} finally {
			RpcUtils.terminateRpcEndpoint(jobMaster, testingTimeout);
		}
	}

	/**
	 * Tests that we can close an unestablished ResourceManager connection.
	 */
	@Test
	public void testCloseUnestablishedResourceManagerConnection() throws Exception {
		final JobMaster jobMaster = createJobMaster(
			configuration,
			jobGraph,
			haServices,
			new TestingJobManagerSharedServicesBuilder().build());

		try {
			jobMaster.start(JobMasterId.generate()).get();

			final TestingResourceManagerGateway firstResourceManagerGateway = createAndRegisterTestingResourceManagerGateway();
			final TestingResourceManagerGateway secondResourceManagerGateway = createAndRegisterTestingResourceManagerGateway();

			final OneShotLatch firstJobManagerRegistration = new OneShotLatch();
			final OneShotLatch secondJobManagerRegistration = new OneShotLatch();

			firstResourceManagerGateway.setRegisterJobManagerFunction((jobMasterId, resourceID, s, jobID) -> {
				firstJobManagerRegistration.trigger();
				return CompletableFuture.completedFuture(firstResourceManagerGateway.getJobMasterRegistrationSuccess());
			});

			secondResourceManagerGateway.setRegisterJobManagerFunction(
				(jobMasterId, resourceID, s, jobID) -> {
					secondJobManagerRegistration.trigger();
					return CompletableFuture.completedFuture(secondResourceManagerGateway.getJobMasterRegistrationSuccess());
				});

			notifyResourceManagerLeaderListeners(firstResourceManagerGateway);

			// wait until we have seen the first registration attempt
			firstJobManagerRegistration.await();

			// this should stop the connection attempts towards the first RM
			notifyResourceManagerLeaderListeners(secondResourceManagerGateway);

			// check that we start registering at the second RM
			secondJobManagerRegistration.await();
		} finally {
			RpcUtils.terminateRpcEndpoint(jobMaster, testingTimeout);
		}
	}

	/**
	 * Tests that we continue reconnecting to the latest known RM after a disconnection
	 * message.
	 */
	@Test
	public void testReconnectionAfterDisconnect() throws Exception {
		final JobMaster jobMaster = createJobMaster(
			configuration,
			jobGraph,
			haServices,
			new TestingJobManagerSharedServicesBuilder().build(),
			heartbeatServices);

		final JobMasterGateway jobMasterGateway = jobMaster.getSelfGateway(JobMasterGateway.class);

		CompletableFuture<Acknowledge> startFuture = jobMaster.start(jobMasterId);

		try {
			// wait for the start to complete
			startFuture.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);
			final TestingResourceManagerGateway testingResourceManagerGateway = createAndRegisterTestingResourceManagerGateway();
			final BlockingQueue<JobMasterId> registrationsQueue = new ArrayBlockingQueue<>(1);

			testingResourceManagerGateway.setRegisterJobManagerFunction((jobMasterId, resourceID, s, jobID) -> {
				registrationsQueue.offer(jobMasterId);
				return CompletableFuture.completedFuture(testingResourceManagerGateway.getJobMasterRegistrationSuccess());
			});

			final ResourceManagerId resourceManagerId = testingResourceManagerGateway.getFencingToken();
			notifyResourceManagerLeaderListeners(testingResourceManagerGateway);

			// wait for first registration attempt
			final JobMasterId firstRegistrationAttempt = registrationsQueue.take();

			assertThat(firstRegistrationAttempt, equalTo(jobMasterId));

			assertThat(registrationsQueue.isEmpty(), is(true));
			jobMasterGateway.disconnectResourceManager(resourceManagerId, new FlinkException("Test exception"));

			// wait for the second registration attempt after the disconnect call
			assertThat(registrationsQueue.take(), equalTo(jobMasterId));
		} finally {
			RpcUtils.terminateRpcEndpoint(jobMaster, testingTimeout);
		}
	}

	/**
	 * Tests that the a JM connects to the leading RM after regaining leadership.
	 */
	@Test
	public void testResourceManagerConnectionAfterRegainingLeadership() throws Exception {
		final JobMaster jobMaster = createJobMaster(
			configuration,
			jobGraph,
			haServices,
			new TestingJobManagerSharedServicesBuilder().build(),
			heartbeatServices);

		CompletableFuture<Acknowledge> startFuture = jobMaster.start(jobMasterId);

		try {
			// wait for the start to complete
			startFuture.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);

			final TestingResourceManagerGateway testingResourceManagerGateway = createAndRegisterTestingResourceManagerGateway();

			final BlockingQueue<JobMasterId> registrationQueue = new ArrayBlockingQueue<>(1);
			testingResourceManagerGateway.setRegisterJobManagerFunction((jobMasterId, resourceID, s, jobID) -> {
				registrationQueue.offer(jobMasterId);
				return CompletableFuture.completedFuture(testingResourceManagerGateway.getJobMasterRegistrationSuccess());
			});

			notifyResourceManagerLeaderListeners(testingResourceManagerGateway);

			final JobMasterId firstRegistrationAttempt = registrationQueue.take();

			assertThat(firstRegistrationAttempt, equalTo(jobMasterId));

			jobMaster.suspend(new FlinkException("Test exception.")).get();

			final JobMasterId jobMasterId2 = JobMasterId.generate();

			jobMaster.start(jobMasterId2).get();

			final JobMasterId secondRegistrationAttempt = registrationQueue.take();

			assertThat(secondRegistrationAttempt, equalTo(jobMasterId2));
		} finally {
			RpcUtils.terminateRpcEndpoint(jobMaster, testingTimeout);
		}
	}

	/**
	 * Tests that input splits assigned to an Execution will be returned to the InputSplitAssigner
	 * if this execution fails.
	 */
	@Test
	public void testRequestNextInputSplitWithLocalFailover() throws Exception {

		configuration.setString(JobManagerOptions.EXECUTION_FAILOVER_STRATEGY,
			FailoverStrategyFactoryLoader.PIPELINED_REGION_RESTART_STRATEGY_NAME);

		final Function<List<List<InputSplit>>, Collection<InputSplit>> expectFailedExecutionInputSplits = inputSplitsPerTask -> inputSplitsPerTask.get(0);

		runRequestNextInputSplitTest(expectFailedExecutionInputSplits);
	}

	@Test
	public void testRequestNextInputSplitWithGlobalFailover() throws Exception {
		configuration.setInteger(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 1);
		configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofSeconds(0));
		configuration.setString(JobManagerOptions.EXECUTION_FAILOVER_STRATEGY, "full");

		final Function<List<List<InputSplit>>, Collection<InputSplit>> expectAllRemainingInputSplits = this::flattenCollection;

		runRequestNextInputSplitTest(expectAllRemainingInputSplits);
	}

	private void runRequestNextInputSplitTest(Function<List<List<InputSplit>>, Collection<InputSplit>> expectedRemainingInputSplits) throws Exception {
		final int parallelism = 2;
		final int splitsPerTask = 2;

		final int totalSplits = parallelism * splitsPerTask;
		final List<TestingInputSplit> allInputSplits = new ArrayList<>(totalSplits);

		for (int i = 0; i < totalSplits; i++) {
			allInputSplits.add(new TestingInputSplit(i));
		}

		final InputSplitSource<TestingInputSplit> inputSplitSource = new TestingInputSplitSource(allInputSplits);

		JobVertex source = new JobVertex("source");
		source.setParallelism(parallelism);
		source.setInputSplitSource(inputSplitSource);
		source.setInvokableClass(AbstractInvokable.class);

		final JobGraph inputSplitJobGraph = new JobGraph(source);

		final ExecutionConfig executionConfig = new ExecutionConfig();
		executionConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(100, 0));
		inputSplitJobGraph.setExecutionConfig(executionConfig);

		final JobMaster jobMaster = createJobMaster(
			configuration,
			inputSplitJobGraph,
			haServices,
			new TestingJobManagerSharedServicesBuilder().build(),
			heartbeatServices);

		CompletableFuture<Acknowledge> startFuture = jobMaster.start(jobMasterId);

		try {
			// wait for the start to complete
			startFuture.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);

			final JobMasterGateway jobMasterGateway = jobMaster.getSelfGateway(JobMasterGateway.class);

			final JobVertexID sourceId = source.getID();

			final List<AccessExecution> executions = getExecutions(jobMasterGateway, sourceId);
			final ExecutionAttemptID initialAttemptId = executions.get(0).getAttemptId();
			final List<List<InputSplit>> inputSplitsPerTask = new ArrayList<>(parallelism);

			// request all input splits
			for (AccessExecution execution : executions) {
				inputSplitsPerTask.add(getInputSplits(splitsPerTask, getInputSplitSupplier(sourceId, jobMasterGateway, execution.getAttemptId())));
			}

			final List<InputSplit> allRequestedInputSplits = flattenCollection(inputSplitsPerTask);
			assertThat(allRequestedInputSplits, containsInAnyOrder(allInputSplits.toArray(EMPTY_TESTING_INPUT_SPLITS)));

			waitUntilAllExecutionsAreScheduled(jobMasterGateway);

			// fail the first execution to trigger a failover
			jobMasterGateway.updateTaskExecutionState(new TaskExecutionState(inputSplitJobGraph.getJobID(), initialAttemptId, ExecutionState.FAILED)).get();

			// wait until the job has been recovered
			waitUntilAllExecutionsAreScheduled(jobMasterGateway);

			final ExecutionAttemptID restartedAttemptId = getFirstExecution(jobMasterGateway, sourceId).getAttemptId();

			final List<InputSplit> inputSplits = getRemainingInputSplits(getInputSplitSupplier(sourceId, jobMasterGateway, restartedAttemptId));

			assertThat(inputSplits, containsInAnyOrder(expectedRemainingInputSplits.apply(inputSplitsPerTask).toArray(EMPTY_TESTING_INPUT_SPLITS)));
		} finally {
			RpcUtils.terminateRpcEndpoint(jobMaster, testingTimeout);
		}
	}

	@Nonnull
	private List<InputSplit> flattenCollection(List<List<InputSplit>> inputSplitsPerTask) {
		return inputSplitsPerTask.stream().flatMap(Collection::stream).collect(Collectors.toList());
	}

	@Nonnull
	private Supplier<SerializedInputSplit> getInputSplitSupplier(JobVertexID jobVertexID, JobMasterGateway jobMasterGateway, ExecutionAttemptID initialAttemptId) {
		return () -> getInputSplit(jobMasterGateway, jobVertexID, initialAttemptId);
	}

	private void waitUntilAllExecutionsAreScheduled(final JobMasterGateway jobMasterGateway) throws Exception {
		final Duration duration = Duration.ofMillis(testingTimeout.toMilliseconds());
		final Deadline deadline = Deadline.fromNow(duration);

		CommonTestUtils.waitUntilCondition(
			() -> getExecutions(jobMasterGateway).stream().allMatch(execution -> execution.getState() == ExecutionState.SCHEDULED),
			deadline);
	}

	private static AccessExecution getFirstExecution(final JobMasterGateway jobMasterGateway, final JobVertexID jobVertexId) {
		final List<AccessExecution> executions = getExecutions(jobMasterGateway, jobVertexId);

		assertThat(executions, hasSize(greaterThanOrEqualTo(1)));
		return executions.get(0);
	}

	private static Collection<AccessExecution> getExecutions(final JobMasterGateway jobMasterGateway) {
		final ArchivedExecutionGraph archivedExecutionGraph = requestExecutionGraph(jobMasterGateway);

		return archivedExecutionGraph.getAllVertices().values()
			.stream()
			.flatMap(vertex -> Arrays.stream(vertex.getTaskVertices()))
			.map(AccessExecutionVertex::getCurrentExecutionAttempt)
			.collect(Collectors.toList());
	}

	private static List<AccessExecution> getExecutions(final JobMasterGateway jobMasterGateway, final JobVertexID jobVertexId) {
		final ArchivedExecutionGraph archivedExecutionGraph = requestExecutionGraph(jobMasterGateway);

		return Optional.ofNullable(archivedExecutionGraph.getAllVertices().get(jobVertexId))
			.map(accessExecutionJobVertex -> Arrays.asList(accessExecutionJobVertex.getTaskVertices()))
			.orElse(Collections.emptyList())
			.stream()
			.map(AccessExecutionVertex::getCurrentExecutionAttempt)
			.collect(Collectors.toList());
	}

	private static ArchivedExecutionGraph requestExecutionGraph(final JobMasterGateway jobMasterGateway) {
		try {
			return jobMasterGateway.requestJob(testingTimeout).get();
		} catch (InterruptedException | ExecutionException e) {
			throw new RuntimeException(e);
		}
	}

	@Nonnull
	private static List<InputSplit> getInputSplits(int numberInputSplits, Supplier<SerializedInputSplit> nextInputSplit) throws Exception {
		final List<InputSplit> actualInputSplits = new ArrayList<>(numberInputSplits);

		for (int i = 0; i < numberInputSplits; i++) {
			final SerializedInputSplit serializedInputSplit = nextInputSplit.get();

			assertThat(serializedInputSplit.isEmpty(), is(false));

			actualInputSplits.add(InstantiationUtil.deserializeObject(serializedInputSplit.getInputSplitData(), ClassLoader.getSystemClassLoader()));
		}

		return actualInputSplits;
	}

	private List<InputSplit> getRemainingInputSplits(Supplier<SerializedInputSplit> nextInputSplit) throws Exception {
		final List<InputSplit> actualInputSplits = new ArrayList<>(16);
		boolean hasMoreInputSplits = true;

		while (hasMoreInputSplits) {
			final SerializedInputSplit serializedInputSplit = nextInputSplit.get();

			if (serializedInputSplit.isEmpty()) {
				hasMoreInputSplits = false;
			} else {
				final InputSplit inputSplit = InstantiationUtil.deserializeObject(serializedInputSplit.getInputSplitData(), ClassLoader.getSystemClassLoader());

				if (inputSplit == null) {
					hasMoreInputSplits = false;
				} else {
					actualInputSplits.add(inputSplit);
				}
			}
		}

		return actualInputSplits;
	}

	private static SerializedInputSplit getInputSplit(
		final JobMasterGateway jobMasterGateway,
		final JobVertexID jobVertexId,
		final ExecutionAttemptID attemptId) {

		try {
			return jobMasterGateway
				.requestNextInputSplit(jobVertexId, attemptId)
				.get();
		} catch (InterruptedException | ExecutionException e) {
			throw new RuntimeException(e);
		}
	}

	private static final class TestingInputSplitSource implements InputSplitSource<TestingInputSplit> {
		private static final long serialVersionUID = -2344684048759139086L;

		private final List<TestingInputSplit> inputSplits;

		private TestingInputSplitSource(List<TestingInputSplit> inputSplits) {
			this.inputSplits = inputSplits;
		}

		@Override
		public TestingInputSplit[] createInputSplits(int minNumSplits) {
			return inputSplits.toArray(EMPTY_TESTING_INPUT_SPLITS);
		}

		@Override
		public InputSplitAssigner getInputSplitAssigner(TestingInputSplit[] inputSplits) {
			return new DefaultInputSplitAssigner(inputSplits);
		}
	}

	private static final class TestingInputSplit implements InputSplit {

		private static final long serialVersionUID = -5404803705463116083L;
		private final int splitNumber;

		TestingInputSplit(int number) {
			this.splitNumber = number;
		}

		public int getSplitNumber() {
			return splitNumber;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}

			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			TestingInputSplit that = (TestingInputSplit) o;
			return splitNumber == that.splitNumber;
		}

		@Override
		public int hashCode() {
			return Objects.hash(splitNumber);
		}
	}

	@Test
	public void testRequestKvStateWithoutRegistration() throws Exception {
		final JobGraph graph = createKvJobGraph();

		final JobMaster jobMaster = createJobMaster(
			configuration,
			graph,
			haServices,
			new TestingJobManagerSharedServicesBuilder().build(),
			heartbeatServices);

		CompletableFuture<Acknowledge> startFuture = jobMaster.start(jobMasterId);
		final JobMasterGateway jobMasterGateway = jobMaster.getSelfGateway(JobMasterGateway.class);

		try {
			// wait for the start to complete
			startFuture.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);

			// lookup location
			try {
				jobMasterGateway.requestKvStateLocation(graph.getJobID(), "unknown").get();
				fail("Expected to fail with UnknownKvStateLocation");
			} catch (Exception e) {
				assertTrue(ExceptionUtils.findThrowable(e, UnknownKvStateLocation.class).isPresent());
			}
		} finally {
			RpcUtils.terminateRpcEndpoint(jobMaster, testingTimeout);
		}
	}

	@Test
	public void testRequestKvStateOfWrongJob() throws Exception {
		final JobGraph graph = createKvJobGraph();

		final JobMaster jobMaster = createJobMaster(
			configuration,
			graph,
			haServices,
			new TestingJobManagerSharedServicesBuilder().build(),
			heartbeatServices);

		CompletableFuture<Acknowledge> startFuture = jobMaster.start(jobMasterId);
		final JobMasterGateway jobMasterGateway = jobMaster.getSelfGateway(JobMasterGateway.class);

		try {
			// wait for the start to complete
			startFuture.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);

			// lookup location
			try {
				jobMasterGateway.requestKvStateLocation(new JobID(), "unknown").get();
				fail("Expected to fail with FlinkJobNotFoundException");
			} catch (Exception e) {
				assertTrue(ExceptionUtils.findThrowable(e, FlinkJobNotFoundException.class).isPresent());
			}
		} finally {
			RpcUtils.terminateRpcEndpoint(jobMaster, testingTimeout);
		}
	}

	@Nonnull
	public JobGraph createKvJobGraph() {
		final JobVertex vertex1 = new JobVertex("v1");
		vertex1.setParallelism(4);
		vertex1.setMaxParallelism(16);
		vertex1.setInvokableClass(BlockingNoOpInvokable.class);

		final JobVertex vertex2 = new JobVertex("v2");
		vertex2.setParallelism(4);
		vertex2.setMaxParallelism(16);
		vertex2.setInvokableClass(BlockingNoOpInvokable.class);

		return new JobGraph(vertex1, vertex2);
	}

	@Test
	public void testRequestKvStateWithIrrelevantRegistration() throws Exception {
		final JobGraph graph = createKvJobGraph();

		final JobMaster jobMaster = createJobMaster(
			configuration,
			graph,
			haServices,
			new TestingJobManagerSharedServicesBuilder().build(),
			heartbeatServices);

		CompletableFuture<Acknowledge> startFuture = jobMaster.start(jobMasterId);
		final JobMasterGateway jobMasterGateway = jobMaster.getSelfGateway(JobMasterGateway.class);

		try {
			// wait for the start to complete
			startFuture.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);

			// register an irrelevant KvState
			try {
				jobMasterGateway.notifyKvStateRegistered(
					new JobID(),
					new JobVertexID(),
					new KeyGroupRange(0, 0),
					"any-name",
					new KvStateID(),
					new InetSocketAddress(InetAddress.getLocalHost(), 1233)).get();
				fail("Expected to fail with FlinkJobNotFoundException.");
			} catch (Exception e) {
				assertTrue(ExceptionUtils.findThrowable(e, FlinkJobNotFoundException.class).isPresent());
			}
		} finally {
			RpcUtils.terminateRpcEndpoint(jobMaster, testingTimeout);
		}
	}

	@Test
	public void testRegisterAndUnregisterKvState() throws Exception {
		final JobGraph graph = createKvJobGraph();
		final List<JobVertex> jobVertices = graph.getVerticesSortedTopologicallyFromSources();
		final JobVertex vertex1 = jobVertices.get(0);

		final JobMaster jobMaster = createJobMaster(
			configuration,
			graph,
			haServices,
			new TestingJobManagerSharedServicesBuilder().build(),
			heartbeatServices);

		CompletableFuture<Acknowledge> startFuture = jobMaster.start(jobMasterId);
		final JobMasterGateway jobMasterGateway = jobMaster.getSelfGateway(JobMasterGateway.class);

		try {
			// wait for the start to complete
			startFuture.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);

			// register a KvState
			final String registrationName = "register-me";
			final KvStateID kvStateID = new KvStateID();
			final KeyGroupRange keyGroupRange = new KeyGroupRange(0, 0);
			final InetSocketAddress address = new InetSocketAddress(InetAddress.getLocalHost(), 1029);

			jobMasterGateway.notifyKvStateRegistered(
				graph.getJobID(),
				vertex1.getID(),
				keyGroupRange,
				registrationName,
				kvStateID,
				address).get();

			final KvStateLocation location = jobMasterGateway.requestKvStateLocation(graph.getJobID(), registrationName).get();

			assertEquals(graph.getJobID(), location.getJobId());
			assertEquals(vertex1.getID(), location.getJobVertexId());
			assertEquals(vertex1.getMaxParallelism(), location.getNumKeyGroups());
			assertEquals(1, location.getNumRegisteredKeyGroups());
			assertEquals(1, keyGroupRange.getNumberOfKeyGroups());
			assertEquals(kvStateID, location.getKvStateID(keyGroupRange.getStartKeyGroup()));
			assertEquals(address, location.getKvStateServerAddress(keyGroupRange.getStartKeyGroup()));

			// unregister the KvState
			jobMasterGateway.notifyKvStateUnregistered(
				graph.getJobID(),
				vertex1.getID(),
				keyGroupRange,
				registrationName).get();

			try {
				jobMasterGateway.requestKvStateLocation(graph.getJobID(), registrationName).get();
				fail("Expected to fail with an UnknownKvStateLocation.");
			} catch (Exception e) {
				assertTrue(ExceptionUtils.findThrowable(e, UnknownKvStateLocation.class).isPresent());
			}
		} finally {
			RpcUtils.terminateRpcEndpoint(jobMaster, testingTimeout);
		}
	}

	@Test
	public void testDuplicatedKvStateRegistrationsFailTask() throws Exception {
		final JobGraph graph = createKvJobGraph();
		final List<JobVertex> jobVertices = graph.getVerticesSortedTopologicallyFromSources();
		final JobVertex vertex1 = jobVertices.get(0);
		final JobVertex vertex2 = jobVertices.get(1);

		final JobMaster jobMaster = createJobMaster(
			configuration,
			graph,
			haServices,
			new TestingJobManagerSharedServicesBuilder().build(),
			heartbeatServices);

		CompletableFuture<Acknowledge> startFuture = jobMaster.start(jobMasterId);
		final JobMasterGateway jobMasterGateway = jobMaster.getSelfGateway(JobMasterGateway.class);

		try {
			// wait for the start to complete
			startFuture.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);

			// duplicate registration fails task

			// register a KvState
			final String registrationName = "duplicate-me";
			final KvStateID kvStateID = new KvStateID();
			final KeyGroupRange keyGroupRange = new KeyGroupRange(0, 0);
			final InetSocketAddress address = new InetSocketAddress(InetAddress.getLocalHost(), 4396);

			jobMasterGateway.notifyKvStateRegistered(
				graph.getJobID(),
				vertex1.getID(),
				keyGroupRange,
				registrationName,
				kvStateID,
				address).get();

			try {
				jobMasterGateway.notifyKvStateRegistered(
					graph.getJobID(),
					vertex2.getID(), // <--- different operator, but...
					keyGroupRange,
					registrationName,  // ...same name
					kvStateID,
					address).get();
				fail("Expected to fail because of clashing registration message.");
			} catch (Exception e) {
				assertTrue(ExceptionUtils.findThrowableWithMessage(e, "Registration name clash").isPresent());
				assertEquals(JobStatus.FAILED, jobMasterGateway.requestJobStatus(testingTimeout).get());
			}
		} finally {
			RpcUtils.terminateRpcEndpoint(jobMaster, testingTimeout);
		}
	}

	/**
	 * Tests the {@link JobMaster#requestPartitionState(IntermediateDataSetID, ResultPartitionID)}
	 * call for a finished result partition.
	 */
	@Test
	public void testRequestPartitionState() throws Exception {
		final JobGraph producerConsumerJobGraph = producerConsumerJobGraph();
		final JobMaster jobMaster = createJobMaster(
			configuration,
			producerConsumerJobGraph,
			haServices,
			new TestingJobManagerSharedServicesBuilder().build(),
			heartbeatServices);

		CompletableFuture<Acknowledge> startFuture = jobMaster.start(jobMasterId);

		try {
			// wait for the start to complete
			startFuture.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);

			final CompletableFuture<TaskDeploymentDescriptor> tddFuture = new CompletableFuture<>();
			final TestingTaskExecutorGateway testingTaskExecutorGateway = new TestingTaskExecutorGatewayBuilder()
				.setSubmitTaskConsumer((taskDeploymentDescriptor, jobMasterId) -> {
					tddFuture.complete(taskDeploymentDescriptor);
					return CompletableFuture.completedFuture(Acknowledge.get());
				})
				.createTestingTaskExecutorGateway();
			final LocalUnresolvedTaskManagerLocation taskManagerLocation = new LocalUnresolvedTaskManagerLocation();

			final JobMasterGateway jobMasterGateway = jobMaster.getSelfGateway(JobMasterGateway.class);

			final Collection<SlotOffer> slotOffers = registerSlotsAtJobMaster(1, jobMasterGateway, testingTaskExecutorGateway, taskManagerLocation);

			assertThat(slotOffers, hasSize(1));

			// obtain tdd for the result partition ids
			final TaskDeploymentDescriptor tdd = tddFuture.get();

			assertThat(tdd.getProducedPartitions(), hasSize(1));
			final ResultPartitionDeploymentDescriptor partition = tdd.getProducedPartitions().iterator().next();

			final ExecutionAttemptID executionAttemptId = tdd.getExecutionAttemptId();
			final ExecutionAttemptID copiedExecutionAttemptId = new ExecutionAttemptID(executionAttemptId);

			// finish the producer task
			jobMasterGateway.updateTaskExecutionState(new TaskExecutionState(producerConsumerJobGraph.getJobID(), executionAttemptId, ExecutionState.FINISHED)).get();

			// request the state of the result partition of the producer
			final ResultPartitionID partitionId = new ResultPartitionID(partition.getPartitionId(), copiedExecutionAttemptId);
			CompletableFuture<ExecutionState> partitionStateFuture = jobMasterGateway.requestPartitionState(partition.getResultId(), partitionId);

			assertThat(partitionStateFuture.get(), equalTo(ExecutionState.FINISHED));

			// ask for unknown result partition
			partitionStateFuture = jobMasterGateway.requestPartitionState(partition.getResultId(), new ResultPartitionID());

			try {
				partitionStateFuture.get();
				fail("Expected failure.");
			} catch (ExecutionException e) {
				assertThat(ExceptionUtils.findThrowable(e, IllegalArgumentException.class).isPresent(), is(true));
			}

			// ask for wrong intermediate data set id
			partitionStateFuture = jobMasterGateway.requestPartitionState(new IntermediateDataSetID(), partitionId);

			try {
				partitionStateFuture.get();
				fail("Expected failure.");
			} catch (ExecutionException e) {
				assertThat(ExceptionUtils.findThrowable(e, IllegalArgumentException.class).isPresent(), is(true));
			}

			// ask for "old" execution
			partitionStateFuture = jobMasterGateway.requestPartitionState(partition.getResultId(), new ResultPartitionID(partition.getPartitionId(), new ExecutionAttemptID()));

			try {
				partitionStateFuture.get();
				fail("Expected failure.");
			} catch (ExecutionException e) {
				assertThat(ExceptionUtils.findThrowable(e, PartitionProducerDisposedException.class).isPresent(), is(true));
			}
		} finally {
			RpcUtils.terminateRpcEndpoint(jobMaster, testingTimeout);
		}
	}

	private void notifyResourceManagerLeaderListeners(TestingResourceManagerGateway testingResourceManagerGateway) {
		rmLeaderRetrievalService.notifyListener(testingResourceManagerGateway.getAddress(), testingResourceManagerGateway.getFencingToken().toUUID());
	}

	/**
	 * Tests that the timeout in {@link JobMasterGateway#triggerSavepoint(String, boolean, Time)}
	 * is respected.
	 */
	@Test
	public void testTriggerSavepointTimeout() throws Exception {
		final JobManagerSharedServices jobManagerSharedServices = new TestingJobManagerSharedServicesBuilder().build();
		final JobMasterConfiguration jobMasterConfiguration = JobMasterConfiguration.fromConfiguration(configuration);

		final SchedulerNGFactory schedulerNGFactory = SchedulerNGFactoryFactory.createSchedulerNGFactory(configuration);

		final JobMaster jobMaster = new JobMaster(
			rpcService,
			jobMasterConfiguration,
			jmResourceId,
			jobGraph,
			haServices,
			SlotPoolFactory.fromConfiguration(configuration),
			jobManagerSharedServices,
			heartbeatServices,
			UnregisteredJobManagerJobMetricGroupFactory.INSTANCE,
			new JobMasterBuilder.TestingOnCompletionActions(),
			testingFatalErrorHandler,
			JobMasterTest.class.getClassLoader(),
			schedulerNGFactory,
			NettyShuffleMaster.INSTANCE,
			NoOpJobMasterPartitionTracker.FACTORY,
			new DefaultExecutionDeploymentTracker(),
			DefaultExecutionDeploymentReconciler::new,
			System.currentTimeMillis()) {

			@Override
			public CompletableFuture<String> triggerSavepoint(
				@Nullable final String targetDirectory,
				final boolean cancelJob,
				final Time timeout) {
				return new CompletableFuture<>();
			}
		};

		try {
			final CompletableFuture<Acknowledge> startFuture = jobMaster.start(jobMasterId);
			startFuture.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);

			final JobMasterGateway jobMasterGateway = jobMaster.getSelfGateway(JobMasterGateway.class);
			final CompletableFuture<String> savepointFutureLowTimeout = jobMasterGateway.triggerSavepoint("/tmp", false, Time.milliseconds(1));
			final CompletableFuture<String> savepointFutureHighTimeout = jobMasterGateway.triggerSavepoint("/tmp", false, RpcUtils.INF_TIMEOUT);

			try {
				savepointFutureLowTimeout.get(testingTimeout.getSize(), testingTimeout.getUnit());
				fail();
			} catch (final ExecutionException e) {
				final Throwable cause = ExceptionUtils.stripExecutionException(e);
				assertThat(cause, instanceOf(TimeoutException.class));
			}

			assertThat(savepointFutureHighTimeout.isDone(), is(equalTo(false)));
		} finally {
			RpcUtils.terminateRpcEndpoint(jobMaster, testingTimeout);
		}
	}

	/**
	 * Tests that the TaskExecutor is released if all of its slots have been freed.
	 */
	@Test
	public void testReleasingTaskExecutorIfNoMoreSlotsRegistered() throws Exception {
		final JobManagerSharedServices jobManagerSharedServices = new TestingJobManagerSharedServicesBuilder().build();

		final JobGraph jobGraph = createSingleVertexJobWithRestartStrategy();

		final JobMaster jobMaster = createJobMaster(
			configuration,
			jobGraph,
			haServices,
			jobManagerSharedServices,
			heartbeatServices);

		final CompletableFuture<JobID> disconnectTaskExecutorFuture = new CompletableFuture<>();
		final CompletableFuture<AllocationID> freedSlotFuture = new CompletableFuture<>();
		final TestingTaskExecutorGateway testingTaskExecutorGateway = new TestingTaskExecutorGatewayBuilder()
			.setFreeSlotFunction(
				(allocationID, throwable) -> {
					freedSlotFuture.complete(allocationID);
					return CompletableFuture.completedFuture(Acknowledge.get());
				})
			.setDisconnectJobManagerConsumer((jobID, throwable) -> disconnectTaskExecutorFuture.complete(jobID))
			.createTestingTaskExecutorGateway();
		final LocalUnresolvedTaskManagerLocation taskManagerLocation = new LocalUnresolvedTaskManagerLocation();

		try {
			jobMaster.start(jobMasterId).get();

			final JobMasterGateway jobMasterGateway = jobMaster.getSelfGateway(JobMasterGateway.class);

			final Collection<SlotOffer> slotOffers = registerSlotsAtJobMaster(1, jobMasterGateway, testingTaskExecutorGateway, taskManagerLocation);

			// check that we accepted the offered slot
			assertThat(slotOffers, hasSize(1));
			final AllocationID allocationId = slotOffers.iterator().next().getAllocationId();

			// now fail the allocation and check that we close the connection to the TaskExecutor
			jobMasterGateway.failSlot(taskManagerLocation.getResourceID(), allocationId, new FlinkException("Fail allocation test exception"));

			// we should free the slot and then disconnect from the TaskExecutor because we use no longer slots from it
			assertThat(freedSlotFuture.get(), equalTo(allocationId));
			assertThat(disconnectTaskExecutorFuture.get(), equalTo(jobGraph.getJobID()));
		} finally {
			RpcUtils.terminateRpcEndpoint(jobMaster, testingTimeout);
		}
	}

	@Test
	public void testTaskExecutorNotReleasedOnFailedAllocationIfPartitionIsAllocated() throws Exception {
		final JobManagerSharedServices jobManagerSharedServices = new TestingJobManagerSharedServicesBuilder().build();

		final JobGraph jobGraph = JobGraphTestUtils.createSingleVertexJobGraph();

		final LocalUnresolvedTaskManagerLocation taskManagerUnresolvedLocation = new LocalUnresolvedTaskManagerLocation();

		final AtomicBoolean isTrackingPartitions = new AtomicBoolean(true);
		final TestingJobMasterPartitionTracker partitionTracker = new TestingJobMasterPartitionTracker();
		partitionTracker.setIsTrackingPartitionsForFunction(ignored -> isTrackingPartitions.get());

		final JobMaster jobMaster = new JobMasterBuilder(jobGraph, rpcService)
			.withConfiguration(configuration)
			.withHighAvailabilityServices(haServices)
			.withJobManagerSharedServices(jobManagerSharedServices)
			.withHeartbeatServices(heartbeatServices)
			.withPartitionTrackerFactory(ignored -> partitionTracker)
			.createJobMaster();

		final CompletableFuture<JobID> disconnectTaskExecutorFuture = new CompletableFuture<>();
		final CompletableFuture<AllocationID> freedSlotFuture = new CompletableFuture<>();
		final TestingTaskExecutorGateway testingTaskExecutorGateway = new TestingTaskExecutorGatewayBuilder()
			.setFreeSlotFunction(
				(allocationID, throwable) -> {
					freedSlotFuture.complete(allocationID);
					return CompletableFuture.completedFuture(Acknowledge.get());
				})
			.setDisconnectJobManagerConsumer((jobID, throwable) -> disconnectTaskExecutorFuture.complete(jobID))
			.createTestingTaskExecutorGateway();

		try {
			jobMaster.start(jobMasterId).get();

			final JobMasterGateway jobMasterGateway = jobMaster.getSelfGateway(JobMasterGateway.class);

			final Collection<SlotOffer> slotOffers = registerSlotsAtJobMaster(1, jobMasterGateway, testingTaskExecutorGateway, taskManagerUnresolvedLocation);

			// check that we accepted the offered slot
			assertThat(slotOffers, hasSize(1));
			final AllocationID allocationId = slotOffers.iterator().next().getAllocationId();

			jobMasterGateway.failSlot(taskManagerUnresolvedLocation.getResourceID(), allocationId, new FlinkException("Fail allocation test exception"));

			// we should free the slot, but not disconnect from the TaskExecutor as we still have an allocated partition
			assertThat(freedSlotFuture.get(), equalTo(allocationId));

			// trigger some request to guarantee ensure the slotAllocationFailure processing if complete
			jobMasterGateway.requestJobStatus(Time.seconds(5)).get();
			assertThat(disconnectTaskExecutorFuture.isDone(), is(false));
		} finally {
			RpcUtils.terminateRpcEndpoint(jobMaster, testingTimeout);
		}
	}

	/**
	 * Tests the updateGlobalAggregate functionality.
	 */
	@Test
	public void testJobMasterAggregatesValuesCorrectly() throws Exception {
		final JobMaster jobMaster = createJobMaster(
			configuration,
			jobGraph,
			haServices,
			new TestingJobManagerSharedServicesBuilder().build(),
			heartbeatServices);

		CompletableFuture<Acknowledge> startFuture = jobMaster.start(jobMasterId);
		final JobMasterGateway jobMasterGateway = jobMaster.getSelfGateway(JobMasterGateway.class);

		try {
			// wait for the start to complete
			startFuture.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);

			CompletableFuture<Object> updateAggregateFuture;

			AggregateFunction<Integer, Integer, Integer> aggregateFunction = createAggregateFunction();

			ClosureCleaner.clean(aggregateFunction, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);
			byte[] serializedAggregateFunction = InstantiationUtil.serializeObject(aggregateFunction);

			updateAggregateFuture = jobMasterGateway.updateGlobalAggregate("agg1", 1, serializedAggregateFunction);
			assertThat(updateAggregateFuture.get(), equalTo(1));

			updateAggregateFuture = jobMasterGateway.updateGlobalAggregate("agg1", 2, serializedAggregateFunction);
			assertThat(updateAggregateFuture.get(), equalTo(3));

			updateAggregateFuture = jobMasterGateway.updateGlobalAggregate("agg1", 3, serializedAggregateFunction);
			assertThat(updateAggregateFuture.get(), equalTo(6));

			updateAggregateFuture = jobMasterGateway.updateGlobalAggregate("agg1", 4, serializedAggregateFunction);
			assertThat(updateAggregateFuture.get(), equalTo(10));

			updateAggregateFuture = jobMasterGateway.updateGlobalAggregate("agg2", 10, serializedAggregateFunction);
			assertThat(updateAggregateFuture.get(), equalTo(10));

			updateAggregateFuture = jobMasterGateway.updateGlobalAggregate("agg2", 23, serializedAggregateFunction);
			assertThat(updateAggregateFuture.get(), equalTo(33));

		} finally {
			RpcUtils.terminateRpcEndpoint(jobMaster, testingTimeout);
		}
	}

	private AggregateFunction<Integer, Integer, Integer> createAggregateFunction() {
		return new AggregateFunction<Integer, Integer, Integer>() {

			@Override
			public Integer createAccumulator() {
				return 0;
			}

			@Override
			public Integer add(Integer value, Integer accumulator) {
				return accumulator + value;
			}

			@Override
			public Integer getResult(Integer accumulator) {
				return accumulator;
			}

			@Override
			public Integer merge(Integer a, Integer b) {
				return add(a, b);
			}
		};
	}

	@Nonnull
	private TestingResourceManagerGateway createAndRegisterTestingResourceManagerGateway() {
		final TestingResourceManagerGateway testingResourceManagerGateway = new TestingResourceManagerGateway();
		rpcService.registerGateway(testingResourceManagerGateway.getAddress(), testingResourceManagerGateway);
		return testingResourceManagerGateway;
	}

	/**
	 * Tests that the job execution is failed if the TaskExecutor disconnects from the
	 * JobMaster.
	 */
	@Test
	public void testJobFailureWhenGracefulTaskExecutorTermination() throws Exception {
		runJobFailureWhenTaskExecutorTerminatesTest(
			heartbeatServices,
			(localTaskManagerLocation, jobMasterGateway) -> jobMasterGateway.disconnectTaskManager(
				localTaskManagerLocation.getResourceID(),
				new FlinkException("Test disconnectTaskManager exception.")),
			(jobMasterGateway, resourceID) -> (ignoredA, ignoredB) -> {});
	}

	@Test
	public void testJobFailureWhenTaskExecutorHeartbeatTimeout() throws Exception {
		final TestingHeartbeatServices testingHeartbeatService = new TestingHeartbeatServices(heartbeatInterval, heartbeatTimeout);

		runJobFailureWhenTaskExecutorTerminatesTest(
			testingHeartbeatService,
			(localTaskManagerLocation, jobMasterGateway) ->
				testingHeartbeatService.triggerHeartbeatTimeout(
					jmResourceId,
					localTaskManagerLocation.getResourceID()),
			(jobMasterGateway, taskManagerResourceId) -> (resourceId, ignored) -> {
				jobMasterGateway.heartbeatFromTaskManager(taskManagerResourceId, TaskExecutorToJobManagerHeartbeatPayload.empty());
			}
		);
	}

	private void runJobFailureWhenTaskExecutorTerminatesTest(
			HeartbeatServices heartbeatServices,
			BiConsumer<LocalUnresolvedTaskManagerLocation, JobMasterGateway> jobReachedRunningState,
			BiFunction<JobMasterGateway, ResourceID, BiConsumer<ResourceID, AllocatedSlotReport>> heartbeatConsumerFunction) throws Exception {
		final JobGraph jobGraph = JobGraphTestUtils.createSingleVertexJobGraph();
		final JobMasterBuilder.TestingOnCompletionActions onCompletionActions = new JobMasterBuilder.TestingOnCompletionActions();
		final JobMaster jobMaster = createJobMaster(
			new Configuration(),
			jobGraph,
			haServices,
			new TestingJobManagerSharedServicesBuilder().build(),
			heartbeatServices,
			onCompletionActions);

		try {
			jobMaster.start(jobMasterId).get();

			final JobMasterGateway jobMasterGateway = jobMaster.getSelfGateway(JobMasterGateway.class);

			final LocalUnresolvedTaskManagerLocation taskManagerUnresolvedLocation = new LocalUnresolvedTaskManagerLocation();
			final CompletableFuture<ExecutionAttemptID> taskDeploymentFuture = new CompletableFuture<>();
			final TestingTaskExecutorGateway taskExecutorGateway = new TestingTaskExecutorGatewayBuilder()
				.setSubmitTaskConsumer((taskDeploymentDescriptor, jobMasterId) -> {
					taskDeploymentFuture.complete(taskDeploymentDescriptor.getExecutionAttemptId());
					return CompletableFuture.completedFuture(Acknowledge.get());
				})
				.setHeartbeatJobManagerConsumer(heartbeatConsumerFunction.apply(jobMasterGateway, taskManagerUnresolvedLocation.getResourceID()))
				.createTestingTaskExecutorGateway();

			final Collection<SlotOffer> slotOffers = registerSlotsAtJobMaster(1, jobMasterGateway, taskExecutorGateway, taskManagerUnresolvedLocation);
			assertThat(slotOffers, hasSize(1));

			final ExecutionAttemptID executionAttemptId = taskDeploymentFuture.get();

			jobMasterGateway.updateTaskExecutionState(new TaskExecutionState(jobGraph.getJobID(), executionAttemptId, ExecutionState.RUNNING)).get();

			jobReachedRunningState.accept(taskManagerUnresolvedLocation, jobMasterGateway);

			final ArchivedExecutionGraph archivedExecutionGraph = onCompletionActions.getJobReachedGloballyTerminalStateFuture().get();

			assertThat(archivedExecutionGraph.getState(), is(JobStatus.FAILED));
		} finally {
			RpcUtils.terminateRpcEndpoint(jobMaster, testingTimeout);
		}
	}

	private Collection<SlotOffer> registerSlotsAtJobMaster(
			int numberSlots,
			JobMasterGateway jobMasterGateway,
			TaskExecutorGateway taskExecutorGateway,
			UnresolvedTaskManagerLocation unresolvedTaskManagerLocation) throws ExecutionException, InterruptedException {
		rpcService.registerGateway(taskExecutorGateway.getAddress(), taskExecutorGateway);

		jobMasterGateway.registerTaskManager(taskExecutorGateway.getAddress(), unresolvedTaskManagerLocation, testingTimeout).get();

		Collection<SlotOffer> slotOffers = IntStream
			.range(0, numberSlots)
			.mapToObj(
				index -> new SlotOffer(new AllocationID(), index, ResourceProfile.ANY))
			.collect(Collectors.toList());

		return jobMasterGateway.offerSlots(unresolvedTaskManagerLocation.getResourceID(), slotOffers, testingTimeout).get();
	}

	private JobGraph producerConsumerJobGraph() {
		final JobVertex producer = new JobVertex("Producer");
		producer.setInvokableClass(NoOpInvokable.class);
		final JobVertex consumer = new JobVertex("Consumer");
		consumer.setInvokableClass(NoOpInvokable.class);

		consumer.connectNewDataSetAsInput(producer, DistributionPattern.POINTWISE, ResultPartitionType.BLOCKING);

		return new JobGraph(producer, consumer);
	}

	private File createSavepoint(long savepointId) throws IOException {
		return createSavepointWithOperatorState(savepointId);
	}

	private File createSavepointWithOperatorState(long savepointId, OperatorID... operatorIds) throws IOException {
		final File savepointFile = temporaryFolder.newFile();
		final Collection<OperatorState> operatorStates = createOperatorState(operatorIds);
		final CheckpointMetadata savepoint = new CheckpointMetadata(savepointId, operatorStates, Collections.emptyList());

		try (FileOutputStream fileOutputStream = new FileOutputStream(savepointFile)) {
			Checkpoints.storeCheckpointMetadata(savepoint, fileOutputStream);
		}

		return savepointFile;
	}

	private Collection<OperatorState> createOperatorState(OperatorID... operatorIds) {
		Random random = new Random();
		Collection<OperatorState> operatorStates = new ArrayList<>(operatorIds.length);

		for (OperatorID operatorId : operatorIds) {
			final OperatorState operatorState = new OperatorState(operatorId, 1, 42);
			final OperatorSubtaskState subtaskState = OperatorSubtaskState.builder()
				.setManagedOperatorState(new OperatorStreamStateHandle(
					Collections.emptyMap(),
					new ByteStreamStateHandle("foobar", new byte[0])))
				.setInputChannelState(singleton(createNewInputChannelStateHandle(10, random)))
				.setResultSubpartitionState(singleton(createNewResultSubpartitionStateHandle(10, random)))
				.build();
			operatorState.putState(0, subtaskState);
			operatorStates.add(operatorState);
		}

		return operatorStates;
	}

	@Nonnull
	private JobGraph createJobGraphWithCheckpointing(SavepointRestoreSettings savepointRestoreSettings) {
		return createJobGraphFromJobVerticesWithCheckpointing(savepointRestoreSettings);
	}

	@Nonnull
	private JobGraph createJobGraphFromJobVerticesWithCheckpointing(SavepointRestoreSettings savepointRestoreSettings, JobVertex... jobVertices) {
		final JobGraph jobGraph = new JobGraph(jobVertices);

		// enable checkpointing which is required to resume from a savepoint
		final CheckpointCoordinatorConfiguration checkpoinCoordinatorConfiguration = new CheckpointCoordinatorConfiguration(
			1000L,
			1000L,
			1000L,
			1,
			CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
			true,
			false,
			false,
			0);
		final JobCheckpointingSettings checkpointingSettings = new JobCheckpointingSettings(
			Collections.emptyList(),
			Collections.emptyList(),
			Collections.emptyList(),
			checkpoinCoordinatorConfiguration,
			null);
		jobGraph.setSnapshotSettings(checkpointingSettings);
		jobGraph.setSavepointRestoreSettings(savepointRestoreSettings);

		return jobGraph;
	}

	@Nonnull
	private JobMaster createJobMaster(
			Configuration configuration,
			JobGraph jobGraph,
			HighAvailabilityServices highAvailabilityServices,
			JobManagerSharedServices jobManagerSharedServices) throws Exception {
		return createJobMaster(
			configuration,
			jobGraph,
			highAvailabilityServices,
			jobManagerSharedServices,
			fastHeartbeatServices);
	}

	@Nonnull
	private JobMaster createJobMaster(
			Configuration configuration,
			JobGraph jobGraph,
			HighAvailabilityServices highAvailabilityServices,
			JobManagerSharedServices jobManagerSharedServices,
			HeartbeatServices heartbeatServices) throws Exception {

		return createJobMaster(
			configuration,
			jobGraph,
			highAvailabilityServices,
			jobManagerSharedServices,
			heartbeatServices,
			new JobMasterBuilder.TestingOnCompletionActions());
	}

	@Nonnull
	private JobMaster createJobMaster(
			Configuration configuration,
			JobGraph jobGraph,
			HighAvailabilityServices highAvailabilityServices,
			JobManagerSharedServices jobManagerSharedServices,
			HeartbeatServices heartbeatServices,
			OnCompletionActions onCompletionActions) throws Exception {

		return new JobMasterBuilder(jobGraph, rpcService)
			.withConfiguration(configuration)
			.withHighAvailabilityServices(highAvailabilityServices)
			.withJobManagerSharedServices(jobManagerSharedServices)
			.withHeartbeatServices(heartbeatServices)
			.withOnCompletionActions(onCompletionActions)
			.withResourceId(jmResourceId)
			.createJobMaster();
	}

	private JobGraph createSingleVertexJobWithRestartStrategy() throws IOException {
		final JobGraph jobGraph = JobGraphTestUtils.createSingleVertexJobGraph();

		final ExecutionConfig executionConfig = new ExecutionConfig();
		executionConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, 0L));
		jobGraph.setExecutionConfig(executionConfig);

		return jobGraph;
	}

	private static final class DummyCheckpointStorageLocation implements CompletedCheckpointStorageLocation {

		private static final long serialVersionUID = 164095949572620688L;

		@Override
		public String getExternalPointer() {
			return null;
		}

		@Override
		public StreamStateHandle getMetadataHandle() {
			return null;
		}

		@Override
		public void disposeStorageLocation() throws IOException {

		}
	}
}
