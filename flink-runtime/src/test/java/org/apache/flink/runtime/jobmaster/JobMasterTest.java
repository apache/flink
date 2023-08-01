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
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HeartbeatManagerOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.core.io.InputSplitSource;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.blocklist.BlockedNode;
import org.apache.flink.runtime.blocklist.DefaultBlocklistHandler;
import org.apache.flink.runtime.checkpoint.CheckpointProperties;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy;
import org.apache.flink.runtime.checkpoint.CheckpointStatsSnapshot;
import org.apache.flink.runtime.checkpoint.CheckpointsCleaner;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.PerJobCheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.StandaloneCompletedCheckpointStore;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.AccessExecution;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.failover.flip1.FailoverStrategyFactoryLoader;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.heartbeat.HeartbeatServicesImpl;
import org.apache.flink.runtime.heartbeat.TestingHeartbeatServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.instance.SimpleSlotContext;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.TestingJobMasterPartitionTracker;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphBuilder;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobResourceRequirements;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobmanager.PartitionProducerDisposedException;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.slotpool.DeclarativeSlotPoolFactory;
import org.apache.flink.runtime.jobmaster.slotpool.FreeSlotInfoTracker;
import org.apache.flink.runtime.jobmaster.slotpool.FreeSlotInfoTrackerTestUtils;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlot;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPool;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPoolService;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPoolServiceFactory;
import org.apache.flink.runtime.jobmaster.slotpool.TestingSlotPoolServiceBuilder;
import org.apache.flink.runtime.jobmaster.utils.JobMasterBuilder;
import org.apache.flink.runtime.leaderretrieval.SettableLeaderRetrievalService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.rpc.exceptions.RecipientUnreachableException;
import org.apache.flink.runtime.scheduler.DefaultSchedulerFactory;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.runtime.scheduler.SchedulerTestingUtils;
import org.apache.flink.runtime.scheduler.TestingSchedulerNG;
import org.apache.flink.runtime.scheduler.TestingSchedulerNGFactory;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.LocalUnresolvedTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.taskmanager.UnresolvedTaskManagerLocation;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.TestLoggerExtension;
import org.apache.flink.util.concurrent.FutureUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.core.testutils.FlinkAssertions.assertThatFuture;
import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createExecutionAttemptId;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link JobMaster}. */
@ExtendWith(TestLoggerExtension.class)
class JobMasterTest {

    private static final TestingInputSplit[] EMPTY_TESTING_INPUT_SPLITS = new TestingInputSplit[0];

    @TempDir private Path temporaryFolder;

    private static final Time testingTimeout = Time.seconds(10L);

    private static final long fastHeartbeatInterval = 1L;
    private static final long fastHeartbeatTimeout = 10L;

    private static final long heartbeatInterval = 1000L;
    private static final long heartbeatTimeout = 5_000_000L;

    private static final JobGraph jobGraph = JobGraphTestUtils.singleNoOpJobGraph();

    private static TestingRpcService rpcService;

    private static HeartbeatServices fastHeartbeatServices;

    private static HeartbeatServices heartbeatServices;

    private Configuration configuration;

    private ResourceID jmResourceId;

    private JobMasterId jobMasterId;

    private TestingHighAvailabilityServices haServices;

    private SettableLeaderRetrievalService rmLeaderRetrievalService;

    private TestingFatalErrorHandler testingFatalErrorHandler;

    @BeforeAll
    static void setupAll() {
        rpcService = new TestingRpcService();

        fastHeartbeatServices =
                new HeartbeatServicesImpl(
                        fastHeartbeatInterval,
                        fastHeartbeatTimeout,
                        HeartbeatManagerOptions.FAILED_RPC_DETECTION_DISABLED);
        heartbeatServices = new HeartbeatServicesImpl(heartbeatInterval, heartbeatTimeout, 1);
    }

    @BeforeEach
    void setup() throws IOException {
        configuration = new Configuration();
        haServices = new TestingHighAvailabilityServices();
        jobMasterId = JobMasterId.generate();
        jmResourceId = ResourceID.generate();

        testingFatalErrorHandler = new TestingFatalErrorHandler();

        haServices.setCheckpointRecoveryFactory(new StandaloneCheckpointRecoveryFactory());

        rmLeaderRetrievalService = new SettableLeaderRetrievalService(null, null);
        haServices.setResourceManagerLeaderRetriever(rmLeaderRetrievalService);

        configuration.setString(
                BlobServerOptions.STORAGE_DIRECTORY,
                Files.createTempDirectory(temporaryFolder, UUID.randomUUID().toString())
                        .toString());
    }

    @AfterEach
    void teardown() throws Exception {
        if (testingFatalErrorHandler != null) {
            testingFatalErrorHandler.rethrowError();
        }

        rpcService.clearGateways();
    }

    @AfterAll
    static void teardownAll() {
        if (rpcService != null) {
            rpcService.closeAsync();
            rpcService = null;
        }
    }

    @Test
    void testTaskManagerRegistrationTriggersHeartbeating() throws Exception {
        final CompletableFuture<ResourceID> heartbeatResourceIdFuture = new CompletableFuture<>();
        final UnresolvedTaskManagerLocation unresolvedTaskManagerLocation =
                new LocalUnresolvedTaskManagerLocation();
        final TestingTaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder()
                        .setHeartbeatJobManagerFunction(
                                (taskManagerId, ignored) -> {
                                    heartbeatResourceIdFuture.complete(taskManagerId);
                                    return FutureUtils.completedVoidFuture();
                                })
                        .createTestingTaskExecutorGateway();

        rpcService.registerGateway(taskExecutorGateway.getAddress(), taskExecutorGateway);

        try (final JobMaster jobMaster =
                new JobMasterBuilder(jobGraph, rpcService)
                        .withResourceId(jmResourceId)
                        .withConfiguration(configuration)
                        .withHighAvailabilityServices(haServices)
                        .withHeartbeatServices(new HeartbeatServicesImpl(1L, 10000L))
                        .createJobMaster()) {

            jobMaster.start();

            final JobMasterGateway jobMasterGateway =
                    jobMaster.getSelfGateway(JobMasterGateway.class);

            // register task manager will trigger monitor heartbeat target, schedule heartbeat
            // request at interval time
            CompletableFuture<RegistrationResponse> registrationResponse =
                    jobMasterGateway.registerTaskManager(
                            jobGraph.getJobID(),
                            TaskManagerRegistrationInformation.create(
                                    taskExecutorGateway.getAddress(),
                                    unresolvedTaskManagerLocation,
                                    TestingUtils.zeroUUID()),
                            testingTimeout);

            // wait for the completion of the registration
            registrationResponse.get();

            assertThat(heartbeatResourceIdFuture.join())
                    .satisfiesAnyOf(
                            resourceID -> assertThat(resourceID).isNull(),
                            resourceID -> assertThat(resourceID).isEqualTo(jmResourceId));
        }
    }

    @Test
    void testHeartbeatTimeoutWithTaskManager() throws Exception {
        runHeartbeatTest(
                new TestingTaskExecutorGatewayBuilder()
                        .setHeartbeatJobManagerFunction(
                                (taskManagerId, ignored) -> FutureUtils.completedVoidFuture()),
                fastHeartbeatServices);
    }

    private void runHeartbeatTest(
            TestingTaskExecutorGatewayBuilder testingTaskExecutorGatewayBuilder,
            HeartbeatServices heartbeatServices)
            throws Exception {
        final CompletableFuture<JobID> disconnectedJobManagerFuture = new CompletableFuture<>();
        final UnresolvedTaskManagerLocation unresolvedTaskManagerLocation =
                new LocalUnresolvedTaskManagerLocation();
        final TestingTaskExecutorGateway taskExecutorGateway =
                testingTaskExecutorGatewayBuilder
                        .setDisconnectJobManagerConsumer(
                                (jobId, throwable) -> disconnectedJobManagerFuture.complete(jobId))
                        .createTestingTaskExecutorGateway();

        rpcService.registerGateway(taskExecutorGateway.getAddress(), taskExecutorGateway);

        try (final JobMaster jobMaster =
                new JobMasterBuilder(jobGraph, rpcService)
                        .withResourceId(jmResourceId)
                        .withConfiguration(configuration)
                        .withHighAvailabilityServices(haServices)
                        .withHeartbeatServices(heartbeatServices)
                        .createJobMaster()) {

            jobMaster.start();

            final JobMasterGateway jobMasterGateway =
                    jobMaster.getSelfGateway(JobMasterGateway.class);

            // register task manager will trigger monitor heartbeat target, schedule heartbeat
            // request at interval time
            CompletableFuture<RegistrationResponse> registrationResponse =
                    jobMasterGateway.registerTaskManager(
                            jobGraph.getJobID(),
                            TaskManagerRegistrationInformation.create(
                                    taskExecutorGateway.getAddress(),
                                    unresolvedTaskManagerLocation,
                                    TestingUtils.zeroUUID()),
                            testingTimeout);

            // wait for the completion of the registration
            registrationResponse.get();

            final JobID disconnectedJobManager =
                    disconnectedJobManagerFuture.get(
                            testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);

            assertThat(disconnectedJobManager).isEqualTo(jobGraph.getJobID());
        }
    }

    @Test
    void testTaskManagerBecomesUnreachableTriggersDisconnect() throws Exception {
        runHeartbeatTest(
                new TestingTaskExecutorGatewayBuilder()
                        .setHeartbeatJobManagerFunction(
                                (taskManagerId, ignored) ->
                                        FutureUtils.completedExceptionally(
                                                new RecipientUnreachableException(
                                                        "sender",
                                                        "recipient",
                                                        "test heartbeat target is unreachable"))),
                heartbeatServices);
    }

    /**
     * Tests that the {@link AllocatedSlotReport} contains up to date information and not stale
     * information about the allocated slots on the {@link JobMaster}.
     *
     * <p>This is a probabilistic test case which only fails if executed repeatedly without the fix
     * for FLINK-12863.
     */
    @Test
    void testAllocatedSlotReportDoesNotContainStaleInformation() throws Exception {
        final CompletableFuture<Void> assertionFuture = new CompletableFuture<>();
        final UnresolvedTaskManagerLocation unresolvedTaskManagerLocation =
                new LocalUnresolvedTaskManagerLocation();
        final AtomicBoolean terminateHeartbeatVerification = new AtomicBoolean(false);
        final OneShotLatch hasReceivedSlotOffers = new OneShotLatch();
        final TestingTaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder()
                        .setHeartbeatJobManagerFunction(
                                (taskManagerId, allocatedSlotReport) -> {
                                    try {
                                        if (hasReceivedSlotOffers.isTriggered()) {
                                            assertThat(allocatedSlotReport.getAllocatedSlotInfos())
                                                    .hasSize(1);
                                        } else {
                                            assertThat(allocatedSlotReport.getAllocatedSlotInfos())
                                                    .isEmpty();
                                        }
                                    } catch (AssertionError e) {
                                        assertionFuture.completeExceptionally(e);
                                    }

                                    if (terminateHeartbeatVerification.get()) {
                                        assertionFuture.complete(null);
                                    }
                                    return FutureUtils.completedVoidFuture();
                                })
                        .createTestingTaskExecutorGateway();

        rpcService.registerGateway(taskExecutorGateway.getAddress(), taskExecutorGateway);

        final JobGraph jobGraph = JobGraphTestUtils.singleNoOpJobGraph();
        try (final JobMaster jobMaster =
                new JobMasterBuilder(jobGraph, rpcService)
                        .withHeartbeatServices(new HeartbeatServicesImpl(5L, 1000L))
                        .withSlotPoolServiceSchedulerFactory(
                                DefaultSlotPoolServiceSchedulerFactory.create(
                                        new TestingSlotPoolFactory(hasReceivedSlotOffers),
                                        new DefaultSchedulerFactory()))
                        .createJobMaster()) {

            jobMaster.start();

            final JobMasterGateway jobMasterGateway =
                    jobMaster.getSelfGateway(JobMasterGateway.class);

            // register task manager will trigger monitor heartbeat target, schedule heartbeat
            // request at interval time
            CompletableFuture<RegistrationResponse> registrationResponse =
                    jobMasterGateway.registerTaskManager(
                            jobGraph.getJobID(),
                            TaskManagerRegistrationInformation.create(
                                    taskExecutorGateway.getAddress(),
                                    unresolvedTaskManagerLocation,
                                    TestingUtils.zeroUUID()),
                            testingTimeout);

            // wait for the completion of the registration
            registrationResponse.get();

            final SlotOffer slotOffer = new SlotOffer(new AllocationID(), 0, ResourceProfile.ANY);

            final CompletableFuture<Collection<SlotOffer>> slotOfferFuture =
                    jobMasterGateway.offerSlots(
                            unresolvedTaskManagerLocation.getResourceID(),
                            Collections.singleton(slotOffer),
                            testingTimeout);

            assertThat(slotOfferFuture.get()).containsExactly(slotOffer);

            terminateHeartbeatVerification.set(true);

            // make sure that no assertion has been violated
            assertionFuture.get();
        }
    }

    private static final class TestingSlotPoolFactory implements SlotPoolServiceFactory {

        private final OneShotLatch hasReceivedSlotOffers;

        public TestingSlotPoolFactory(OneShotLatch hasReceivedSlotOffers) {
            this.hasReceivedSlotOffers = hasReceivedSlotOffers;
        }

        @Nonnull
        @Override
        public SlotPoolService createSlotPoolService(
                @Nonnull JobID jobId, DeclarativeSlotPoolFactory declarativeSlotPoolFactory) {
            return new TestingSlotPool(jobId, hasReceivedSlotOffers);
        }
    }

    private static final class TestingSlotPool implements SlotPool, SlotPoolService {

        private final JobID jobId;

        private final OneShotLatch hasReceivedSlotOffers;

        private final Map<ResourceID, Collection<SlotInfo>> registeredSlots;

        private TestingSlotPool(JobID jobId, OneShotLatch hasReceivedSlotOffers) {
            this.jobId = jobId;
            this.hasReceivedSlotOffers = hasReceivedSlotOffers;
            this.registeredSlots = new HashMap<>(16);
        }

        @Override
        public void start(
                JobMasterId jobMasterId,
                String newJobManagerAddress,
                ComponentMainThreadExecutor jmMainThreadScheduledExecutor) {}

        @Override
        public void close() {
            clear();
        }

        private void clear() {
            registeredSlots.clear();
        }

        @Override
        public void connectToResourceManager(ResourceManagerGateway resourceManagerGateway) {
            throw new UnsupportedOperationException(
                    "TestingSlotPool does not support this operation.");
        }

        @Override
        public void disconnectResourceManager() {
            throw new UnsupportedOperationException(
                    "TestingSlotPool does not support this operation.");
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
        public void releaseFreeSlotsOnTaskManager(ResourceID taskManagerId, Exception cause) {
            throw new UnsupportedOperationException(
                    "TestingSlotPool does not support this operation.");
        }

        @Override
        public Collection<SlotOffer> offerSlots(
                TaskManagerLocation taskManagerLocation,
                TaskManagerGateway taskManagerGateway,
                Collection<SlotOffer> offers) {
            hasReceivedSlotOffers.trigger();
            final Collection<SlotInfo> slotInfos =
                    Optional.ofNullable(registeredSlots.get(taskManagerLocation.getResourceID()))
                            .orElseThrow(
                                    () -> new FlinkRuntimeException("TaskManager not registered."));

            int slotIndex = slotInfos.size();

            for (SlotOffer offer : offers) {
                slotInfos.add(
                        new SimpleSlotContext(
                                offer.getAllocationId(),
                                taskManagerLocation,
                                slotIndex,
                                taskManagerGateway));
                slotIndex++;
            }

            return offers;
        }

        @Override
        public Optional<ResourceID> failAllocation(
                @Nullable ResourceID resourceID, AllocationID allocationId, Exception cause) {
            throw new UnsupportedOperationException(
                    "TestingSlotPool does not support this operation.");
        }

        @Override
        public FreeSlotInfoTracker getFreeSlotInfoTracker() {
            Map<AllocationID, SlotInfo> freeSlots =
                    registeredSlots.values().stream()
                            .flatMap(Collection::stream)
                            .collect(Collectors.toMap(SlotInfo::getAllocationId, s -> s));
            return FreeSlotInfoTrackerTestUtils.createDefaultFreeSlotInfoTracker(freeSlots);
        }

        @Override
        public Collection<SlotInfo> getAllocatedSlotsInformation() {
            return Collections.emptyList();
        }

        @Override
        public Optional<PhysicalSlot> allocateAvailableSlot(
                @Nonnull SlotRequestId slotRequestId,
                @Nonnull AllocationID allocationID,
                @Nonnull ResourceProfile requirementProfile) {
            throw new UnsupportedOperationException(
                    "TestingSlotPool does not support this operation.");
        }

        @Nonnull
        @Override
        public CompletableFuture<PhysicalSlot> requestNewAllocatedSlot(
                @Nonnull SlotRequestId slotRequestId,
                @Nonnull ResourceProfile resourceProfile,
                @Nonnull Collection<AllocationID> preferredAllocations,
                @Nullable Time timeout) {
            return new CompletableFuture<>();
        }

        @Nonnull
        @Override
        public CompletableFuture<PhysicalSlot> requestNewAllocatedBatchSlot(
                @Nonnull SlotRequestId slotRequestId,
                @Nonnull ResourceProfile resourceProfile,
                @Nonnull Collection<AllocationID> preferredAllocations) {
            return new CompletableFuture<>();
        }

        @Override
        public void disableBatchSlotRequestTimeoutCheck() {
            // no action and no exception is expected
        }

        @Override
        public AllocatedSlotReport createAllocatedSlotReport(ResourceID taskManagerId) {
            final Collection<SlotInfo> slotInfos =
                    registeredSlots.getOrDefault(taskManagerId, Collections.emptyList());

            final List<AllocatedSlotInfo> allocatedSlotInfos =
                    slotInfos.stream()
                            .map(
                                    slotInfo ->
                                            new AllocatedSlotInfo(
                                                    slotInfo.getPhysicalSlotNumber(),
                                                    slotInfo.getAllocationId()))
                            .collect(Collectors.toList());

            return new AllocatedSlotReport(jobId, allocatedSlotInfos);
        }

        @Override
        public void setIsJobRestarting(boolean isJobRestarting) {}

        @Override
        public void releaseSlot(@Nonnull SlotRequestId slotRequestId, @Nullable Throwable cause) {
            throw new UnsupportedOperationException(
                    "TestingSlotPool does not support this operation.");
        }
    }

    @Test
    void testHeartbeatTimeoutWithResourceManager() throws Exception {
        final String resourceManagerAddress = "rm";
        final ResourceManagerId resourceManagerId = ResourceManagerId.generate();
        final ResourceID rmResourceId = new ResourceID(resourceManagerAddress);

        final TestingResourceManagerGateway resourceManagerGateway =
                new TestingResourceManagerGateway(
                        resourceManagerId, rmResourceId, resourceManagerAddress, "localhost");

        final CompletableFuture<Tuple3<JobMasterId, ResourceID, JobID>>
                jobManagerRegistrationFuture = new CompletableFuture<>();
        final CompletableFuture<JobID> disconnectedJobManagerFuture = new CompletableFuture<>();
        final CountDownLatch registrationAttempts = new CountDownLatch(2);

        resourceManagerGateway.setRegisterJobManagerFunction(
                (jobMasterId, resourceID, s, jobID) -> {
                    jobManagerRegistrationFuture.complete(
                            Tuple3.of(jobMasterId, resourceID, jobID));
                    registrationAttempts.countDown();

                    return CompletableFuture.completedFuture(
                            resourceManagerGateway.getJobMasterRegistrationSuccess());
                });

        resourceManagerGateway.setDisconnectJobManagerConsumer(
                tuple -> disconnectedJobManagerFuture.complete(tuple.f0));

        rpcService.registerGateway(resourceManagerAddress, resourceManagerGateway);

        try (final JobMaster jobMaster =
                new JobMasterBuilder(jobGraph, rpcService)
                        .withJobMasterId(jobMasterId)
                        .withResourceId(jmResourceId)
                        .withConfiguration(configuration)
                        .withHighAvailabilityServices(haServices)
                        .withHeartbeatServices(fastHeartbeatServices)
                        .createJobMaster()) {

            jobMaster.start();

            // define a leader and see that a registration happens
            rmLeaderRetrievalService.notifyListener(
                    resourceManagerAddress, resourceManagerId.toUUID());

            // register job manager success will trigger monitor heartbeat target between jm and rm
            final Tuple3<JobMasterId, ResourceID, JobID> registrationInformation =
                    jobManagerRegistrationFuture.get(
                            testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);

            assertThat(registrationInformation.f0).isEqualTo(jobMasterId);
            assertThat(registrationInformation.f1).isEqualTo(jmResourceId);
            assertThat(registrationInformation.f2).isEqualTo(jobGraph.getJobID());

            final JobID disconnectedJobManager =
                    disconnectedJobManagerFuture.get(
                            testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);

            // heartbeat timeout should trigger disconnect JobManager from ResourceManager
            assertThat(disconnectedJobManager).isEqualTo(jobGraph.getJobID());

            // the JobMaster should try to reconnect to the RM
            registrationAttempts.await();
        }
    }

    @Test
    void testResourceManagerBecomesUnreachableTriggersDisconnect() throws Exception {
        final String resourceManagerAddress = "rm";
        final ResourceManagerId resourceManagerId = ResourceManagerId.generate();
        final ResourceID rmResourceId = new ResourceID(resourceManagerAddress);

        final TestingResourceManagerGateway resourceManagerGateway =
                new TestingResourceManagerGateway(
                        resourceManagerId, rmResourceId, resourceManagerAddress, "localhost");

        final CompletableFuture<JobID> disconnectedJobManagerFuture = new CompletableFuture<>();
        final CountDownLatch registrationAttempts = new CountDownLatch(2);

        final Queue<CompletableFuture<RegistrationResponse>> connectionResponses =
                new ArrayDeque<>(2);
        connectionResponses.add(
                CompletableFuture.completedFuture(
                        resourceManagerGateway.getJobMasterRegistrationSuccess()));
        connectionResponses.add(new CompletableFuture<>());

        resourceManagerGateway.setRegisterJobManagerFunction(
                (jobMasterId, resourceID, s, jobID) -> {
                    registrationAttempts.countDown();
                    return connectionResponses.poll();
                });

        resourceManagerGateway.setDisconnectJobManagerConsumer(
                tuple -> disconnectedJobManagerFuture.complete(tuple.f0));
        resourceManagerGateway.setJobMasterHeartbeatFunction(
                ignored ->
                        FutureUtils.completedExceptionally(
                                new RecipientUnreachableException(
                                        "sender", "recipient", "resource manager is unreachable")));

        rpcService.registerGateway(resourceManagerAddress, resourceManagerGateway);

        try (final JobMaster jobMaster =
                new JobMasterBuilder(jobGraph, rpcService)
                        .withJobMasterId(jobMasterId)
                        .withResourceId(jmResourceId)
                        .withConfiguration(configuration)
                        .withHighAvailabilityServices(haServices)
                        .withHeartbeatServices(heartbeatServices)
                        .createJobMaster()) {

            jobMaster.start();

            // define a leader and see that a registration happens
            rmLeaderRetrievalService.notifyListener(
                    resourceManagerAddress, resourceManagerId.toUUID());

            final JobMasterGateway jobMasterGateway =
                    jobMaster.getSelfGateway(JobMasterGateway.class);

            CommonTestUtils.waitUntilCondition(
                    () -> {
                        jobMasterGateway.heartbeatFromResourceManager(rmResourceId);
                        return disconnectedJobManagerFuture.isDone();
                    },
                    50L);

            // heartbeat timeout should trigger disconnect JobManager from ResourceManager
            assertThat(disconnectedJobManagerFuture.join()).isEqualTo(jobGraph.getJobID());

            // the JobMaster should try to reconnect to the RM
            registrationAttempts.await();
        }
    }

    /**
     * Tests that a JobMaster will restore the given JobGraph from its savepoint upon initial
     * submission.
     */
    @Test
    void testRestoringFromSavepoint() throws Exception {

        // create savepoint data
        final long savepointId = 42L;
        final File savepointFile = createSavepoint(savepointId);

        // set savepoint settings
        final SavepointRestoreSettings savepointRestoreSettings =
                SavepointRestoreSettings.forPath(savepointFile.getAbsolutePath(), true);
        final JobGraph jobGraph = createJobGraphWithCheckpointing(savepointRestoreSettings);

        final StandaloneCompletedCheckpointStore completedCheckpointStore =
                new StandaloneCompletedCheckpointStore(1);
        final CheckpointRecoveryFactory testingCheckpointRecoveryFactory =
                PerJobCheckpointRecoveryFactory.withoutCheckpointStoreRecovery(
                        maxCheckpoints -> completedCheckpointStore);
        haServices.setCheckpointRecoveryFactory(testingCheckpointRecoveryFactory);

        try (final JobMaster jobMaster =
                new JobMasterBuilder(jobGraph, rpcService)
                        .withHighAvailabilityServices(haServices)
                        .createJobMaster()) {

            // we need to start and register the required slots to let the adaptive scheduler
            // restore from the savepoint
            jobMaster.start();

            final OneShotLatch taskSubmitLatch = new OneShotLatch();

            registerSlotsAtJobMaster(
                    1,
                    jobMaster.getSelfGateway(JobMasterGateway.class),
                    jobGraph.getJobID(),
                    new TestingTaskExecutorGatewayBuilder()
                            .setSubmitTaskConsumer(
                                    (taskDeploymentDescriptor, jobMasterId) -> {
                                        taskSubmitLatch.trigger();
                                        return CompletableFuture.completedFuture(Acknowledge.get());
                                    })
                            .createTestingTaskExecutorGateway(),
                    new LocalUnresolvedTaskManagerLocation());

            // wait until a task has submitted because this guarantees that the ExecutionGraph has
            // been created
            taskSubmitLatch.await();
            final CompletedCheckpoint savepointCheckpoint =
                    completedCheckpointStore.getLatestCheckpoint();

            assertThat(savepointCheckpoint).isNotNull();

            assertThat(savepointCheckpoint.getCheckpointID()).isEqualTo(savepointId);
        }
    }

    /** Tests that an existing checkpoint will have precedence over an savepoint. */
    @Test
    void testCheckpointPrecedesSavepointRecovery() throws Exception {

        // create savepoint data
        final long savepointId = 42L;
        final File savepointFile = createSavepoint(savepointId);

        // set savepoint settings
        final SavepointRestoreSettings savepointRestoreSettings =
                SavepointRestoreSettings.forPath("" + savepointFile.getAbsolutePath(), true);
        final JobGraph jobGraph = createJobGraphWithCheckpointing(savepointRestoreSettings);

        final long checkpointId = 1L;

        final CompletedCheckpoint completedCheckpoint =
                new CompletedCheckpoint(
                        jobGraph.getJobID(),
                        checkpointId,
                        1L,
                        1L,
                        Collections.emptyMap(),
                        null,
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                        new DummyCheckpointStorageLocation(),
                        null);

        final StandaloneCompletedCheckpointStore completedCheckpointStore =
                new StandaloneCompletedCheckpointStore(1);
        completedCheckpointStore.addCheckpointAndSubsumeOldestOne(
                completedCheckpoint, new CheckpointsCleaner(), () -> {});
        final CheckpointRecoveryFactory testingCheckpointRecoveryFactory =
                PerJobCheckpointRecoveryFactory.withoutCheckpointStoreRecovery(
                        maxCheckpoints -> completedCheckpointStore);
        haServices.setCheckpointRecoveryFactory(testingCheckpointRecoveryFactory);

        try (final JobMaster jobMaster =
                new JobMasterBuilder(jobGraph, rpcService).createJobMaster()) {

            // starting the JobMaster should have read the savepoint
            final CompletedCheckpoint savepointCheckpoint =
                    completedCheckpointStore.getLatestCheckpoint();

            assertThat(savepointCheckpoint).isNotNull();

            assertThat(savepointCheckpoint.getCheckpointID()).isEqualTo(checkpointId);
        }
    }

    /** Tests that we can close an unestablished ResourceManager connection. */
    @Test
    void testCloseUnestablishedResourceManagerConnection() throws Exception {
        try (final JobMaster jobMaster =
                new JobMasterBuilder(jobGraph, rpcService)
                        .withConfiguration(configuration)
                        .withHighAvailabilityServices(haServices)
                        .createJobMaster()) {

            jobMaster.start();

            final TestingResourceManagerGateway firstResourceManagerGateway =
                    createAndRegisterTestingResourceManagerGateway();
            final TestingResourceManagerGateway secondResourceManagerGateway =
                    createAndRegisterTestingResourceManagerGateway();

            final OneShotLatch firstJobManagerRegistration = new OneShotLatch();
            final OneShotLatch secondJobManagerRegistration = new OneShotLatch();

            firstResourceManagerGateway.setRegisterJobManagerFunction(
                    (jobMasterId, resourceID, s, jobID) -> {
                        firstJobManagerRegistration.trigger();
                        return CompletableFuture.completedFuture(
                                firstResourceManagerGateway.getJobMasterRegistrationSuccess());
                    });

            secondResourceManagerGateway.setRegisterJobManagerFunction(
                    (jobMasterId, resourceID, s, jobID) -> {
                        secondJobManagerRegistration.trigger();
                        return CompletableFuture.completedFuture(
                                secondResourceManagerGateway.getJobMasterRegistrationSuccess());
                    });

            notifyResourceManagerLeaderListeners(firstResourceManagerGateway);

            // wait until we have seen the first registration attempt
            firstJobManagerRegistration.await();

            // this should stop the connection attempts towards the first RM
            notifyResourceManagerLeaderListeners(secondResourceManagerGateway);

            // check that we start registering at the second RM
            secondJobManagerRegistration.await();
        }
    }

    /** Tests that we continue reconnecting to the latest known RM after a disconnection message. */
    @Test
    void testReconnectionAfterDisconnect() throws Exception {
        try (final JobMaster jobMaster =
                new JobMasterBuilder(jobGraph, rpcService)
                        .withJobMasterId(jobMasterId)
                        .withConfiguration(configuration)
                        .withHighAvailabilityServices(haServices)
                        .withHeartbeatServices(heartbeatServices)
                        .createJobMaster()) {

            jobMaster.start();

            final JobMasterGateway jobMasterGateway =
                    jobMaster.getSelfGateway(JobMasterGateway.class);

            final TestingResourceManagerGateway testingResourceManagerGateway =
                    createAndRegisterTestingResourceManagerGateway();
            final BlockingQueue<JobMasterId> registrationsQueue = new ArrayBlockingQueue<>(1);

            testingResourceManagerGateway.setRegisterJobManagerFunction(
                    (jobMasterId, resourceID, s, jobID) -> {
                        registrationsQueue.offer(jobMasterId);
                        return CompletableFuture.completedFuture(
                                testingResourceManagerGateway.getJobMasterRegistrationSuccess());
                    });

            final ResourceManagerId resourceManagerId =
                    testingResourceManagerGateway.getFencingToken();
            notifyResourceManagerLeaderListeners(testingResourceManagerGateway);

            // wait for first registration attempt
            final JobMasterId firstRegistrationAttempt = registrationsQueue.take();

            assertThat(firstRegistrationAttempt).isEqualTo(jobMasterId);

            assertThat(registrationsQueue).isEmpty();
            jobMasterGateway.disconnectResourceManager(
                    resourceManagerId, new FlinkException("Test exception"));

            // wait for the second registration attempt after the disconnect call
            assertThat(registrationsQueue.take()).isEqualTo(jobMasterId);
        }
    }

    /** Tests that the a JM connects to the leading RM after regaining leadership. */
    @Test
    void testResourceManagerConnectionAfterStart() throws Exception {
        try (final JobMaster jobMaster =
                new JobMasterBuilder(jobGraph, rpcService)
                        .withJobMasterId(jobMasterId)
                        .withConfiguration(configuration)
                        .withHighAvailabilityServices(haServices)
                        .withHeartbeatServices(heartbeatServices)
                        .createJobMaster()) {

            final TestingResourceManagerGateway testingResourceManagerGateway =
                    createAndRegisterTestingResourceManagerGateway();

            final BlockingQueue<JobMasterId> registrationQueue = new ArrayBlockingQueue<>(1);
            testingResourceManagerGateway.setRegisterJobManagerFunction(
                    (jobMasterId, resourceID, s, jobID) -> {
                        registrationQueue.offer(jobMasterId);
                        return CompletableFuture.completedFuture(
                                testingResourceManagerGateway.getJobMasterRegistrationSuccess());
                    });

            notifyResourceManagerLeaderListeners(testingResourceManagerGateway);

            jobMaster.start();

            final JobMasterId firstRegistrationAttempt = registrationQueue.take();

            assertThat(firstRegistrationAttempt).isEqualTo(jobMasterId);
        }
    }

    /**
     * Tests that input splits assigned to an Execution will be returned to the InputSplitAssigner
     * if this execution fails.
     */
    @Test
    @Tag("org.apache.flink.testutils.junit.FailsWithAdaptiveScheduler") // FLINK-21450
    void testRequestNextInputSplitWithLocalFailover() throws Exception {

        configuration.setString(
                JobManagerOptions.EXECUTION_FAILOVER_STRATEGY,
                FailoverStrategyFactoryLoader.PIPELINED_REGION_RESTART_STRATEGY_NAME);

        final Function<List<List<InputSplit>>, Collection<InputSplit>>
                expectFailedExecutionInputSplits = inputSplitsPerTask -> inputSplitsPerTask.get(0);

        runRequestNextInputSplitTest(expectFailedExecutionInputSplits);
    }

    @Test
    void testRequestNextInputSplitWithGlobalFailover() throws Exception {
        configuration.setInteger(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 1);
        configuration.set(
                RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofSeconds(0));
        configuration.setString(JobManagerOptions.EXECUTION_FAILOVER_STRATEGY, "full");

        final Function<List<List<InputSplit>>, Collection<InputSplit>>
                expectAllRemainingInputSplits = this::flattenCollection;

        runRequestNextInputSplitTest(expectAllRemainingInputSplits);
    }

    private void runRequestNextInputSplitTest(
            Function<List<List<InputSplit>>, Collection<InputSplit>> expectedRemainingInputSplits)
            throws Exception {
        final int parallelism = 2;
        final int splitsPerTask = 2;

        final int totalSplits = parallelism * splitsPerTask;
        final List<TestingInputSplit> allInputSplits = new ArrayList<>(totalSplits);

        for (int i = 0; i < totalSplits; i++) {
            allInputSplits.add(new TestingInputSplit(i));
        }

        final InputSplitSource<TestingInputSplit> inputSplitSource =
                new TestingInputSplitSource(allInputSplits);

        JobVertex source = new JobVertex("source");
        source.setParallelism(parallelism);
        source.setInputSplitSource(inputSplitSource);
        source.setInvokableClass(AbstractInvokable.class);

        final ExecutionConfig executionConfig = new ExecutionConfig();
        executionConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(100, 0));

        final JobGraph inputSplitJobGraph =
                JobGraphBuilder.newStreamingJobGraphBuilder()
                        .addJobVertex(source)
                        .setExecutionConfig(executionConfig)
                        .build();

        try (final JobMaster jobMaster =
                new JobMasterBuilder(inputSplitJobGraph, rpcService)
                        .withConfiguration(configuration)
                        .withHighAvailabilityServices(haServices)
                        .withHeartbeatServices(heartbeatServices)
                        .createJobMaster()) {

            jobMaster.start();

            final JobMasterGateway jobMasterGateway =
                    jobMaster.getSelfGateway(JobMasterGateway.class);

            registerSlotsRequiredForJobExecution(
                    jobMasterGateway, inputSplitJobGraph.getJobID(), parallelism);

            waitUntilAllExecutionsAreScheduledOrDeployed(jobMasterGateway);

            final JobVertexID sourceId = source.getID();

            final List<AccessExecution> executions = getExecutions(jobMasterGateway, sourceId);
            final ExecutionAttemptID initialAttemptId = executions.get(0).getAttemptId();
            final List<List<InputSplit>> inputSplitsPerTask = new ArrayList<>(parallelism);

            // request all input splits
            for (AccessExecution execution : executions) {
                inputSplitsPerTask.add(
                        getInputSplits(
                                splitsPerTask,
                                getInputSplitSupplier(
                                        sourceId, jobMasterGateway, execution.getAttemptId())));
            }

            final List<InputSplit> allRequestedInputSplits = flattenCollection(inputSplitsPerTask);
            assertThat(allRequestedInputSplits)
                    .containsExactlyInAnyOrder(allInputSplits.toArray(EMPTY_TESTING_INPUT_SPLITS));

            // fail the first execution to trigger a failover
            jobMasterGateway
                    .updateTaskExecutionState(
                            new TaskExecutionState(initialAttemptId, ExecutionState.FAILED))
                    .get();

            // wait until the job has been recovered
            waitUntilAllExecutionsAreScheduledOrDeployed(jobMasterGateway);

            final ExecutionAttemptID restartedAttemptId =
                    getFirstExecution(jobMasterGateway, sourceId).getAttemptId();

            final List<InputSplit> inputSplits =
                    getRemainingInputSplits(
                            getInputSplitSupplier(sourceId, jobMasterGateway, restartedAttemptId));

            assertThat(inputSplits)
                    .containsExactlyInAnyOrder(
                            expectedRemainingInputSplits
                                    .apply(inputSplitsPerTask)
                                    .toArray(EMPTY_TESTING_INPUT_SPLITS));
        }
    }

    @Nonnull
    private List<InputSplit> flattenCollection(List<List<InputSplit>> inputSplitsPerTask) {
        return inputSplitsPerTask.stream().flatMap(Collection::stream).collect(Collectors.toList());
    }

    @Nonnull
    private Supplier<SerializedInputSplit> getInputSplitSupplier(
            JobVertexID jobVertexID,
            JobMasterGateway jobMasterGateway,
            ExecutionAttemptID initialAttemptId) {
        return () -> getInputSplit(jobMasterGateway, jobVertexID, initialAttemptId);
    }

    private void waitUntilAllExecutionsAreScheduledOrDeployed(
            final JobMasterGateway jobMasterGateway) throws Exception {

        CommonTestUtils.waitUntilCondition(
                () -> {
                    final Collection<AccessExecution> executions = getExecutions(jobMasterGateway);
                    return !executions.isEmpty()
                            && executions.stream()
                                    .allMatch(
                                            execution ->
                                                    execution.getState() == ExecutionState.SCHEDULED
                                                            || execution.getState()
                                                                    == ExecutionState.DEPLOYING);
                });
    }

    private static AccessExecution getFirstExecution(
            final JobMasterGateway jobMasterGateway, final JobVertexID jobVertexId) {
        final List<AccessExecution> executions = getExecutions(jobMasterGateway, jobVertexId);

        assertThat(executions.size()).isGreaterThanOrEqualTo(1);
        return executions.get(0);
    }

    private static Collection<AccessExecution> getExecutions(
            final JobMasterGateway jobMasterGateway) {
        final ArchivedExecutionGraph archivedExecutionGraph =
                requestExecutionGraph(jobMasterGateway).getArchivedExecutionGraph();

        return archivedExecutionGraph.getAllVertices().values().stream()
                .flatMap(vertex -> Arrays.stream(vertex.getTaskVertices()))
                .map(AccessExecutionVertex::getCurrentExecutionAttempt)
                .collect(Collectors.toList());
    }

    private static List<AccessExecution> getExecutions(
            final JobMasterGateway jobMasterGateway, final JobVertexID jobVertexId) {
        final ArchivedExecutionGraph archivedExecutionGraph =
                requestExecutionGraph(jobMasterGateway).getArchivedExecutionGraph();

        return Optional.ofNullable(archivedExecutionGraph.getAllVertices().get(jobVertexId))
                .map(
                        accessExecutionJobVertex ->
                                Arrays.asList(accessExecutionJobVertex.getTaskVertices()))
                .orElse(Collections.emptyList()).stream()
                .map(AccessExecutionVertex::getCurrentExecutionAttempt)
                .collect(Collectors.toList());
    }

    private static ExecutionGraphInfo requestExecutionGraph(
            final JobMasterGateway jobMasterGateway) {
        try {
            return jobMasterGateway.requestJob(testingTimeout).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Nonnull
    private static List<InputSplit> getInputSplits(
            int numberInputSplits, Supplier<SerializedInputSplit> nextInputSplit) throws Exception {
        final List<InputSplit> actualInputSplits = new ArrayList<>(numberInputSplits);

        for (int i = 0; i < numberInputSplits; i++) {
            final SerializedInputSplit serializedInputSplit = nextInputSplit.get();

            assertThat(serializedInputSplit.isEmpty()).isFalse();

            actualInputSplits.add(
                    InstantiationUtil.deserializeObject(
                            serializedInputSplit.getInputSplitData(),
                            ClassLoader.getSystemClassLoader()));
        }

        return actualInputSplits;
    }

    private List<InputSplit> getRemainingInputSplits(Supplier<SerializedInputSplit> nextInputSplit)
            throws Exception {
        final List<InputSplit> actualInputSplits = new ArrayList<>(16);
        boolean hasMoreInputSplits = true;

        while (hasMoreInputSplits) {
            final SerializedInputSplit serializedInputSplit = nextInputSplit.get();

            if (serializedInputSplit.isEmpty()) {
                hasMoreInputSplits = false;
            } else {
                final InputSplit inputSplit =
                        InstantiationUtil.deserializeObject(
                                serializedInputSplit.getInputSplitData(),
                                ClassLoader.getSystemClassLoader());

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
            return jobMasterGateway.requestNextInputSplit(jobVertexId, attemptId).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private static final class TestingInputSplitSource
            implements InputSplitSource<TestingInputSplit> {
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

    /**
     * Tests the {@link JobMaster#requestPartitionState(IntermediateDataSetID, ResultPartitionID)}
     * call for a finished result partition.
     */
    @Test
    void testRequestPartitionState() throws Exception {
        final JobGraph producerConsumerJobGraph = producerConsumerJobGraph();
        try (final JobMaster jobMaster =
                new JobMasterBuilder(producerConsumerJobGraph, rpcService)
                        .withConfiguration(configuration)
                        .withHighAvailabilityServices(haServices)
                        .withHeartbeatServices(heartbeatServices)
                        .createJobMaster()) {

            jobMaster.start();

            final CompletableFuture<TaskDeploymentDescriptor> tddFuture = new CompletableFuture<>();
            final TestingTaskExecutorGateway testingTaskExecutorGateway =
                    new TestingTaskExecutorGatewayBuilder()
                            .setSubmitTaskConsumer(
                                    (taskDeploymentDescriptor, jobMasterId) -> {
                                        tddFuture.complete(taskDeploymentDescriptor);
                                        return CompletableFuture.completedFuture(Acknowledge.get());
                                    })
                            .createTestingTaskExecutorGateway();
            final LocalUnresolvedTaskManagerLocation taskManagerLocation =
                    new LocalUnresolvedTaskManagerLocation();

            final JobMasterGateway jobMasterGateway =
                    jobMaster.getSelfGateway(JobMasterGateway.class);

            final Collection<SlotOffer> slotOffers =
                    registerSlotsAtJobMaster(
                            1,
                            jobMasterGateway,
                            producerConsumerJobGraph.getJobID(),
                            testingTaskExecutorGateway,
                            taskManagerLocation);

            assertThat(slotOffers).hasSize(1);

            // obtain tdd for the result partition ids
            final TaskDeploymentDescriptor tdd = tddFuture.get();

            assertThat(tdd.getProducedPartitions()).hasSize(1);
            final ResultPartitionDeploymentDescriptor partition =
                    tdd.getProducedPartitions().iterator().next();

            final ExecutionAttemptID executionAttemptId = tdd.getExecutionAttemptId();
            final ExecutionAttemptID copiedExecutionAttemptId =
                    InstantiationUtil.clone(executionAttemptId);

            // finish the producer task
            jobMasterGateway
                    .updateTaskExecutionState(
                            SchedulerTestingUtils.createFinishedTaskExecutionState(
                                    executionAttemptId))
                    .get();

            // request the state of the result partition of the producer
            final ResultPartitionID partitionId =
                    new ResultPartitionID(partition.getPartitionId(), copiedExecutionAttemptId);
            CompletableFuture<ExecutionState> partitionStateFuture =
                    jobMasterGateway.requestPartitionState(partition.getResultId(), partitionId);

            assertThat(partitionStateFuture.get()).isEqualTo(ExecutionState.FINISHED);

            // ask for unknown result partition
            partitionStateFuture =
                    jobMasterGateway.requestPartitionState(
                            partition.getResultId(), new ResultPartitionID());

            assertThatThrownBy(partitionStateFuture::get)
                    .hasRootCauseInstanceOf(IllegalArgumentException.class);

            // ask for wrong intermediate data set id
            partitionStateFuture =
                    jobMasterGateway.requestPartitionState(
                            new IntermediateDataSetID(), partitionId);

            assertThatThrownBy(partitionStateFuture::get)
                    .hasRootCauseInstanceOf(IllegalArgumentException.class);

            // ask for "old" execution
            partitionStateFuture =
                    jobMasterGateway.requestPartitionState(
                            partition.getResultId(),
                            new ResultPartitionID(
                                    partition.getPartitionId(), createExecutionAttemptId()));

            assertThatThrownBy(partitionStateFuture::get)
                    .hasRootCauseInstanceOf(PartitionProducerDisposedException.class);
        }
    }

    private void notifyResourceManagerLeaderListeners(
            TestingResourceManagerGateway testingResourceManagerGateway) {
        rmLeaderRetrievalService.notifyListener(
                testingResourceManagerGateway.getAddress(),
                testingResourceManagerGateway.getFencingToken().toUUID());
    }

    /**
     * Tests that the timeout in {@link JobMasterGateway#triggerSavepoint(String, boolean,
     * SavepointFormatType, Time)} is respected.
     */
    @Test
    void testTriggerSavepointTimeout() throws Exception {
        final TestingSchedulerNG testingSchedulerNG =
                TestingSchedulerNG.newBuilder()
                        .setTriggerSavepointFunction(
                                (ignoredA, ignoredB, formatType) -> new CompletableFuture<>())
                        .build();

        try (final JobMaster jobMaster =
                new JobMasterBuilder(jobGraph, rpcService)
                        .withFatalErrorHandler(testingFatalErrorHandler)
                        .withSlotPoolServiceSchedulerFactory(
                                DefaultSlotPoolServiceSchedulerFactory.create(
                                        TestingSlotPoolServiceBuilder.newBuilder(),
                                        new TestingSchedulerNGFactory(testingSchedulerNG)))
                        .createJobMaster()) {

            jobMaster.start();

            final JobMasterGateway jobMasterGateway =
                    jobMaster.getSelfGateway(JobMasterGateway.class);
            final CompletableFuture<String> savepointFutureLowTimeout =
                    jobMasterGateway.triggerSavepoint(
                            "/tmp", false, SavepointFormatType.CANONICAL, Time.milliseconds(1));
            final CompletableFuture<String> savepointFutureHighTimeout =
                    jobMasterGateway.triggerSavepoint(
                            "/tmp", false, SavepointFormatType.CANONICAL, RpcUtils.INF_TIMEOUT);

            assertThatThrownBy(
                            () ->
                                    savepointFutureLowTimeout.get(
                                            testingTimeout.getSize(), testingTimeout.getUnit()))
                    .hasRootCauseInstanceOf(TimeoutException.class);

            assertThat(savepointFutureHighTimeout).isNotDone();
        }
    }

    /** Tests that the TaskExecutor is released if all of its slots have been freed. */
    @Test
    void testReleasingTaskExecutorIfNoMoreSlotsRegistered() throws Exception {

        final JobGraph jobGraph = createSingleVertexJobWithRestartStrategy();

        try (final JobMaster jobMaster =
                new JobMasterBuilder(jobGraph, rpcService)
                        .withConfiguration(configuration)
                        .withHighAvailabilityServices(haServices)
                        .withHeartbeatServices(heartbeatServices)
                        .createJobMaster()) {

            final CompletableFuture<JobID> disconnectTaskExecutorFuture = new CompletableFuture<>();
            final CompletableFuture<AllocationID> freedSlotFuture = new CompletableFuture<>();
            final TestingTaskExecutorGateway testingTaskExecutorGateway =
                    new TestingTaskExecutorGatewayBuilder()
                            .setFreeSlotFunction(
                                    (allocationID, throwable) -> {
                                        freedSlotFuture.complete(allocationID);
                                        return CompletableFuture.completedFuture(Acknowledge.get());
                                    })
                            .setDisconnectJobManagerConsumer(
                                    (jobID, throwable) ->
                                            disconnectTaskExecutorFuture.complete(jobID))
                            .createTestingTaskExecutorGateway();
            final LocalUnresolvedTaskManagerLocation taskManagerLocation =
                    new LocalUnresolvedTaskManagerLocation();

            jobMaster.start();

            final JobMasterGateway jobMasterGateway =
                    jobMaster.getSelfGateway(JobMasterGateway.class);

            final Collection<SlotOffer> slotOffers =
                    registerSlotsAtJobMaster(
                            1,
                            jobMasterGateway,
                            jobGraph.getJobID(),
                            testingTaskExecutorGateway,
                            taskManagerLocation);

            // check that we accepted the offered slot
            assertThat(slotOffers).hasSize(1);
            final AllocationID allocationId = slotOffers.iterator().next().getAllocationId();

            // now fail the allocation and check that we close the connection to the TaskExecutor
            jobMasterGateway.failSlot(
                    taskManagerLocation.getResourceID(),
                    allocationId,
                    new FlinkException("Fail allocation test exception"));

            // we should free the slot and then disconnect from the TaskExecutor because we use no
            // longer slots from it
            assertThat(freedSlotFuture.get()).isEqualTo(allocationId);
            assertThat(disconnectTaskExecutorFuture.get()).isEqualTo(jobGraph.getJobID());
        }
    }

    @Test
    void testTaskExecutorNotReleasedOnFailedAllocationIfPartitionIsAllocated() throws Exception {
        final JobManagerSharedServices jobManagerSharedServices =
                new TestingJobManagerSharedServicesBuilder().build();

        final JobGraph jobGraph = JobGraphTestUtils.singleNoOpJobGraph();

        final LocalUnresolvedTaskManagerLocation taskManagerUnresolvedLocation =
                new LocalUnresolvedTaskManagerLocation();

        final AtomicBoolean isTrackingPartitions = new AtomicBoolean(true);
        final TestingJobMasterPartitionTracker partitionTracker =
                new TestingJobMasterPartitionTracker();
        partitionTracker.setIsTrackingPartitionsForFunction(ignored -> isTrackingPartitions.get());

        try (final JobMaster jobMaster =
                new JobMasterBuilder(jobGraph, rpcService)
                        .withConfiguration(configuration)
                        .withHighAvailabilityServices(haServices)
                        .withHeartbeatServices(heartbeatServices)
                        .withPartitionTrackerFactory(ignored -> partitionTracker)
                        .createJobMaster()) {

            final CompletableFuture<JobID> disconnectTaskExecutorFuture = new CompletableFuture<>();
            final CompletableFuture<AllocationID> freedSlotFuture = new CompletableFuture<>();
            final TestingTaskExecutorGateway testingTaskExecutorGateway =
                    new TestingTaskExecutorGatewayBuilder()
                            .setFreeSlotFunction(
                                    (allocationID, throwable) -> {
                                        freedSlotFuture.complete(allocationID);
                                        return CompletableFuture.completedFuture(Acknowledge.get());
                                    })
                            .setDisconnectJobManagerConsumer(
                                    (jobID, throwable) ->
                                            disconnectTaskExecutorFuture.complete(jobID))
                            .createTestingTaskExecutorGateway();

            jobMaster.start();

            final JobMasterGateway jobMasterGateway =
                    jobMaster.getSelfGateway(JobMasterGateway.class);

            final Collection<SlotOffer> slotOffers =
                    registerSlotsAtJobMaster(
                            1,
                            jobMasterGateway,
                            jobGraph.getJobID(),
                            testingTaskExecutorGateway,
                            taskManagerUnresolvedLocation);

            // check that we accepted the offered slot
            assertThat(slotOffers).hasSize(1);
            final AllocationID allocationId = slotOffers.iterator().next().getAllocationId();

            jobMasterGateway.failSlot(
                    taskManagerUnresolvedLocation.getResourceID(),
                    allocationId,
                    new FlinkException("Fail allocation test exception"));

            // we should free the slot, but not disconnect from the TaskExecutor as we still have an
            // allocated partition
            assertThat(freedSlotFuture.get()).isEqualTo(allocationId);

            // trigger some request to guarantee ensure the slotAllocationFailure processing if
            // complete
            jobMasterGateway.requestJobStatus(Time.seconds(5)).get();
            assertThat(disconnectTaskExecutorFuture).isNotDone();
        }
    }

    /** Tests the updateGlobalAggregate functionality. */
    @Test
    void testJobMasterAggregatesValuesCorrectly() throws Exception {
        try (final JobMaster jobMaster =
                new JobMasterBuilder(jobGraph, rpcService)
                        .withConfiguration(configuration)
                        .withHighAvailabilityServices(haServices)
                        .withHeartbeatServices(heartbeatServices)
                        .createJobMaster()) {

            jobMaster.start();
            final JobMasterGateway jobMasterGateway =
                    jobMaster.getSelfGateway(JobMasterGateway.class);

            CompletableFuture<Object> updateAggregateFuture;

            AggregateFunction<Integer, Integer, Integer> aggregateFunction =
                    createAggregateFunction();

            ClosureCleaner.clean(
                    aggregateFunction, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);
            byte[] serializedAggregateFunction =
                    InstantiationUtil.serializeObject(aggregateFunction);

            updateAggregateFuture =
                    jobMasterGateway.updateGlobalAggregate("agg1", 1, serializedAggregateFunction);
            assertThat(updateAggregateFuture.get()).isEqualTo(1);

            updateAggregateFuture =
                    jobMasterGateway.updateGlobalAggregate("agg1", 2, serializedAggregateFunction);
            assertThat(updateAggregateFuture.get()).isEqualTo(3);

            updateAggregateFuture =
                    jobMasterGateway.updateGlobalAggregate("agg1", 3, serializedAggregateFunction);
            assertThat(updateAggregateFuture.get()).isEqualTo(6);

            updateAggregateFuture =
                    jobMasterGateway.updateGlobalAggregate("agg1", 4, serializedAggregateFunction);
            assertThat(updateAggregateFuture.get()).isEqualTo(10);

            updateAggregateFuture =
                    jobMasterGateway.updateGlobalAggregate("agg2", 10, serializedAggregateFunction);
            assertThat(updateAggregateFuture.get()).isEqualTo(10);

            updateAggregateFuture =
                    jobMasterGateway.updateGlobalAggregate("agg2", 23, serializedAggregateFunction);
            assertThat(updateAggregateFuture.get()).isEqualTo(33);
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
        final TestingResourceManagerGateway testingResourceManagerGateway =
                new TestingResourceManagerGateway();
        rpcService.registerGateway(
                testingResourceManagerGateway.getAddress(), testingResourceManagerGateway);
        return testingResourceManagerGateway;
    }

    /**
     * Tests that the job execution is failed if the TaskExecutor disconnects from the JobMaster.
     */
    @Test
    void testJobFailureWhenGracefulTaskExecutorTermination() throws Exception {
        runJobFailureWhenTaskExecutorTerminatesTest(
                heartbeatServices,
                (localTaskManagerLocation, jobMasterGateway) ->
                        jobMasterGateway.disconnectTaskManager(
                                localTaskManagerLocation.getResourceID(),
                                new FlinkException("Test disconnectTaskManager exception.")));
    }

    @Test
    void testJobFailureWhenTaskExecutorHeartbeatTimeout() throws Exception {
        final TestingHeartbeatServices testingHeartbeatService =
                new TestingHeartbeatServices(heartbeatInterval, heartbeatTimeout);

        runJobFailureWhenTaskExecutorTerminatesTest(
                testingHeartbeatService,
                (localTaskManagerLocation, jobMasterGateway) ->
                        testingHeartbeatService.triggerHeartbeatTimeout(
                                jmResourceId, localTaskManagerLocation.getResourceID()));
    }

    /**
     * Tests that the JobMaster rejects a TaskExecutor registration attempt if the expected and
     * actual JobID are not equal. See FLINK-21606.
     */
    @Test
    void testJobMasterRejectsTaskExecutorRegistrationIfJobIdsAreNotEqual() throws Exception {
        try (final JobMaster jobMaster =
                new JobMasterBuilder(jobGraph, rpcService).createJobMaster()) {

            jobMaster.start();

            final CompletableFuture<RegistrationResponse> registrationResponse =
                    jobMaster.registerTaskManager(
                            new JobID(),
                            TaskManagerRegistrationInformation.create(
                                    "foobar",
                                    new LocalUnresolvedTaskManagerLocation(),
                                    TestingUtils.zeroUUID()),
                            testingTimeout);

            assertThat(registrationResponse.get()).isInstanceOf(JMTMRegistrationRejection.class);
        }
    }

    @Test
    void testJobMasterAcknowledgesDuplicateTaskExecutorRegistrations() throws Exception {
        try (final JobMaster jobMaster =
                new JobMasterBuilder(jobGraph, rpcService).createJobMaster()) {

            final TestingTaskExecutorGateway testingTaskExecutorGateway =
                    new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway();
            rpcService.registerGateway(
                    testingTaskExecutorGateway.getAddress(), testingTaskExecutorGateway);

            jobMaster.start();

            final TaskManagerRegistrationInformation taskManagerRegistrationInformation =
                    TaskManagerRegistrationInformation.create(
                            testingTaskExecutorGateway.getAddress(),
                            new LocalUnresolvedTaskManagerLocation(),
                            UUID.randomUUID());

            final CompletableFuture<RegistrationResponse> firstRegistrationResponse =
                    jobMaster.registerTaskManager(
                            jobGraph.getJobID(),
                            taskManagerRegistrationInformation,
                            testingTimeout);
            final CompletableFuture<RegistrationResponse> secondRegistrationResponse =
                    jobMaster.registerTaskManager(
                            jobGraph.getJobID(),
                            taskManagerRegistrationInformation,
                            testingTimeout);

            assertThat(firstRegistrationResponse.get()).isInstanceOf(JMTMRegistrationSuccess.class);
            assertThat(secondRegistrationResponse.get())
                    .isInstanceOf(JMTMRegistrationSuccess.class);
        }
    }

    @Test
    void testJobMasterDisconnectsOldTaskExecutorIfNewSessionIsSeen() throws Exception {
        try (final JobMaster jobMaster =
                new JobMasterBuilder(jobGraph, rpcService).createJobMaster()) {

            final CompletableFuture<Void> firstTaskExecutorDisconnectedFuture =
                    new CompletableFuture<>();
            final TestingTaskExecutorGateway firstTaskExecutorGateway =
                    new TestingTaskExecutorGatewayBuilder()
                            .setAddress("firstTaskExecutor")
                            .setDisconnectJobManagerConsumer(
                                    (jobID, throwable) ->
                                            firstTaskExecutorDisconnectedFuture.complete(null))
                            .createTestingTaskExecutorGateway();
            final TestingTaskExecutorGateway secondTaskExecutorGateway =
                    new TestingTaskExecutorGatewayBuilder()
                            .setAddress("secondTaskExecutor")
                            .createTestingTaskExecutorGateway();

            rpcService.registerGateway(
                    firstTaskExecutorGateway.getAddress(), firstTaskExecutorGateway);
            rpcService.registerGateway(
                    secondTaskExecutorGateway.getAddress(), secondTaskExecutorGateway);

            jobMaster.start();

            final LocalUnresolvedTaskManagerLocation taskManagerLocation =
                    new LocalUnresolvedTaskManagerLocation();
            final UUID firstTaskManagerSessionId = UUID.randomUUID();

            final CompletableFuture<RegistrationResponse> firstRegistrationResponse =
                    jobMaster.registerTaskManager(
                            jobGraph.getJobID(),
                            TaskManagerRegistrationInformation.create(
                                    firstTaskExecutorGateway.getAddress(),
                                    taskManagerLocation,
                                    firstTaskManagerSessionId),
                            testingTimeout);
            assertThat(firstRegistrationResponse.get()).isInstanceOf(JMTMRegistrationSuccess.class);

            final UUID secondTaskManagerSessionId = UUID.randomUUID();
            final CompletableFuture<RegistrationResponse> secondRegistrationResponse =
                    jobMaster.registerTaskManager(
                            jobGraph.getJobID(),
                            TaskManagerRegistrationInformation.create(
                                    secondTaskExecutorGateway.getAddress(),
                                    taskManagerLocation,
                                    secondTaskManagerSessionId),
                            testingTimeout);

            assertThat(secondRegistrationResponse.get())
                    .isInstanceOf(JMTMRegistrationSuccess.class);
            // the first TaskExecutor should be disconnected
            firstTaskExecutorDisconnectedFuture.get();
        }
    }

    @Test
    void testJobMasterOnlyTerminatesAfterTheSchedulerHasClosed() throws Exception {
        final CompletableFuture<Void> schedulerTerminationFuture = new CompletableFuture<>();
        final TestingSchedulerNG testingSchedulerNG =
                TestingSchedulerNG.newBuilder()
                        .setCloseAsyncSupplier(() -> schedulerTerminationFuture)
                        .build();

        try (final JobMaster jobMaster =
                new JobMasterBuilder(jobGraph, rpcService)
                        .withSlotPoolServiceSchedulerFactory(
                                DefaultSlotPoolServiceSchedulerFactory.create(
                                        TestingSlotPoolServiceBuilder.newBuilder(),
                                        new TestingSchedulerNGFactory(testingSchedulerNG)))
                        .createJobMaster()) {

            jobMaster.start();

            final CompletableFuture<Void> jobMasterTerminationFuture = jobMaster.closeAsync();

            assertThatThrownBy(() -> jobMasterTerminationFuture.get(10L, TimeUnit.MILLISECONDS))
                    .as("Expected TimeoutException because the JobMaster should not terminate.")
                    .isInstanceOf(TimeoutException.class);

            schedulerTerminationFuture.complete(null);

            jobMasterTerminationFuture.get();
        }
    }

    @Test
    void testJobMasterAcceptsSlotsWhenJobIsRestarting() throws Exception {
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        configuration.set(
                RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofDays(1));
        final int numberSlots = 1;
        try (final JobMaster jobMaster =
                new JobMasterBuilder(jobGraph, rpcService)
                        .withConfiguration(configuration)
                        .createJobMaster()) {

            jobMaster.start();

            final JobMasterGateway jobMasterGateway =
                    jobMaster.getSelfGateway(JobMasterGateway.class);

            final LocalUnresolvedTaskManagerLocation unresolvedTaskManagerLocation =
                    new LocalUnresolvedTaskManagerLocation();
            registerSlotsAtJobMaster(
                    numberSlots,
                    jobMasterGateway,
                    jobGraph.getJobID(),
                    new TestingTaskExecutorGatewayBuilder()
                            .setAddress("firstTaskManager")
                            .createTestingTaskExecutorGateway(),
                    unresolvedTaskManagerLocation);

            CommonTestUtils.waitUntilCondition(
                    () ->
                            jobMasterGateway.requestJobStatus(testingTimeout).get()
                                    == JobStatus.RUNNING);

            jobMasterGateway.disconnectTaskManager(
                    unresolvedTaskManagerLocation.getResourceID(),
                    new FlinkException("Test exception."));

            CommonTestUtils.waitUntilCondition(
                    () ->
                            jobMasterGateway.requestJobStatus(testingTimeout).get()
                                    == JobStatus.RESTARTING);

            assertThat(
                            registerSlotsAtJobMaster(
                                    numberSlots,
                                    jobMasterGateway,
                                    jobGraph.getJobID(),
                                    new TestingTaskExecutorGatewayBuilder()
                                            .setAddress("secondTaskManager")
                                            .createTestingTaskExecutorGateway(),
                                    new LocalUnresolvedTaskManagerLocation()))
                    .hasSize(numberSlots);
        }
    }

    @Test
    void testBlockResourcesWillTriggerReleaseFreeSlots() throws Exception {
        JobVertex jobVertex = new JobVertex("jobVertex");
        jobVertex.setInvokableClass(NoOpInvokable.class);
        jobVertex.setParallelism(2);
        final JobGraph jobGraph = JobGraphTestUtils.batchJobGraph(jobVertex);

        final LocalUnresolvedTaskManagerLocation taskManagerLocation =
                new LocalUnresolvedTaskManagerLocation();

        final CompletableFuture<AllocationID> freedSlotFuture1 = new CompletableFuture<>();
        final CompletableFuture<AllocationID> freedSlotFuture2 = new CompletableFuture<>();
        final TestingTaskExecutorGateway testingTaskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder()
                        .setFreeSlotFunction(
                                (allocationID, throwable) -> {
                                    if (!freedSlotFuture1.isDone()) {
                                        freedSlotFuture1.complete(allocationID);
                                    } else if (!freedSlotFuture2.isDone()) {
                                        freedSlotFuture2.complete(allocationID);
                                    }
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                })
                        .createTestingTaskExecutorGateway();

        try (final JobMaster jobMaster =
                new JobMasterBuilder(jobGraph, rpcService)
                        .withConfiguration(configuration)
                        .withHighAvailabilityServices(haServices)
                        .withHeartbeatServices(heartbeatServices)
                        .withBlocklistHandlerFactory(
                                new DefaultBlocklistHandler.Factory(Duration.ofMillis((100L))))
                        .createJobMaster()) {

            jobMaster.start();

            final JobMasterGateway jobMasterGateway = jobMaster.getGateway();

            final Collection<SlotOffer> slotOffers =
                    registerSlotsAtJobMaster(
                            2,
                            jobMasterGateway,
                            jobGraph.getJobID(),
                            testingTaskExecutorGateway,
                            taskManagerLocation);

            // check that we accepted the offered slot
            assertThat(slotOffers).hasSize(2);

            waitUntilAllExecutionsAreScheduledOrDeployed(jobMasterGateway);

            // 1 slot reserved, 1 slot free
            jobMasterGateway
                    .updateTaskExecutionState(
                            SchedulerTestingUtils.createFinishedTaskExecutionState(
                                    getExecutions(jobMasterGateway)
                                            .iterator()
                                            .next()
                                            .getAttemptId()))
                    .get();

            BlockedNode blockedNode =
                    new BlockedNode(
                            taskManagerLocation.getNodeId(),
                            "Test cause",
                            System.currentTimeMillis());

            jobMasterGateway.notifyNewBlockedNodes(Collections.singleton(blockedNode)).get();

            assertThat(freedSlotFuture1).isDone();
            assertThat(freedSlotFuture2).isNotDone();
        }
    }

    @Test
    void testNewlyAddedBlockedNodesWillBeSynchronizedToResourceManager() throws Exception {
        try (final JobMaster jobMaster =
                new JobMasterBuilder(jobGraph, rpcService)
                        .withConfiguration(configuration)
                        .withHighAvailabilityServices(haServices)
                        .withBlocklistHandlerFactory(
                                new DefaultBlocklistHandler.Factory(Duration.ofMillis(100L)))
                        .createJobMaster()) {

            jobMaster.start();

            final JobMasterGateway jobMasterGateway = jobMaster.getGateway();

            CompletableFuture<Void> jobManagerRegistrationFutureA = new CompletableFuture<>();
            CompletableFuture<Void> jobManagerRegistrationFutureB = new CompletableFuture<>();

            CompletableFuture<Collection<BlockedNode>> firstReceivedBlockedNodeFutureOfA =
                    new CompletableFuture<>();
            CompletableFuture<Collection<BlockedNode>> secondReceivedBlockedNodeFutureOfA =
                    new CompletableFuture<>();

            CompletableFuture<Collection<BlockedNode>> firstReceivedBlockedNodeFutureOfB =
                    new CompletableFuture<>();

            final TestingResourceManagerGateway resourceManagerGatewayA =
                    createResourceManagerGateway(
                            firstReceivedBlockedNodeFutureOfA,
                            secondReceivedBlockedNodeFutureOfA,
                            jobManagerRegistrationFutureA);
            final TestingResourceManagerGateway resourceManagerGatewayB =
                    createResourceManagerGateway(
                            firstReceivedBlockedNodeFutureOfB,
                            new CompletableFuture<>(),
                            jobManagerRegistrationFutureB);

            // notify resource manager A as leader
            notifyResourceManagerLeaderListeners(resourceManagerGatewayA);

            // wait job manager register
            jobManagerRegistrationFutureA.get();

            // add blocked node 1
            BlockedNode blockedNode1 = new BlockedNode("node1", "Test exception", Long.MAX_VALUE);
            jobMasterGateway.notifyNewBlockedNodes(Collections.singleton(blockedNode1)).get();

            assertThat(firstReceivedBlockedNodeFutureOfA.get()).containsExactly(blockedNode1);

            // notify resource manager B as leader
            notifyResourceManagerLeaderListeners(resourceManagerGatewayB);

            // wait job manager register
            jobManagerRegistrationFutureB.get();

            // add blocked node 2
            BlockedNode blockedNode2 = new BlockedNode("node2", "Test exception", Long.MAX_VALUE);
            jobMasterGateway.notifyNewBlockedNodes(Collections.singleton(blockedNode2)).get();

            assertThat(firstReceivedBlockedNodeFutureOfB.get())
                    .containsExactlyInAnyOrder(blockedNode1, blockedNode2);
            assertThat(secondReceivedBlockedNodeFutureOfA).isNotDone();
        }
    }

    @Test
    public void testSuccessfulResourceRequirementsUpdate() throws Exception {
        final CompletableFuture<JobResourceRequirements> schedulerUpdateFuture =
                new CompletableFuture<>();
        final TestingSchedulerNG scheduler =
                TestingSchedulerNG.newBuilder()
                        .setUpdateJobResourceRequirementsConsumer(schedulerUpdateFuture::complete)
                        .build();
        try (final JobMaster jobMaster =
                new JobMasterBuilder(jobGraph, rpcService)
                        .withConfiguration(configuration)
                        .withHighAvailabilityServices(haServices)
                        .withSlotPoolServiceSchedulerFactory(
                                DefaultSlotPoolServiceSchedulerFactory.create(
                                        TestingSlotPoolServiceBuilder.newBuilder(),
                                        new TestingSchedulerNGFactory(scheduler)))
                        .createJobMaster()) {
            jobMaster.start();
            final JobMasterGateway jobMasterGateway =
                    jobMaster.getSelfGateway(JobMasterGateway.class);

            final JobResourceRequirements.Builder jobResourceRequirementsBuilder =
                    JobResourceRequirements.newBuilder();
            for (JobVertex jobVertex : jobGraph.getVertices()) {
                jobResourceRequirementsBuilder.setParallelismForJobVertex(jobVertex.getID(), 1, 2);
            }

            final JobResourceRequirements newRequirements = jobResourceRequirementsBuilder.build();
            final CompletableFuture<Acknowledge> jobMasterUpdateFuture =
                    jobMasterGateway.updateJobResourceRequirements(newRequirements);

            assertThatFuture(jobMasterUpdateFuture).eventuallySucceeds();
            assertThatFuture(schedulerUpdateFuture).eventuallySucceeds().isEqualTo(newRequirements);
        }
    }

    @Test
    public void testResourceRequirementsAreRequestedFromTheScheduler() throws Exception {
        final JobResourceRequirements jobResourceRequirements = JobResourceRequirements.empty();
        final TestingSchedulerNG scheduler =
                TestingSchedulerNG.newBuilder()
                        .setRequestJobResourceRequirementsSupplier(() -> jobResourceRequirements)
                        .build();
        try (final JobMaster jobMaster =
                new JobMasterBuilder(jobGraph, rpcService)
                        .withSlotPoolServiceSchedulerFactory(
                                DefaultSlotPoolServiceSchedulerFactory.create(
                                        TestingSlotPoolServiceBuilder.newBuilder(),
                                        new TestingSchedulerNGFactory(scheduler)))
                        .createJobMaster()) {
            jobMaster.start();
            final JobMasterGateway jobMasterGateway =
                    jobMaster.getSelfGateway(JobMasterGateway.class);
            assertThatFuture(jobMasterGateway.requestJobResourceRequirements())
                    .eventuallySucceeds()
                    .isEqualTo(jobResourceRequirements);
        }
    }

    @Test
    void testRetrievingCheckpointStats() throws Exception {
        // create savepoint data
        final long savepointId = 42L;
        final File savepointFile = createSavepoint(savepointId);

        // set savepoint settings
        final SavepointRestoreSettings savepointRestoreSettings =
                SavepointRestoreSettings.forPath(savepointFile.getAbsolutePath(), true);
        final int parallelism = 2;
        final JobGraph jobGraph =
                createJobGraphWithCheckpointing(parallelism, savepointRestoreSettings);

        final StandaloneCompletedCheckpointStore completedCheckpointStore =
                new StandaloneCompletedCheckpointStore(1);
        final CheckpointRecoveryFactory testingCheckpointRecoveryFactory =
                PerJobCheckpointRecoveryFactory.withoutCheckpointStoreRecovery(
                        maxCheckpoints -> completedCheckpointStore);
        haServices.setCheckpointRecoveryFactory(testingCheckpointRecoveryFactory);

        try (final JobMaster jobMaster =
                new JobMasterBuilder(jobGraph, rpcService)
                        .withHighAvailabilityServices(haServices)
                        .createJobMaster()) {

            // we need to start and register the required slots to let the adaptive scheduler
            // restore from the savepoint
            jobMaster.start();

            final JobMasterGateway jobMasterGateway = jobMaster.getGateway();

            // AdaptiveScheduler-specific requirement: the AdaptiveScheduler triggers the
            // ExecutionGraph creation only after it received the correct amount of slots
            registerSlotsAtJobMaster(
                    parallelism,
                    jobMasterGateway,
                    jobGraph.getJobID(),
                    new TestingTaskExecutorGatewayBuilder()
                            .setAddress("firstTaskManager")
                            .createTestingTaskExecutorGateway(),
                    new LocalUnresolvedTaskManagerLocation());

            CommonTestUtils.waitUntilCondition(
                    () ->
                            jobMasterGateway.requestJobStatus(testingTimeout).get()
                                    == JobStatus.RUNNING);

            CheckpointStatsSnapshot checkpointStatsSnapshot =
                    jobMaster.getGateway().requestCheckpointStats(testingTimeout).get();

            // assert that the checkpoint snapshot reflects the latest completed checkpoint
            assertThat(checkpointStatsSnapshot.getLatestRestoredCheckpoint().getCheckpointId())
                    .isEqualTo(savepointId);
        }
    }

    private TestingResourceManagerGateway createResourceManagerGateway(
            CompletableFuture<Collection<BlockedNode>> firstReceivedBlockedNodeFuture,
            CompletableFuture<Collection<BlockedNode>> secondReceivedBlockedNodeFuture,
            CompletableFuture<Void> jobManagerRegistrationFuture) {
        final TestingResourceManagerGateway resourceManagerGateway =
                new TestingResourceManagerGateway();

        rpcService.registerGateway(resourceManagerGateway.getAddress(), resourceManagerGateway);

        resourceManagerGateway.setNotifyNewBlockedNodesFunction(
                blockedNodes -> {
                    if (!firstReceivedBlockedNodeFuture.isDone()) {
                        firstReceivedBlockedNodeFuture.complete(blockedNodes);
                    } else if (!secondReceivedBlockedNodeFuture.isDone()) {
                        secondReceivedBlockedNodeFuture.complete(blockedNodes);
                    }
                    return CompletableFuture.completedFuture(Acknowledge.get());
                });

        resourceManagerGateway.setRegisterJobManagerFunction(
                (ignoredA, ignoredB, ignoredC, ignoredD) -> {
                    jobManagerRegistrationFuture.complete(null);
                    return CompletableFuture.completedFuture(
                            resourceManagerGateway.getJobMasterRegistrationSuccess());
                });
        return resourceManagerGateway;
    }

    private void runJobFailureWhenTaskExecutorTerminatesTest(
            HeartbeatServices heartbeatServices,
            BiConsumer<LocalUnresolvedTaskManagerLocation, JobMasterGateway> jobReachedRunningState)
            throws Exception {
        final JobGraph jobGraph = JobGraphTestUtils.singleNoOpJobGraph();
        final JobMasterBuilder.TestingOnCompletionActions onCompletionActions =
                new JobMasterBuilder.TestingOnCompletionActions();

        try (final JobMaster jobMaster =
                new JobMasterBuilder(jobGraph, rpcService)
                        .withResourceId(jmResourceId)
                        .withHighAvailabilityServices(haServices)
                        .withHeartbeatServices(heartbeatServices)
                        .withOnCompletionActions(onCompletionActions)
                        .createJobMaster()) {

            jobMaster.start();

            final JobMasterGateway jobMasterGateway =
                    jobMaster.getSelfGateway(JobMasterGateway.class);

            final LocalUnresolvedTaskManagerLocation taskManagerUnresolvedLocation =
                    new LocalUnresolvedTaskManagerLocation();
            final CompletableFuture<ExecutionAttemptID> taskDeploymentFuture =
                    new CompletableFuture<>();
            final TestingTaskExecutorGateway taskExecutorGateway =
                    new TestingTaskExecutorGatewayBuilder()
                            .setSubmitTaskConsumer(
                                    (taskDeploymentDescriptor, jobMasterId) -> {
                                        taskDeploymentFuture.complete(
                                                taskDeploymentDescriptor.getExecutionAttemptId());
                                        return CompletableFuture.completedFuture(Acknowledge.get());
                                    })
                            .createTestingTaskExecutorGateway();

            final Collection<SlotOffer> slotOffers =
                    registerSlotsAtJobMaster(
                            1,
                            jobMasterGateway,
                            jobGraph.getJobID(),
                            taskExecutorGateway,
                            taskManagerUnresolvedLocation);
            assertThat(slotOffers).hasSize(1);

            final ExecutionAttemptID executionAttemptId = taskDeploymentFuture.get();

            jobMasterGateway
                    .updateTaskExecutionState(
                            new TaskExecutionState(executionAttemptId, ExecutionState.INITIALIZING))
                    .get();
            jobMasterGateway
                    .updateTaskExecutionState(
                            new TaskExecutionState(executionAttemptId, ExecutionState.RUNNING))
                    .get();

            jobReachedRunningState.accept(taskManagerUnresolvedLocation, jobMasterGateway);

            final ArchivedExecutionGraph archivedExecutionGraph =
                    onCompletionActions
                            .getJobReachedGloballyTerminalStateFuture()
                            .get()
                            .getArchivedExecutionGraph();

            assertThat(archivedExecutionGraph.getState()).isEqualTo(JobStatus.FAILED);
        }
    }

    private Collection<SlotOffer> registerSlotsAtJobMaster(
            int numberSlots,
            JobMasterGateway jobMasterGateway,
            JobID jobId,
            TaskExecutorGateway taskExecutorGateway,
            UnresolvedTaskManagerLocation unresolvedTaskManagerLocation)
            throws ExecutionException, InterruptedException {
        rpcService.registerGateway(taskExecutorGateway.getAddress(), taskExecutorGateway);

        jobMasterGateway
                .registerTaskManager(
                        jobId,
                        TaskManagerRegistrationInformation.create(
                                taskExecutorGateway.getAddress(),
                                unresolvedTaskManagerLocation,
                                TestingUtils.zeroUUID()),
                        testingTimeout)
                .get();

        Collection<SlotOffer> slotOffers =
                IntStream.range(0, numberSlots)
                        .mapToObj(
                                index ->
                                        new SlotOffer(
                                                new AllocationID(), index, ResourceProfile.ANY))
                        .collect(Collectors.toList());

        return jobMasterGateway
                .offerSlots(
                        unresolvedTaskManagerLocation.getResourceID(), slotOffers, testingTimeout)
                .get();
    }

    private JobGraph producerConsumerJobGraph() {
        final JobVertex producer = new JobVertex("Producer");
        producer.setInvokableClass(NoOpInvokable.class);
        producer.setParallelism(1);
        final JobVertex consumer = new JobVertex("Consumer");
        consumer.setInvokableClass(NoOpInvokable.class);
        consumer.setParallelism(1);

        consumer.connectNewDataSetAsInput(
                producer, DistributionPattern.POINTWISE, ResultPartitionType.BLOCKING);

        return JobGraphTestUtils.batchJobGraph(producer, consumer);
    }

    private File createSavepoint(long savepointId) throws IOException {
        return TestUtils.createSavepointWithOperatorState(
                Files.createTempFile(temporaryFolder, UUID.randomUUID().toString(), "").toFile(),
                savepointId);
    }

    private JobGraph createJobGraphWithCheckpointing(
            SavepointRestoreSettings savepointRestoreSettings) {
        return createJobGraphWithCheckpointing(1, savepointRestoreSettings);
    }

    private JobGraph createJobGraphWithCheckpointing(
            int parallelism, SavepointRestoreSettings savepointRestoreSettings) {
        final JobVertex source = new JobVertex("source");
        source.setInvokableClass(NoOpInvokable.class);
        source.setParallelism(parallelism);

        return TestUtils.createJobGraphFromJobVerticesWithCheckpointing(
                savepointRestoreSettings, source);
    }

    private JobGraph createSingleVertexJobWithRestartStrategy() throws IOException {
        final JobGraph jobGraph = JobGraphTestUtils.singleNoOpJobGraph();

        final ExecutionConfig executionConfig = new ExecutionConfig();
        executionConfig.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, 0L));
        jobGraph.setExecutionConfig(executionConfig);

        return jobGraph;
    }

    private static final class DummyCheckpointStorageLocation
            implements CompletedCheckpointStorageLocation {

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
        public void disposeStorageLocation() throws IOException {}
    }

    private static void registerSlotsRequiredForJobExecution(
            JobMasterGateway jobMasterGateway, JobID jobId, int numSlots)
            throws ExecutionException, InterruptedException {
        final TaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder()
                        .setCancelTaskFunction(
                                executionAttemptId -> {
                                    jobMasterGateway.updateTaskExecutionState(
                                            new TaskExecutionState(
                                                    executionAttemptId, ExecutionState.CANCELED));
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                })
                        .createTestingTaskExecutorGateway();
        JobMasterTestUtils.registerTaskExecutorAndOfferSlots(
                rpcService, jobMasterGateway, jobId, numSlots, taskExecutorGateway, testingTimeout);
    }
}
