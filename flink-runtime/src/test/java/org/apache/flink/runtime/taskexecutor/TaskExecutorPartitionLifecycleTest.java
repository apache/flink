/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.flink.core.testutils.BlockerSync;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.blob.NoOpTaskExecutorBlobService;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.externalresource.ExternalResourceInfoProvider;
import org.apache.flink.runtime.heartbeat.HeartbeatServicesImpl;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironment;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironmentBuilder;
import org.apache.flink.runtime.io.network.partition.PartitionTestUtils;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.TaskExecutorPartitionInfo;
import org.apache.flink.runtime.io.network.partition.TaskExecutorPartitionTracker;
import org.apache.flink.runtime.io.network.partition.TaskExecutorPartitionTrackerImpl;
import org.apache.flink.runtime.io.network.partition.TestingTaskExecutorPartitionTracker;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobmaster.JMTMRegistrationSuccess;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGateway;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGatewayBuilder;
import org.apache.flink.runtime.leaderretrieval.SettableLeaderRetrievalService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.security.token.DelegationTokenReceiverRepository;
import org.apache.flink.runtime.shuffle.ShuffleEnvironment;
import org.apache.flink.runtime.state.TaskExecutorLocalStateStoresManager;
import org.apache.flink.runtime.taskexecutor.slot.TaskSlotTable;
import org.apache.flink.runtime.taskexecutor.slot.TaskSlotUtils;
import org.apache.flink.runtime.taskmanager.NoOpTaskManagerActions;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.testutils.TestFileUtils;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.testutils.junit.utils.TempDirUtils;
import org.apache.flink.util.Reference;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.concurrent.Executors;
import org.apache.flink.util.function.TriConsumer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.StreamSupport;

import static org.apache.flink.core.testutils.FlinkAssertions.assertThatFuture;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the partition-lifecycle logic in the {@link TaskExecutor}. */
class TaskExecutorPartitionLifecycleTest {

    private static final Time timeout = Time.seconds(10L);

    private static TestingRpcService rpc;

    private final TestingHighAvailabilityServices haServices =
            new TestingHighAvailabilityServices();
    private final SettableLeaderRetrievalService jobManagerLeaderRetriever =
            new SettableLeaderRetrievalService();
    private final SettableLeaderRetrievalService resourceManagerLeaderRetriever =
            new SettableLeaderRetrievalService();
    private final JobID jobId = new JobID();

    @TempDir private Path tempDir;

    @RegisterExtension
    private static final TestExecutorExtension<ScheduledExecutorService>
            TEST_EXECUTOR_SERVICE_RESOURCE = TestingUtils.defaultExecutorExtension();

    @BeforeEach
    void setup() {
        haServices.setResourceManagerLeaderRetriever(resourceManagerLeaderRetriever);
        haServices.setJobMasterLeaderRetriever(jobId, jobManagerLeaderRetriever);
    }

    @AfterEach
    void shutdown() {
        rpc.clearGateways();
    }

    @BeforeAll
    static void setupClass() {
        rpc = new TestingRpcService();
    }

    @AfterAll
    static void shutdownClass() throws ExecutionException, InterruptedException {
        rpc.closeAsync().get();
    }

    @Test
    void testJobMasterConnectionTerminationAfterExternalRelease() throws Exception {
        testJobMasterConnectionTerminationAfterExternalReleaseOrPromotion(
                ((taskExecutorGateway, jobID, resultPartitionID) ->
                        taskExecutorGateway.releasePartitions(
                                jobID, Collections.singleton(resultPartitionID))));
    }

    @Test
    void testJobMasterConnectionTerminationAfterExternalPromotion() throws Exception {
        testJobMasterConnectionTerminationAfterExternalReleaseOrPromotion(
                ((taskExecutorGateway, jobID, resultPartitionID) ->
                        taskExecutorGateway.promotePartitions(
                                jobID, Collections.singleton(resultPartitionID))));
    }

    private void testJobMasterConnectionTerminationAfterExternalReleaseOrPromotion(
            TriConsumer<TaskExecutorGateway, JobID, ResultPartitionID> releaseOrPromoteCall)
            throws Exception {
        final CompletableFuture<Void> disconnectFuture = new CompletableFuture<>();
        final JobMasterGateway jobMasterGateway =
                new TestingJobMasterGatewayBuilder()
                        .setDisconnectTaskManagerFunction(
                                resourceID -> {
                                    disconnectFuture.complete(null);
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                })
                        .build();

        final DefaultJobTable jobTable = DefaultJobTable.create();

        final TaskManagerServices taskManagerServices =
                new TaskManagerServicesBuilder()
                        .setJobTable(jobTable)
                        .setShuffleEnvironment(new NettyShuffleEnvironmentBuilder().build())
                        .setTaskSlotTable(createTaskSlotTable())
                        .build();

        final TestingTaskExecutorPartitionTracker partitionTracker =
                new TestingTaskExecutorPartitionTracker();

        final AtomicBoolean trackerIsTrackingPartitions = new AtomicBoolean(false);
        partitionTracker.setIsTrackingPartitionsForFunction(
                jobId -> trackerIsTrackingPartitions.get());

        final CompletableFuture<Collection<ResultPartitionID>> firstReleasePartitionsCallFuture =
                new CompletableFuture<>();
        partitionTracker.setStopTrackingAndReleasePartitionsConsumer(
                firstReleasePartitionsCallFuture::complete);

        final ResultPartitionDeploymentDescriptor resultPartitionDeploymentDescriptor =
                PartitionTestUtils.createPartitionDeploymentDescriptor(
                        ResultPartitionType.BLOCKING);
        final ResultPartitionID resultPartitionId =
                resultPartitionDeploymentDescriptor.getShuffleDescriptor().getResultPartitionID();

        final TestingTaskExecutor taskExecutor =
                createTestingTaskExecutor(taskManagerServices, partitionTracker);

        try {
            taskExecutor.start();
            taskExecutor.waitUntilStarted();

            TaskSubmissionTestEnvironment.registerJobMasterConnection(
                    jobTable,
                    jobId,
                    jobMasterGateway,
                    new NoOpTaskManagerActions(),
                    taskExecutor.getMainThreadExecutableForTesting());

            final TaskExecutorGateway taskExecutorGateway =
                    taskExecutor.getSelfGateway(TaskExecutorGateway.class);

            trackerIsTrackingPartitions.set(true);
            assertThat(firstReleasePartitionsCallFuture).isNotDone();

            taskExecutorGateway.releasePartitions(
                    jobId, Collections.singleton(new ResultPartitionID()));

            // at this point we only know that the TE has entered releasePartitions; we cannot be
            // certain whether it
            // has already checked whether it should disconnect or not
            firstReleasePartitionsCallFuture.get();

            // connection should be kept alive since the table still contains partitions
            assertThat(disconnectFuture).isNotDone();

            trackerIsTrackingPartitions.set(false);

            // the TM should check whether partitions are still stored, and afterwards terminate the
            // connection
            releaseOrPromoteCall.accept(taskExecutorGateway, jobId, resultPartitionId);

            disconnectFuture.get();
        } finally {
            RpcUtils.terminateRpcEndpoint(taskExecutor);
        }
    }

    @Test
    void testPartitionReleaseAfterJobMasterDisconnect() throws Exception {
        final CompletableFuture<JobID> releasePartitionsForJobFuture = new CompletableFuture<>();
        testPartitionRelease(
                partitionTracker ->
                        partitionTracker.setStopTrackingAndReleaseAllPartitionsConsumer(
                                releasePartitionsForJobFuture::complete),
                (jobId, resultPartitionDeploymentDescriptor, taskExecutor, taskExecutorGateway) -> {
                    taskExecutorGateway.disconnectJobManager(jobId, new Exception("test"));

                    assertThatFuture(releasePartitionsForJobFuture)
                            .eventuallySucceeds()
                            .isEqualTo(jobId);
                });
    }

    @Test
    void testPartitionReleaseAfterReleaseCall() throws Exception {
        final CompletableFuture<Collection<ResultPartitionID>> releasePartitionsFuture =
                new CompletableFuture<>();
        testPartitionRelease(
                partitionTracker ->
                        partitionTracker.setStopTrackingAndReleasePartitionsConsumer(
                                releasePartitionsFuture::complete),
                (jobId, resultPartitionDeploymentDescriptor, taskExecutor, taskExecutorGateway) -> {
                    final ResultPartitionID resultPartitionId =
                            resultPartitionDeploymentDescriptor
                                    .getShuffleDescriptor()
                                    .getResultPartitionID();

                    taskExecutorGateway.releasePartitions(
                            jobId, Collections.singleton(resultPartitionId));

                    assertThat(releasePartitionsFuture.get()).contains(resultPartitionId);
                    assertThat(releasePartitionsFuture.get()).contains(resultPartitionId);
                });
    }

    @Test
    void testPartitionPromotion() throws Exception {
        final CompletableFuture<Collection<ResultPartitionID>> promotePartitionsFuture =
                new CompletableFuture<>();
        testPartitionRelease(
                partitionTracker ->
                        partitionTracker.setPromotePartitionsConsumer(
                                promotePartitionsFuture::complete),
                (jobId, resultPartitionDeploymentDescriptor, taskExecutor, taskExecutorGateway) -> {
                    final ResultPartitionID resultPartitionId =
                            resultPartitionDeploymentDescriptor
                                    .getShuffleDescriptor()
                                    .getResultPartitionID();

                    taskExecutorGateway.promotePartitions(
                            jobId, Collections.singleton(resultPartitionId));

                    assertThat(promotePartitionsFuture.get()).contains(resultPartitionId);
                });
    }

    @Test
    void testClusterPartitionRelease() throws Exception {
        final CompletableFuture<Collection<IntermediateDataSetID>> releasePartitionsFuture =
                new CompletableFuture<>();
        testPartitionRelease(
                partitionTracker ->
                        partitionTracker.setReleaseClusterPartitionsConsumer(
                                releasePartitionsFuture::complete),
                (jobId, resultPartitionDeploymentDescriptor, taskExecutor, taskExecutorGateway) -> {
                    final IntermediateDataSetID dataSetId =
                            resultPartitionDeploymentDescriptor.getResultId();

                    taskExecutorGateway.releaseClusterPartitions(
                            Collections.singleton(dataSetId), timeout);

                    assertThat(releasePartitionsFuture.get()).contains(dataSetId);
                });
    }

    @Test
    void testBlockingLocalPartitionReleaseDoesNotBlockTaskExecutor() throws Exception {
        BlockerSync sync = new BlockerSync();
        ResultPartitionManager blockingResultPartitionManager =
                new ResultPartitionManager() {
                    @Override
                    public void releasePartition(ResultPartitionID partitionId, Throwable cause) {
                        sync.blockNonInterruptible();
                        super.releasePartition(partitionId, cause);
                    }
                };

        NettyShuffleEnvironment shuffleEnvironment =
                new NettyShuffleEnvironmentBuilder()
                        .setResultPartitionManager(blockingResultPartitionManager)
                        .setIoExecutor(TEST_EXECUTOR_SERVICE_RESOURCE.getExecutor())
                        .build();

        final CompletableFuture<ResultPartitionID> startTrackingFuture = new CompletableFuture<>();
        final TaskExecutorPartitionTracker partitionTracker =
                new TaskExecutorPartitionTrackerImpl(shuffleEnvironment) {
                    @Override
                    public void startTrackingPartition(
                            JobID producingJobId, TaskExecutorPartitionInfo partitionInfo) {
                        super.startTrackingPartition(producingJobId, partitionInfo);
                        startTrackingFuture.complete(partitionInfo.getResultPartitionId());
                    }
                };

        try {
            internalTestPartitionRelease(
                    partitionTracker,
                    shuffleEnvironment,
                    startTrackingFuture,
                    (jobId,
                            resultPartitionDeploymentDescriptor,
                            taskExecutor,
                            taskExecutorGateway) -> {
                        final IntermediateDataSetID dataSetId =
                                resultPartitionDeploymentDescriptor.getResultId();

                        taskExecutorGateway.releaseClusterPartitions(
                                Collections.singleton(dataSetId), timeout);

                        // execute some operation to check whether the TaskExecutor is blocked
                        taskExecutorGateway.canBeReleased().get(5, TimeUnit.SECONDS);
                    });
        } finally {
            sync.releaseBlocker();
        }
    }

    private void testPartitionRelease(
            PartitionTrackerSetup partitionTrackerSetup, TestAction testAction) throws Exception {
        final TestingTaskExecutorPartitionTracker partitionTracker =
                new TestingTaskExecutorPartitionTracker();
        final CompletableFuture<ResultPartitionID> startTrackingFuture = new CompletableFuture<>();
        partitionTracker.setStartTrackingPartitionsConsumer(
                (jobId, partitionInfo) ->
                        startTrackingFuture.complete(partitionInfo.getResultPartitionId()));
        partitionTrackerSetup.accept(partitionTracker);

        internalTestPartitionRelease(
                partitionTracker,
                new NettyShuffleEnvironmentBuilder().build(),
                startTrackingFuture,
                testAction);
    }

    private void internalTestPartitionRelease(
            TaskExecutorPartitionTracker partitionTracker,
            ShuffleEnvironment<?, ?> shuffleEnvironment,
            CompletableFuture<ResultPartitionID> startTrackingFuture,
            TestAction testAction)
            throws Exception {

        final ResultPartitionDeploymentDescriptor taskResultPartitionDescriptor =
                PartitionTestUtils.createPartitionDeploymentDescriptor(
                        ResultPartitionType.BLOCKING);
        final ExecutionAttemptID eid1 =
                taskResultPartitionDescriptor
                        .getShuffleDescriptor()
                        .getResultPartitionID()
                        .getProducerId();

        final TaskDeploymentDescriptor taskDeploymentDescriptor =
                TaskExecutorSubmissionTest.createTaskDeploymentDescriptor(
                        jobId,
                        "job",
                        eid1,
                        new SerializedValue<>(new ExecutionConfig()),
                        "Sender",
                        1,
                        1,
                        new Configuration(),
                        new Configuration(),
                        TestingInvokable.class.getName(),
                        Collections.singletonList(taskResultPartitionDescriptor),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList());

        final TaskSlotTable<Task> taskSlotTable = createTaskSlotTable();

        final TaskExecutorLocalStateStoresManager localStateStoresManager =
                new TaskExecutorLocalStateStoresManager(
                        false,
                        Reference.owned(new File[] {TempDirUtils.newFolder(tempDir)}),
                        Executors.directExecutor());

        final TaskManagerServices taskManagerServices =
                new TaskManagerServicesBuilder()
                        .setTaskSlotTable(taskSlotTable)
                        .setTaskStateManager(localStateStoresManager)
                        .setShuffleEnvironment(shuffleEnvironment)
                        .build();

        final CompletableFuture<Void> taskFinishedFuture = new CompletableFuture<>();
        final OneShotLatch slotOfferedLatch = new OneShotLatch();

        final TestingJobMasterGateway jobMasterGateway =
                new TestingJobMasterGatewayBuilder()
                        .setRegisterTaskManagerFunction(
                                (ignoredJobId, ignoredTaskManagerRegistrationInformation) ->
                                        CompletableFuture.completedFuture(
                                                new JMTMRegistrationSuccess(ResourceID.generate())))
                        .setOfferSlotsFunction(
                                (resourceID, slotOffers) -> {
                                    slotOfferedLatch.trigger();
                                    return CompletableFuture.completedFuture(slotOffers);
                                })
                        .setUpdateTaskExecutionStateFunction(
                                taskExecutionState -> {
                                    if (taskExecutionState.getExecutionState()
                                            == ExecutionState.FINISHED) {
                                        taskFinishedFuture.complete(null);
                                    }
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                })
                        .build();

        final TestingTaskExecutor taskExecutor =
                createTestingTaskExecutor(taskManagerServices, partitionTracker);

        final CompletableFuture<SlotReport> initialSlotReportFuture = new CompletableFuture<>();

        final TestingResourceManagerGateway testingResourceManagerGateway =
                new TestingResourceManagerGateway();
        testingResourceManagerGateway.setSendSlotReportFunction(
                resourceIDInstanceIDSlotReportTuple3 -> {
                    initialSlotReportFuture.complete(resourceIDInstanceIDSlotReportTuple3.f2);
                    return CompletableFuture.completedFuture(Acknowledge.get());
                });
        testingResourceManagerGateway.setRegisterTaskExecutorFunction(
                input ->
                        CompletableFuture.completedFuture(
                                new TaskExecutorRegistrationSuccess(
                                        new InstanceID(),
                                        testingResourceManagerGateway.getOwnResourceId(),
                                        new ClusterInformation("blobServerHost", 55555),
                                        null)));

        try {
            taskExecutor.start();
            taskExecutor.waitUntilStarted();

            final TaskExecutorGateway taskExecutorGateway =
                    taskExecutor.getSelfGateway(TaskExecutorGateway.class);

            final String jobMasterAddress = "jm";
            rpc.registerGateway(jobMasterAddress, jobMasterGateway);
            rpc.registerGateway(
                    testingResourceManagerGateway.getAddress(), testingResourceManagerGateway);

            // inform the task manager about the job leader
            taskManagerServices.getJobLeaderService().addJob(jobId, jobMasterAddress);
            jobManagerLeaderRetriever.notifyListener(jobMasterAddress, UUID.randomUUID());
            resourceManagerLeaderRetriever.notifyListener(
                    testingResourceManagerGateway.getAddress(),
                    testingResourceManagerGateway.getFencingToken().toUUID());

            final Optional<SlotStatus> slotStatusOptional =
                    StreamSupport.stream(initialSlotReportFuture.get().spliterator(), false)
                            .findAny();

            assertThat(slotStatusOptional).isPresent();

            final SlotStatus slotStatus = slotStatusOptional.get();

            while (true) {
                try {
                    taskExecutorGateway
                            .requestSlot(
                                    slotStatus.getSlotID(),
                                    jobId,
                                    taskDeploymentDescriptor.getAllocationId(),
                                    ResourceProfile.ZERO,
                                    jobMasterAddress,
                                    testingResourceManagerGateway.getFencingToken(),
                                    timeout)
                            .get();
                    break;
                } catch (Exception e) {
                    // the proper establishment of the RM connection is tracked
                    // asynchronously, so we have to poll here until it went through
                    // until then, slot requests will fail with an exception
                    Thread.sleep(50);
                }
            }

            TestingInvokable.sync = new BlockerSync();

            // Wait till the slot has been successfully offered before submitting the task.
            // This ensures TM has been successfully registered to JM.
            slotOfferedLatch.await();

            taskExecutorGateway
                    .submitTask(
                            taskDeploymentDescriptor, jobMasterGateway.getFencingToken(), timeout)
                    .get();

            TestingInvokable.sync.awaitBlocker();

            // the task is still running => the partition is in in-progress and should be tracked
            assertThatFuture(startTrackingFuture)
                    .eventuallySucceeds()
                    .isEqualTo(
                            taskResultPartitionDescriptor
                                    .getShuffleDescriptor()
                                    .getResultPartitionID());

            TestingInvokable.sync.releaseBlocker();
            taskFinishedFuture.get(timeout.getSize(), timeout.getUnit());

            testAction.accept(
                    jobId, taskResultPartitionDescriptor, taskExecutor, taskExecutorGateway);
        } finally {
            RpcUtils.terminateRpcEndpoint(taskExecutor);
        }

        // the shutdown of the backing shuffle environment releases all partitions
        // the book-keeping is not aware of this
        assertThat(shuffleEnvironment.getPartitionsOccupyingLocalResources()).isEmpty();
    }

    /** Test invokable which completes the given future when executed. */
    public static class TestingInvokable extends AbstractInvokable {

        static BlockerSync sync;

        public TestingInvokable(Environment environment) {
            super(environment);
        }

        @Override
        public void invoke() throws Exception {
            sync.block();
        }
    }

    private TestingTaskExecutor createTestingTaskExecutor(
            TaskManagerServices taskManagerServices, TaskExecutorPartitionTracker partitionTracker)
            throws IOException {
        final Configuration configuration = new Configuration();
        return new TestingTaskExecutor(
                rpc,
                TaskManagerConfiguration.fromConfiguration(
                        configuration,
                        TaskExecutorResourceUtils.resourceSpecFromConfigForLocalExecution(
                                configuration),
                        InetAddress.getLoopbackAddress().getHostAddress(),
                        TestFileUtils.createTempDir()),
                haServices,
                taskManagerServices,
                ExternalResourceInfoProvider.NO_EXTERNAL_RESOURCES,
                new HeartbeatServicesImpl(10_000L, 30_000L),
                UnregisteredMetricGroups.createUnregisteredTaskManagerMetricGroup(),
                null,
                NoOpTaskExecutorBlobService.INSTANCE,
                new TestingFatalErrorHandler(),
                partitionTracker,
                new DelegationTokenReceiverRepository(configuration, null));
    }

    private static TaskSlotTable<Task> createTaskSlotTable() {
        return TaskSlotUtils.createTaskSlotTable(
                1, timeout, TEST_EXECUTOR_SERVICE_RESOURCE.getExecutor());
    }

    @FunctionalInterface
    private interface PartitionTrackerSetup {
        void accept(TestingTaskExecutorPartitionTracker partitionTracker) throws Exception;
    }

    @FunctionalInterface
    private interface TestAction {
        void accept(
                JobID jobId,
                ResultPartitionDeploymentDescriptor resultPartitionDeploymentDescriptor,
                TaskExecutor taskExecutor,
                TaskExecutorGateway taskExecutorGateway)
                throws Exception;
    }
}
