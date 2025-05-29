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

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.CleanupOptions;
import org.apache.flink.core.execution.RecoveryClaimMode;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.checkpoint.EmbeddedCompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.PerJobCheckpointRecoveryFactory;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.dispatcher.cleanup.DispatcherResourceCleanerFactory;
import org.apache.flink.runtime.dispatcher.cleanup.TestingRetryStrategies;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.highavailability.JobResultEntry;
import org.apache.flink.runtime.highavailability.JobResultStore;
import org.apache.flink.runtime.highavailability.nonha.embedded.EmbeddedJobResultStore;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphBuilder;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.jobmanager.ExecutionPlanStore;
import org.apache.flink.runtime.jobmaster.JobManagerRunner;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.leaderelection.LeaderInformation;
import org.apache.flink.runtime.leaderelection.TestingLeaderElection;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.TestingExecutionPlanStore;
import org.apache.flink.runtime.testutils.TestingJobResultStore;
import org.apache.flink.util.concurrent.FutureUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** An integration test for various fail-over scenarios of the {@link Dispatcher} component. */
public class DispatcherCleanupITCase extends AbstractDispatcherTest {

    private final BlockingQueue<RpcEndpoint> toTerminate = new LinkedBlockingQueue<>();

    @Before
    public void setUp() throws Exception {
        super.setUp();
        haServices.setCheckpointRecoveryFactory(
                new PerJobCheckpointRecoveryFactory<EmbeddedCompletedCheckpointStore>(
                        (maxCheckpoints,
                                previous,
                                sharedStateRegistryFactory,
                                ioExecutor,
                                recoveryClaimMode) -> {
                            if (previous != null) {
                                // First job cleanup still succeeded for the
                                // CompletedCheckpointStore because the JobGraph cleanup happens
                                // after the JobManagerRunner closing
                                assertThat(previous.getShutdownStatus()).isPresent();
                                assertThat(previous.getAllCheckpoints()).isEmpty();
                                return new EmbeddedCompletedCheckpointStore(
                                        maxCheckpoints,
                                        previous.getAllCheckpoints(),
                                        sharedStateRegistryFactory.create(
                                                ioExecutor,
                                                previous.getAllCheckpoints(),
                                                recoveryClaimMode));
                            }
                            return new EmbeddedCompletedCheckpointStore(
                                    maxCheckpoints,
                                    Collections.emptyList(),
                                    sharedStateRegistryFactory.create(
                                            ioExecutor,
                                            Collections.emptyList(),
                                            RecoveryClaimMode.DEFAULT));
                        }));
    }

    @After
    public void tearDown() {
        while (!toTerminate.isEmpty()) {
            final RpcEndpoint endpoint = toTerminate.poll();
            try {
                RpcUtils.terminateRpcEndpoint(endpoint);
            } catch (Exception e) {
                // Ignore.
            }
        }
    }

    @Test
    public void testCleanupThroughRetries() throws Exception {
        final JobGraph jobGraph = createJobGraph();
        final JobID jobId = jobGraph.getJobID();

        // ExecutionPlanStore
        final AtomicInteger actualGlobalCleanupCallCount = new AtomicInteger();
        final OneShotLatch successfulCleanupLatch = new OneShotLatch();
        final int numberOfErrors = 5;
        final RuntimeException temporaryError =
                new RuntimeException("Expected RuntimeException: Unable to remove job graph.");
        final AtomicInteger failureCount = new AtomicInteger(numberOfErrors);
        final ExecutionPlanStore executionPlanStore =
                TestingExecutionPlanStore.newBuilder()
                        .setGlobalCleanupFunction(
                                (ignoredJobId, ignoredExecutor) -> {
                                    actualGlobalCleanupCallCount.incrementAndGet();

                                    if (failureCount.getAndDecrement() > 0) {
                                        return FutureUtils.completedExceptionally(temporaryError);
                                    }

                                    successfulCleanupLatch.trigger();
                                    return FutureUtils.completedVoidFuture();
                                })
                        .build();

        executionPlanStore.start(NoOpExecutionPlanListener.INSTANCE);
        haServices.setExecutionPlanStore(executionPlanStore);

        // Construct leader election.
        final TestingLeaderElection leaderElection = new TestingLeaderElection();
        haServices.setJobMasterLeaderElection(jobId, leaderElection);

        // start the dispatcher with enough retries on cleanup
        final JobManagerRunnerRegistry jobManagerRunnerRegistry =
                new DefaultJobManagerRunnerRegistry(2);
        final Dispatcher dispatcher =
                createTestingDispatcherBuilder()
                        .setResourceCleanerFactory(
                                new DispatcherResourceCleanerFactory(
                                        ForkJoinPool.commonPool(),
                                        TestingRetryStrategies.createWithNumberOfRetries(
                                                numberOfErrors),
                                        jobManagerRunnerRegistry,
                                        haServices.getExecutionPlanStore(),
                                        blobServer,
                                        haServices,
                                        UnregisteredMetricGroups
                                                .createUnregisteredJobManagerMetricGroup()))
                        .build(rpcService);
        dispatcher.start();

        toTerminate.add(dispatcher);
        final CompletableFuture<LeaderInformation> confirmedLeaderInformation =
                leaderElection.isLeader(UUID.randomUUID());
        final DispatcherGateway dispatcherGateway =
                dispatcher.getSelfGateway(DispatcherGateway.class);
        dispatcherGateway.submitJob(jobGraph, TIMEOUT).get();

        waitForJobToFinish(confirmedLeaderInformation, dispatcherGateway, jobId);

        successfulCleanupLatch.await();

        assertThat(actualGlobalCleanupCallCount.get()).isEqualTo(numberOfErrors + 1);

        assertThat(haServices.getExecutionPlanStore().getJobIds())
                .as("The JobGraph should be removed from ExecutionPlanStore.")
                .isEmpty();

        CommonTestUtils.waitUntilCondition(
                () -> haServices.getJobResultStore().hasJobResultEntryAsync(jobId).get());
    }

    @Test
    public void testCleanupNotCancellable() throws Exception {
        final JobGraph jobGraph = createJobGraph();
        final JobID jobId = jobGraph.getJobID();

        final JobResultStore jobResultStore = new EmbeddedJobResultStore();
        jobResultStore
                .createDirtyResultAsync(
                        new JobResultEntry(TestingJobResultStore.createSuccessfulJobResult(jobId)))
                .get();
        haServices.setJobResultStore(jobResultStore);

        // Instantiates JobManagerRunner
        final CompletableFuture<Void> jobManagerRunnerCleanupFuture = new CompletableFuture<>();
        final AtomicReference<JobManagerRunner> jobManagerRunnerEntry = new AtomicReference<>();
        final JobManagerRunnerRegistry jobManagerRunnerRegistry =
                TestingJobManagerRunnerRegistry.newSingleJobBuilder(jobManagerRunnerEntry)
                        .withLocalCleanupAsyncFunction(
                                (actualJobId, executor) -> jobManagerRunnerCleanupFuture)
                        .build();

        final Dispatcher dispatcher =
                createTestingDispatcherBuilder()
                        .setJobManagerRunnerRegistry(jobManagerRunnerRegistry)
                        .build(rpcService);
        dispatcher.start();

        toTerminate.add(dispatcher);

        CommonTestUtils.waitUntilCondition(() -> jobManagerRunnerEntry.get() != null);

        assertThat(haServices.getJobResultStore().hasDirtyJobResultEntryAsync(jobId).get())
                .as("The JobResultStore should have this job still marked as dirty.")
                .isTrue();

        final DispatcherGateway dispatcherGateway =
                dispatcher.getSelfGateway(DispatcherGateway.class);

        assertThatThrownBy(() -> dispatcherGateway.cancelJob(jobId, TIMEOUT).get())
                .hasCauseInstanceOf(JobCancellationFailedException.class);
        jobManagerRunnerCleanupFuture.complete(null);

        CommonTestUtils.waitUntilCondition(
                () -> haServices.getJobResultStore().hasCleanJobResultEntryAsync(jobId).get());
    }

    @Test
    public void testCleanupAfterLeadershipChange() throws Exception {
        final JobGraph jobGraph = createJobGraph();
        final JobID jobId = jobGraph.getJobID();

        // Construct execution plan store.
        final AtomicInteger actualGlobalCleanupCallCount = new AtomicInteger();
        final OneShotLatch firstCleanupTriggered = new OneShotLatch();
        final CompletableFuture<JobID> successfulJobGraphCleanup = new CompletableFuture<>();
        final ExecutionPlanStore executionPlanStore =
                TestingExecutionPlanStore.newBuilder()
                        .setGlobalCleanupFunction(
                                (actualJobId, ignoredExecutor) -> {
                                    final int callCount =
                                            actualGlobalCleanupCallCount.getAndIncrement();
                                    firstCleanupTriggered.trigger();

                                    if (callCount < 1) {
                                        return FutureUtils.completedExceptionally(
                                                new RuntimeException(
                                                        "Expected RuntimeException: Unable to remove job graph."));
                                    }

                                    successfulJobGraphCleanup.complete(actualJobId);
                                    return FutureUtils.completedVoidFuture();
                                })
                        .build();

        executionPlanStore.start(NoOpExecutionPlanListener.INSTANCE);
        haServices.setExecutionPlanStore(executionPlanStore);

        // Construct leader election.
        final TestingLeaderElection leaderElection = new TestingLeaderElection();
        haServices.setJobMasterLeaderElection(jobId, leaderElection);

        // start the dispatcher with no retries on cleanup
        configuration.set(
                CleanupOptions.CLEANUP_STRATEGY,
                CleanupOptions.NONE_PARAM_VALUES.iterator().next());
        final Dispatcher dispatcher = createTestingDispatcherBuilder().build(rpcService);
        dispatcher.start();

        toTerminate.add(dispatcher);
        final CompletableFuture<LeaderInformation> confirmedLeaderInformation =
                leaderElection.isLeader(UUID.randomUUID());
        final DispatcherGateway dispatcherGateway =
                dispatcher.getSelfGateway(DispatcherGateway.class);
        dispatcherGateway.submitJob(jobGraph, TIMEOUT).get();

        waitForJobToFinish(confirmedLeaderInformation, dispatcherGateway, jobId);
        firstCleanupTriggered.await();

        assertThat(actualGlobalCleanupCallCount.get())
                .as("The cleanup should have been triggered only once.")
                .isOne();
        assertThat(successfulJobGraphCleanup.isDone())
                .as("The cleanup should not have reached the successful cleanup code path.")
                .isFalse();

        assertThat(haServices.getExecutionPlanStore().getJobIds())
                .as("The JobGraph is still stored in the ExecutionPlanStore.")
                .containsExactly(jobId);
        assertThat(
                        haServices.getJobResultStore().getDirtyResults().stream()
                                .map(JobResult::getJobId)
                                .collect(Collectors.toSet()))
                .as("The JobResultStore should have this job marked as dirty.")
                .containsExactly(jobId);

        // Run a second dispatcher, that restores our finished job.
        final Dispatcher secondDispatcher =
                createTestingDispatcherBuilder()
                        .setRecoveredDirtyJobs(haServices.getJobResultStore().getDirtyResults())
                        .build(rpcService);
        secondDispatcher.start();

        toTerminate.add(secondDispatcher);
        leaderElection.isLeader(UUID.randomUUID());

        CommonTestUtils.waitUntilCondition(
                () -> haServices.getJobResultStore().getDirtyResults().isEmpty());

        assertThat(haServices.getExecutionPlanStore().getJobIds())
                .as("The JobGraph is not stored in the ExecutionPlanStore.")
                .isEmpty();
        assertThat(haServices.getJobResultStore().hasJobResultEntryAsync(jobId).get())
                .as("The JobResultStore has the job listed as clean.")
                .isTrue();

        assertThat(successfulJobGraphCleanup.get()).isEqualTo(jobId);

        assertThat(actualGlobalCleanupCallCount.get()).isEqualTo(2);
    }

    private void waitForJobToFinish(
            CompletableFuture<LeaderInformation> confirmedLeaderInformation,
            DispatcherGateway dispatcherGateway,
            JobID jobId)
            throws Exception {
        final JobMasterGateway jobMasterGateway =
                connectToLeadingJobMaster(confirmedLeaderInformation).get();
        try (final JobMasterTester tester =
                new JobMasterTester(rpcService, jobId, jobMasterGateway)) {
            final CompletableFuture<List<TaskDeploymentDescriptor>> descriptorsFuture =
                    tester.deployVertices(2);
            awaitStatus(dispatcherGateway, jobId, JobStatus.RUNNING);
            tester.transitionTo(descriptorsFuture.get(), ExecutionState.INITIALIZING).get();
            tester.transitionTo(descriptorsFuture.get(), ExecutionState.RUNNING).get();
            tester.getCheckpointFuture(1L).get();
            tester.transitionTo(descriptorsFuture.get(), ExecutionState.FINISHED).get();
        }
        awaitStatus(dispatcherGateway, jobId, JobStatus.FINISHED);
    }

    private JobGraph createJobGraph() {
        final JobVertex firstVertex = new JobVertex("first");
        firstVertex.setInvokableClass(NoOpInvokable.class);
        firstVertex.setParallelism(1);

        final JobVertex secondVertex = new JobVertex("second");
        secondVertex.setInvokableClass(NoOpInvokable.class);
        secondVertex.setParallelism(1);

        final CheckpointCoordinatorConfiguration checkpointCoordinatorConfiguration =
                CheckpointCoordinatorConfiguration.builder()
                        .setCheckpointInterval(20L)
                        .setMinPauseBetweenCheckpoints(20L)
                        .setCheckpointTimeout(10_000L)
                        .build();
        final JobCheckpointingSettings checkpointingSettings =
                new JobCheckpointingSettings(checkpointCoordinatorConfiguration, null);
        return JobGraphBuilder.newStreamingJobGraphBuilder()
                .addJobVertex(firstVertex)
                .addJobVertex(secondVertex)
                .setJobCheckpointingSettings(checkpointingSettings)
                .build();
    }

    private static CompletableFuture<JobMasterGateway> connectToLeadingJobMaster(
            CompletableFuture<LeaderInformation> confirmedLeaderInformation) {
        return confirmedLeaderInformation.thenCompose(
                leaderInformation ->
                        rpcService.connect(
                                leaderInformation.getLeaderAddress(),
                                JobMasterId.fromUuidOrNull(leaderInformation.getLeaderSessionID()),
                                JobMasterGateway.class));
    }
}
