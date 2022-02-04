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
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.checkpoint.EmbeddedCompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.PerJobCheckpointRecoveryFactory;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.dispatcher.cleanup.DispatcherResourceCleanerFactory;
import org.apache.flink.runtime.dispatcher.cleanup.TestingRetryStrategies;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphBuilder;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.jobmanager.JobGraphStore;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.leaderelection.TestingLeaderElectionService;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.TestingJobGraphStore;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TimeUtils;
import org.apache.flink.util.concurrent.FutureUtils;

import org.hamcrest.CoreMatchers;
import org.hamcrest.collection.IsEmptyCollection;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertTrue;

/** An integration test for various fail-over scenarios of the {@link Dispatcher} component. */
public class DispatcherCleanupITCase extends AbstractDispatcherTest {

    private final BlockingQueue<RpcEndpoint> toTerminate = new LinkedBlockingQueue<>();

    @Before
    public void setUp() throws Exception {
        super.setUp();
        haServices.setCheckpointRecoveryFactory(
                new PerJobCheckpointRecoveryFactory<EmbeddedCompletedCheckpointStore>(
                        (maxCheckpoints, previous, sharedStateRegistryFactory, ioExecutor) -> {
                            if (previous != null) {
                                // First job cleanup still succeeded for the
                                // CompletedCheckpointStore because the JobGraph cleanup happens
                                // after the JobManagerRunner closing
                                assertTrue(previous.getShutdownStatus().isPresent());
                                assertTrue(previous.getAllCheckpoints().isEmpty());
                                return new EmbeddedCompletedCheckpointStore(
                                        maxCheckpoints,
                                        previous.getAllCheckpoints(),
                                        sharedStateRegistryFactory.create(
                                                ioExecutor, previous.getAllCheckpoints()));
                            }
                            return new EmbeddedCompletedCheckpointStore(
                                    maxCheckpoints,
                                    Collections.emptyList(),
                                    sharedStateRegistryFactory.create(
                                            ioExecutor, Collections.emptyList()));
                        }));
    }

    @After
    public void tearDown() {
        while (!toTerminate.isEmpty()) {
            final RpcEndpoint endpoint = toTerminate.poll();
            try {
                RpcUtils.terminateRpcEndpoint(endpoint, TIMEOUT);
            } catch (Exception e) {
                // Ignore.
            }
        }
    }

    @Test
    public void testCleanupThroughRetries() throws Exception {
        final JobGraph jobGraph = createJobGraph();
        final JobID jobId = jobGraph.getJobID();

        // JobGraphStore
        final AtomicInteger actualGlobalCleanupCallCount = new AtomicInteger();
        final OneShotLatch successfulCleanupLatch = new OneShotLatch();
        final int numberOfErrors = 5;
        final RuntimeException temporaryError =
                new RuntimeException("Expected RuntimeException: Unable to remove job graph.");
        final JobGraphStore jobGraphStore =
                createAndStartJobGraphStoreWithCleanupFailures(
                        numberOfErrors,
                        temporaryError,
                        actualGlobalCleanupCallCount,
                        successfulCleanupLatch);
        haServices.setJobGraphStore(jobGraphStore);

        // Construct leader election service.
        final TestingLeaderElectionService leaderElectionService =
                new TestingLeaderElectionService();
        haServices.setJobMasterLeaderElectionService(jobId, leaderElectionService);

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
                                        haServices.getJobGraphStore(),
                                        blobServer,
                                        haServices,
                                        UnregisteredMetricGroups
                                                .createUnregisteredJobManagerMetricGroup()))
                        .build();
        dispatcher.start();

        toTerminate.add(dispatcher);
        leaderElectionService.isLeader(UUID.randomUUID());
        final DispatcherGateway dispatcherGateway =
                dispatcher.getSelfGateway(DispatcherGateway.class);
        dispatcherGateway.submitJob(jobGraph, TIMEOUT).get();

        waitForJobToFinish(leaderElectionService, dispatcherGateway, jobId);

        successfulCleanupLatch.await();

        assertThat(actualGlobalCleanupCallCount.get(), equalTo(numberOfErrors + 1));

        assertThat(
                "The JobGraph should be removed from JobGraphStore.",
                haServices.getJobGraphStore().getJobIds(),
                IsEmptyCollection.empty());

        CommonTestUtils.waitUntilCondition(
                () -> haServices.getJobResultStore().hasJobResultEntry(jobId),
                Deadline.fromNow(Duration.ofMinutes(5)),
                "The JobResultStore should have this job marked as clean.");
    }

    @Test
    public void testCleanupAfterLeadershipChange() throws Exception {
        final JobGraph jobGraph = createJobGraph();
        final JobID jobId = jobGraph.getJobID();

        // Construct job graph store.
        final AtomicInteger actualGlobalCleanupCallCount = new AtomicInteger();
        final OneShotLatch successfulCleanupLatch = new OneShotLatch();
        final RuntimeException temporaryError = new RuntimeException("Unable to remove job graph.");
        final JobGraphStore jobGraphStore =
                createAndStartJobGraphStoreWithCleanupFailures(
                        1, temporaryError, actualGlobalCleanupCallCount, successfulCleanupLatch);
        haServices.setJobGraphStore(jobGraphStore);

        // Construct leader election service.
        final TestingLeaderElectionService leaderElectionService =
                new TestingLeaderElectionService();
        haServices.setJobMasterLeaderElectionService(jobId, leaderElectionService);

        // start the dispatcher with no retries on cleanup
        final CountDownLatch jobGraphRemovalErrorReceived = new CountDownLatch(1);
        final Dispatcher dispatcher =
                createTestingDispatcherBuilder()
                        .setFatalErrorHandler(
                                throwable -> {
                                    final Optional<Throwable> maybeError =
                                            ExceptionUtils.findThrowable(
                                                    throwable, temporaryError::equals);
                                    if (maybeError.isPresent()) {
                                        jobGraphRemovalErrorReceived.countDown();
                                    } else {
                                        testingFatalErrorHandlerResource
                                                .getFatalErrorHandler()
                                                .onFatalError(throwable);
                                    }
                                })
                        .build();
        dispatcher.start();

        toTerminate.add(dispatcher);
        leaderElectionService.isLeader(UUID.randomUUID());
        final DispatcherGateway dispatcherGateway =
                dispatcher.getSelfGateway(DispatcherGateway.class);
        dispatcherGateway.submitJob(jobGraph, TIMEOUT).get();

        waitForJobToFinish(leaderElectionService, dispatcherGateway, jobId);
        jobGraphRemovalErrorReceived.await();

        // Remove job master leadership.
        leaderElectionService.notLeader();

        // This will clear internal state of election service, so a new contender can register.
        leaderElectionService.stop();

        assertThat(successfulCleanupLatch.isTriggered(), CoreMatchers.is(false));

        assertThat(
                "The JobGraph is still stored in the JobGraphStore.",
                haServices.getJobGraphStore().getJobIds(),
                CoreMatchers.is(Collections.singleton(jobId)));
        assertThat(
                "The JobResultStore has this job marked as dirty.",
                haServices.getJobResultStore().getDirtyResults().stream()
                        .map(JobResult::getJobId)
                        .collect(Collectors.toSet()),
                CoreMatchers.is(Collections.singleton(jobId)));

        // Run a second dispatcher, that restores our finished job.
        final Dispatcher secondDispatcher =
                createTestingDispatcherBuilder()
                        .setRecoveredDirtyJobs(haServices.getJobResultStore().getDirtyResults())
                        .build();
        secondDispatcher.start();

        toTerminate.add(secondDispatcher);
        leaderElectionService.isLeader(UUID.randomUUID());

        CommonTestUtils.waitUntilCondition(
                () -> haServices.getJobResultStore().getDirtyResults().isEmpty(),
                Deadline.fromNow(TimeUtils.toDuration(TIMEOUT)));

        assertThat(
                "The JobGraph is not stored in the JobGraphStore.",
                haServices.getJobGraphStore().getJobIds(),
                IsEmptyCollection.empty());
        assertTrue(
                "The JobResultStore has the job listed as clean.",
                haServices.getJobResultStore().hasJobResultEntry(jobId));

        // wait for the successful cleanup to be triggered
        successfulCleanupLatch.await();

        assertThat(actualGlobalCleanupCallCount.get(), equalTo(2));
    }

    private JobGraphStore createAndStartJobGraphStoreWithCleanupFailures(
            int numberOfCleanupFailures,
            Throwable throwable,
            AtomicInteger actualCleanupCallCount,
            OneShotLatch successfulCleanupLatch)
            throws Exception {
        final AtomicInteger failureCount = new AtomicInteger(numberOfCleanupFailures);
        final JobGraphStore jobGraphStore =
                TestingJobGraphStore.newBuilder()
                        .setGlobalCleanupFunction(
                                (ignoredJobId, ignoredExecutor) -> {
                                    actualCleanupCallCount.incrementAndGet();

                                    if (failureCount.getAndDecrement() > 0) {
                                        return FutureUtils.completedExceptionally(throwable);
                                    }

                                    successfulCleanupLatch.trigger();
                                    return FutureUtils.completedVoidFuture();
                                })
                        .build();

        jobGraphStore.start(null);
        return jobGraphStore;
    }

    private void waitForJobToFinish(
            TestingLeaderElectionService leaderElectionService,
            DispatcherGateway dispatcherGateway,
            JobID jobId)
            throws Exception {
        final JobMasterGateway jobMasterGateway =
                connectToLeadingJobMaster(leaderElectionService).get();
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
            TestingLeaderElectionService leaderElectionService) {
        return leaderElectionService
                .getConfirmationFuture()
                .thenCompose(
                        leaderConnectionInfo ->
                                rpcService.connect(
                                        leaderConnectionInfo.getAddress(),
                                        JobMasterId.fromUuidOrNull(
                                                leaderConnectionInfo.getLeaderSessionId()),
                                        JobMasterGateway.class));
    }
}
