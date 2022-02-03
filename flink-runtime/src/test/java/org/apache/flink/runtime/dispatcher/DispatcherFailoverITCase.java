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
import org.apache.flink.runtime.checkpoint.EmbeddedCompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.PerJobCheckpointRecoveryFactory;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphBuilder;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.leaderelection.TestingLeaderElectionService;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.runtime.testutils.TestingJobGraphStore;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.concurrent.FutureUtils;

import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/** An integration test for various fail-over scenarios of the {@link Dispatcher} component. */
public class DispatcherFailoverITCase extends AbstractDispatcherTest {

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
    public void testRecoverFromCheckpointAfterJobGraphRemovalOfTerminatedJobFailed()
            throws Exception {
        final JobGraph jobGraph = createJobGraph();
        final JobID jobId = jobGraph.getJobID();

        // Construct job graph store.
        final Error temporaryError = new Error("Unable to remove job graph.");
        final AtomicReference<? extends Error> temporaryErrorRef =
                new AtomicReference<>(temporaryError);
        final TestingJobGraphStore jobGraphStore =
                TestingJobGraphStore.newBuilder()
                        .setGlobalCleanupFunction(
                                (ignoredJobId, ignoredExecutor) -> {
                                    final Error error = temporaryErrorRef.getAndSet(null);
                                    if (error != null) {
                                        throw error;
                                    }

                                    return FutureUtils.completedVoidFuture();
                                })
                        .build();
        jobGraphStore.start(null);
        haServices.setJobGraphStore(jobGraphStore);

        // Construct leader election service.
        final TestingLeaderElectionService leaderElectionService =
                new TestingLeaderElectionService();
        haServices.setJobMasterLeaderElectionService(jobId, leaderElectionService);

        // Start the first dispatcher and submit the job.
        final CountDownLatch jobGraphRemovalErrorReceived = new CountDownLatch(1);
        final Dispatcher dispatcher =
                createRecoveredDispatcher(
                        throwable -> {
                            final Optional<Error> maybeError =
                                    ExceptionUtils.findThrowable(throwable, Error.class);
                            if (maybeError.isPresent() && temporaryError.equals(maybeError.get())) {
                                jobGraphRemovalErrorReceived.countDown();
                            } else {
                                testingFatalErrorHandlerResource
                                        .getFatalErrorHandler()
                                        .onFatalError(throwable);
                            }
                        });
        toTerminate.add(dispatcher);
        leaderElectionService.isLeader(UUID.randomUUID());
        final DispatcherGateway dispatcherGateway =
                dispatcher.getSelfGateway(DispatcherGateway.class);
        dispatcherGateway.submitJob(jobGraph, TIMEOUT).get();

        // Run vertices, checkpoint and finish.
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
        jobGraphRemovalErrorReceived.await();

        // Remove job master leadership.
        leaderElectionService.notLeader();

        // This will clear internal state of election service, so a new contender can register.
        leaderElectionService.stop();

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
        final Dispatcher secondDispatcher = createRecoveredDispatcher(null);
        toTerminate.add(secondDispatcher);

        // new Dispatcher becomes new leader
        leaderElectionService.isLeader(UUID.randomUUID());

        assertThrows(
                "No JobMaster will be instantiated because of the JobResult is already persisted in the JobResultStore",
                TimeoutException.class,
                () ->
                        connectToLeadingJobMaster(leaderElectionService)
                                .get(100, TimeUnit.MILLISECONDS));
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

    private TestingDispatcher createRecoveredDispatcher(
            @Nullable FatalErrorHandler fatalErrorHandler) throws Exception {
        final List<JobGraph> jobGraphs = new ArrayList<>();
        for (JobID jobId : haServices.getJobGraphStore().getJobIds()) {
            // there shouldn't be an overlap between dirty JobResults and recovered JobGraphs
            if (!haServices.getJobResultStore().hasJobResultEntry(jobId)) {
                jobGraphs.add(haServices.getJobGraphStore().recoverJobGraph(jobId));
            }
        }
        final TestingDispatcher dispatcher =
                createTestingDispatcherBuilder()
                        .setRecoveredJobs(jobGraphs)
                        .setRecoveredDirtyJobs(haServices.getJobResultStore().getDirtyResults())
                        .setFatalErrorHandler(
                                fatalErrorHandler == null
                                        ? testingFatalErrorHandlerResource.getFatalErrorHandler()
                                        : fatalErrorHandler)
                        .build();
        dispatcher.start();
        return dispatcher;
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
