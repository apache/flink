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

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.dispatcher.cleanup.TestingCleanupRunnerFactory;
import org.apache.flink.runtime.dispatcher.cleanup.TestingResourceCleanerFactory;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.heartbeat.HeartbeatServicesImpl;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServicesBuilder;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.jobmaster.TestingJobManagerRunner;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedExecutionGraphBuilder;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.TestingJobResultStore;
import org.apache.flink.runtime.util.TestingFatalErrorHandlerResource;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.concurrent.FutureUtils;

import org.assertj.core.api.Assertions;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

/** Tests for the {@link MiniDispatcher}. */
public class MiniDispatcherTest extends TestLogger {

    private static final Time timeout = Time.seconds(10L);

    @ClassRule public static TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Rule
    public final TestingFatalErrorHandlerResource testingFatalErrorHandlerResource =
            new TestingFatalErrorHandlerResource();

    private static JobGraph jobGraph;

    private static ExecutionGraphInfo executionGraphInfo;

    private static TestingRpcService rpcService;

    private static Configuration configuration;

    private static BlobServer blobServer;

    private final TestingResourceManagerGateway resourceManagerGateway =
            new TestingResourceManagerGateway();

    private final HeartbeatServices heartbeatServices = new HeartbeatServicesImpl(1000L, 1000L);

    private final ExecutionGraphInfoStore executionGraphInfoStore =
            new MemoryExecutionGraphInfoStore();

    private TestingHighAvailabilityServices highAvailabilityServices;

    private TestingJobMasterServiceLeadershipRunnerFactory testingJobManagerRunnerFactory;
    private TestingCleanupRunnerFactory testingCleanupRunnerFactory;

    private CompletableFuture<Void> localCleanupResultFuture;
    private CompletableFuture<Void> globalCleanupResultFuture;

    @BeforeClass
    public static void setupClass() throws IOException {
        jobGraph = JobGraphTestUtils.singleNoOpJobGraph();

        executionGraphInfo =
                new ExecutionGraphInfo(
                        new ArchivedExecutionGraphBuilder()
                                .setJobID(jobGraph.getJobID())
                                .setState(JobStatus.FINISHED)
                                .build());

        rpcService = new TestingRpcService();
        configuration = new Configuration();

        blobServer =
                new BlobServer(configuration, temporaryFolder.newFolder(), new VoidBlobStore());
    }

    @Before
    public void setup() throws Exception {
        highAvailabilityServices = new TestingHighAvailabilityServicesBuilder().build();

        testingJobManagerRunnerFactory = new TestingJobMasterServiceLeadershipRunnerFactory();
        testingCleanupRunnerFactory = new TestingCleanupRunnerFactory();

        // the default setting shouldn't block the cleanup
        localCleanupResultFuture = FutureUtils.completedVoidFuture();
        globalCleanupResultFuture = FutureUtils.completedVoidFuture();
    }

    @AfterClass
    public static void teardownClass()
            throws IOException, InterruptedException, ExecutionException, TimeoutException {
        if (blobServer != null) {
            blobServer.close();
        }

        if (rpcService != null) {
            RpcUtils.terminateRpcService(rpcService);
        }
    }

    /** Tests that the {@link MiniDispatcher} recovers the single job with which it was started. */
    @Test
    public void testSingleJobRecovery() throws Exception {
        final MiniDispatcher miniDispatcher =
                createMiniDispatcher(ClusterEntrypoint.ExecutionMode.DETACHED);

        miniDispatcher.start();

        try {
            final TestingJobManagerRunner testingJobManagerRunner =
                    testingJobManagerRunnerFactory.takeCreatedJobManagerRunner();

            assertThat(testingJobManagerRunner.getJobID(), is(jobGraph.getJobID()));
        } finally {
            RpcUtils.terminateRpcEndpoint(miniDispatcher);
        }
    }

    /** Tests that the {@link MiniDispatcher} recovers the single job with which it was started. */
    @Test
    public void testDirtyJobResultCleanup() throws Exception {
        final JobID jobId = new JobID();
        final MiniDispatcher miniDispatcher =
                createMiniDispatcher(
                        ClusterEntrypoint.ExecutionMode.DETACHED,
                        null,
                        TestingJobResultStore.createSuccessfulJobResult(jobId));

        miniDispatcher.start();

        try {
            final TestingJobManagerRunner testingCleanupRunner =
                    testingCleanupRunnerFactory.takeCreatedJobManagerRunner();
            assertThat(testingCleanupRunner.getJobID(), is(jobId));
        } finally {
            RpcUtils.terminateRpcEndpoint(miniDispatcher);
        }
    }

    /**
     * Tests that in detached mode, the {@link MiniDispatcher} will complete the future that signals
     * job termination.
     */
    @Test
    public void testTerminationAfterJobCompletion() throws Exception {
        globalCleanupResultFuture = new CompletableFuture<>();
        final MiniDispatcher miniDispatcher =
                createMiniDispatcher(ClusterEntrypoint.ExecutionMode.DETACHED);

        miniDispatcher.start();

        try {
            // wait until we have submitted the job
            final TestingJobManagerRunner testingJobManagerRunner =
                    testingJobManagerRunnerFactory.takeCreatedJobManagerRunner();

            testingJobManagerRunner.completeResultFuture(executionGraphInfo);

            CommonTestUtils.waitUntilCondition(
                    () ->
                            !highAvailabilityServices
                                    .getJobResultStore()
                                    .getDirtyResults()
                                    .isEmpty());

            assertFalse(
                    "The shutdownFuture should not be completed before the cleanup is triggered.",
                    miniDispatcher.getShutDownFuture().isDone());

            globalCleanupResultFuture.complete(null);

            miniDispatcher.getShutDownFuture().get();
        } finally {
            // we have to complete the future to make the job and, as a consequence, the
            // MiniDispatcher terminate
            globalCleanupResultFuture.complete(null);
            RpcUtils.terminateRpcEndpoint(miniDispatcher);
        }
    }

    /**
     * Tests that in detached mode, the {@link MiniDispatcher} will not complete the future that
     * signals job termination if the JobStatus is not globally terminal state.
     */
    @Test
    public void testNotTerminationWithoutGloballyTerminalState() throws Exception {
        final MiniDispatcher miniDispatcher =
                createMiniDispatcher(ClusterEntrypoint.ExecutionMode.DETACHED);
        miniDispatcher.start();

        try {
            // wait until we have submitted the job
            final TestingJobManagerRunner testingJobManagerRunner =
                    testingJobManagerRunnerFactory.takeCreatedJobManagerRunner();

            testingJobManagerRunner.completeResultFuture(
                    new ExecutionGraphInfo(
                            new ArchivedExecutionGraphBuilder()
                                    .setJobID(jobGraph.getJobID())
                                    .setState(JobStatus.SUSPENDED)
                                    .build()));

            testingJobManagerRunner.getTerminationFuture().get();
            Assertions.assertThat(miniDispatcher.getShutDownFuture()).isNotDone();
        } finally {
            RpcUtils.terminateRpcEndpoint(miniDispatcher);
        }
    }

    /**
     * Tests that the {@link MiniDispatcher} only terminates in {@link
     * ClusterEntrypoint.ExecutionMode#NORMAL} after it has served the {@link
     * org.apache.flink.runtime.jobmaster.JobResult} once.
     */
    @Test
    public void testJobResultRetrieval() throws Exception {
        final MiniDispatcher miniDispatcher =
                createMiniDispatcher(ClusterEntrypoint.ExecutionMode.NORMAL);

        miniDispatcher.start();

        try {
            // wait until we have submitted the job
            final TestingJobManagerRunner testingJobManagerRunner =
                    testingJobManagerRunnerFactory.takeCreatedJobManagerRunner();

            testingJobManagerRunner.completeResultFuture(executionGraphInfo);

            assertFalse(miniDispatcher.getTerminationFuture().isDone());

            final DispatcherGateway dispatcherGateway =
                    miniDispatcher.getSelfGateway(DispatcherGateway.class);

            final CompletableFuture<JobResult> jobResultFuture =
                    dispatcherGateway.requestJobResult(jobGraph.getJobID(), timeout);

            final JobResult jobResult = jobResultFuture.get();

            assertThat(jobResult.getJobId(), is(jobGraph.getJobID()));
        } finally {
            RpcUtils.terminateRpcEndpoint(miniDispatcher);
        }
    }

    @Test
    public void testShutdownIfJobCancelledInNormalMode() throws Exception {
        final MiniDispatcher miniDispatcher =
                createMiniDispatcher(ClusterEntrypoint.ExecutionMode.NORMAL);
        miniDispatcher.start();

        try {
            // wait until we have submitted the job
            final TestingJobManagerRunner testingJobManagerRunner =
                    testingJobManagerRunnerFactory.takeCreatedJobManagerRunner();

            assertFalse(miniDispatcher.getTerminationFuture().isDone());

            final DispatcherGateway dispatcherGateway =
                    miniDispatcher.getSelfGateway(DispatcherGateway.class);

            dispatcherGateway.cancelJob(jobGraph.getJobID(), Time.seconds(10L));
            testingJobManagerRunner.completeResultFuture(
                    new ExecutionGraphInfo(
                            new ArchivedExecutionGraphBuilder()
                                    .setJobID(jobGraph.getJobID())
                                    .setState(JobStatus.CANCELED)
                                    .build()));

            ApplicationStatus applicationStatus = miniDispatcher.getShutDownFuture().get();
            assertThat(applicationStatus, is(ApplicationStatus.CANCELED));
        } finally {
            RpcUtils.terminateRpcEndpoint(miniDispatcher);
        }
    }

    // --------------------------------------------------------
    // Utilities
    // --------------------------------------------------------

    private MiniDispatcher createMiniDispatcher(ClusterEntrypoint.ExecutionMode executionMode)
            throws Exception {
        return createMiniDispatcher(executionMode, jobGraph, null);
    }

    private MiniDispatcher createMiniDispatcher(
            ClusterEntrypoint.ExecutionMode executionMode,
            @Nullable JobGraph recoveredJobGraph,
            @Nullable JobResult recoveredDirtyJob)
            throws Exception {
        final JobManagerRunnerRegistry jobManagerRunnerRegistry =
                new DefaultJobManagerRunnerRegistry(2);
        return new MiniDispatcher(
                rpcService,
                DispatcherId.generate(),
                new DispatcherServices(
                        configuration,
                        highAvailabilityServices,
                        () -> CompletableFuture.completedFuture(resourceManagerGateway),
                        blobServer,
                        heartbeatServices,
                        executionGraphInfoStore,
                        testingFatalErrorHandlerResource.getFatalErrorHandler(),
                        VoidHistoryServerArchivist.INSTANCE,
                        null,
                        new DispatcherOperationCaches(),
                        UnregisteredMetricGroups.createUnregisteredJobManagerMetricGroup(),
                        highAvailabilityServices.getJobGraphStore(),
                        highAvailabilityServices.getJobResultStore(),
                        testingJobManagerRunnerFactory,
                        testingCleanupRunnerFactory,
                        ForkJoinPool.commonPool(),
                        Collections.emptySet()),
                recoveredJobGraph,
                recoveredDirtyJob,
                (dispatcher, scheduledExecutor, errorHandler) -> new NoOpDispatcherBootstrap(),
                jobManagerRunnerRegistry,
                TestingResourceCleanerFactory.builder()
                        // JobManagerRunnerRegistry needs to be added explicitly
                        // because cleaning it will trigger the closeAsync latch
                        // provided by TestingJobManagerRunner
                        .withLocallyCleanableResource(jobManagerRunnerRegistry)
                        .withGloballyCleanableResource(
                                (jobId, ignoredExecutor) -> globalCleanupResultFuture)
                        .withLocallyCleanableResource(
                                (jobId, ignoredExecutor) -> localCleanupResultFuture)
                        .build(),
                executionMode);
    }
}
