/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.dispatcher.runner;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.testutils.FlinkAssertions;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.client.DuplicateJobSubmissionException;
import org.apache.flink.runtime.client.JobSubmissionException;
import org.apache.flink.runtime.highavailability.JobResultStore;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobmanager.ExecutionPlanStore;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.testutils.TestingExecutionPlanStore;
import org.apache.flink.runtime.testutils.TestingJobResultStore;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.runtime.webmonitor.TestingDispatcherGateway;
import org.apache.flink.streaming.api.graph.ExecutionPlan;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLoggerExtension;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.ThrowingConsumer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.apache.flink.core.testutils.FlinkAssertions.STREAM_THROWABLE;
import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.apache.flink.core.testutils.FlinkAssertions.assertThatFuture;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link SessionDispatcherLeaderProcess}. */
@ExtendWith(TestLoggerExtension.class)
class SessionDispatcherLeaderProcessTest {

    private static final JobGraph JOB_GRAPH = JobGraphTestUtils.emptyJobGraph();

    private static ExecutorService ioExecutor;

    private final UUID leaderSessionId = UUID.randomUUID();

    private TestingFatalErrorHandler fatalErrorHandler;

    private ExecutionPlanStore executionPlanStore;
    private JobResultStore jobResultStore;

    private AbstractDispatcherLeaderProcess.DispatcherGatewayServiceFactory
            dispatcherServiceFactory;

    @BeforeAll
    static void setupClass() {
        ioExecutor = Executors.newSingleThreadExecutor();
    }

    @BeforeEach
    void setup() {
        fatalErrorHandler = new TestingFatalErrorHandler();
        executionPlanStore = TestingExecutionPlanStore.newBuilder().build();
        jobResultStore = TestingJobResultStore.builder().build();
        dispatcherServiceFactory =
                createFactoryBasedOnGenericSupplier(
                        () -> TestingDispatcherGatewayService.newBuilder().build());
    }

    @AfterEach
    void teardown() throws Exception {
        if (fatalErrorHandler != null) {
            fatalErrorHandler.rethrowError();
            fatalErrorHandler = null;
        }
    }

    @AfterAll
    static void teardownClass() {
        if (ioExecutor != null) {
            ExecutorUtils.gracefulShutdown(5L, TimeUnit.SECONDS, ioExecutor);
        }
    }

    @Test
    void start_afterClose_doesNotHaveAnEffect() throws Exception {
        final SessionDispatcherLeaderProcess dispatcherLeaderProcess =
                createDispatcherLeaderProcess();

        dispatcherLeaderProcess.close();
        dispatcherLeaderProcess.start();

        assertThat(dispatcherLeaderProcess.getState())
                .isEqualTo(SessionDispatcherLeaderProcess.State.STOPPED);
    }

    @Test
    void testStartTriggeringDispatcherServiceCreation() throws Exception {
        dispatcherServiceFactory =
                createFactoryBasedOnGenericSupplier(
                        () -> TestingDispatcherGatewayService.newBuilder().build());

        try (final SessionDispatcherLeaderProcess dispatcherLeaderProcess =
                createDispatcherLeaderProcess()) {
            dispatcherLeaderProcess.start();
            assertThat(dispatcherLeaderProcess.getState())
                    .isEqualTo(SessionDispatcherLeaderProcess.State.RUNNING);
        }
    }

    @Test
    void testRecoveryWithExecutionPlanButNoDirtyJobResult() throws Exception {
        testJobRecovery(
                Collections.singleton(JOB_GRAPH),
                Collections.emptySet(),
                actualRecoveredExecutionPlans ->
                        assertThat(actualRecoveredExecutionPlans)
                                .singleElement()
                                .isEqualTo(JOB_GRAPH),
                actualRecoveredDirtyJobResults ->
                        assertThat(actualRecoveredDirtyJobResults).isEmpty());
    }

    @Test
    void testRecoveryWithExecutionPlanAndMatchingDirtyJobResult() throws Exception {
        final JobResult matchingDirtyJobResult =
                TestingJobResultStore.createSuccessfulJobResult(JOB_GRAPH.getJobID());

        testJobRecovery(
                Collections.singleton(JOB_GRAPH),
                Collections.singleton(matchingDirtyJobResult),
                actualRecoveredExecutionPlans ->
                        assertThat(actualRecoveredExecutionPlans).isEmpty(),
                actualRecoveredDirtyJobResults ->
                        assertThat(actualRecoveredDirtyJobResults)
                                .singleElement()
                                .isEqualTo(matchingDirtyJobResult));
    }

    @Test
    void testRecoveryWithMultipleExecutionPlansAndOneMatchingDirtyJobResult() throws Exception {
        final JobResult matchingDirtyJobResult =
                TestingJobResultStore.createSuccessfulJobResult(JOB_GRAPH.getJobID());
        final ExecutionPlan otherExecutionPlan = JobGraphTestUtils.emptyJobGraph();

        testJobRecovery(
                Arrays.asList(otherExecutionPlan, JOB_GRAPH),
                Collections.singleton(matchingDirtyJobResult),
                actualRecoveredExecutionPlans ->
                        assertThat(actualRecoveredExecutionPlans)
                                .singleElement()
                                .isEqualTo(otherExecutionPlan),
                actualRecoveredDirtyJobResults ->
                        assertThat(actualRecoveredDirtyJobResults)
                                .singleElement()
                                .isEqualTo(matchingDirtyJobResult));
    }

    @Test
    void testRecoveryWithoutExecutionPlanButDirtyJobResult() throws Exception {
        final JobResult dirtyJobResult =
                TestingJobResultStore.createSuccessfulJobResult(new JobID());

        testJobRecovery(
                Collections.emptyList(),
                Collections.singleton(dirtyJobResult),
                actualRecoveredExecutionPlans ->
                        assertThat(actualRecoveredExecutionPlans).isEmpty(),
                actualRecoveredDirtyJobResults ->
                        assertThat(actualRecoveredDirtyJobResults)
                                .singleElement()
                                .isEqualTo(dirtyJobResult));
    }

    private void testJobRecovery(
            Collection<ExecutionPlan> executionPlansToRecover,
            Set<JobResult> dirtyJobResults,
            Consumer<Collection<ExecutionPlan>> recoveredExecutionPlanAssertion,
            Consumer<Collection<JobResult>> recoveredDirtyJobResultAssertion)
            throws Exception {
        executionPlanStore =
                TestingExecutionPlanStore.newBuilder()
                        .setInitialExecutionPlans(executionPlansToRecover)
                        .build();

        jobResultStore =
                TestingJobResultStore.builder()
                        .withGetDirtyResultsSupplier(() -> dirtyJobResults)
                        .build();

        final CompletableFuture<Collection<ExecutionPlan>> recoveredExecutionPlansFuture =
                new CompletableFuture<>();
        final CompletableFuture<Collection<JobResult>> recoveredDirtyJobResultsFuture =
                new CompletableFuture<>();
        dispatcherServiceFactory =
                (ignoredDispatcherId,
                        recoveredJobs,
                        recoveredDirtyJobResults,
                        ignoredExecutionPlanWriter,
                        ignoredJobResultStore) -> {
                    recoveredExecutionPlansFuture.complete(recoveredJobs);
                    recoveredDirtyJobResultsFuture.complete(recoveredDirtyJobResults);
                    return TestingDispatcherGatewayService.newBuilder().build();
                };

        try (final SessionDispatcherLeaderProcess dispatcherLeaderProcess =
                createDispatcherLeaderProcess()) {
            dispatcherLeaderProcess.start();

            recoveredExecutionPlanAssertion.accept(recoveredExecutionPlansFuture.get());
            recoveredDirtyJobResultAssertion.accept(recoveredDirtyJobResultsFuture.get());
        }
    }

    @Test
    void testRecoveryWhileExecutionPlanRecoveryIsScheduledConcurrently() throws Exception {
        final JobResult dirtyJobResult =
                TestingJobResultStore.createSuccessfulJobResult(new JobID());

        OneShotLatch recoveryInitiatedLatch = new OneShotLatch();
        OneShotLatch jobGraphAddedLatch = new OneShotLatch();

        executionPlanStore =
                TestingExecutionPlanStore.newBuilder()
                        // mimic behavior when recovering a ExecutionPlan that is marked for
                        // deletion
                        .setRecoverExecutionPlanFunction((jobId, jobs) -> null)
                        .build();

        jobResultStore =
                TestingJobResultStore.builder()
                        .withGetDirtyResultsSupplier(
                                () -> {
                                    recoveryInitiatedLatch.trigger();
                                    try {
                                        jobGraphAddedLatch.await();
                                    } catch (InterruptedException e) {
                                        Thread.currentThread().interrupt();
                                    }
                                    return Collections.singleton(dirtyJobResult);
                                })
                        .build();

        final CompletableFuture<Collection<ExecutionPlan>> recoveredExecutionPlansFuture =
                new CompletableFuture<>();
        final CompletableFuture<Collection<JobResult>> recoveredDirtyJobResultsFuture =
                new CompletableFuture<>();
        dispatcherServiceFactory =
                (ignoredDispatcherId,
                        recoveredJobs,
                        recoveredDirtyJobResults,
                        ignoredExecutionPlanWriter,
                        ignoredJobResultStore) -> {
                    recoveredExecutionPlansFuture.complete(recoveredJobs);
                    recoveredDirtyJobResultsFuture.complete(recoveredDirtyJobResults);
                    return TestingDispatcherGatewayService.newBuilder().build();
                };

        try (final SessionDispatcherLeaderProcess dispatcherLeaderProcess =
                createDispatcherLeaderProcess()) {
            dispatcherLeaderProcess.start();

            // start returns without the initial recovery being completed
            // mimic ZK message about an added jobgraph while the recovery is ongoing
            recoveryInitiatedLatch.await();
            dispatcherLeaderProcess.onAddedExecutionPlan(dirtyJobResult.getJobId());
            jobGraphAddedLatch.trigger();

            assertThatFuture(recoveredExecutionPlansFuture)
                    .eventuallySucceeds()
                    .satisfies(
                            recoverExecutionPlans -> assertThat(recoverExecutionPlans).isEmpty());
            assertThatFuture(recoveredDirtyJobResultsFuture)
                    .eventuallySucceeds()
                    .satisfies(
                            recoveredDirtyJobResults ->
                                    assertThat(recoveredDirtyJobResults)
                                            .containsExactly(dirtyJobResult));
        }
    }

    @Test
    void closeAsync_stopsExecutionPlanStoreAndDispatcher() throws Exception {
        final CompletableFuture<Void> jobGraphStopFuture = new CompletableFuture<>();
        executionPlanStore =
                TestingExecutionPlanStore.newBuilder()
                        .setStopRunnable(() -> jobGraphStopFuture.complete(null))
                        .build();

        final CompletableFuture<Void> dispatcherServiceTerminationFuture =
                new CompletableFuture<>();
        dispatcherServiceFactory =
                createFactoryBasedOnGenericSupplier(
                        () ->
                                TestingDispatcherGatewayService.newBuilder()
                                        .setTerminationFuture(dispatcherServiceTerminationFuture)
                                        .withManualTerminationFutureCompletion()
                                        .build());

        try (final SessionDispatcherLeaderProcess dispatcherLeaderProcess =
                createDispatcherLeaderProcess()) {
            dispatcherLeaderProcess.start();

            // wait for the creation of the DispatcherGatewayService
            dispatcherLeaderProcess.getDispatcherGateway().get();

            final CompletableFuture<Void> terminationFuture = dispatcherLeaderProcess.closeAsync();

            assertThat(jobGraphStopFuture).isNotDone();
            assertThat(terminationFuture).isNotDone();

            dispatcherServiceTerminationFuture.complete(null);

            // verify that we shut down the ExecutionPlanStore
            jobGraphStopFuture.get();

            // verify that we completed the dispatcher leader process shut down
            terminationFuture.get();
        }
    }

    @Test
    void unexpectedDispatcherServiceTerminationWhileRunning_callsFatalErrorHandler() {
        final CompletableFuture<Void> terminationFuture = new CompletableFuture<>();
        dispatcherServiceFactory =
                createFactoryBasedOnGenericSupplier(
                        () ->
                                TestingDispatcherGatewayService.newBuilder()
                                        .setTerminationFuture(terminationFuture)
                                        .build());

        final SessionDispatcherLeaderProcess dispatcherLeaderProcess =
                createDispatcherLeaderProcess();
        dispatcherLeaderProcess.start();

        final FlinkException expectedFailure = new FlinkException("Expected test failure.");
        terminationFuture.completeExceptionally(expectedFailure);

        final Throwable error = fatalErrorHandler.getErrorFuture().join();
        assertThat(error).rootCause().isEqualTo(expectedFailure);

        fatalErrorHandler.clearError();
    }

    @Test
    void unexpectedDispatcherServiceTerminationWhileNotRunning_doesNotCallFatalErrorHandler() {
        final CompletableFuture<Void> terminationFuture = new CompletableFuture<>();
        dispatcherServiceFactory =
                createFactoryBasedOnGenericSupplier(
                        () ->
                                TestingDispatcherGatewayService.newBuilder()
                                        .setTerminationFuture(terminationFuture)
                                        .withManualTerminationFutureCompletion()
                                        .build());
        final SessionDispatcherLeaderProcess dispatcherLeaderProcess =
                createDispatcherLeaderProcess();
        dispatcherLeaderProcess.start();

        dispatcherLeaderProcess.closeAsync();

        final FlinkException expectedFailure = new FlinkException("Expected test failure.");
        terminationFuture.completeExceptionally(expectedFailure);

        assertThatThrownBy(() -> fatalErrorHandler.getErrorFuture().get(10, TimeUnit.MILLISECONDS))
                .isInstanceOf(TimeoutException.class);
    }

    @Test
    void confirmLeaderSessionFuture_completesAfterDispatcherServiceHasBeenStarted()
            throws Exception {
        final OneShotLatch createDispatcherServiceLatch = new OneShotLatch();
        final String dispatcherAddress = "myAddress";
        final TestingDispatcherGateway dispatcherGateway =
                TestingDispatcherGateway.newBuilder().setAddress(dispatcherAddress).build();

        dispatcherServiceFactory =
                createFactoryBasedOnGenericSupplier(
                        () -> {
                            try {
                                createDispatcherServiceLatch.await();
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                            return TestingDispatcherGatewayService.newBuilder()
                                    .setDispatcherGateway(dispatcherGateway)
                                    .build();
                        });

        try (final SessionDispatcherLeaderProcess dispatcherLeaderProcess =
                createDispatcherLeaderProcess()) {
            final CompletableFuture<String> confirmLeaderSessionFuture =
                    dispatcherLeaderProcess.getLeaderAddressFuture();

            dispatcherLeaderProcess.start();

            assertThat(confirmLeaderSessionFuture).isNotDone();

            createDispatcherServiceLatch.trigger();

            assertThatFuture(confirmLeaderSessionFuture)
                    .eventuallySucceeds()
                    .isEqualTo(dispatcherAddress);
        }
    }

    @Test
    void closeAsync_duringJobRecovery_preventsDispatcherServiceCreation() throws Exception {
        final OneShotLatch jobRecoveryStartedLatch = new OneShotLatch();
        final OneShotLatch completeJobRecoveryLatch = new OneShotLatch();
        final OneShotLatch createDispatcherServiceLatch = new OneShotLatch();

        this.executionPlanStore =
                TestingExecutionPlanStore.newBuilder()
                        .setJobIdsFunction(
                                storedJobs -> {
                                    jobRecoveryStartedLatch.trigger();
                                    completeJobRecoveryLatch.await();
                                    return storedJobs;
                                })
                        .build();

        this.dispatcherServiceFactory =
                createFactoryBasedOnGenericSupplier(
                        () -> {
                            createDispatcherServiceLatch.trigger();
                            return TestingDispatcherGatewayService.newBuilder().build();
                        });

        try (final SessionDispatcherLeaderProcess dispatcherLeaderProcess =
                createDispatcherLeaderProcess()) {
            dispatcherLeaderProcess.start();

            jobRecoveryStartedLatch.await();

            dispatcherLeaderProcess.closeAsync();

            completeJobRecoveryLatch.trigger();

            assertThatThrownBy(
                            () -> createDispatcherServiceLatch.await(10L, TimeUnit.MILLISECONDS),
                            "No dispatcher service should be created after the process has been stopped.")
                    .isInstanceOf(TimeoutException.class);
        }
    }

    @Test
    void onRemovedExecutionPlan_terminatesRunningJob() throws Exception {
        executionPlanStore =
                TestingExecutionPlanStore.newBuilder()
                        .setInitialExecutionPlans(Collections.singleton(JOB_GRAPH))
                        .build();

        final CompletableFuture<JobID> terminateJobFuture = new CompletableFuture<>();
        final TestingDispatcherGatewayService testingDispatcherService =
                TestingDispatcherGatewayService.newBuilder()
                        .setOnRemovedJobGraphFunction(
                                jobID -> {
                                    terminateJobFuture.complete(jobID);
                                    return FutureUtils.completedVoidFuture();
                                })
                        .build();

        dispatcherServiceFactory =
                createFactoryBasedOnGenericSupplier(() -> testingDispatcherService);

        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        try (final SessionDispatcherLeaderProcess dispatcherLeaderProcess =
                createDispatcherLeaderProcess()) {
            dispatcherLeaderProcess.start();

            // wait for the dispatcher process to be created
            dispatcherLeaderProcess.getDispatcherGateway().get();

            // now remove the Job from the ExecutionPlanStore and notify the dispatcher service
            executionPlanStore.globalCleanupAsync(JOB_GRAPH.getJobID(), executorService).join();
            dispatcherLeaderProcess.onRemovedExecutionPlan(JOB_GRAPH.getJobID());

            assertThat(terminateJobFuture.get()).isEqualTo(JOB_GRAPH.getJobID());
        } finally {
            assertThat(executorService.shutdownNow()).isEmpty();
        }
    }

    @Test
    void onRemovedExecutionPlan_failingRemovalCall_failsFatally() throws Exception {
        final FlinkException testException = new FlinkException("Test exception");

        final TestingDispatcherGatewayService testingDispatcherService =
                TestingDispatcherGatewayService.newBuilder()
                        .setOnRemovedJobGraphFunction(
                                jobID -> FutureUtils.completedExceptionally(testException))
                        .build();

        dispatcherServiceFactory =
                createFactoryBasedOnGenericSupplier(() -> testingDispatcherService);

        try (final SessionDispatcherLeaderProcess dispatcherLeaderProcess =
                createDispatcherLeaderProcess()) {
            dispatcherLeaderProcess.start();

            // wait for the dispatcher process to be created
            dispatcherLeaderProcess.getDispatcherGateway().get();

            // now notify the dispatcher service
            dispatcherLeaderProcess.onRemovedExecutionPlan(JOB_GRAPH.getJobID());

            final Throwable fatalError = fatalErrorHandler.getErrorFuture().join();

            assertThat(fatalError).hasCause(testException);

            fatalErrorHandler.clearError();
        }
    }

    @Test
    void onAddedExecutionPlan_submitsRecoveredJob() throws Exception {
        final CompletableFuture<ExecutionPlan> submittedJobFuture = new CompletableFuture<>();
        final TestingDispatcherGateway testingDispatcherGateway =
                TestingDispatcherGateway.newBuilder()
                        .setSubmitFunction(
                                submittedJob -> {
                                    submittedJobFuture.complete(submittedJob);
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                })
                        .build();

        dispatcherServiceFactory =
                createFactoryBasedOnGenericSupplier(
                        () ->
                                TestingDispatcherGatewayService.newBuilder()
                                        .setDispatcherGateway(testingDispatcherGateway)
                                        .build());

        try (final SessionDispatcherLeaderProcess dispatcherLeaderProcess =
                createDispatcherLeaderProcess()) {
            dispatcherLeaderProcess.start();

            // wait first for the dispatcher service to be created
            dispatcherLeaderProcess.getDispatcherGateway().get();

            executionPlanStore.putExecutionPlan(JOB_GRAPH);
            dispatcherLeaderProcess.onAddedExecutionPlan(JOB_GRAPH.getJobID());

            final ExecutionPlan submittedExecutionPlan = submittedJobFuture.get();

            assertThat(submittedExecutionPlan.getJobID()).isEqualTo(JOB_GRAPH.getJobID());
        }
    }

    @Test
    void onAddedExecutionPlan_ifNotRunning_isBeingIgnored() throws Exception {
        final CompletableFuture<JobID> recoveredJobFuture = new CompletableFuture<>();
        executionPlanStore =
                TestingExecutionPlanStore.newBuilder()
                        .setRecoverExecutionPlanFunction(
                                (jobId, jobGraphs) -> {
                                    recoveredJobFuture.complete(jobId);
                                    return jobGraphs.get(jobId);
                                })
                        .build();

        try (final SessionDispatcherLeaderProcess dispatcherLeaderProcess =
                createDispatcherLeaderProcess()) {
            dispatcherLeaderProcess.start();

            // wait until the process has started the dispatcher
            dispatcherLeaderProcess.getDispatcherGateway().get();

            // now add the job graph
            executionPlanStore.putExecutionPlan(JOB_GRAPH);

            dispatcherLeaderProcess.closeAsync();

            dispatcherLeaderProcess.onAddedExecutionPlan(JOB_GRAPH.getJobID());

            assertThatThrownBy(
                            () -> recoveredJobFuture.get(10L, TimeUnit.MILLISECONDS),
                            "onAddedExecutionPlan should be ignored if the leader process is not running.")
                    .isInstanceOf(TimeoutException.class);
        }
    }

    @Test
    void onAddedExecutionPlan_failingRecovery_propagatesTheFailure() throws Exception {
        final FlinkException expectedFailure = new FlinkException("Expected failure");
        executionPlanStore =
                TestingExecutionPlanStore.newBuilder()
                        .setRecoverExecutionPlanFunction(
                                (ignoredA, ignoredB) -> {
                                    throw expectedFailure;
                                })
                        .build();

        try (final SessionDispatcherLeaderProcess dispatcherLeaderProcess =
                createDispatcherLeaderProcess()) {
            dispatcherLeaderProcess.start();

            // wait first for the dispatcher service to be created
            dispatcherLeaderProcess.getDispatcherGateway().get();

            executionPlanStore.putExecutionPlan(JOB_GRAPH);
            dispatcherLeaderProcess.onAddedExecutionPlan(JOB_GRAPH.getJobID());

            assertThatFuture(fatalErrorHandler.getErrorFuture())
                    .eventuallySucceeds()
                    .extracting(FlinkAssertions::chainOfCauses, STREAM_THROWABLE)
                    .contains(expectedFailure);

            assertThat(dispatcherLeaderProcess.getState())
                    .isEqualTo(SessionDispatcherLeaderProcess.State.STOPPED);

            fatalErrorHandler.clearError();
        }
    }

    @Test
    void recoverJobs_withRecoveryFailure_failsFatally() throws Exception {
        final FlinkException testException = new FlinkException("Test exception");
        executionPlanStore =
                TestingExecutionPlanStore.newBuilder()
                        .setRecoverExecutionPlanFunction(
                                (ignoredA, ignoredB) -> {
                                    throw testException;
                                })
                        .setInitialExecutionPlans(Collections.singleton(JOB_GRAPH))
                        .build();

        runJobRecoveryFailureTest(testException);
    }

    @Test
    void recoverJobs_withJobIdRecoveryFailure_failsFatally() throws Exception {
        final FlinkException testException = new FlinkException("Test exception");
        executionPlanStore =
                TestingExecutionPlanStore.newBuilder()
                        .setJobIdsFunction(
                                ignored -> {
                                    throw testException;
                                })
                        .build();

        runJobRecoveryFailureTest(testException);
    }

    private void runJobRecoveryFailureTest(FlinkException testException) throws Exception {
        try (final SessionDispatcherLeaderProcess dispatcherLeaderProcess =
                createDispatcherLeaderProcess()) {
            dispatcherLeaderProcess.start();

            // we expect that a fatal error occurred
            assertThatFuture(fatalErrorHandler.getErrorFuture())
                    .eventuallySucceeds()
                    .satisfies(
                            error ->
                                    assertThat(error)
                                            .satisfies(
                                                    anyCauseMatches(
                                                            testException.getClass(),
                                                            testException.getMessage())));

            fatalErrorHandler.clearError();
        }
    }

    @Test
    void onAddedExecutionPlan_failingRecoveredJobSubmission_failsFatally() throws Exception {
        final TestingDispatcherGateway dispatcherGateway =
                TestingDispatcherGateway.newBuilder()
                        .setSubmitFunction(
                                jobGraph ->
                                        FutureUtils.completedExceptionally(
                                                new JobSubmissionException(
                                                        jobGraph.getJobID(), "test exception")))
                        .build();

        runOnAddedExecutionPlanTest(
                dispatcherGateway, this::verifyOnAddedExecutionPlanResultFailsFatally);
    }

    private void verifyOnAddedExecutionPlanResultFailsFatally(
            TestingFatalErrorHandler fatalErrorHandler) {
        final Throwable actualCause = fatalErrorHandler.getErrorFuture().join();

        assertThat(actualCause)
                .extracting(FlinkAssertions::chainOfCauses, FlinkAssertions.STREAM_THROWABLE)
                .hasAtLeastOneElementOfType(JobSubmissionException.class);

        fatalErrorHandler.clearError();
    }

    @Test
    void onAddedExecutionPlan_duplicateJobSubmissionDueToFalsePositive_willBeIgnored()
            throws Exception {
        final TestingDispatcherGateway dispatcherGateway =
                TestingDispatcherGateway.newBuilder()
                        .setSubmitFunction(
                                jobGraph ->
                                        FutureUtils.completedExceptionally(
                                                DuplicateJobSubmissionException.of(
                                                        jobGraph.getJobID())))
                        .build();

        runOnAddedExecutionPlanTest(
                dispatcherGateway, this::verifyOnAddedExecutionPlanResultDidNotFail);
    }

    private void runOnAddedExecutionPlanTest(
            TestingDispatcherGateway dispatcherGateway,
            ThrowingConsumer<TestingFatalErrorHandler, Exception> verificationLogic)
            throws Exception {
        executionPlanStore =
                TestingExecutionPlanStore.newBuilder()
                        .setInitialExecutionPlans(Collections.singleton(JOB_GRAPH))
                        .build();
        dispatcherServiceFactory =
                createFactoryBasedOnExecutionPlans(
                        jobGraphs -> {
                            assertThat(jobGraphs).containsExactlyInAnyOrder(JOB_GRAPH);

                            return TestingDispatcherGatewayService.newBuilder()
                                    .setDispatcherGateway(dispatcherGateway)
                                    .build();
                        });

        try (final SessionDispatcherLeaderProcess dispatcherLeaderProcess =
                createDispatcherLeaderProcess()) {
            dispatcherLeaderProcess.start();

            dispatcherLeaderProcess.getDispatcherGateway().get();

            dispatcherLeaderProcess.onAddedExecutionPlan(JOB_GRAPH.getJobID());

            verificationLogic.accept(fatalErrorHandler);
        }
    }

    private AbstractDispatcherLeaderProcess.DispatcherGatewayServiceFactory
            createFactoryBasedOnExecutionPlans(
                    Function<
                                    Collection<ExecutionPlan>,
                                    AbstractDispatcherLeaderProcess.DispatcherGatewayService>
                            createFunction) {
        return (ignoredDispatcherId,
                recoveredJobs,
                ignoredRecoveredDirtyJobResults,
                ignoredExecutionPlanWriter,
                ignoredJobResultStore) -> createFunction.apply(recoveredJobs);
    }

    private AbstractDispatcherLeaderProcess.DispatcherGatewayServiceFactory
            createFactoryBasedOnGenericSupplier(
                    Supplier<AbstractDispatcherLeaderProcess.DispatcherGatewayService> supplier) {
        return (ignoredDispatcherId,
                ignoredRecoveredJobs,
                ignoredRecoveredDirtyJobResults,
                ignoredExecutionPlanWriter,
                ignoredJobResultStore) -> supplier.get();
    }

    private void verifyOnAddedExecutionPlanResultDidNotFail(
            TestingFatalErrorHandler fatalErrorHandler) {
        assertThatThrownBy(
                        () -> fatalErrorHandler.getErrorFuture().get(10L, TimeUnit.MILLISECONDS),
                        "Expected that duplicate job submissions due to false job recoveries are ignored.")
                .isInstanceOf(TimeoutException.class);
    }

    private SessionDispatcherLeaderProcess createDispatcherLeaderProcess() {
        return SessionDispatcherLeaderProcess.create(
                leaderSessionId,
                dispatcherServiceFactory,
                executionPlanStore,
                jobResultStore,
                ioExecutor,
                fatalErrorHandler);
    }
}
