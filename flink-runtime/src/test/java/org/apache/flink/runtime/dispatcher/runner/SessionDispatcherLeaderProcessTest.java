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
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.client.DuplicateJobSubmissionException;
import org.apache.flink.runtime.client.JobSubmissionException;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobmanager.JobGraphStore;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.testutils.TestingJobGraphStore;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.runtime.webmonitor.TestingDispatcherGateway;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.ThrowingConsumer;
import org.apache.flink.util.function.TriFunctionWithException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.core.testutils.FlinkMatchers.containsCause;
import static org.apache.flink.core.testutils.FlinkMatchers.willNotComplete;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests for the {@link SessionDispatcherLeaderProcess}. */
public class SessionDispatcherLeaderProcessTest extends TestLogger {

    private static final JobGraph JOB_GRAPH = JobGraphTestUtils.emptyJobGraph();

    private static ExecutorService ioExecutor;

    private final UUID leaderSessionId = UUID.randomUUID();

    private TestingFatalErrorHandler fatalErrorHandler;

    private JobGraphStore jobGraphStore;

    private TestingDispatcherServiceFactory dispatcherServiceFactory;

    @BeforeClass
    public static void setupClass() {
        ioExecutor = Executors.newSingleThreadExecutor();
    }

    @Before
    public void setup() {
        fatalErrorHandler = new TestingFatalErrorHandler();
        jobGraphStore = TestingJobGraphStore.newBuilder().build();
        dispatcherServiceFactory = TestingDispatcherServiceFactory.newBuilder().build();
    }

    @After
    public void teardown() throws Exception {
        if (fatalErrorHandler != null) {
            fatalErrorHandler.rethrowError();
            fatalErrorHandler = null;
        }
    }

    @AfterClass
    public static void teardownClass() {
        if (ioExecutor != null) {
            ExecutorUtils.gracefulShutdown(5L, TimeUnit.SECONDS, ioExecutor);
        }
    }

    @Test
    public void start_afterClose_doesNotHaveAnEffect() throws Exception {
        final SessionDispatcherLeaderProcess dispatcherLeaderProcess =
                createDispatcherLeaderProcess();

        dispatcherLeaderProcess.close();
        dispatcherLeaderProcess.start();

        assertThat(
                dispatcherLeaderProcess.getState(),
                is(SessionDispatcherLeaderProcess.State.STOPPED));
    }

    @Test
    public void start_triggersJobGraphRecoveryAndDispatcherServiceCreation() throws Exception {
        jobGraphStore =
                TestingJobGraphStore.newBuilder()
                        .setInitialJobGraphs(Collections.singleton(JOB_GRAPH))
                        .build();

        final CompletableFuture<Collection<JobGraph>> recoveredJobGraphsFuture =
                new CompletableFuture<>();
        dispatcherServiceFactory =
                TestingDispatcherServiceFactory.newBuilder()
                        .setCreateFunction(
                                (fencingToken, recoveredJobGraphs, jobGraphStore) -> {
                                    recoveredJobGraphsFuture.complete(recoveredJobGraphs);
                                    return TestingDispatcherGatewayService.newBuilder().build();
                                })
                        .build();

        try (final SessionDispatcherLeaderProcess dispatcherLeaderProcess =
                createDispatcherLeaderProcess()) {
            dispatcherLeaderProcess.start();
            assertThat(
                    dispatcherLeaderProcess.getState(),
                    is(SessionDispatcherLeaderProcess.State.RUNNING));

            final Collection<JobGraph> recoveredJobGraphs = recoveredJobGraphsFuture.get();

            assertThat(recoveredJobGraphs, containsInAnyOrder(JOB_GRAPH));
        }
    }

    @Test
    public void closeAsync_stopsJobGraphStoreAndDispatcher() throws Exception {
        final CompletableFuture<Void> jobGraphStopFuture = new CompletableFuture<>();
        jobGraphStore =
                TestingJobGraphStore.newBuilder()
                        .setStopRunnable(() -> jobGraphStopFuture.complete(null))
                        .build();

        final CompletableFuture<Void> dispatcherServiceTerminationFuture =
                new CompletableFuture<>();
        dispatcherServiceFactory =
                TestingDispatcherServiceFactory.newBuilder()
                        .setCreateFunction(
                                (ignoredA, ignoredB, ignoredC) ->
                                        TestingDispatcherGatewayService.newBuilder()
                                                .setTerminationFuture(
                                                        dispatcherServiceTerminationFuture)
                                                .withManualTerminationFutureCompletion()
                                                .build())
                        .build();

        try (final SessionDispatcherLeaderProcess dispatcherLeaderProcess =
                createDispatcherLeaderProcess()) {
            dispatcherLeaderProcess.start();

            // wait for the creation of the DispatcherGatewayService
            dispatcherLeaderProcess.getDispatcherGateway().get();

            final CompletableFuture<Void> terminationFuture = dispatcherLeaderProcess.closeAsync();

            assertThat(jobGraphStopFuture.isDone(), is(false));
            assertThat(terminationFuture.isDone(), is(false));

            dispatcherServiceTerminationFuture.complete(null);

            // verify that we shut down the JobGraphStore
            jobGraphStopFuture.get();

            // verify that we completed the dispatcher leader process shut down
            terminationFuture.get();
        }
    }

    @Test
    public void unexpectedDispatcherServiceTerminationWhileRunning_callsFatalErrorHandler() {
        final CompletableFuture<Void> terminationFuture = new CompletableFuture<>();
        dispatcherServiceFactory =
                TestingDispatcherServiceFactory.newBuilder()
                        .setCreateFunction(
                                (ignoredA, ignoredB, ignoredC) ->
                                        TestingDispatcherGatewayService.newBuilder()
                                                .setTerminationFuture(terminationFuture)
                                                .build())
                        .build();
        final SessionDispatcherLeaderProcess dispatcherLeaderProcess =
                createDispatcherLeaderProcess();
        dispatcherLeaderProcess.start();

        final FlinkException expectedFailure = new FlinkException("Expected test failure.");
        terminationFuture.completeExceptionally(expectedFailure);

        final Throwable error = fatalErrorHandler.getErrorFuture().join();
        assertThat(error, containsCause(expectedFailure));

        fatalErrorHandler.clearError();
    }

    @Test
    public void
            unexpectedDispatcherServiceTerminationWhileNotRunning_doesNotCallFatalErrorHandler() {
        final CompletableFuture<Void> terminationFuture = new CompletableFuture<>();
        dispatcherServiceFactory =
                TestingDispatcherServiceFactory.newBuilder()
                        .setCreateFunction(
                                (ignoredA, ignoredB, ignoredC) ->
                                        TestingDispatcherGatewayService.newBuilder()
                                                .setTerminationFuture(terminationFuture)
                                                .withManualTerminationFutureCompletion()
                                                .build())
                        .build();
        final SessionDispatcherLeaderProcess dispatcherLeaderProcess =
                createDispatcherLeaderProcess();
        dispatcherLeaderProcess.start();

        dispatcherLeaderProcess.closeAsync();

        final FlinkException expectedFailure = new FlinkException("Expected test failure.");
        terminationFuture.completeExceptionally(expectedFailure);

        assertThat(fatalErrorHandler.getErrorFuture(), willNotComplete(Duration.ofMillis(10)));
    }

    @Test
    public void confirmLeaderSessionFuture_completesAfterDispatcherServiceHasBeenStarted()
            throws Exception {
        final OneShotLatch createDispatcherServiceLatch = new OneShotLatch();
        final String dispatcherAddress = "myAddress";
        final TestingDispatcherGateway dispatcherGateway =
                new TestingDispatcherGateway.Builder().setAddress(dispatcherAddress).build();

        dispatcherServiceFactory =
                TestingDispatcherServiceFactory.newBuilder()
                        .setCreateFunction(
                                TriFunctionWithException.unchecked(
                                        (ignoredA, ignoredB, ignoredC) -> {
                                            createDispatcherServiceLatch.await();
                                            return TestingDispatcherGatewayService.newBuilder()
                                                    .setDispatcherGateway(dispatcherGateway)
                                                    .build();
                                        }))
                        .build();

        try (final SessionDispatcherLeaderProcess dispatcherLeaderProcess =
                createDispatcherLeaderProcess()) {
            final CompletableFuture<String> confirmLeaderSessionFuture =
                    dispatcherLeaderProcess.getLeaderAddressFuture();

            dispatcherLeaderProcess.start();

            assertThat(confirmLeaderSessionFuture.isDone(), is(false));

            createDispatcherServiceLatch.trigger();

            assertThat(confirmLeaderSessionFuture.get(), is(dispatcherAddress));
        }
    }

    @Test
    public void closeAsync_duringJobRecovery_preventsDispatcherServiceCreation() throws Exception {
        final OneShotLatch jobRecoveryStartedLatch = new OneShotLatch();
        final OneShotLatch completeJobRecoveryLatch = new OneShotLatch();
        final OneShotLatch createDispatcherServiceLatch = new OneShotLatch();

        this.jobGraphStore =
                TestingJobGraphStore.newBuilder()
                        .setJobIdsFunction(
                                storedJobs -> {
                                    jobRecoveryStartedLatch.trigger();
                                    completeJobRecoveryLatch.await();
                                    return storedJobs;
                                })
                        .build();

        this.dispatcherServiceFactory =
                TestingDispatcherServiceFactory.newBuilder()
                        .setCreateFunction(
                                (ignoredA, ignoredB, ignoredC) -> {
                                    createDispatcherServiceLatch.trigger();
                                    return TestingDispatcherGatewayService.newBuilder().build();
                                })
                        .build();

        try (final SessionDispatcherLeaderProcess dispatcherLeaderProcess =
                createDispatcherLeaderProcess()) {
            dispatcherLeaderProcess.start();

            jobRecoveryStartedLatch.await();

            dispatcherLeaderProcess.closeAsync();

            completeJobRecoveryLatch.trigger();

            try {
                createDispatcherServiceLatch.await(10L, TimeUnit.MILLISECONDS);
                fail("No dispatcher service should be created after the process has been stopped.");
            } catch (TimeoutException expected) {
            }
        }
    }

    @Test
    public void onRemovedJobGraph_terminatesRunningJob() throws Exception {
        jobGraphStore =
                TestingJobGraphStore.newBuilder()
                        .setInitialJobGraphs(Collections.singleton(JOB_GRAPH))
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
                TestingDispatcherServiceFactory.newBuilder()
                        .setCreateFunction(
                                (dispatcherId, jobGraphs, jobGraphWriter) ->
                                        testingDispatcherService)
                        .build();

        try (final SessionDispatcherLeaderProcess dispatcherLeaderProcess =
                createDispatcherLeaderProcess()) {
            dispatcherLeaderProcess.start();

            // wait for the dispatcher process to be created
            dispatcherLeaderProcess.getDispatcherGateway().get();

            // now remove the Job from the JobGraphStore and notify the dispatcher service
            jobGraphStore.removeJobGraph(JOB_GRAPH.getJobID());
            dispatcherLeaderProcess.onRemovedJobGraph(JOB_GRAPH.getJobID());

            assertThat(terminateJobFuture.get(), is(JOB_GRAPH.getJobID()));
        }
    }

    @Test
    public void onRemovedJobGraph_failingRemovalCall_failsFatally() throws Exception {
        final FlinkException testException = new FlinkException("Test exception");

        final TestingDispatcherGatewayService testingDispatcherService =
                TestingDispatcherGatewayService.newBuilder()
                        .setOnRemovedJobGraphFunction(
                                jobID -> FutureUtils.completedExceptionally(testException))
                        .build();

        dispatcherServiceFactory =
                TestingDispatcherServiceFactory.newBuilder()
                        .setCreateFunction(
                                (dispatcherId, jobGraphs, jobGraphWriter) ->
                                        testingDispatcherService)
                        .build();

        try (final SessionDispatcherLeaderProcess dispatcherLeaderProcess =
                createDispatcherLeaderProcess()) {
            dispatcherLeaderProcess.start();

            // wait for the dispatcher process to be created
            dispatcherLeaderProcess.getDispatcherGateway().get();

            // now notify the dispatcher service
            dispatcherLeaderProcess.onRemovedJobGraph(JOB_GRAPH.getJobID());

            final Throwable fatalError = fatalErrorHandler.getErrorFuture().join();

            assertTrue(
                    ExceptionUtils.findThrowable(fatalError, cause -> cause.equals(testException))
                            .isPresent());

            fatalErrorHandler.clearError();
        }
    }

    @Test
    public void onAddedJobGraph_submitsRecoveredJob() throws Exception {
        final CompletableFuture<JobGraph> submittedJobFuture = new CompletableFuture<>();
        final TestingDispatcherGateway testingDispatcherGateway =
                new TestingDispatcherGateway.Builder()
                        .setSubmitFunction(
                                submittedJob -> {
                                    submittedJobFuture.complete(submittedJob);
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                })
                        .build();

        dispatcherServiceFactory = createDispatcherServiceFactoryFor(testingDispatcherGateway);

        try (final SessionDispatcherLeaderProcess dispatcherLeaderProcess =
                createDispatcherLeaderProcess()) {
            dispatcherLeaderProcess.start();

            // wait first for the dispatcher service to be created
            dispatcherLeaderProcess.getDispatcherGateway().get();

            jobGraphStore.putJobGraph(JOB_GRAPH);
            dispatcherLeaderProcess.onAddedJobGraph(JOB_GRAPH.getJobID());

            final JobGraph submittedJobGraph = submittedJobFuture.get();

            assertThat(submittedJobGraph.getJobID(), is(JOB_GRAPH.getJobID()));
        }
    }

    @Test
    public void onAddedJobGraph_ifNotRunning_isBeingIgnored() throws Exception {
        final CompletableFuture<JobID> recoveredJobFuture = new CompletableFuture<>();
        jobGraphStore =
                TestingJobGraphStore.newBuilder()
                        .setRecoverJobGraphFunction(
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
            jobGraphStore.putJobGraph(JOB_GRAPH);

            dispatcherLeaderProcess.closeAsync();

            dispatcherLeaderProcess.onAddedJobGraph(JOB_GRAPH.getJobID());

            try {
                recoveredJobFuture.get(10L, TimeUnit.MILLISECONDS);
                fail("onAddedJobGraph should be ignored if the leader process is not running.");
            } catch (TimeoutException expected) {
            }
        }
    }

    @Test
    public void onAddedJobGraph_failingRecovery_propagatesTheFailure() throws Exception {
        final FlinkException expectedFailure = new FlinkException("Expected failure");
        jobGraphStore =
                TestingJobGraphStore.newBuilder()
                        .setRecoverJobGraphFunction(
                                (ignoredA, ignoredB) -> {
                                    throw expectedFailure;
                                })
                        .build();

        try (final SessionDispatcherLeaderProcess dispatcherLeaderProcess =
                createDispatcherLeaderProcess()) {
            dispatcherLeaderProcess.start();

            // wait first for the dispatcher service to be created
            dispatcherLeaderProcess.getDispatcherGateway().get();

            jobGraphStore.putJobGraph(JOB_GRAPH);
            dispatcherLeaderProcess.onAddedJobGraph(JOB_GRAPH.getJobID());

            final CompletableFuture<Throwable> errorFuture = fatalErrorHandler.getErrorFuture();
            final Throwable throwable = errorFuture.get();
            assertThat(
                    ExceptionUtils.findThrowable(throwable, expectedFailure::equals).isPresent(),
                    is(true));

            assertThat(
                    dispatcherLeaderProcess.getState(),
                    is(SessionDispatcherLeaderProcess.State.STOPPED));

            fatalErrorHandler.clearError();
        }
    }

    @Test
    public void recoverJobs_withRecoveryFailure_failsFatally() throws Exception {
        final FlinkException testException = new FlinkException("Test exception");
        jobGraphStore =
                TestingJobGraphStore.newBuilder()
                        .setRecoverJobGraphFunction(
                                (ignoredA, ignoredB) -> {
                                    throw testException;
                                })
                        .setInitialJobGraphs(Collections.singleton(JOB_GRAPH))
                        .build();

        runJobRecoveryFailureTest(testException);
    }

    @Test
    public void recoverJobs_withJobIdRecoveryFailure_failsFatally() throws Exception {
        final FlinkException testException = new FlinkException("Test exception");
        jobGraphStore =
                TestingJobGraphStore.newBuilder()
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
            final Throwable error = fatalErrorHandler.getErrorFuture().get();
            assertThat(
                    ExceptionUtils.findThrowableWithMessage(error, testException.getMessage())
                            .isPresent(),
                    is(true));

            fatalErrorHandler.clearError();
        }
    }

    @Test
    public void onAddedJobGraph_failingRecoveredJobSubmission_failsFatally() throws Exception {
        final TestingDispatcherGateway dispatcherGateway =
                new TestingDispatcherGateway.Builder()
                        .setSubmitFunction(
                                jobGraph ->
                                        FutureUtils.completedExceptionally(
                                                new JobSubmissionException(
                                                        jobGraph.getJobID(), "test exception")))
                        .build();

        runOnAddedJobGraphTest(dispatcherGateway, this::verifyOnAddedJobGraphResultFailsFatally);
    }

    private void verifyOnAddedJobGraphResultFailsFatally(
            TestingFatalErrorHandler fatalErrorHandler) {
        final Throwable actualCause = fatalErrorHandler.getErrorFuture().join();

        assertTrue(
                ExceptionUtils.findThrowable(actualCause, JobSubmissionException.class)
                        .isPresent());

        fatalErrorHandler.clearError();
    }

    @Test
    public void onAddedJobGraph_duplicateJobSubmissionDueToFalsePositive_willBeIgnored()
            throws Exception {
        final TestingDispatcherGateway dispatcherGateway =
                new TestingDispatcherGateway.Builder()
                        .setSubmitFunction(
                                jobGraph ->
                                        FutureUtils.completedExceptionally(
                                                DuplicateJobSubmissionException.of(
                                                        jobGraph.getJobID())))
                        .build();

        runOnAddedJobGraphTest(dispatcherGateway, this::verifyOnAddedJobGraphResultDidNotFail);
    }

    private void runOnAddedJobGraphTest(
            TestingDispatcherGateway dispatcherGateway,
            ThrowingConsumer<TestingFatalErrorHandler, Exception> verificationLogic)
            throws Exception {
        jobGraphStore =
                TestingJobGraphStore.newBuilder()
                        .setInitialJobGraphs(Collections.singleton(JOB_GRAPH))
                        .build();
        dispatcherServiceFactory =
                TestingDispatcherServiceFactory.newBuilder()
                        .setCreateFunction(
                                (dispatcherId, jobGraphs, jobGraphWriter) -> {
                                    assertThat(jobGraphs, containsInAnyOrder(JOB_GRAPH));

                                    return TestingDispatcherGatewayService.newBuilder()
                                            .setDispatcherGateway(dispatcherGateway)
                                            .build();
                                })
                        .build();

        try (final SessionDispatcherLeaderProcess dispatcherLeaderProcess =
                createDispatcherLeaderProcess()) {
            dispatcherLeaderProcess.start();

            dispatcherLeaderProcess.getDispatcherGateway().get();

            dispatcherLeaderProcess.onAddedJobGraph(JOB_GRAPH.getJobID());

            verificationLogic.accept(fatalErrorHandler);
        }
    }

    private void verifyOnAddedJobGraphResultDidNotFail(TestingFatalErrorHandler fatalErrorHandler)
            throws Exception {
        try {
            fatalErrorHandler.getErrorFuture().get(10L, TimeUnit.MILLISECONDS);
            fail(
                    "Expected that duplicate job submissions due to false job recoveries are ignored.");
        } catch (TimeoutException expected) {
        }
    }

    private TestingDispatcherServiceFactory createDispatcherServiceFactoryFor(
            TestingDispatcherGateway testingDispatcherGateway) {
        return TestingDispatcherServiceFactory.newBuilder()
                .setCreateFunction(
                        (ignoredA, ignoredB, ignoredC) ->
                                TestingDispatcherGatewayService.newBuilder()
                                        .setDispatcherGateway(testingDispatcherGateway)
                                        .build())
                .build();
    }

    private SessionDispatcherLeaderProcess createDispatcherLeaderProcess() {
        return SessionDispatcherLeaderProcess.create(
                leaderSessionId,
                dispatcherServiceFactory,
                jobGraphStore,
                ioExecutor,
                fatalErrorHandler);
    }
}
