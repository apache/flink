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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.execution.librarycache.TestingClassLoaderLease;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.highavailability.JobResultEntry;
import org.apache.flink.runtime.highavailability.JobResultStore;
import org.apache.flink.runtime.highavailability.nonha.embedded.EmbeddedJobResultStore;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmaster.factories.JobMasterServiceProcessFactory;
import org.apache.flink.runtime.jobmaster.factories.TestingJobMasterServiceProcessFactory;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGatewayBuilder;
import org.apache.flink.runtime.leaderelection.DefaultLeaderElectionService;
import org.apache.flink.runtime.leaderelection.LeaderElection;
import org.apache.flink.runtime.leaderelection.LeaderInformation;
import org.apache.flink.runtime.leaderelection.LeaderInformationRegister;
import org.apache.flink.runtime.leaderelection.TestingLeaderElection;
import org.apache.flink.runtime.leaderelection.TestingLeaderElectionDriver;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.runtime.testutils.TestingJobResultStore;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.ThrowingRunnable;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.apache.flink.core.testutils.FlinkAssertions.assertThatFuture;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link JobMasterServiceLeadershipRunner}. */
class JobMasterServiceLeadershipRunnerTest {

    private static final Time TESTING_TIMEOUT = Time.seconds(10);

    private static JobGraph jobGraph;

    private TestingLeaderElection leaderElection;

    private TestingFatalErrorHandler fatalErrorHandler;

    private JobResultStore jobResultStore;

    @BeforeAll
    static void setupClass() {

        final JobVertex jobVertex = new JobVertex("Test vertex");
        jobVertex.setInvokableClass(NoOpInvokable.class);
        jobGraph = JobGraphTestUtils.streamingJobGraph(jobVertex);
    }

    @BeforeEach
    void setup() {
        leaderElection = new TestingLeaderElection();
        jobResultStore = new EmbeddedJobResultStore();
        fatalErrorHandler = new TestingFatalErrorHandler();
    }

    @AfterEach
    void tearDown() throws Exception {
        leaderElection.close();
        fatalErrorHandler.rethrowError();
    }

    @Test
    void testShutDownSignalsJobAsNotFinished() throws Exception {
        try (JobManagerRunner jobManagerRunner =
                newJobMasterServiceLeadershipRunnerBuilder().build()) {
            jobManagerRunner.start();

            final CompletableFuture<JobManagerRunnerResult> resultFuture =
                    jobManagerRunner.getResultFuture();

            assertThat(resultFuture).isNotDone();

            jobManagerRunner.closeAsync();

            assertJobNotFinished(resultFuture);
            assertThatFuture(jobManagerRunner.getJobMasterGateway()).eventuallyFails();
        }
    }

    @Test
    void testCloseReleasesClassLoaderLease() throws Exception {
        final OneShotLatch closeClassLoaderLeaseLatch = new OneShotLatch();

        final TestingClassLoaderLease classLoaderLease =
                TestingClassLoaderLease.newBuilder()
                        .setCloseRunnable(closeClassLoaderLeaseLatch::trigger)
                        .build();

        try (JobManagerRunner jobManagerRunner =
                newJobMasterServiceLeadershipRunnerBuilder()
                        .setClassLoaderLease(classLoaderLease)
                        .build()) {
            jobManagerRunner.start();

            jobManagerRunner.close();

            closeClassLoaderLeaseLatch.await();
        }
    }

    /**
     * Tests that the {@link JobMasterServiceLeadershipRunner} always waits for the previous
     * leadership operation (granting or revoking leadership) to finish before starting a new
     * leadership operation.
     */
    @Test
    void testConcurrentLeadershipOperationsBlockingClose() throws Exception {
        final CompletableFuture<Void> terminationFuture = new CompletableFuture<>();

        final JobManagerRunner jobManagerRunner =
                newJobMasterServiceLeadershipRunnerBuilder()
                        .withJobMasterServiceProcesses(
                                TestingJobMasterServiceProcess.newBuilder()
                                        .setCloseAsyncSupplier(() -> terminationFuture)
                                        .build(),
                                TestingJobMasterServiceProcess.newBuilder().build())
                        .build();

        jobManagerRunner.start();

        leaderElection.isLeader(UUID.randomUUID()).get();

        leaderElection.notLeader();

        final UUID expectedLeaderSessionID = UUID.randomUUID();
        final CompletableFuture<LeaderInformation> confirmedLeaderInformation =
                leaderElection.isLeader(expectedLeaderSessionID);

        assertThatFuture(confirmedLeaderInformation)
                .as("The new leadership should wait first for the suspension to happen.")
                .willNotCompleteWithin(Duration.ofMillis(1));

        terminationFuture.complete(null);

        assertThatFuture(confirmedLeaderInformation)
                .eventuallySucceeds()
                .extracting(LeaderInformation::getLeaderSessionID)
                .isEqualTo(expectedLeaderSessionID);
    }

    @Test
    void testExceptionallyCompletedResultFutureFromJobMasterServiceProcessIsForwarded()
            throws Exception {
        final CompletableFuture<JobManagerRunnerResult> resultFuture = new CompletableFuture<>();
        final TestingJobMasterServiceProcess testingJobMasterServiceProcess =
                TestingJobMasterServiceProcess.newBuilder()
                        .setGetResultFutureSupplier(() -> resultFuture)
                        .build();

        JobManagerRunner jobManagerRunner =
                newJobMasterServiceLeadershipRunnerBuilder()
                        .withSingleJobMasterServiceProcess(testingJobMasterServiceProcess)
                        .build();

        jobManagerRunner.start();

        leaderElection.isLeader(UUID.randomUUID()).get();

        final FlinkException cause =
                new FlinkException("The JobMasterService failed unexpectedly.");
        resultFuture.completeExceptionally(cause);

        assertThatFuture(jobManagerRunner.getResultFuture())
                .eventuallyFailsWith(ExecutionException.class)
                .withCause(cause);
    }

    @Test
    void testJobMasterCreationFailureCompletesJobManagerRunnerWithInitializationError()
            throws Exception {

        final FlinkException testException = new FlinkException("Test exception");

        final CompletableFuture<JobManagerRunnerResult> completedResultFuture =
                CompletableFuture.completedFuture(
                        JobManagerRunnerResult.forInitializationFailure(
                                createFailedExecutionGraphInfo(testException), testException));
        final JobMasterServiceLeadershipRunner jobManagerRunner =
                newJobMasterServiceLeadershipRunnerBuilder()
                        .withSingleJobMasterServiceProcess(
                                TestingJobMasterServiceProcess.newBuilder()
                                        .setGetResultFutureSupplier(() -> completedResultFuture)
                                        .build())
                        .build();

        jobManagerRunner.start();

        leaderElection.isLeader(UUID.randomUUID());

        final JobManagerRunnerResult jobManagerRunnerResult =
                jobManagerRunner.getResultFuture().join();
        assertThat(jobManagerRunnerResult.isInitializationFailure()).isTrue();

        assertThat(jobManagerRunnerResult.getInitializationFailure()).isEqualTo(testException);
    }

    @Nonnull
    private ExecutionGraphInfo createFailedExecutionGraphInfo(FlinkException testException) {
        return new ExecutionGraphInfo(
                ArchivedExecutionGraph.createSparseArchivedExecutionGraph(
                        jobGraph.getJobID(),
                        jobGraph.getName(),
                        JobStatus.FAILED,
                        testException,
                        null,
                        1L));
    }

    @Test
    void testJobMasterServiceProcessIsTerminatedOnClose() throws Exception {
        final CompletableFuture<Void> terminationFuture = new CompletableFuture<>();

        final JobManagerRunner jobManagerRunner =
                newJobMasterServiceLeadershipRunnerBuilder()
                        .withSingleJobMasterServiceProcess(
                                TestingJobMasterServiceProcess.newBuilder()
                                        .setCloseAsyncSupplier(
                                                () -> {
                                                    terminationFuture.complete(null);
                                                    return terminationFuture;
                                                })
                                        .build())
                        .build();

        jobManagerRunner.start();

        leaderElection.isLeader(UUID.randomUUID());

        jobManagerRunner.closeAsync().join();

        assertJobNotFinished(jobManagerRunner.getResultFuture());
        assertThat(terminationFuture).isDone();
    }

    @Test
    void testJobMasterServiceProcessShutdownOnLeadershipLoss() throws Exception {
        final CompletableFuture<Void> terminationFuture = new CompletableFuture<>();
        final JobManagerRunner jobManagerRunner =
                newJobMasterServiceLeadershipRunnerBuilder()
                        .withSingleJobMasterServiceProcess(
                                TestingJobMasterServiceProcess.newBuilder()
                                        .setCloseAsyncSupplier(
                                                () -> {
                                                    terminationFuture.complete(null);
                                                    return terminationFuture;
                                                })
                                        .build())
                        .build();

        jobManagerRunner.start();

        leaderElection.isLeader(UUID.randomUUID());

        leaderElection.notLeader();

        assertThat(terminationFuture).isDone();
    }

    @Test
    void testCancellationIsForwardedToJobMasterService() throws Exception {
        final CompletableFuture<JobMasterGateway> jobMasterGatewayFuture =
                new CompletableFuture<>();
        final JobManagerRunner jobManagerRunner =
                newJobMasterServiceLeadershipRunnerBuilder()
                        .withSingleJobMasterServiceProcess(
                                TestingJobMasterServiceProcess.newBuilder()
                                        .setGetJobMasterGatewayFutureSupplier(
                                                () -> jobMasterGatewayFuture)
                                        .build())
                        .build();

        jobManagerRunner.start();

        leaderElection.isLeader(UUID.randomUUID());

        // cancel during init
        CompletableFuture<Acknowledge> cancellationFuture =
                jobManagerRunner.cancel(TESTING_TIMEOUT);

        assertThat(cancellationFuture).isNotDone();

        AtomicBoolean cancelCalled = new AtomicBoolean(false);
        JobMasterGateway jobMasterGateway =
                new TestingJobMasterGatewayBuilder()
                        .setCancelFunction(
                                () -> {
                                    cancelCalled.set(true);
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                })
                        .build();

        jobMasterGatewayFuture.complete(jobMasterGateway);

        // assert that cancellation future completes when cancellation completes.
        cancellationFuture.get();
        assertThat(cancelCalled).isTrue();
    }

    @Test
    void testJobInformationOperationsDuringInitialization() throws Exception {

        final JobManagerRunner jobManagerRunner =
                newJobMasterServiceLeadershipRunnerBuilder()
                        .withSingleJobMasterServiceProcess(
                                TestingJobMasterServiceProcess.newBuilder()
                                        .setIsInitializedAndRunningSupplier(() -> false)
                                        .build())
                        .build();

        jobManagerRunner.start();

        // assert initializing while waiting for leadership
        assertInitializingStates(jobManagerRunner);

        // assign leadership
        leaderElection.isLeader(UUID.randomUUID());

        // assert initializing while not yet initialized
        assertInitializingStates(jobManagerRunner);
    }

    private static void assertInitializingStates(JobManagerRunner jobManagerRunner)
            throws ExecutionException, InterruptedException {
        assertThat(jobManagerRunner.requestJobStatus(TESTING_TIMEOUT).get())
                .isEqualTo(JobStatus.INITIALIZING);
        assertThat(jobManagerRunner.getResultFuture()).isNotDone();
        assertThat(
                        jobManagerRunner
                                .requestJob(TESTING_TIMEOUT)
                                .get()
                                .getArchivedExecutionGraph()
                                .getState())
                .isEqualTo(JobStatus.INITIALIZING);

        assertThat(jobManagerRunner.requestJobDetails(TESTING_TIMEOUT).get().getStatus())
                .isEqualTo(JobStatus.INITIALIZING);
    }

    // It can happen that a series of leadership operations happens while the JobMaster
    // initialization is blocked. This test is to ensure that we are not starting-stopping
    // JobMasters for all pending leadership grants, but only for the latest.
    @Test
    void testSkippingOfEnqueuedLeadershipOperations() throws Exception {
        final CompletableFuture<Void> firstTerminationFuture = new CompletableFuture<>();
        final CompletableFuture<Void> secondTerminationFuture = new CompletableFuture<>();

        final JobManagerRunner jobManagerRunner =
                newJobMasterServiceLeadershipRunnerBuilder()
                        .withJobMasterServiceProcesses(
                                TestingJobMasterServiceProcess.newBuilder()
                                        .setCloseAsyncSupplier(() -> firstTerminationFuture)
                                        .setIsInitializedAndRunningSupplier(() -> false)
                                        .build(),
                                TestingJobMasterServiceProcess.newBuilder()
                                        .setCloseAsyncSupplier(
                                                () -> {
                                                    secondTerminationFuture.complete(null);
                                                    return secondTerminationFuture;
                                                })
                                        .build())
                        .build();

        jobManagerRunner.start();

        // first leadership assignment to get into blocking initialization
        leaderElection.isLeader(UUID.randomUUID());

        assertThat(jobManagerRunner.requestJobStatus(TESTING_TIMEOUT).get())
                .isEqualTo(JobStatus.INITIALIZING);

        // we are now blocked on the initialization, enqueue some operations:
        for (int i = 0; i < 10; i++) {
            leaderElection.notLeader();
            leaderElection.isLeader(UUID.randomUUID());
        }

        firstTerminationFuture.complete(null);

        jobManagerRunner.closeAsync();

        // this ensures that the second JobMasterServiceProcess is taken
        assertThat(secondTerminationFuture).isDone();
    }

    @Test
    void testCancellationFailsWhenInitializationFails() throws Exception {
        final FlinkException testException = new FlinkException("test exception");
        runCancellationFailsTest(
                resultFuture ->
                        resultFuture.complete(
                                JobManagerRunnerResult.forInitializationFailure(
                                        createFailedExecutionGraphInfo(testException),
                                        testException)));
    }

    @Test
    void testCancellationFailsWhenExceptionOccurs() throws Exception {
        final FlinkException testException = new FlinkException("test exception");
        runCancellationFailsTest(resultFuture -> resultFuture.completeExceptionally(testException));
    }

    void runCancellationFailsTest(Consumer<CompletableFuture<JobManagerRunnerResult>> testAction)
            throws Exception {
        final CompletableFuture<JobManagerRunnerResult> jobManagerRunnerResultFuture =
                new CompletableFuture<>();
        final JobManagerRunner jobManagerRunner =
                newJobMasterServiceLeadershipRunnerBuilder()
                        .withSingleJobMasterServiceProcess(
                                TestingJobMasterServiceProcess.newBuilder()
                                        .setGetJobMasterGatewayFutureSupplier(
                                                CompletableFuture::new)
                                        .setGetResultFutureSupplier(
                                                () -> jobManagerRunnerResultFuture)
                                        .setIsInitializedAndRunningSupplier(() -> false)
                                        .build())
                        .build();

        jobManagerRunner.start();

        leaderElection.isLeader(UUID.randomUUID());

        // cancel while initializing
        assertThat(jobManagerRunner.requestJobStatus(TESTING_TIMEOUT).get())
                .isEqualTo(JobStatus.INITIALIZING);

        CompletableFuture<Acknowledge> cancelFuture = jobManagerRunner.cancel(TESTING_TIMEOUT);
        assertThat(cancelFuture).isNotDone();

        testAction.accept(jobManagerRunnerResultFuture);
        assertThatThrownBy(cancelFuture::get).hasMessageContaining("Cancellation failed.");
    }

    @Test
    void testResultFutureCompletionOfOutdatedLeaderIsIgnored() throws Exception {
        final CompletableFuture<JobManagerRunnerResult> resultFuture = new CompletableFuture<>();
        final JobMasterServiceLeadershipRunner jobManagerRunner =
                newJobMasterServiceLeadershipRunnerBuilder()
                        .withSingleJobMasterServiceProcess(
                                TestingJobMasterServiceProcess.newBuilder()
                                        .setGetResultFutureSupplier(() -> resultFuture)
                                        .build())
                        .build();

        jobManagerRunner.start();

        leaderElection.isLeader(UUID.randomUUID()).join();

        leaderElection.notLeader();

        resultFuture.complete(
                JobManagerRunnerResult.forSuccess(
                        createFailedExecutionGraphInfo(new FlinkException("test exception"))));

        assertThatFuture(jobManagerRunner.getResultFuture()).eventuallyFails();
    }

    @Test
    void testJobMasterGatewayIsInvalidatedOnLeadershipChanges() throws Exception {
        final JobMasterServiceLeadershipRunner jobManagerRunner =
                newJobMasterServiceLeadershipRunnerBuilder()
                        .withSingleJobMasterServiceProcess(
                                TestingJobMasterServiceProcess.newBuilder()
                                        .setGetJobMasterGatewayFutureSupplier(
                                                CompletableFuture::new)
                                        .build())
                        .build();

        jobManagerRunner.start();

        final CompletableFuture<JobMasterGateway> jobMasterGateway =
                jobManagerRunner.getJobMasterGateway();

        leaderElection.isLeader(UUID.randomUUID());

        leaderElection.notLeader();

        assertThatFuture(jobMasterGateway).eventuallyFails();
    }

    @Test
    void testLeaderAddressOfOutdatedLeaderIsIgnored() throws Exception {
        final CompletableFuture<String> leaderAddressFuture = new CompletableFuture<>();
        final JobMasterServiceLeadershipRunner jobManagerRunner =
                newJobMasterServiceLeadershipRunnerBuilder()
                        .withSingleJobMasterServiceProcess(
                                TestingJobMasterServiceProcess.newBuilder()
                                        .setGetLeaderAddressFutureSupplier(
                                                () -> leaderAddressFuture)
                                        .build())
                        .build();

        jobManagerRunner.start();

        final CompletableFuture<LeaderInformation> leaderFuture =
                leaderElection.isLeader(UUID.randomUUID());

        leaderElection.notLeader();

        leaderAddressFuture.complete("foobar");

        assertThatFuture(leaderFuture).willNotCompleteWithin(Duration.ofMillis(5));
    }

    @Test
    void testInitialJobStatusIsInitializing() throws Exception {
        final JobMasterServiceLeadershipRunner jobManagerRunner =
                newJobMasterServiceLeadershipRunnerBuilder().build();

        jobManagerRunner.start();

        assertThat(jobManagerRunner.requestJobStatus(TESTING_TIMEOUT).join())
                .isEqualTo(JobStatus.INITIALIZING);
    }

    @Test
    void testCancellationChangesJobStatusToCancelling() throws Exception {
        final JobMasterServiceLeadershipRunner jobManagerRunner =
                newJobMasterServiceLeadershipRunnerBuilder().build();

        jobManagerRunner.start();

        jobManagerRunner.cancel(TESTING_TIMEOUT);

        assertThat(jobManagerRunner.requestJobStatus(TESTING_TIMEOUT).join())
                .isEqualTo(JobStatus.CANCELLING);
    }

    @Test
    void testJobStatusCancellingIsClearedOnLeadershipLoss() throws Exception {
        CompletableFuture<Void> terminationFuture = new CompletableFuture<>();
        final JobMasterServiceLeadershipRunner jobManagerRunner =
                newJobMasterServiceLeadershipRunnerBuilder()
                        .withSingleJobMasterServiceProcess(
                                TestingJobMasterServiceProcess.newBuilder()
                                        .setCloseAsyncSupplier(
                                                () -> {
                                                    terminationFuture.complete(null);
                                                    return terminationFuture;
                                                })
                                        .setIsInitializedAndRunningSupplier(
                                                () -> !terminationFuture.isDone())
                                        .build())
                        .build();

        jobManagerRunner.start();

        jobManagerRunner.cancel(TESTING_TIMEOUT);

        leaderElection.isLeader(UUID.randomUUID());
        leaderElection.notLeader();

        assertThat(jobManagerRunner.requestJobStatus(TESTING_TIMEOUT).join())
                .isEqualTo(JobStatus.INITIALIZING);
    }

    @Test
    void testJobMasterServiceProcessClosingExceptionIsForwardedToResultFuture() throws Exception {
        final CompletableFuture<Void> terminationFuture = new CompletableFuture<>();
        final JobMasterServiceLeadershipRunner jobManagerRunner =
                newJobMasterServiceLeadershipRunnerBuilder()
                        .withSingleJobMasterServiceProcess(
                                TestingJobMasterServiceProcess.newBuilder()
                                        .setCloseAsyncSupplier(() -> terminationFuture)
                                        .build())
                        .build();

        jobManagerRunner.start();

        leaderElection.isLeader(UUID.randomUUID());
        leaderElection.notLeader();

        final FlinkException testException = new FlinkException("Test exception");
        terminationFuture.completeExceptionally(testException);

        assertThatFuture(jobManagerRunner.getResultFuture())
                .eventuallyFailsWith(ExecutionException.class)
                .satisfies(cause -> assertThat(cause).hasRootCause(testException));
    }

    @Test
    void testJobMasterServiceProcessCreationFailureIsForwardedToResultFuture() throws Exception {
        final FlinkRuntimeException testException = new FlinkRuntimeException("Test exception");
        final JobMasterServiceLeadershipRunner jobManagerRunner =
                newJobMasterServiceLeadershipRunnerBuilder()
                        .setJobMasterServiceProcessFactory(
                                TestingJobMasterServiceProcessFactory.newBuilder()
                                        .setJobMasterServiceProcessFunction(
                                                ignored -> {
                                                    throw testException;
                                                })
                                        .build())
                        .build();

        jobManagerRunner.start();

        leaderElection.isLeader(UUID.randomUUID());

        assertThatFuture(jobManagerRunner.getResultFuture())
                .eventuallyFailsWith(ExecutionException.class)
                .satisfies(cause -> assertThat(cause).hasRootCause(testException));
    }

    @Test
    void testJobAlreadyDone() throws Exception {
        final JobID jobId = new JobID();
        final JobResult jobResult =
                TestingJobResultStore.createJobResult(jobId, ApplicationStatus.UNKNOWN);
        jobResultStore.createDirtyResultAsync(new JobResultEntry(jobResult)).get();
        try (JobManagerRunner jobManagerRunner =
                newJobMasterServiceLeadershipRunnerBuilder()
                        .setJobMasterServiceProcessFactory(
                                TestingJobMasterServiceProcessFactory.newBuilder()
                                        .setJobId(jobId)
                                        .build())
                        .build()) {
            jobManagerRunner.start();
            leaderElection.isLeader(UUID.randomUUID());

            final CompletableFuture<JobManagerRunnerResult> resultFuture =
                    jobManagerRunner.getResultFuture();

            JobManagerRunnerResult result = resultFuture.get();
            assertThat(result.getExecutionGraphInfo().getArchivedExecutionGraph().getState())
                    .isEqualTo(JobStatus.FAILED);
        }
    }

    @Disabled
    @Test
    void testJobMasterServiceLeadershipRunnerCloseWhenElectionServiceGrantLeaderShip()
            throws Exception {
        final AtomicReference<LeaderInformationRegister> storedLeaderInformation =
                new AtomicReference<>(LeaderInformationRegister.empty());

        final TestingLeaderElectionDriver.Factory driverFactory =
                new TestingLeaderElectionDriver.Factory(
                        TestingLeaderElectionDriver.newBuilder(
                                new AtomicBoolean(), storedLeaderInformation, new AtomicBoolean()));

        // we need to use DefaultLeaderElectionService here because JobMasterServiceLeadershipRunner
        // in connection with the DefaultLeaderElectionService generates the nested locking
        final DefaultLeaderElectionService defaultLeaderElectionService =
                new DefaultLeaderElectionService(driverFactory, fatalErrorHandler);

        final TestingLeaderElectionDriver currentLeaderDriver =
                driverFactory.assertAndGetOnlyCreatedDriver();

        // latch to detect when we reached the first synchronized section having a lock on the
        // JobMasterServiceProcess#stop side
        final OneShotLatch closeAsyncCalledTrigger = new OneShotLatch();
        // latch to halt the JobMasterServiceProcess#stop before calling stop on the
        // DefaultLeaderElectionService instance (and entering the LeaderElectionService's
        // synchronized block)
        final OneShotLatch triggerClassLoaderLeaseRelease = new OneShotLatch();

        final JobMasterServiceProcess jobMasterServiceProcess =
                TestingJobMasterServiceProcess.newBuilder()
                        .setGetJobMasterGatewayFutureSupplier(CompletableFuture::new)
                        .setGetResultFutureSupplier(CompletableFuture::new)
                        .setGetLeaderAddressFutureSupplier(
                                () -> CompletableFuture.completedFuture("unused address"))
                        .setCloseAsyncSupplier(
                                () -> {
                                    closeAsyncCalledTrigger.trigger();
                                    // we have to return a completed future because we need the
                                    // follow-up task to run in the calling thread to make the
                                    // follow-up code block being executed in the synchronized block
                                    return CompletableFuture.completedFuture(null);
                                })
                        .build();
        final String componentId = "random-component-id";
        final LeaderElection leaderElection =
                defaultLeaderElectionService.createLeaderElection(componentId);
        try (final JobMasterServiceLeadershipRunner jobManagerRunner =
                newJobMasterServiceLeadershipRunnerBuilder()
                        .setClassLoaderLease(
                                TestingClassLoaderLease.newBuilder()
                                        .setCloseRunnable(
                                                () -> {
                                                    try {
                                                        // we want to wait with releasing to halt
                                                        // before calling stop on the
                                                        // DefaultLeaderElectionService
                                                        triggerClassLoaderLeaseRelease.await();
                                                        // In order to reproduce the deadlock, we
                                                        // need to ensure that
                                                        // leaderContender#grantLeadership can be
                                                        // called after
                                                        // JobMasterServiceLeadershipRunner obtains
                                                        // its own lock. Unfortunately, This will
                                                        // change the running status of
                                                        // DefaultLeaderElectionService
                                                        // to false, which will cause the
                                                        // notification of leadership to be
                                                        // ignored. The issue is that we
                                                        // don't have any means of verify that we're
                                                        // in the synchronized block of
                                                        // DefaultLeaderElectionService#lock in
                                                        // DefaultLeaderElectionService#onGrantLeadership,
                                                        // but we trigger this implicitly through
                                                        // TestingLeaderElectionDriver#grantLeadership(UUID).
                                                        // Adding a short sleep can ensure that
                                                        // another thread successfully receives the
                                                        // leadership notification, so that the
                                                        // deadlock problem can recur.
                                                        Thread.sleep(5);
                                                    } catch (InterruptedException e) {
                                                        ExceptionUtils.checkInterrupted(e);
                                                    }
                                                })
                                        .build())
                        .setJobMasterServiceProcessFactory(
                                TestingJobMasterServiceProcessFactory.newBuilder()
                                        .setJobMasterServiceProcessFunction(
                                                ignoredSessionId -> jobMasterServiceProcess)
                                        .build())
                        .setLeaderElection(leaderElection)
                        .build()) {
            jobManagerRunner.start();

            // grant leadership to create jobMasterServiceProcess
            final UUID leaderSessionID = UUID.randomUUID();
            defaultLeaderElectionService.onGrantLeadership(leaderSessionID);

            while (!currentLeaderDriver.hasLeadership()
                    || !leaderElection.hasLeadership(leaderSessionID)) {
                Thread.sleep(100);
            }

            final CheckedThread contenderCloseThread = createCheckedThread(jobManagerRunner::close);
            contenderCloseThread.start();

            // waiting for the contender reaching the synchronized section of the stop call
            closeAsyncCalledTrigger.await();

            final CheckedThread grantLeadershipThread =
                    createCheckedThread(
                            () -> {
                                // DefaultLeaderElectionService enforces a proper event handling
                                // order (i.e. no two grant or revoke events should appear after
                                // each other). This requires the leadership to be revoked before
                                // regaining leadership in this test.
                                defaultLeaderElectionService.onRevokeLeadership();
                                defaultLeaderElectionService.onGrantLeadership(UUID.randomUUID());
                            });
            grantLeadershipThread.start();

            // finalize ClassloaderLease release to trigger DefaultLeaderElectionService#stop
            triggerClassLoaderLeaseRelease.trigger();

            contenderCloseThread.sync();
            grantLeadershipThread.sync();
        }
    }

    private static CheckedThread createCheckedThread(
            ThrowingRunnable<? extends Exception> callback) {
        return new CheckedThread() {
            @Override
            public void go() throws Exception {
                callback.run();
            }
        };
    }

    private void assertJobNotFinished(CompletableFuture<JobManagerRunnerResult> resultFuture)
            throws ExecutionException, InterruptedException {
        final JobManagerRunnerResult jobManagerRunnerResult = resultFuture.get();
        assertThat(
                        jobManagerRunnerResult
                                .getExecutionGraphInfo()
                                .getArchivedExecutionGraph()
                                .getState())
                .isEqualTo(JobStatus.SUSPENDED);
    }

    public JobMasterServiceLeadershipRunnerBuilder newJobMasterServiceLeadershipRunnerBuilder() {
        return new JobMasterServiceLeadershipRunnerBuilder();
    }

    public class JobMasterServiceLeadershipRunnerBuilder {
        private JobMasterServiceProcessFactory jobMasterServiceProcessFactory =
                TestingJobMasterServiceProcessFactory.newBuilder().build();
        private LibraryCacheManager.ClassLoaderLease classLoaderLease =
                TestingClassLoaderLease.newBuilder().build();

        private LeaderElection leaderElection =
                JobMasterServiceLeadershipRunnerTest.this.leaderElection;

        public JobMasterServiceLeadershipRunnerBuilder setClassLoaderLease(
                LibraryCacheManager.ClassLoaderLease classLoaderLease) {
            this.classLoaderLease = classLoaderLease;
            return this;
        }

        public JobMasterServiceLeadershipRunnerBuilder setJobMasterServiceProcessFactory(
                JobMasterServiceProcessFactory jobMasterServiceProcessFactory) {
            this.jobMasterServiceProcessFactory = jobMasterServiceProcessFactory;
            return this;
        }

        public JobMasterServiceLeadershipRunnerBuilder setLeaderElection(
                LeaderElection leaderElection) {
            this.leaderElection = leaderElection;
            return this;
        }

        public JobMasterServiceLeadershipRunner build() {
            return new JobMasterServiceLeadershipRunner(
                    jobMasterServiceProcessFactory,
                    leaderElection,
                    jobResultStore,
                    classLoaderLease,
                    fatalErrorHandler);
        }

        public JobMasterServiceLeadershipRunnerBuilder withSingleJobMasterServiceProcess(
                JobMasterServiceProcess jobMasterServiceProcess) {
            return withJobMasterServiceProcesses(jobMasterServiceProcess);
        }

        public JobMasterServiceLeadershipRunnerBuilder withJobMasterServiceProcesses(
                JobMasterServiceProcess... jobMasterServiceProcesses) {
            final Queue<JobMasterServiceProcess> jobMasterServiceProcessQueue =
                    new ArrayDeque<>(Arrays.asList(jobMasterServiceProcesses));
            this.jobMasterServiceProcessFactory =
                    TestingJobMasterServiceProcessFactory.newBuilder()
                            .setJobMasterServiceProcessFunction(
                                    ignored ->
                                            Preconditions.checkNotNull(
                                                    jobMasterServiceProcessQueue.poll()))
                            .build();
            return this;
        }
    }
}
