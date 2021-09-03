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
import org.apache.flink.core.testutils.FlinkMatchers;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.execution.librarycache.TestingClassLoaderLease;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.highavailability.RunningJobsRegistry;
import org.apache.flink.runtime.highavailability.nonha.standalone.StandaloneRunningJobsRegistry;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmaster.factories.JobMasterServiceProcessFactory;
import org.apache.flink.runtime.jobmaster.factories.TestingJobMasterServiceProcessFactory;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGatewayBuilder;
import org.apache.flink.runtime.leaderelection.TestingLeaderElectionService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nonnull;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.apache.flink.core.testutils.FlinkMatchers.containsCause;
import static org.apache.flink.core.testutils.FlinkMatchers.containsMessage;
import static org.apache.flink.core.testutils.FlinkMatchers.willNotComplete;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests for the {@link JobMasterServiceLeadershipRunner}. */
public class JobMasterServiceLeadershipRunnerTest extends TestLogger {

    private static final Time TESTING_TIMEOUT = Time.seconds(10);

    @ClassRule public static TemporaryFolder temporaryFolder = new TemporaryFolder();

    private static JobGraph jobGraph;

    private TestingLeaderElectionService leaderElectionService;

    private TestingFatalErrorHandler fatalErrorHandler;

    private RunningJobsRegistry runningJobsRegistry;

    @BeforeClass
    public static void setupClass() {

        final JobVertex jobVertex = new JobVertex("Test vertex");
        jobVertex.setInvokableClass(NoOpInvokable.class);
        jobGraph = JobGraphTestUtils.streamingJobGraph(jobVertex);
    }

    @Before
    public void setup() {
        leaderElectionService = new TestingLeaderElectionService();
        runningJobsRegistry = new StandaloneRunningJobsRegistry();
        fatalErrorHandler = new TestingFatalErrorHandler();
    }

    @After
    public void tearDown() throws Exception {
        fatalErrorHandler.rethrowError();
    }

    @Test
    public void testShutDownSignalsJobAsNotFinished() throws Exception {
        try (JobManagerRunner jobManagerRunner =
                newJobMasterServiceLeadershipRunnerBuilder().build()) {
            jobManagerRunner.start();

            final CompletableFuture<JobManagerRunnerResult> resultFuture =
                    jobManagerRunner.getResultFuture();

            assertThat(resultFuture.isDone(), is(false));

            jobManagerRunner.closeAsync();

            assertJobNotFinished(resultFuture);
            assertThat(
                    jobManagerRunner.getJobMasterGateway(),
                    FlinkMatchers.futureWillCompleteExceptionally(Duration.ofMillis(5L)));
        }
    }

    @Test
    public void testCloseReleasesClassLoaderLease() throws Exception {
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
    public void testConcurrentLeadershipOperationsBlockingClose() throws Exception {
        final CompletableFuture<Void> terminationFuture = new CompletableFuture<>();

        final JobManagerRunner jobManagerRunner =
                newJobMasterServiceLeadershipRunnerBuilder()
                        .withJobMasterServiceProcesses(
                                TestingJobMasterServiceProcess.newBuilder()
                                        .setTerminationFuture(terminationFuture)
                                        .withManualTerminationFutureCompletion()
                                        .build(),
                                TestingJobMasterServiceProcess.newBuilder().build())
                        .build();

        jobManagerRunner.start();

        leaderElectionService.isLeader(UUID.randomUUID()).get();

        leaderElectionService.notLeader();

        final CompletableFuture<UUID> leaderFuture =
                leaderElectionService.isLeader(UUID.randomUUID());

        // the new leadership should wait first for the suspension to happen
        assertThat(leaderFuture.isDone(), is(false));

        try {
            leaderFuture.get(1L, TimeUnit.MILLISECONDS);
            fail("Granted leadership even though the JobMaster has not been suspended.");
        } catch (TimeoutException expected) {
            // expected
        }

        terminationFuture.complete(null);

        leaderFuture.get();
    }

    @Test
    public void testExceptionallyCompletedResultFutureFromJobMasterServiceProcessIsForwarded()
            throws Exception {
        final CompletableFuture<JobManagerRunnerResult> resultFuture = new CompletableFuture<>();
        final TestingJobMasterServiceProcess testingJobMasterServiceProcess =
                TestingJobMasterServiceProcess.newBuilder()
                        .setJobManagerRunnerResultFuture(resultFuture)
                        .build();

        JobManagerRunner jobManagerRunner =
                newJobMasterServiceLeadershipRunnerBuilder()
                        .withSingleJobMasterServiceProcess(testingJobMasterServiceProcess)
                        .build();

        jobManagerRunner.start();

        leaderElectionService.isLeader(UUID.randomUUID()).get();

        final FlinkException cause =
                new FlinkException("The JobMasterService failed unexpectedly.");
        resultFuture.completeExceptionally(cause);

        assertThat(
                jobManagerRunner.getResultFuture(),
                FlinkMatchers.futureWillCompleteExceptionally(
                        cause::equals,
                        Duration.ofMillis(5L),
                        "Wrong cause of failed result future"));
    }

    @Test
    public void testJobMasterCreationFailureCompletesJobManagerRunnerWithInitializationError()
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
                                        .setJobManagerRunnerResultFuture(completedResultFuture)
                                        .build())
                        .build();

        jobManagerRunner.start();

        leaderElectionService.isLeader(UUID.randomUUID());

        final JobManagerRunnerResult jobManagerRunnerResult =
                jobManagerRunner.getResultFuture().join();
        assertTrue(jobManagerRunnerResult.isInitializationFailure());

        assertThat(jobManagerRunnerResult.getInitializationFailure(), containsCause(testException));
    }

    @Nonnull
    private ExecutionGraphInfo createFailedExecutionGraphInfo(FlinkException testException) {
        return new ExecutionGraphInfo(
                ArchivedExecutionGraph.createFromInitializingJob(
                        jobGraph.getJobID(),
                        jobGraph.getName(),
                        JobStatus.FAILED,
                        testException,
                        null,
                        1L));
    }

    @Test
    public void testJobMasterServiceProcessIsTerminatedOnClose() throws Exception {
        final CompletableFuture<Void> terminationFuture = new CompletableFuture<>();

        final JobManagerRunner jobManagerRunner =
                newJobMasterServiceLeadershipRunnerBuilder()
                        .withSingleJobMasterServiceProcess(
                                TestingJobMasterServiceProcess.newBuilder()
                                        .setTerminationFuture(terminationFuture)
                                        .build())
                        .build();

        jobManagerRunner.start();

        leaderElectionService.isLeader(UUID.randomUUID());

        jobManagerRunner.closeAsync().join();

        assertJobNotFinished(jobManagerRunner.getResultFuture());
        assertTrue(terminationFuture.isDone());
    }

    @Test
    public void testJobMasterServiceProcessShutdownOnLeadershipLoss() throws Exception {
        final CompletableFuture<Void> terminationFuture = new CompletableFuture<>();
        final JobManagerRunner jobManagerRunner =
                newJobMasterServiceLeadershipRunnerBuilder()
                        .withSingleJobMasterServiceProcess(
                                TestingJobMasterServiceProcess.newBuilder()
                                        .setTerminationFuture(terminationFuture)
                                        .build())
                        .build();

        jobManagerRunner.start();

        leaderElectionService.isLeader(UUID.randomUUID());

        leaderElectionService.notLeader();

        assertTrue(terminationFuture.isDone());
    }

    @Test
    public void testCancellationIsForwardedToJobMasterService() throws Exception {
        final CompletableFuture<JobMasterGateway> jobMasterGatewayFuture =
                new CompletableFuture<>();
        final JobManagerRunner jobManagerRunner =
                newJobMasterServiceLeadershipRunnerBuilder()
                        .withSingleJobMasterServiceProcess(
                                TestingJobMasterServiceProcess.newBuilder()
                                        .setJobMasterGatewayFuture(jobMasterGatewayFuture)
                                        .build())
                        .build();

        jobManagerRunner.start();

        leaderElectionService.isLeader(UUID.randomUUID());

        // cancel during init
        CompletableFuture<Acknowledge> cancellationFuture =
                jobManagerRunner.cancel(TESTING_TIMEOUT);

        assertThat(cancellationFuture.isDone(), is(false));

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
        assertThat(cancelCalled.get(), is(true));
    }

    @Test
    public void testJobInformationOperationsDuringInitialization() throws Exception {

        final JobManagerRunner jobManagerRunner =
                newJobMasterServiceLeadershipRunnerBuilder()
                        .withSingleJobMasterServiceProcess(
                                TestingJobMasterServiceProcess.newBuilder()
                                        .setIsInitialized(false)
                                        .build())
                        .build();

        jobManagerRunner.start();

        // assert initializing while waiting for leadership
        assertInitializingStates(jobManagerRunner);

        // assign leadership
        leaderElectionService.isLeader(UUID.randomUUID());

        // assert initializing while not yet initialized
        assertInitializingStates(jobManagerRunner);
    }

    private static void assertInitializingStates(JobManagerRunner jobManagerRunner)
            throws ExecutionException, InterruptedException {
        assertThat(
                jobManagerRunner.requestJobStatus(TESTING_TIMEOUT).get(),
                is(JobStatus.INITIALIZING));
        assertThat(jobManagerRunner.getResultFuture().isDone(), is(false));
        assertThat(
                jobManagerRunner
                        .requestJob(TESTING_TIMEOUT)
                        .get()
                        .getArchivedExecutionGraph()
                        .getState(),
                is(JobStatus.INITIALIZING));

        assertThat(
                jobManagerRunner.requestJobDetails(TESTING_TIMEOUT).get().getStatus(),
                is(JobStatus.INITIALIZING));
    }

    // It can happen that a series of leadership operations happens while the JobMaster
    // initialization is blocked. This test is to ensure that we are not starting-stopping
    // JobMasters for all pending leadership grants, but only for the latest.
    @Test
    public void testSkippingOfEnqueuedLeadershipOperations() throws Exception {
        final CompletableFuture<Void> firstTerminationFuture = new CompletableFuture<>();
        final CompletableFuture<Void> secondTerminationFuture = new CompletableFuture<>();

        final JobManagerRunner jobManagerRunner =
                newJobMasterServiceLeadershipRunnerBuilder()
                        .withJobMasterServiceProcesses(
                                TestingJobMasterServiceProcess.newBuilder()
                                        .setTerminationFuture(firstTerminationFuture)
                                        .withManualTerminationFutureCompletion()
                                        .setIsInitialized(false)
                                        .build(),
                                TestingJobMasterServiceProcess.newBuilder()
                                        .setTerminationFuture(secondTerminationFuture)
                                        .build())
                        .build();

        jobManagerRunner.start();

        // first leadership assignment to get into blocking initialization
        leaderElectionService.isLeader(UUID.randomUUID());

        assertThat(
                jobManagerRunner.requestJobStatus(TESTING_TIMEOUT).get(),
                is(JobStatus.INITIALIZING));

        // we are now blocked on the initialization, enqueue some operations:
        for (int i = 0; i < 10; i++) {
            leaderElectionService.notLeader();
            leaderElectionService.isLeader(UUID.randomUUID());
        }

        firstTerminationFuture.complete(null);

        jobManagerRunner.closeAsync();

        // this ensures that the second JobMasterServiceProcess is taken
        assertTrue(secondTerminationFuture.isDone());
    }

    @Test
    public void testCancellationFailsWhenInitializationFails() throws Exception {
        final FlinkException testException = new FlinkException("test exception");
        runCancellationFailsTest(
                resultFuture ->
                        resultFuture.complete(
                                JobManagerRunnerResult.forInitializationFailure(
                                        createFailedExecutionGraphInfo(testException),
                                        testException)));
    }

    @Test
    public void testCancellationFailsWhenExceptionOccurs() throws Exception {
        final FlinkException testException = new FlinkException("test exception");
        runCancellationFailsTest(resultFuture -> resultFuture.completeExceptionally(testException));
    }

    public void runCancellationFailsTest(
            Consumer<CompletableFuture<JobManagerRunnerResult>> testAction) throws Exception {
        final CompletableFuture<JobManagerRunnerResult> jobManagerRunnerResultFuture =
                new CompletableFuture<>();
        final JobManagerRunner jobManagerRunner =
                newJobMasterServiceLeadershipRunnerBuilder()
                        .withSingleJobMasterServiceProcess(
                                TestingJobMasterServiceProcess.newBuilder()
                                        .setIsInitialized(false)
                                        .setJobMasterGatewayFuture(new CompletableFuture<>())
                                        .setJobManagerRunnerResultFuture(
                                                jobManagerRunnerResultFuture)
                                        .build())
                        .build();

        jobManagerRunner.start();

        leaderElectionService.isLeader(UUID.randomUUID());

        // cancel while initializing
        assertThat(
                jobManagerRunner.requestJobStatus(TESTING_TIMEOUT).get(),
                is(JobStatus.INITIALIZING));

        CompletableFuture<Acknowledge> cancelFuture = jobManagerRunner.cancel(TESTING_TIMEOUT);
        assertThat(cancelFuture.isDone(), is(false));

        testAction.accept(jobManagerRunnerResultFuture);

        try {
            cancelFuture.get();
            fail();
        } catch (Throwable t) {
            assertThat(t, containsMessage("Cancellation failed."));
        }
    }

    @Test
    public void testResultFutureCompletionOfOutdatedLeaderIsIgnored() throws Exception {
        final CompletableFuture<JobManagerRunnerResult> resultFuture = new CompletableFuture<>();
        final JobMasterServiceLeadershipRunner jobManagerRunner =
                newJobMasterServiceLeadershipRunnerBuilder()
                        .withSingleJobMasterServiceProcess(
                                TestingJobMasterServiceProcess.newBuilder()
                                        .setJobManagerRunnerResultFuture(resultFuture)
                                        .build())
                        .build();

        jobManagerRunner.start();

        leaderElectionService.isLeader(UUID.randomUUID()).join();

        leaderElectionService.notLeader();

        resultFuture.complete(
                JobManagerRunnerResult.forSuccess(
                        createFailedExecutionGraphInfo(new FlinkException("test exception"))));

        assertThat(jobManagerRunner.getResultFuture(), willNotComplete(Duration.ofMillis(5L)));
    }

    @Test
    public void testJobMasterGatewayIsInvalidatedOnLeadershipChanges() throws Exception {
        final JobMasterServiceLeadershipRunner jobManagerRunner =
                newJobMasterServiceLeadershipRunnerBuilder()
                        .withSingleJobMasterServiceProcess(
                                TestingJobMasterServiceProcess.newBuilder()
                                        .setJobMasterGatewayFuture(new CompletableFuture<>())
                                        .build())
                        .build();

        jobManagerRunner.start();

        final CompletableFuture<JobMasterGateway> jobMasterGateway =
                jobManagerRunner.getJobMasterGateway();

        leaderElectionService.isLeader(UUID.randomUUID());

        leaderElectionService.notLeader();

        assertThat(
                jobMasterGateway,
                FlinkMatchers.futureWillCompleteExceptionally(Duration.ofMillis(5L)));
    }

    @Test
    public void testLeaderAddressOfOutdatedLeaderIsIgnored() throws Exception {
        final CompletableFuture<String> leaderAddressFuture = new CompletableFuture<>();
        final JobMasterServiceLeadershipRunner jobManagerRunner =
                newJobMasterServiceLeadershipRunnerBuilder()
                        .withSingleJobMasterServiceProcess(
                                TestingJobMasterServiceProcess.newBuilder()
                                        .setLeaderAddressFuture(leaderAddressFuture)
                                        .build())
                        .build();

        jobManagerRunner.start();

        final CompletableFuture<UUID> leaderFuture =
                leaderElectionService.isLeader(UUID.randomUUID());

        leaderElectionService.notLeader();

        leaderAddressFuture.complete("foobar");

        assertThat(leaderFuture, willNotComplete(Duration.ofMillis(5L)));
    }

    @Test
    public void testInitialJobStatusIsInitializing() throws Exception {
        final JobMasterServiceLeadershipRunner jobManagerRunner =
                newJobMasterServiceLeadershipRunnerBuilder().build();

        jobManagerRunner.start();

        assertThat(
                jobManagerRunner.requestJobStatus(TESTING_TIMEOUT).join(),
                is(JobStatus.INITIALIZING));
    }

    @Test
    public void testCancellationChangesJobStatusToCancelling() throws Exception {
        final JobMasterServiceLeadershipRunner jobManagerRunner =
                newJobMasterServiceLeadershipRunnerBuilder().build();

        jobManagerRunner.start();

        jobManagerRunner.cancel(TESTING_TIMEOUT);

        assertThat(
                jobManagerRunner.requestJobStatus(TESTING_TIMEOUT).join(),
                is(JobStatus.CANCELLING));
    }

    @Test
    public void testJobStatusCancellingIsClearedOnLeadershipLoss() throws Exception {
        final JobMasterServiceLeadershipRunner jobManagerRunner =
                newJobMasterServiceLeadershipRunnerBuilder().build();

        jobManagerRunner.start();

        jobManagerRunner.cancel(TESTING_TIMEOUT);

        leaderElectionService.isLeader(UUID.randomUUID());
        leaderElectionService.notLeader();

        assertThat(
                jobManagerRunner.requestJobStatus(TESTING_TIMEOUT).join(),
                is(JobStatus.INITIALIZING));
    }

    @Test
    public void testJobMasterServiceProcessClosingExceptionIsForwardedToResultFuture()
            throws Exception {
        final CompletableFuture<Void> terminationFuture = new CompletableFuture<>();
        final JobMasterServiceLeadershipRunner jobManagerRunner =
                newJobMasterServiceLeadershipRunnerBuilder()
                        .withSingleJobMasterServiceProcess(
                                TestingJobMasterServiceProcess.newBuilder()
                                        .setTerminationFuture(terminationFuture)
                                        .withManualTerminationFutureCompletion()
                                        .build())
                        .build();

        jobManagerRunner.start();

        leaderElectionService.isLeader(UUID.randomUUID());
        leaderElectionService.notLeader();

        final FlinkException testException = new FlinkException("Test exception");
        terminationFuture.completeExceptionally(testException);

        assertThat(
                jobManagerRunner.getResultFuture(),
                FlinkMatchers.futureWillCompleteExceptionally(
                        cause ->
                                ExceptionUtils.findThrowable(cause, testException::equals)
                                        .isPresent(),
                        Duration.ofMillis(5L),
                        "Result future should be completed exceptionally."));
    }

    @Test
    public void testJobMasterServiceProcessCreationFailureIsForwardedToResultFuture()
            throws Exception {
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

        leaderElectionService.isLeader(UUID.randomUUID());

        assertThat(
                jobManagerRunner.getResultFuture(),
                FlinkMatchers.futureWillCompleteExceptionally(
                        cause ->
                                ExceptionUtils.findThrowable(cause, testException::equals)
                                        .isPresent(),
                        Duration.ofMillis(5L),
                        "Result future should be completed exceptionally."));
    }

    @Test
    public void testJobAlreadyDone() throws Exception {
        JobID jobID = new JobID();
        try (JobManagerRunner jobManagerRunner =
                newJobMasterServiceLeadershipRunnerBuilder()
                        .setJobMasterServiceProcessFactory(
                                TestingJobMasterServiceProcessFactory.newBuilder()
                                        .setJobId(jobID)
                                        .build())
                        .build()) {
            runningJobsRegistry.setJobFinished(jobID);
            jobManagerRunner.start();
            leaderElectionService.isLeader(UUID.randomUUID());

            final CompletableFuture<JobManagerRunnerResult> resultFuture =
                    jobManagerRunner.getResultFuture();

            JobManagerRunnerResult result = resultFuture.get();
            assertEquals(
                    JobStatus.FAILED,
                    result.getExecutionGraphInfo().getArchivedExecutionGraph().getState());
        }
    }

    private void assertJobNotFinished(CompletableFuture<JobManagerRunnerResult> resultFuture)
            throws ExecutionException, InterruptedException {
        final JobManagerRunnerResult jobManagerRunnerResult = resultFuture.get();
        assertEquals(
                jobManagerRunnerResult
                        .getExecutionGraphInfo()
                        .getArchivedExecutionGraph()
                        .getState(),
                JobStatus.SUSPENDED);
    }

    public JobMasterServiceLeadershipRunnerBuilder newJobMasterServiceLeadershipRunnerBuilder() {
        return new JobMasterServiceLeadershipRunnerBuilder();
    }

    public class JobMasterServiceLeadershipRunnerBuilder {
        private JobMasterServiceProcessFactory jobMasterServiceProcessFactory =
                TestingJobMasterServiceProcessFactory.newBuilder().build();
        private LibraryCacheManager.ClassLoaderLease classLoaderLease =
                TestingClassLoaderLease.newBuilder().build();

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

        public JobMasterServiceLeadershipRunner build() {
            return new JobMasterServiceLeadershipRunner(
                    jobMasterServiceProcessFactory,
                    leaderElectionService,
                    runningJobsRegistry,
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
