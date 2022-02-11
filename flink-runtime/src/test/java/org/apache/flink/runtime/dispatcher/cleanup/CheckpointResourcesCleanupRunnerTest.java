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

package org.apache.flink.runtime.dispatcher.cleanup;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.CheckpointsCleaner;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.TestingCheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.TestingCheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.TestingCompletedCheckpointStore;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.dispatcher.UnavailableDispatcherOperationException;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.ErrorInfo;
import org.apache.flink.runtime.jobmaster.JobManagerRunnerResult;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.SharedStateRegistryFactory;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedThrowable;
import org.apache.flink.util.concurrent.Executors;
import org.apache.flink.util.function.ThrowingConsumer;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code CheckpointResourcesCleanupRunnerTest} tests the {@link CheckpointResourcesCleanupRunner}
 * implementation.
 */
public class CheckpointResourcesCleanupRunnerTest {

    private static final ThrowingConsumer<CheckpointResourcesCleanupRunner, ? extends Exception>
            BEFORE_START = ignored -> {};
    private static final ThrowingConsumer<CheckpointResourcesCleanupRunner, ? extends Exception>
            AFTER_START = CheckpointResourcesCleanupRunner::start;
    private static final ThrowingConsumer<CheckpointResourcesCleanupRunner, ? extends Exception>
            AFTER_CLOSE =
                    runner -> {
                        runner.start();
                        runner.close();
                    };

    @Test
    public void testIsInitializedBeforeStart() throws Exception {
        testIsInitialized(BEFORE_START);
    }

    @Test
    public void testIsInitializedAfterStart() throws Exception {
        testIsInitialized(AFTER_START);
    }

    @Test
    public void testIsInitializedAfterClose() throws Exception {
        testIsInitialized(AFTER_CLOSE);
    }

    private static void testIsInitialized(
            ThrowingConsumer<CheckpointResourcesCleanupRunner, ? extends Exception>
                    preCheckLifecycleHandling)
            throws Exception {
        final CheckpointResourcesCleanupRunner testInstance = new TestInstanceBuilder().build();
        preCheckLifecycleHandling.accept(testInstance);

        assertThat(testInstance.isInitialized()).isTrue();
    }

    @Test
    public void testCloseAsyncBeforeStart() {
        final CheckpointResourcesCleanupRunner testInstance = new TestInstanceBuilder().build();
        assertThat(testInstance.closeAsync()).isNotCompleted();
    }

    @Test
    public void testSuccessfulCloseAsyncAfterStart() throws Exception {
        final CompletableFuture<JobStatus> completedCheckpointStoreShutdownFuture =
                new CompletableFuture<>();
        final CompletableFuture<JobStatus> checkpointIdCounterShutdownFuture =
                new CompletableFuture<>();

        final HaltingCheckpointRecoveryFactory checkpointRecoveryFactory =
                new HaltingCheckpointRecoveryFactory(
                        completedCheckpointStoreShutdownFuture, checkpointIdCounterShutdownFuture);
        final CheckpointResourcesCleanupRunner testInstance =
                new TestInstanceBuilder()
                        .withCheckpointRecoveryFactory(checkpointRecoveryFactory)
                        .withExecutor(ForkJoinPool.commonPool())
                        .build();
        testInstance.start();

        assertThat(completedCheckpointStoreShutdownFuture)
                .as("The CompletedCheckpointStore shouldn't have been shut down, yet.")
                .isNotCompleted();
        assertThat(checkpointIdCounterShutdownFuture)
                .as("The CheckpointIDCounter shouldn't have been shut down, yet.")
                .isNotCompleted();

        assertThat(testInstance.closeAsync())
                .as(
                        "closeAsync shouldn't have been completed, yet, since the shutdown of the components is not completed.")
                .isNotCompleted();

        checkpointRecoveryFactory.triggerCreation();

        assertThat(completedCheckpointStoreShutdownFuture)
                .as("The CompletedCheckpointStore should have been shut down properly.")
                .succeedsWithin(Duration.ofMillis(100))
                .isEqualTo(JobStatus.FINISHED);
        assertThat(checkpointIdCounterShutdownFuture)
                .as("The CheckpointIDCounter should have been shut down properly.")
                .succeedsWithin(Duration.ofMillis(100))
                .isEqualTo(JobStatus.FINISHED);

        assertThat(testInstance.closeAsync()).succeedsWithin(Duration.ofMillis(100));
    }

    @Test
    public void testCloseAsyncAfterStartAndErrorInCompletedCheckpointStoreShutdown()
            throws Exception {
        final CompletableFuture<JobStatus> checkpointIdCounterShutdownFuture =
                new CompletableFuture<>();

        final HaltingCheckpointRecoveryFactory checkpointRecoveryFactory =
                new HaltingCheckpointRecoveryFactory(
                        TestingCompletedCheckpointStore.builder()
                                .withShutdownConsumer(
                                        (ignoredJobStatus, ignoredCheckpointsCleaner) -> {
                                            throw new RuntimeException(
                                                    "Expected RuntimeException simulating an error during shutdown.");
                                        })
                                .build(),
                        TestingCheckpointIDCounter.createStoreWithShutdownCheckAndNoStartAction(
                                checkpointIdCounterShutdownFuture));
        final CheckpointResourcesCleanupRunner testInstance =
                new TestInstanceBuilder()
                        .withCheckpointRecoveryFactory(checkpointRecoveryFactory)
                        .withExecutor(ForkJoinPool.commonPool())
                        .build();
        testInstance.start();

        assertThat(checkpointIdCounterShutdownFuture)
                .as("The CheckpointIDCounter shouldn't have been shut down, yet.")
                .isNotCompleted();

        assertThat(testInstance.closeAsync())
                .as(
                        "closeAsync shouldn't have been completed, yet, since the shutdown of the components is not completed.")
                .isNotCompleted();

        checkpointRecoveryFactory.triggerCreation();

        assertThat(checkpointIdCounterShutdownFuture)
                .as("The CheckpointIDCounter should have been shut down properly.")
                .succeedsWithin(Duration.ofMillis(100))
                .isEqualTo(JobStatus.FINISHED);

        assertThat(testInstance.closeAsync())
                .failsWithin(Duration.ofMillis(100))
                .withThrowableOfType(ExecutionException.class)
                .withCauseInstanceOf(RuntimeException.class);
    }

    @Test
    public void testCloseAsyncAfterStartAndErrorInCheckpointIDCounterShutdown() throws Exception {
        final CompletableFuture<JobStatus> completedCheckpointStoreShutdownFuture =
                new CompletableFuture<>();

        final HaltingCheckpointRecoveryFactory checkpointRecoveryFactory =
                new HaltingCheckpointRecoveryFactory(
                        TestingCompletedCheckpointStore
                                .createStoreWithShutdownCheckAndNoCompletedCheckpoints(
                                        completedCheckpointStoreShutdownFuture),
                        TestingCheckpointIDCounter.builder()
                                .withShutdownConsumer(
                                        ignoredJobStatus -> {
                                            throw new RuntimeException(
                                                    "Expected RuntimeException simulating an error during shutdown.");
                                        })
                                .build());
        final CheckpointResourcesCleanupRunner testInstance =
                new TestInstanceBuilder()
                        .withCheckpointRecoveryFactory(checkpointRecoveryFactory)
                        .withExecutor(ForkJoinPool.commonPool())
                        .build();
        testInstance.start();

        assertThat(completedCheckpointStoreShutdownFuture)
                .as("The CompletedCheckpointStore shouldn't have been shut down, yet.")
                .isNotCompleted();

        assertThat(testInstance.closeAsync())
                .as(
                        "closeAsync shouldn't have been completed, yet, since the shutdown of the components is not completed.")
                .isNotCompleted();

        checkpointRecoveryFactory.triggerCreation();

        assertThat(completedCheckpointStoreShutdownFuture)
                .as("The CompletedCheckpointStore should have been shut down properly.")
                .succeedsWithin(Duration.ofMillis(100))
                .isEqualTo(JobStatus.FINISHED);

        assertThat(testInstance.closeAsync())
                .failsWithin(Duration.ofMillis(100))
                .withThrowableOfType(ExecutionException.class)
                .withCauseInstanceOf(RuntimeException.class);
    }

    @Test
    public void testResultFutureWithSuccessBeforeStart() throws Exception {
        testResultFutureWithSuccess(BEFORE_START);
    }

    @Test
    public void testResultFutureWithSuccessAfterStart() throws Exception {
        testResultFutureWithSuccess(AFTER_START);
    }

    @Test
    public void testResultFutureWithSuccessAfterClose() throws Exception {
        testResultFutureWithSuccess(AFTER_CLOSE);
    }

    private static void testResultFutureWithSuccess(
            ThrowingConsumer<CheckpointResourcesCleanupRunner, ? extends Exception>
                    preCheckLifecycleHandling)
            throws Exception {
        testResultFuture(createDummySuccessJobResult(), preCheckLifecycleHandling);
    }

    @Test
    public void testResultFutureWithErrorBeforeStart() throws Exception {
        testResultFutureWithError(BEFORE_START);
    }

    @Test
    public void testResultFutureWithErrorAfterStart() throws Exception {
        testResultFutureWithError(AFTER_START);
    }

    @Test
    public void testResultFutureWithErrorAfterClose() throws Exception {
        testResultFutureWithError(AFTER_CLOSE);
    }

    private static void testResultFutureWithError(
            ThrowingConsumer<CheckpointResourcesCleanupRunner, ? extends Exception>
                    preCheckLifecycleHandling)
            throws Exception {
        final SerializedThrowable expectedError =
                new SerializedThrowable(new Exception("Expected exception"));
        final CompletableFuture<JobManagerRunnerResult> actualResult =
                testResultFuture(
                        createJobResultWithFailure(expectedError), preCheckLifecycleHandling);

        assertThat(actualResult)
                .succeedsWithin(Duration.ZERO)
                .extracting(JobManagerRunnerResult::getExecutionGraphInfo)
                .extracting(ExecutionGraphInfo::getArchivedExecutionGraph)
                .extracting(AccessExecutionGraph::getFailureInfo)
                .extracting(ErrorInfo::getException)
                .isEqualTo(expectedError);
    }

    private static CompletableFuture<JobManagerRunnerResult> testResultFuture(
            JobResult jobResult,
            ThrowingConsumer<CheckpointResourcesCleanupRunner, ? extends Exception>
                    preCheckLifecycleHandling)
            throws Exception {
        final CheckpointResourcesCleanupRunner testInstance =
                new TestInstanceBuilder().withJobResult(jobResult).build();
        preCheckLifecycleHandling.accept(testInstance);

        assertThat(testInstance.getResultFuture())
                .succeedsWithin(Duration.ZERO)
                .extracting(JobManagerRunnerResult::isSuccess)
                .isEqualTo(true);

        return testInstance.getResultFuture();
    }

    @Test
    public void testGetJobID() {
        final JobID jobId = new JobID();
        final CheckpointResourcesCleanupRunner testInstance =
                new TestInstanceBuilder()
                        .withJobResult(createJobResult(jobId, ApplicationStatus.CANCELED))
                        .build();
        assertThat(testInstance.getJobID()).isEqualTo(jobId);
    }

    @Test
    public void testGetJobMasterGatewayBeforeStart() throws Exception {
        testGetJobMasterGateway(BEFORE_START);
    }

    @Test
    public void testGetJobMasterGatewayAfterStart() throws Exception {
        testGetJobMasterGateway(AFTER_START);
    }

    @Test
    public void testGetJobMasterGatewayAfterClose() throws Exception {
        testGetJobMasterGateway(AFTER_CLOSE);
    }

    private static void testGetJobMasterGateway(
            ThrowingConsumer<CheckpointResourcesCleanupRunner, ? extends Exception>
                    preCheckLifecycleHandling)
            throws Exception {
        final CheckpointResourcesCleanupRunner testInstance = new TestInstanceBuilder().build();
        preCheckLifecycleHandling.accept(testInstance);

        assertThat(testInstance.getJobMasterGateway())
                .failsWithin(Duration.ZERO)
                .withThrowableOfType(ExecutionException.class)
                .withCauseInstanceOf(UnavailableDispatcherOperationException.class);
    }

    @Test
    public void testRequestJob_ExceptionHistory() {
        testRequestJob(
                createDummySuccessJobResult(),
                System.currentTimeMillis(),
                actualExecutionGraphInfo ->
                        assertThat(actualExecutionGraphInfo)
                                .extracting(ExecutionGraphInfo::getExceptionHistory)
                                .asList()
                                .isEmpty());
    }

    @Test
    public void testRequestJob_JobName() {
        testRequestJobExecutionGraph(
                createDummySuccessJobResult(),
                System.currentTimeMillis(),
                actualExecutionGraph ->
                        assertThat(actualExecutionGraph)
                                .extracting(AccessExecutionGraph::getJobName)
                                .isEqualTo("unknown"));
    }

    @Test
    public void testRequestJob_JobId() {
        final JobResult jobResult = createDummySuccessJobResult();
        testRequestJobExecutionGraph(
                jobResult,
                System.currentTimeMillis(),
                actualExecutionGraph ->
                        assertThat(actualExecutionGraph)
                                .extracting(AccessExecutionGraph::getJobID)
                                .isEqualTo(jobResult.getJobId()));
    }

    @Test
    public void testRequestJob_JobState() {
        final JobResult jobResult = createDummySuccessJobResult();
        testRequestJobExecutionGraph(
                jobResult,
                System.currentTimeMillis(),
                actualExecutionGraph ->
                        assertThat(actualExecutionGraph)
                                .extracting(AccessExecutionGraph::getState)
                                .isEqualTo(jobResult.getApplicationStatus().deriveJobStatus()));
    }

    @Test
    public void testRequestJob_InitiatizationTimestamp() {
        final long initializationTimestamp = System.currentTimeMillis();
        testRequestJobExecutionGraph(
                createDummySuccessJobResult(),
                initializationTimestamp,
                actualExecutionGraph ->
                        assertThat(actualExecutionGraph.getStatusTimestamp(JobStatus.INITIALIZING))
                                .isEqualTo(initializationTimestamp));
    }

    @Test
    public void testRequestJobWithFailure() {
        final SerializedThrowable expectedError =
                new SerializedThrowable(new Exception("Expected exception"));
        final JobResult jobResult = createJobResultWithFailure(expectedError);
        testRequestJobExecutionGraph(
                jobResult,
                System.currentTimeMillis(),
                actualExecutionGraph ->
                        assertThat(actualExecutionGraph)
                                .extracting(AccessExecutionGraph::getFailureInfo)
                                .extracting(ErrorInfo::getException)
                                .isEqualTo(expectedError));
    }

    private static void testRequestJobExecutionGraph(
            JobResult jobResult,
            long initializationTimestamp,
            ThrowingConsumer<AccessExecutionGraph, ? extends Exception> assertion) {
        testRequestJob(
                jobResult,
                initializationTimestamp,
                actualExecutionGraphInfo ->
                        assertThat(actualExecutionGraphInfo)
                                .extracting(ExecutionGraphInfo::getArchivedExecutionGraph)
                                .satisfies(assertion::accept));
    }

    private static void testRequestJob(
            JobResult jobResult,
            long initializationTimestamp,
            ThrowingConsumer<ExecutionGraphInfo, ? extends Exception> assertion) {
        final CheckpointResourcesCleanupRunner testInstance =
                new TestInstanceBuilder()
                        .withJobResult(jobResult)
                        .withInitializationTimestamp(initializationTimestamp)
                        .build();

        final CompletableFuture<ExecutionGraphInfo> response =
                testInstance.requestJob(Time.milliseconds(0));
        assertThat(response).succeedsWithin(Duration.ZERO).satisfies(assertion::accept);
    }

    private static JobResult createDummySuccessJobResult() {
        return createJobResult(new JobID(), ApplicationStatus.SUCCEEDED);
    }

    private static JobResult createJobResultWithFailure(SerializedThrowable throwable) {
        return new JobResult.Builder()
                .jobId(new JobID())
                .applicationStatus(ApplicationStatus.FAILED)
                .serializedThrowable(throwable)
                .netRuntime(1)
                .build();
    }

    private static JobResult createJobResult(JobID jobId, ApplicationStatus applicationStatus) {
        return new JobResult.Builder()
                .jobId(jobId)
                .applicationStatus(applicationStatus)
                .netRuntime(1)
                .build();
    }

    private static CheckpointRecoveryFactory createCheckpointRecoveryFactory() {
        return new TestingCheckpointRecoveryFactory(
                TestingCompletedCheckpointStore
                        .createStoreWithShutdownCheckAndNoCompletedCheckpoints(
                                new CompletableFuture<>()),
                TestingCheckpointIDCounter.createStoreWithShutdownCheckAndNoStartAction(
                        new CompletableFuture<>()));
    }

    private static class TestInstanceBuilder {

        private JobResult jobResult = createDummySuccessJobResult();
        private CheckpointRecoveryFactory checkpointRecoveryFactory =
                createCheckpointRecoveryFactory();
        private CheckpointsCleaner checkpointsCleaner = new CheckpointsCleaner();
        private SharedStateRegistryFactory sharedStateRegistryFactory =
                SharedStateRegistry.DEFAULT_FACTORY;
        private Executor executor = Executors.directExecutor();
        private Configuration configuration = new Configuration();
        private long initializationTimestamp = System.currentTimeMillis();

        public TestInstanceBuilder withJobResult(JobResult jobResult) {
            this.jobResult = jobResult;
            return this;
        }

        public TestInstanceBuilder withCheckpointRecoveryFactory(
                CheckpointRecoveryFactory checkpointRecoveryFactory) {
            this.checkpointRecoveryFactory = checkpointRecoveryFactory;
            return this;
        }

        public TestInstanceBuilder withCheckpointsCleaner(CheckpointsCleaner checkpointsCleaner) {
            this.checkpointsCleaner = checkpointsCleaner;
            return this;
        }

        public TestInstanceBuilder withSharedStateRegistryFactory(
                SharedStateRegistryFactory sharedStateRegistryFactory) {
            this.sharedStateRegistryFactory = sharedStateRegistryFactory;
            return this;
        }

        public TestInstanceBuilder withExecutor(Executor executor) {
            this.executor = executor;
            return this;
        }

        public TestInstanceBuilder withConfiguration(Configuration configuration) {
            this.configuration = configuration;
            return this;
        }

        public TestInstanceBuilder withInitializationTimestamp(long initializationTimestamp) {
            this.initializationTimestamp = initializationTimestamp;
            return this;
        }

        public CheckpointResourcesCleanupRunner build() {
            return new CheckpointResourcesCleanupRunner(
                    jobResult,
                    checkpointRecoveryFactory,
                    checkpointsCleaner,
                    sharedStateRegistryFactory,
                    configuration,
                    executor,
                    initializationTimestamp);
        }
    }

    private static class HaltingCheckpointRecoveryFactory implements CheckpointRecoveryFactory {

        private final CompletedCheckpointStore completedCheckpointStore;
        private final CheckpointIDCounter checkpointIDCounter;

        private final OneShotLatch creationLatch = new OneShotLatch();

        public HaltingCheckpointRecoveryFactory(
                CompletableFuture<JobStatus> completableCheckpointStoreShutDownFuture,
                CompletableFuture<JobStatus> checkpointIDCounterShutDownFuture) {
            this(
                    TestingCompletedCheckpointStore
                            .createStoreWithShutdownCheckAndNoCompletedCheckpoints(
                                    completableCheckpointStoreShutDownFuture),
                    TestingCheckpointIDCounter.createStoreWithShutdownCheckAndNoStartAction(
                            checkpointIDCounterShutDownFuture));
        }

        public HaltingCheckpointRecoveryFactory(
                CompletedCheckpointStore completedCheckpointStore,
                CheckpointIDCounter checkpointIDCounter) {
            this.completedCheckpointStore = Preconditions.checkNotNull(completedCheckpointStore);
            this.checkpointIDCounter = Preconditions.checkNotNull(checkpointIDCounter);
        }

        @Override
        public CompletedCheckpointStore createRecoveredCompletedCheckpointStore(
                JobID jobId,
                int maxNumberOfCheckpointsToRetain,
                SharedStateRegistryFactory sharedStateRegistryFactory,
                Executor ioExecutor)
                throws Exception {
            creationLatch.await();
            return completedCheckpointStore;
        }

        @Override
        public CheckpointIDCounter createCheckpointIDCounter(JobID jobId) throws Exception {
            creationLatch.await();
            return checkpointIDCounter;
        }

        public void triggerCreation() {
            creationLatch.trigger();
        }
    }
}
