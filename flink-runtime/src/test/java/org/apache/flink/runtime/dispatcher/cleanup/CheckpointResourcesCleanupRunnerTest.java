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
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.TestingCheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.TestingCheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.TestingCompletedCheckpointStore;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.dispatcher.UnavailableDispatcherOperationException;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.jobgraph.RestoreMode;
import org.apache.flink.runtime.jobmaster.JobManagerRunnerResult;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.SharedStateRegistryFactory;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedThrowable;
import org.apache.flink.util.concurrent.Executors;
import org.apache.flink.util.function.ThrowingConsumer;

import org.junit.jupiter.api.Test;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Function;

import static org.apache.flink.core.testutils.FlinkAssertions.assertThatFuture;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code CheckpointResourcesCleanupRunnerTest} tests the {@link CheckpointResourcesCleanupRunner}
 * implementation.
 */
class CheckpointResourcesCleanupRunnerTest {

    private static final Time TIMEOUT_FOR_REQUESTS = Time.milliseconds(0);

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
    void testIsInitializedBeforeStart() throws Exception {
        testIsInitialized(BEFORE_START);
    }

    @Test
    void testIsInitializedAfterStart() throws Exception {
        testIsInitialized(AFTER_START);
    }

    @Test
    void testIsInitializedAfterClose() throws Exception {
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
    void testCloseAsyncBeforeStart() {
        final CheckpointResourcesCleanupRunner testInstance = new TestInstanceBuilder().build();
        assertThat(testInstance.closeAsync()).isNotCompleted();
    }

    @Test
    void testSuccessfulCloseAsyncAfterStart() throws Exception {
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

        assertThatFuture(completedCheckpointStoreShutdownFuture)
                .as("The CompletedCheckpointStore should have been shut down properly.")
                .eventuallySucceeds()
                .isEqualTo(JobStatus.FINISHED);
        assertThatFuture(checkpointIdCounterShutdownFuture)
                .as("The CheckpointIDCounter should have been shut down properly.")
                .eventuallySucceeds()
                .isEqualTo(JobStatus.FINISHED);

        assertThatFuture(testInstance.closeAsync()).eventuallySucceeds();
    }

    @Test
    void testCloseAsyncAfterStartAndErrorInCompletedCheckpointStoreShutdown() throws Exception {
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

        assertThatFuture(checkpointIdCounterShutdownFuture)
                .as("The CheckpointIDCounter should have been shut down properly.")
                .eventuallySucceeds()
                .isEqualTo(JobStatus.FINISHED);

        assertThatFuture(testInstance.closeAsync())
                .eventuallyFailsWith(ExecutionException.class)
                .withCauseInstanceOf(RuntimeException.class);
    }

    @Test
    void testCloseAsyncAfterStartAndErrorInCheckpointIDCounterShutdown() throws Exception {
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

        assertThatFuture(completedCheckpointStoreShutdownFuture)
                .as("The CompletedCheckpointStore should have been shut down properly.")
                .eventuallySucceeds()
                .isEqualTo(JobStatus.FINISHED);

        assertThatFuture(testInstance.closeAsync())
                .eventuallyFailsWith(ExecutionException.class)
                .withCauseInstanceOf(RuntimeException.class);
    }

    @Test
    void testCancellationBeforeStart() throws Exception {
        final CheckpointResourcesCleanupRunner testInstance =
                new TestInstanceBuilder().withExecutor(ForkJoinPool.commonPool()).build();

        assertThatFuture(testInstance.cancel(TIMEOUT_FOR_REQUESTS))
                .eventuallyFailsWith(ExecutionException.class)
                .withCauseInstanceOf(FlinkException.class);
        assertThat(testInstance.closeAsync())
                .as("The closeAsync result shouldn't be completed, yet.")
                .isNotCompleted()
                .as("The closeAsync result shouldn't be cancelled.")
                .isNotCancelled();
    }

    @Test
    void testCancellationAfterStart() throws Exception {
        final HaltingCheckpointRecoveryFactory checkpointRecoveryFactory =
                new HaltingCheckpointRecoveryFactory(
                        new CompletableFuture<>(), new CompletableFuture<>());
        final CheckpointResourcesCleanupRunner testInstance =
                new TestInstanceBuilder()
                        .withCheckpointRecoveryFactory(checkpointRecoveryFactory)
                        .withExecutor(ForkJoinPool.commonPool())
                        .build();
        AFTER_START.accept(testInstance);
        assertThatFuture(testInstance.cancel(TIMEOUT_FOR_REQUESTS))
                .eventuallyFailsWith(ExecutionException.class)
                .withCauseInstanceOf(FlinkException.class);
        assertThat(testInstance.closeAsync())
                .as("The closeAsync result shouldn't be completed, yet.")
                .isNotCompleted()
                .as("The closeAsync result shouldn't be cancelled.")
                .isNotCancelled();
    }

    @Test
    void testCancellationAfterClose() throws Exception {
        final CheckpointResourcesCleanupRunner testInstance =
                new TestInstanceBuilder().withExecutor(ForkJoinPool.commonPool()).build();
        AFTER_CLOSE.accept(testInstance);
        assertThatFuture(testInstance.cancel(TIMEOUT_FOR_REQUESTS))
                .eventuallyFailsWith(ExecutionException.class)
                .withCauseInstanceOf(FlinkException.class);
        assertThat(testInstance.closeAsync())
                .as("The closeAsync result should be completed by now.")
                .isCompleted()
                .as("The closeAsync result shouldn't be cancelled.")
                .isNotCancelled();
    }

    @Test
    void testResultFutureWithSuccessBeforeStart() throws Exception {
        assertThat(getResultFutureFromTestInstance(createDummySuccessJobResult(), BEFORE_START))
                .isNotCompleted();
    }

    @Test
    void testResultFutureWithSuccessAfterStart() throws Exception {
        testResultFutureWithSuccessfulResultAfterStart(AFTER_START);
    }

    @Test
    void testResultFutureWithSuccessAfterClose() throws Exception {
        testResultFutureWithSuccessfulResultAfterStart(AFTER_CLOSE);
    }

    private void testResultFutureWithSuccessfulResultAfterStart(
            ThrowingConsumer<CheckpointResourcesCleanupRunner, ? extends Exception>
                    preCheckLifecycleHandling)
            throws Exception {
        final CompletableFuture<JobManagerRunnerResult> actualResult =
                getResultFutureFromTestInstance(
                        createDummySuccessJobResult(), preCheckLifecycleHandling);

        assertThat(actualResult)
                .isCompletedWithValueMatching(
                        JobManagerRunnerResult::isSuccess,
                        "The JobManagerRunner should have succeeded.");
    }

    @Test
    void testResultFutureWithErrorBeforeStart() throws Exception {
        final CompletableFuture<JobManagerRunnerResult> resultFuture =
                getResultFutureFromTestInstance(
                        createJobResultWithFailure(
                                new SerializedThrowable(new Exception("Expected exception"))),
                        BEFORE_START);
        assertThat(resultFuture).isNotCompleted();
    }

    @Test
    void testResultFutureWithErrorAfterStart() throws Exception {
        testResultFutureWithErrorAfterStart(AFTER_START);
    }

    @Test
    void testResultFutureWithErrorAfterClose() throws Exception {
        testResultFutureWithErrorAfterStart(AFTER_CLOSE);
    }

    private static void testResultFutureWithErrorAfterStart(
            ThrowingConsumer<CheckpointResourcesCleanupRunner, ? extends Exception>
                    preCheckLifecycleHandling)
            throws Exception {
        final SerializedThrowable expectedError =
                new SerializedThrowable(new Exception("Expected exception"));
        final CompletableFuture<JobManagerRunnerResult> actualResult =
                getResultFutureFromTestInstance(
                        createJobResultWithFailure(expectedError), preCheckLifecycleHandling);

        assertThat(actualResult)
                .isCompletedWithValueMatching(
                        jobManagerRunnerResult ->
                                Objects.requireNonNull(
                                                jobManagerRunnerResult
                                                        .getExecutionGraphInfo()
                                                        .getArchivedExecutionGraph()
                                                        .getFailureInfo())
                                        .getException()
                                        .equals(expectedError),
                        "JobManagerRunner should have failed with expected error");
    }

    private static CompletableFuture<JobManagerRunnerResult> getResultFutureFromTestInstance(
            JobResult jobResult,
            ThrowingConsumer<CheckpointResourcesCleanupRunner, ? extends Exception>
                    preCheckLifecycleHandling)
            throws Exception {
        final CheckpointResourcesCleanupRunner testInstance =
                new TestInstanceBuilder().withJobResult(jobResult).build();
        preCheckLifecycleHandling.accept(testInstance);

        return testInstance.getResultFuture();
    }

    @Test
    void testGetJobID() {
        final JobID jobId = new JobID();
        final CheckpointResourcesCleanupRunner testInstance =
                new TestInstanceBuilder()
                        .withJobResult(createJobResult(jobId, ApplicationStatus.CANCELED))
                        .build();
        assertThat(testInstance.getJobID()).isEqualTo(jobId);
    }

    @Test
    void testGetJobMasterGatewayBeforeStart() throws Exception {
        testGetJobMasterGateway(BEFORE_START);
    }

    @Test
    void testGetJobMasterGatewayAfterStart() throws Exception {
        testGetJobMasterGateway(AFTER_START);
    }

    @Test
    void testGetJobMasterGatewayAfterClose() throws Exception {
        testGetJobMasterGateway(AFTER_CLOSE);
    }

    private static void testGetJobMasterGateway(
            ThrowingConsumer<CheckpointResourcesCleanupRunner, ? extends Exception>
                    preCheckLifecycleHandling)
            throws Exception {
        final CheckpointResourcesCleanupRunner testInstance = new TestInstanceBuilder().build();
        preCheckLifecycleHandling.accept(testInstance);

        assertThatThrownBy(() -> testInstance.getJobMasterGateway().get())
                .isInstanceOf(ExecutionException.class)
                .hasCauseExactlyInstanceOf(UnavailableDispatcherOperationException.class);
    }

    @Test
    void testRequestJob_ExceptionHistory() {
        testRequestJob(
                createDummySuccessJobResult(),
                System.currentTimeMillis(),
                actualExecutionGraphInfo ->
                        !actualExecutionGraphInfo.getExceptionHistory().iterator().hasNext());
    }

    @Test
    void testRequestJob_JobName() {
        testRequestJobExecutionGraph(
                createDummySuccessJobResult(),
                System.currentTimeMillis(),
                actualExecutionGraph -> actualExecutionGraph.getJobName().equals("unknown"));
    }

    @Test
    void testRequestJob_JobId() {
        final JobResult jobResult = createDummySuccessJobResult();
        testRequestJobExecutionGraph(
                jobResult,
                System.currentTimeMillis(),
                actualExecutionGraph ->
                        actualExecutionGraph.getJobID().equals(jobResult.getJobId()));
    }

    @Test
    void testRequestJob_JobState() {
        final JobResult jobResult = createDummySuccessJobResult();
        testRequestJobExecutionGraph(
                jobResult,
                System.currentTimeMillis(),
                actualExecutionGraph ->
                        actualExecutionGraph
                                .getState()
                                .equals(jobResult.getApplicationStatus().deriveJobStatus()));
    }

    @Test
    void testRequestJob_InitiatizationTimestamp() {
        final long initializationTimestamp = System.currentTimeMillis();
        testRequestJobExecutionGraph(
                createDummySuccessJobResult(),
                initializationTimestamp,
                actualExecutionGraph ->
                        actualExecutionGraph.getStatusTimestamp(JobStatus.INITIALIZING)
                                == initializationTimestamp);
    }

    @Test
    void testRequestJobWithFailure() {
        final SerializedThrowable expectedError =
                new SerializedThrowable(new Exception("Expected exception"));
        final JobResult jobResult = createJobResultWithFailure(expectedError);
        testRequestJobExecutionGraph(
                jobResult,
                System.currentTimeMillis(),
                actualExecutionGraph ->
                        Objects.requireNonNull(actualExecutionGraph.getFailureInfo())
                                .getException()
                                .equals(expectedError));
    }

    private static void testRequestJobExecutionGraph(
            JobResult jobResult,
            long initializationTimestamp,
            Function<AccessExecutionGraph, Boolean> assertion) {
        testRequestJob(
                jobResult,
                initializationTimestamp,
                actualExecutionGraphInfo ->
                        assertion.apply(actualExecutionGraphInfo.getArchivedExecutionGraph()));
    }

    private static void testRequestJob(
            JobResult jobResult,
            long initializationTimestamp,
            Function<ExecutionGraphInfo, Boolean> assertion) {
        final CheckpointResourcesCleanupRunner testInstance =
                new TestInstanceBuilder()
                        .withJobResult(jobResult)
                        .withInitializationTimestamp(initializationTimestamp)
                        .build();

        final CompletableFuture<ExecutionGraphInfo> response =
                testInstance.requestJob(TIMEOUT_FOR_REQUESTS);
        assertThat(response).isCompletedWithValueMatching(assertion::apply);
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
                Executor ioExecutor,
                RestoreMode restoreMode)
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
