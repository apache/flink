/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.flink.core.testutils.FlinkAssertions;
import org.apache.flink.runtime.client.JobInitializationException;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ErrorInfo;
import org.apache.flink.runtime.jobmaster.factories.TestingJobMasterServiceFactory;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGateway;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGatewayBuilder;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedExecutionGraphBuilder;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.runtime.scheduler.exceptionhistory.RootExceptionHistoryEntry;
import org.apache.flink.util.SerializedThrowable;

import org.apache.flink.shaded.guava31.com.google.common.collect.Iterables;

import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import static org.apache.flink.core.testutils.FlinkAssertions.assertThatFuture;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link DefaultJobMasterServiceProcess}. */
class DefaultJobMasterServiceProcessTest {
    private static final JobID jobId = new JobID();
    private static final Function<Throwable, ArchivedExecutionGraph>
            failedArchivedExecutionGraphFactory =
                    (throwable ->
                            ArchivedExecutionGraph.createSparseArchivedExecutionGraph(
                                    jobId, "test", JobStatus.FAILED, throwable, null, 1337));

    @Test
    void testInitializationFailureCompletesResultFuture() {
        final CompletableFuture<JobMasterService> jobMasterServiceFuture =
                new CompletableFuture<>();
        DefaultJobMasterServiceProcess serviceProcess = createTestInstance(jobMasterServiceFuture);
        final RuntimeException originalCause = new RuntimeException("Init error");
        jobMasterServiceFuture.completeExceptionally(originalCause);

        final JobManagerRunnerResult actualJobManagerResult =
                serviceProcess.getResultFuture().join();
        assertThat(actualJobManagerResult.isInitializationFailure()).isTrue();
        final Throwable initializationFailure = actualJobManagerResult.getInitializationFailure();

        assertThat(initializationFailure)
                .isInstanceOf(JobInitializationException.class)
                .hasCause(originalCause);
    }

    @Test
    void testInitializationFailureSetsFailureInfoProperly()
            throws ExecutionException, InterruptedException {
        final CompletableFuture<JobMasterService> jobMasterServiceFuture =
                new CompletableFuture<>();
        DefaultJobMasterServiceProcess serviceProcess = createTestInstance(jobMasterServiceFuture);
        final RuntimeException originalCause = new RuntimeException("Expected RuntimeException");

        long beforeFailureTimestamp = System.currentTimeMillis();
        jobMasterServiceFuture.completeExceptionally(originalCause);
        long afterFailureTimestamp = System.currentTimeMillis();

        final JobManagerRunnerResult result = serviceProcess.getResultFuture().get();
        final ErrorInfo executionGraphFailure =
                result.getExecutionGraphInfo().getArchivedExecutionGraph().getFailureInfo();

        assertThat(executionGraphFailure).isNotNull();
        assertInitializationException(
                executionGraphFailure.getException(),
                originalCause,
                executionGraphFailure.getTimestamp(),
                beforeFailureTimestamp,
                afterFailureTimestamp);
    }

    @Test
    void testInitializationFailureSetsExceptionHistoryProperly()
            throws ExecutionException, InterruptedException {
        final CompletableFuture<JobMasterService> jobMasterServiceFuture =
                new CompletableFuture<>();
        DefaultJobMasterServiceProcess serviceProcess = createTestInstance(jobMasterServiceFuture);
        final RuntimeException originalCause = new RuntimeException("Expected RuntimeException");

        long beforeFailureTimestamp = System.currentTimeMillis();
        jobMasterServiceFuture.completeExceptionally(originalCause);
        long afterFailureTimestamp = System.currentTimeMillis();

        final RootExceptionHistoryEntry entry =
                Iterables.getOnlyElement(
                        serviceProcess
                                .getResultFuture()
                                .get()
                                .getExecutionGraphInfo()
                                .getExceptionHistory());

        assertInitializationException(
                entry.getException(),
                originalCause,
                entry.getTimestamp(),
                beforeFailureTimestamp,
                afterFailureTimestamp);
        assertThat(entry.isGlobal()).isTrue();
    }

    private static void assertInitializationException(
            SerializedThrowable actualException,
            Throwable expectedCause,
            long actualTimestamp,
            long expectedLowerTimestampThreshold,
            long expectedUpperTimestampThreshold) {
        final Throwable deserializedException =
                actualException.deserializeError(Thread.currentThread().getContextClassLoader());

        assertThat(deserializedException)
                .isInstanceOf(JobInitializationException.class)
                .hasCause(expectedCause);

        assertThat(actualTimestamp)
                .isGreaterThanOrEqualTo(expectedLowerTimestampThreshold)
                .isLessThanOrEqualTo(expectedUpperTimestampThreshold);
    }

    @Test
    void testCloseAfterInitializationFailure() throws Exception {
        final CompletableFuture<JobMasterService> jobMasterServiceFuture =
                new CompletableFuture<>();
        DefaultJobMasterServiceProcess serviceProcess = createTestInstance(jobMasterServiceFuture);
        jobMasterServiceFuture.completeExceptionally(new RuntimeException("Init error"));

        serviceProcess.closeAsync().get();
        assertThat(serviceProcess.getResultFuture())
                .isCompletedWithValueMatching(JobManagerRunnerResult::isInitializationFailure);
        assertThat(serviceProcess.getJobMasterGatewayFuture()).isCompletedExceptionally();
    }

    @Test
    void testCloseAfterInitializationSuccess() throws Exception {
        final CompletableFuture<JobMasterService> jobMasterServiceFuture =
                new CompletableFuture<>();
        DefaultJobMasterServiceProcess serviceProcess = createTestInstance(jobMasterServiceFuture);
        TestingJobMasterService testingJobMasterService = new TestingJobMasterService();
        jobMasterServiceFuture.complete(testingJobMasterService);

        serviceProcess.closeAsync().get();
        assertThat(testingJobMasterService.isClosed()).isTrue();
        assertThatFuture(serviceProcess.getResultFuture())
                .eventuallyFailsWith(ExecutionException.class)
                .extracting(FlinkAssertions::chainOfCauses, FlinkAssertions.STREAM_THROWABLE)
                .anySatisfy(t -> assertThat(t).isInstanceOf(JobNotFinishedException.class));
    }

    @Test
    void testJobMasterTerminationIsHandled() {
        final CompletableFuture<JobMasterService> jobMasterServiceFuture =
                new CompletableFuture<>();
        DefaultJobMasterServiceProcess serviceProcess = createTestInstance(jobMasterServiceFuture);
        CompletableFuture<Void> jobMasterTerminationFuture = new CompletableFuture<>();
        TestingJobMasterService testingJobMasterService =
                new TestingJobMasterService("localhost", jobMasterTerminationFuture, null);
        jobMasterServiceFuture.complete(testingJobMasterService);

        RuntimeException testException = new RuntimeException("Fake exception from JobMaster");
        jobMasterTerminationFuture.completeExceptionally(testException);

        assertThatFuture(serviceProcess.getResultFuture())
                .eventuallyFailsWith(ExecutionException.class)
                .extracting(FlinkAssertions::chainOfCauses, FlinkAssertions.STREAM_THROWABLE)
                .anySatisfy(t -> assertThat(t).isEqualTo(testException));
    }

    @Test
    void testJobMasterGatewayGetsForwarded() {
        final CompletableFuture<JobMasterService> jobMasterServiceFuture =
                new CompletableFuture<>();
        DefaultJobMasterServiceProcess serviceProcess = createTestInstance(jobMasterServiceFuture);
        TestingJobMasterGateway testingGateway = new TestingJobMasterGatewayBuilder().build();
        TestingJobMasterService testingJobMasterService =
                new TestingJobMasterService("localhost", null, testingGateway);
        jobMasterServiceFuture.complete(testingJobMasterService);

        assertThat(serviceProcess.getJobMasterGatewayFuture()).isCompletedWithValue(testingGateway);
    }

    @Test
    void testLeaderAddressGetsForwarded() throws Exception {
        final CompletableFuture<JobMasterService> jobMasterServiceFuture =
                new CompletableFuture<>();
        DefaultJobMasterServiceProcess serviceProcess = createTestInstance(jobMasterServiceFuture);
        String testingAddress = "yolohost";
        TestingJobMasterService testingJobMasterService =
                new TestingJobMasterService(testingAddress, null, null);
        jobMasterServiceFuture.complete(testingJobMasterService);

        assertThat(serviceProcess.getLeaderAddressFuture()).isCompletedWithValue(testingAddress);
    }

    @Test
    void testIsNotInitialized() {
        DefaultJobMasterServiceProcess serviceProcess =
                createTestInstance(new CompletableFuture<>());
        assertThat(serviceProcess.isInitializedAndRunning()).isFalse();
    }

    @Test
    void testIsInitialized() {
        final CompletableFuture<JobMasterService> jobMasterServiceFuture =
                new CompletableFuture<>();
        DefaultJobMasterServiceProcess serviceProcess = createTestInstance(jobMasterServiceFuture);

        jobMasterServiceFuture.complete(new TestingJobMasterService());

        assertThat(serviceProcess.isInitializedAndRunning()).isTrue();
    }

    @Test
    void testIsNotInitializedAfterClosing() {
        final CompletableFuture<JobMasterService> jobMasterServiceFuture =
                new CompletableFuture<>();
        DefaultJobMasterServiceProcess serviceProcess = createTestInstance(jobMasterServiceFuture);

        jobMasterServiceFuture.complete(new TestingJobMasterService());

        serviceProcess.closeAsync();

        assertThat(serviceProcess.isInitializedAndRunning()).isFalse();
    }

    @Test
    void testSuccessOnTerminalState() throws Exception {
        final CompletableFuture<JobMasterService> jobMasterServiceFuture =
                new CompletableFuture<>();
        DefaultJobMasterServiceProcess serviceProcess = createTestInstance(jobMasterServiceFuture);
        jobMasterServiceFuture.complete(new TestingJobMasterService());
        ArchivedExecutionGraph archivedExecutionGraph =
                new ArchivedExecutionGraphBuilder().setState(JobStatus.FINISHED).build();

        serviceProcess.jobReachedGloballyTerminalState(
                new ExecutionGraphInfo(archivedExecutionGraph));

        assertThat(serviceProcess.getResultFuture())
                .isCompletedWithValueMatching(JobManagerRunnerResult::isSuccess)
                .isCompletedWithValueMatching(
                        r ->
                                r.getExecutionGraphInfo().getArchivedExecutionGraph().getState()
                                        == JobStatus.FINISHED);
    }

    private DefaultJobMasterServiceProcess createTestInstance(
            CompletableFuture<JobMasterService> jobMasterServiceFuture) {

        return new DefaultJobMasterServiceProcess(
                jobId,
                UUID.randomUUID(),
                new TestingJobMasterServiceFactory(ignored -> jobMasterServiceFuture),
                failedArchivedExecutionGraphFactory);
    }
}
