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
import org.apache.flink.core.testutils.FlinkMatchers;
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
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava30.com.google.common.collect.Iterables;

import org.hamcrest.CoreMatchers;
import org.junit.Test;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import static org.apache.flink.core.testutils.FlinkMatchers.containsCause;
import static org.apache.flink.core.testutils.FlinkMatchers.containsMessage;
import static org.apache.flink.core.testutils.FlinkMatchers.futureWillCompleteExceptionally;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests for the {@link DefaultJobMasterServiceProcess}. */
public class DefaultJobMasterServiceProcessTest extends TestLogger {
    private static final Duration TIMEOUT = Duration.ofSeconds(10);
    private static final JobID jobId = new JobID();
    private static final Function<Throwable, ArchivedExecutionGraph>
            failedArchivedExecutionGraphFactory =
                    (throwable ->
                            ArchivedExecutionGraph.createFromInitializingJob(
                                    jobId, "test", JobStatus.FAILED, throwable, null, 1337));

    @Test
    public void testInitializationFailureCompletesResultFuture() {
        final CompletableFuture<JobMasterService> jobMasterServiceFuture =
                new CompletableFuture<>();
        DefaultJobMasterServiceProcess serviceProcess = createTestInstance(jobMasterServiceFuture);
        final RuntimeException originalCause = new RuntimeException("Init error");
        jobMasterServiceFuture.completeExceptionally(originalCause);

        assertTrue(serviceProcess.getResultFuture().join().isInitializationFailure());
        final Throwable initializationFailure =
                serviceProcess.getResultFuture().join().getInitializationFailure();
        assertThat(initializationFailure, containsCause(JobInitializationException.class));
        assertThat(initializationFailure, containsCause(originalCause));
    }

    @Test
    public void testInitializationFailureSetsFailureInfoProperly()
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

        assertThat(executionGraphFailure, is(notNullValue()));
        assertInitializationException(
                executionGraphFailure.getException(),
                originalCause,
                executionGraphFailure.getTimestamp(),
                beforeFailureTimestamp,
                afterFailureTimestamp);
    }

    @Test
    public void testInitializationFailureSetsExceptionHistoryProperly()
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
        assertThat(entry.isGlobal(), is(true));
    }

    private static void assertInitializationException(
            SerializedThrowable actualException,
            Throwable expectedCause,
            long actualTimestamp,
            long expectedLowerTimestampThreshold,
            long expectedUpperTimestampThreshold) {
        final Throwable deserializedException =
                actualException.deserializeError(Thread.currentThread().getContextClassLoader());

        assertThat(
                deserializedException, CoreMatchers.instanceOf(JobInitializationException.class));
        assertThat(deserializedException, FlinkMatchers.containsCause(expectedCause));

        assertThat(actualTimestamp, greaterThanOrEqualTo(expectedLowerTimestampThreshold));
        assertThat(actualTimestamp, lessThanOrEqualTo(expectedUpperTimestampThreshold));
    }

    @Test
    public void testCloseAfterInitializationFailure() throws Exception {
        final CompletableFuture<JobMasterService> jobMasterServiceFuture =
                new CompletableFuture<>();
        DefaultJobMasterServiceProcess serviceProcess = createTestInstance(jobMasterServiceFuture);
        jobMasterServiceFuture.completeExceptionally(new RuntimeException("Init error"));

        serviceProcess.closeAsync().get();
        assertTrue(serviceProcess.getResultFuture().join().isInitializationFailure());
        assertThat(serviceProcess.getJobMasterGatewayFuture().isCompletedExceptionally(), is(true));
    }

    @Test
    public void testCloseAfterInitializationSuccess() throws Exception {
        final CompletableFuture<JobMasterService> jobMasterServiceFuture =
                new CompletableFuture<>();
        DefaultJobMasterServiceProcess serviceProcess = createTestInstance(jobMasterServiceFuture);
        TestingJobMasterService testingJobMasterService = new TestingJobMasterService();
        jobMasterServiceFuture.complete(testingJobMasterService);

        serviceProcess.closeAsync().get();
        assertThat(testingJobMasterService.isClosed(), is(true));
        assertThat(
                serviceProcess.getResultFuture(),
                futureWillCompleteExceptionally(JobNotFinishedException.class, TIMEOUT));
    }

    @Test
    public void testJobMasterTerminationIsHandled() {
        final CompletableFuture<JobMasterService> jobMasterServiceFuture =
                new CompletableFuture<>();
        DefaultJobMasterServiceProcess serviceProcess = createTestInstance(jobMasterServiceFuture);
        CompletableFuture<Void> jobMasterTerminationFuture = new CompletableFuture<>();
        TestingJobMasterService testingJobMasterService =
                new TestingJobMasterService("localhost", jobMasterTerminationFuture, null);
        jobMasterServiceFuture.complete(testingJobMasterService);

        RuntimeException testException = new RuntimeException("Fake exception from JobMaster");
        jobMasterTerminationFuture.completeExceptionally(testException);

        try {
            serviceProcess.getResultFuture().get();
            fail("Expect failure");
        } catch (Throwable t) {
            assertThat(t, containsCause(RuntimeException.class));
            assertThat(t, containsMessage(testException.getMessage()));
        }
    }

    @Test
    public void testJobMasterGatewayGetsForwarded() throws Exception {
        final CompletableFuture<JobMasterService> jobMasterServiceFuture =
                new CompletableFuture<>();
        DefaultJobMasterServiceProcess serviceProcess = createTestInstance(jobMasterServiceFuture);
        TestingJobMasterGateway testingGateway = new TestingJobMasterGatewayBuilder().build();
        TestingJobMasterService testingJobMasterService =
                new TestingJobMasterService("localhost", null, testingGateway);
        jobMasterServiceFuture.complete(testingJobMasterService);

        assertThat(serviceProcess.getJobMasterGatewayFuture().get(), is(testingGateway));
    }

    @Test
    public void testLeaderAddressGetsForwarded() throws Exception {
        final CompletableFuture<JobMasterService> jobMasterServiceFuture =
                new CompletableFuture<>();
        DefaultJobMasterServiceProcess serviceProcess = createTestInstance(jobMasterServiceFuture);
        String testingAddress = "yolohost";
        TestingJobMasterService testingJobMasterService =
                new TestingJobMasterService(testingAddress, null, null);
        jobMasterServiceFuture.complete(testingJobMasterService);

        assertThat(serviceProcess.getLeaderAddressFuture().get(), is(testingAddress));
    }

    @Test
    public void testIsNotInitialized() {
        DefaultJobMasterServiceProcess serviceProcess =
                createTestInstance(new CompletableFuture<>());
        assertThat(serviceProcess.isInitializedAndRunning(), is(false));
    }

    @Test
    public void testIsInitialized() {
        final CompletableFuture<JobMasterService> jobMasterServiceFuture =
                new CompletableFuture<>();
        DefaultJobMasterServiceProcess serviceProcess = createTestInstance(jobMasterServiceFuture);

        jobMasterServiceFuture.complete(new TestingJobMasterService());

        assertThat(serviceProcess.isInitializedAndRunning(), is(true));
    }

    @Test
    public void testIsNotInitializedAfterClosing() {
        final CompletableFuture<JobMasterService> jobMasterServiceFuture =
                new CompletableFuture<>();
        DefaultJobMasterServiceProcess serviceProcess = createTestInstance(jobMasterServiceFuture);

        jobMasterServiceFuture.complete(new TestingJobMasterService());

        serviceProcess.closeAsync();

        assertFalse(serviceProcess.isInitializedAndRunning());
    }

    @Test
    public void testSuccessOnTerminalState() throws Exception {
        final CompletableFuture<JobMasterService> jobMasterServiceFuture =
                new CompletableFuture<>();
        DefaultJobMasterServiceProcess serviceProcess = createTestInstance(jobMasterServiceFuture);
        jobMasterServiceFuture.complete(new TestingJobMasterService());
        ArchivedExecutionGraph archivedExecutionGraph =
                new ArchivedExecutionGraphBuilder().setState(JobStatus.FINISHED).build();

        serviceProcess.jobReachedGloballyTerminalState(
                new ExecutionGraphInfo(archivedExecutionGraph));

        assertThat(serviceProcess.getResultFuture().get().isSuccess(), is(true));
        assertThat(
                serviceProcess
                        .getResultFuture()
                        .get()
                        .getExecutionGraphInfo()
                        .getArchivedExecutionGraph()
                        .getState(),
                is(JobStatus.FINISHED));
    }

    private DefaultJobMasterServiceProcess createTestInstance(
            CompletableFuture<JobMasterService> jobMasterServiceFuture) {

        return new DefaultJobMasterServiceProcess(
                jobId,
                UUID.randomUUID(),
                new TestingJobMasterServiceFactory(() -> jobMasterServiceFuture),
                failedArchivedExecutionGraphFactory);
    }
}
