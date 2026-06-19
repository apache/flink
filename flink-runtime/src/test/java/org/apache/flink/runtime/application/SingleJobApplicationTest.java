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

package org.apache.flink.runtime.application;

import org.apache.flink.api.common.ApplicationState;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphBuilder;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.webmonitor.TestingDispatcherGateway;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.ManuallyTriggeredScheduledExecutor;
import org.apache.flink.util.concurrent.ScheduledExecutor;
import org.apache.flink.util.concurrent.ScheduledExecutorServiceAdapter;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link SingleJobApplication}. */
public class SingleJobApplicationTest {

    private static final int TIMEOUT_SECONDS = 10;

    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(4);
    private final ScheduledExecutor scheduledExecutor =
            new ScheduledExecutorServiceAdapter(executor);
    private java.util.concurrent.Executor mainThreadExecutor =
            Executors.newSingleThreadScheduledExecutor();

    private JobGraph jobGraph;

    private JobID jobId;

    private SingleJobApplication application;

    @BeforeEach
    void setup() {
        jobGraph = JobGraphBuilder.newStreamingJobGraphBuilder().setJobName("Test Job").build();
        jobId = jobGraph.getJobID();
        application = new SingleJobApplication(jobGraph);
    }

    @AfterEach
    void cleanup() {
        ExecutorUtils.gracefulShutdown(5, TimeUnit.SECONDS, executor);
    }

    @Test
    void testInitialization() {
        assertThat(application.getApplicationId().toHexString()).isEqualTo(jobId.toHexString());
        assertThat(application.getApplicationStatus()).isEqualTo(ApplicationState.CREATED);
        assertThat(application.getName()).isEqualTo("SingleJobApplication(Test Job)");
        assertThat(application.getJobs()).isEmpty();
    }

    @Test
    void testExecute() throws Exception {
        final CompletableFuture<JobID> submittedJobId = new CompletableFuture<>();

        final TestingDispatcherGateway.Builder dispatcherBuilder =
                TestingDispatcherGateway.newBuilder()
                        .setSubmitFunction(
                                jobGraph -> {
                                    submittedJobId.complete(jobGraph.getJobID());
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                });

        final CompletableFuture<Acknowledge> executeFuture =
                application.execute(
                        dispatcherBuilder.build(),
                        scheduledExecutor,
                        mainThreadExecutor,
                        exception -> {});

        executeFuture.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertThat(application.getApplicationStatus()).isEqualTo(ApplicationState.RUNNING);
        assertThat(submittedJobId).isCompletedWithValue(jobId);
    }

    @Test
    void testExecuteFailure() throws Exception {
        final TestingDispatcherGateway.Builder dispatcherBuilder =
                TestingDispatcherGateway.newBuilder()
                        .setSubmitFunction(
                                jobGraph ->
                                        FutureUtils.completedExceptionally(
                                                new FlinkRuntimeException(
                                                        "Job submission failed")));

        final CompletableFuture<Acknowledge> executeFuture =
                application.execute(
                        dispatcherBuilder.build(),
                        scheduledExecutor,
                        mainThreadExecutor,
                        exception -> {});

        assertException(executeFuture, FlinkRuntimeException.class);
        assertThat(application.getApplicationStatus()).isEqualTo(ApplicationState.FAILED);
    }

    @Test
    void testRecoveredApplication() throws Exception {
        application = new SingleJobApplication(jobGraph, true);
        final CompletableFuture<JobID> submittedJobId = new CompletableFuture<>();

        final TestingDispatcherGateway.Builder dispatcherBuilder =
                TestingDispatcherGateway.newBuilder()
                        .setSubmitFunction(
                                jobGraph -> {
                                    submittedJobId.completeExceptionally(
                                            new FlinkRuntimeException(
                                                    "Should not submit job in recovered application"));
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                });

        final CompletableFuture<Acknowledge> executeFuture =
                application.execute(
                        dispatcherBuilder.build(),
                        scheduledExecutor,
                        mainThreadExecutor,
                        exception -> {});

        executeFuture.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertThat(application.getApplicationStatus()).isEqualTo(ApplicationState.RUNNING);
        assertThat(submittedJobId).isNotCompleted();
    }

    @Test
    void testCancelWhenCreated() {
        // cancel when the application is in CREATED state
        application.cancel();

        assertThat(application.getApplicationStatus()).isEqualTo(ApplicationState.CANCELED);
    }

    @Test
    void testCancel() throws ExecutionException, InterruptedException, TimeoutException {
        final CompletableFuture<JobID> canceledJobId = new CompletableFuture<>();

        final TestingDispatcherGateway.Builder dispatcherBuilder =
                TestingDispatcherGateway.newBuilder()
                        .setSubmitFunction(
                                jobGraph -> CompletableFuture.completedFuture(Acknowledge.get()))
                        .setCancelJobFunction(
                                jobId -> {
                                    canceledJobId.complete(jobGraph.getJobID());
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                });

        // execute the application first to get it into RUNNING state
        final CompletableFuture<Acknowledge> executeFuture =
                application.execute(
                        dispatcherBuilder.build(),
                        scheduledExecutor,
                        mainThreadExecutor,
                        exception -> {});

        executeFuture.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertThat(application.getApplicationStatus()).isEqualTo(ApplicationState.RUNNING);

        application.cancel();

        assertThat(application.getApplicationStatus()).isEqualTo(ApplicationState.CANCELING);
        assertThat(canceledJobId).isCompletedWithValue(jobId);
    }

    @Test
    void testCancelFailure() throws Exception {
        final CompletableFuture<Void> errorHandlerFuture = new CompletableFuture<>();

        final TestingDispatcherGateway.Builder dispatcherBuilder =
                TestingDispatcherGateway.newBuilder()
                        .setSubmitFunction(
                                jobGraph -> CompletableFuture.completedFuture(Acknowledge.get()))
                        .setCancelJobFunction(
                                jobId ->
                                        FutureUtils.completedExceptionally(
                                                new FlinkRuntimeException(
                                                        "Job cancellation failed")));

        // execute the application first to get it into RUNNING state
        final CompletableFuture<Acknowledge> executeFuture =
                application.execute(
                        dispatcherBuilder.build(),
                        scheduledExecutor,
                        mainThreadExecutor,
                        errorHandlerFuture::completeExceptionally);

        executeFuture.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertThat(application.getApplicationStatus()).isEqualTo(ApplicationState.RUNNING);

        application.cancel();

        // error handler should be called
        assertException(errorHandlerFuture, FlinkRuntimeException.class);
    }

    @Test
    void testJobStatusChangesToCanceledDueToApplicationCancellation() throws Exception {
        // change mainThreadExecutor to be manually triggered to control the state transition
        mainThreadExecutor = new ManuallyTriggeredScheduledExecutor();
        final ManuallyTriggeredScheduledExecutor manuallyTriggeredMainThreadExecutor =
                (ManuallyTriggeredScheduledExecutor) mainThreadExecutor;

        final TestingDispatcherGateway.Builder dispatcherBuilder =
                TestingDispatcherGateway.newBuilder()
                        .setSubmitFunction(
                                jobGraph -> CompletableFuture.completedFuture(Acknowledge.get()));

        // execute the application first to get it into RUNNING state
        final CompletableFuture<Acknowledge> executeFuture =
                application.execute(
                        dispatcherBuilder.build(),
                        scheduledExecutor,
                        mainThreadExecutor,
                        exception -> {});

        executeFuture.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertThat(application.getApplicationStatus()).isEqualTo(ApplicationState.RUNNING);

        application.cancel();

        assertThat(application.getApplicationStatus()).isEqualTo(ApplicationState.CANCELING);

        // simulate job status change to CANCELED
        application.jobStatusChanges(jobId, JobStatus.CANCELED, System.currentTimeMillis());

        manuallyTriggeredMainThreadExecutor.trigger();

        assertThat(application.getApplicationStatus()).isEqualTo(ApplicationState.CANCELED);
    }

    @ParameterizedTest
    @EnumSource(value = JobStatus.class)
    void testJobStatusChanges(JobStatus status)
            throws ExecutionException, InterruptedException, TimeoutException {
        // change mainThreadExecutor to be manually triggered to control the state transition
        mainThreadExecutor = new ManuallyTriggeredScheduledExecutor();
        final ManuallyTriggeredScheduledExecutor manuallyTriggeredMainThreadExecutor =
                (ManuallyTriggeredScheduledExecutor) mainThreadExecutor;

        if (status.isGloballyTerminalState()) {
            final TestingDispatcherGateway.Builder dispatcherBuilder =
                    TestingDispatcherGateway.newBuilder()
                            .setSubmitFunction(
                                    jobGraph ->
                                            CompletableFuture.completedFuture(Acknowledge.get()));

            // execute the application first to get it into RUNNING state
            final CompletableFuture<Acknowledge> executeFuture =
                    application.execute(
                            dispatcherBuilder.build(),
                            scheduledExecutor,
                            mainThreadExecutor,
                            exception -> {});

            executeFuture.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            assertThat(application.getApplicationStatus()).isEqualTo(ApplicationState.RUNNING);

            // simulate job status change to a globally terminal state
            application.jobStatusChanges(jobId, status, System.currentTimeMillis());

            manuallyTriggeredMainThreadExecutor.trigger();

            assertThat(application.getApplicationStatus())
                    .isEqualTo(ApplicationState.fromJobStatus(status));
        } else {
            // simulate job status change to a non-globally-terminal state
            application.jobStatusChanges(jobId, status, System.currentTimeMillis());

            // should not trigger state transition
            assertThat(manuallyTriggeredMainThreadExecutor.numQueuedRunnables()).isZero();

            // should remain in CREATED state as non-terminal states are ignored
            assertThat(application.getApplicationStatus()).isEqualTo(ApplicationState.CREATED);
        }
    }

    private static <T, E extends Throwable> E assertException(
            CompletableFuture<T> future, Class<E> exceptionClass) throws Exception {

        try {
            future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (Throwable e) {
            Optional<E> maybeException = ExceptionUtils.findThrowable(e, exceptionClass);
            if (!maybeException.isPresent()) {
                throw e;
            }
            return maybeException.get();
        }
        throw new Exception(
                "Future should have completed exceptionally with "
                        + exceptionClass.getCanonicalName()
                        + ".");
    }
}
