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

package org.apache.flink.client.deployment.application;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.SerializedThrowable;
import org.apache.flink.util.concurrent.ScheduledExecutor;
import org.apache.flink.util.concurrent.ScheduledExecutorServiceAdapter;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests the {@link JobStatusPollingUtils}. */
class JobStatusPollingUtilsTest {

    @Test
    void testPolling() {
        final int maxAttemptCounter = 3;
        final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        try {
            final ScheduledExecutor scheduledExecutor =
                    new ScheduledExecutorServiceAdapter(executor);

            final CallCountingJobStatusSupplier jobStatusSupplier =
                    new CallCountingJobStatusSupplier(maxAttemptCounter);

            final CompletableFuture<JobResult> result =
                    JobStatusPollingUtils.pollJobResultAsync(
                            jobStatusSupplier,
                            () ->
                                    CompletableFuture.completedFuture(
                                            createSuccessfulJobResult(new JobID(0, 0))),
                            scheduledExecutor,
                            10);

            result.join();

            assertThat(jobStatusSupplier.getAttemptCounter()).isEqualTo(maxAttemptCounter);

        } finally {
            ExecutorUtils.gracefulShutdown(5, TimeUnit.SECONDS, executor);
        }
    }

    @Test
    void testHappyPath() throws ExecutionException, InterruptedException {
        final int maxAttemptCounter = 1;
        final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        try {
            final ScheduledExecutor scheduledExecutor =
                    new ScheduledExecutorServiceAdapter(executor);

            final CallCountingJobStatusSupplier jobStatusSupplier =
                    new CallCountingJobStatusSupplier(maxAttemptCounter);

            final CompletableFuture<JobResult> result =
                    JobStatusPollingUtils.pollJobResultAsync(
                            jobStatusSupplier,
                            () ->
                                    CompletableFuture.completedFuture(
                                            createSuccessfulJobResult(new JobID(0, 0))),
                            scheduledExecutor,
                            10);

            result.join();

            assertThat(jobStatusSupplier.getAttemptCounter()).isEqualTo(maxAttemptCounter);
            assertThat(result).isCompletedWithValueMatching(JobResult::isSuccess);

        } finally {
            ExecutorUtils.gracefulShutdown(5, TimeUnit.SECONDS, executor);
        }
    }

    @Test
    void testFailedJobResult() throws ExecutionException, InterruptedException {
        final int maxAttemptCounter = 1;
        final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        try {
            final ScheduledExecutor scheduledExecutor =
                    new ScheduledExecutorServiceAdapter(executor);

            final CallCountingJobStatusSupplier jobStatusSupplier =
                    new CallCountingJobStatusSupplier(maxAttemptCounter);

            final CompletableFuture<JobResult> result =
                    JobStatusPollingUtils.pollJobResultAsync(
                            jobStatusSupplier,
                            () ->
                                    CompletableFuture.completedFuture(
                                            createFailedJobResult(new JobID(0, 0))),
                            scheduledExecutor,
                            10);

            result.join();

            assertThat(jobStatusSupplier.getAttemptCounter()).isEqualTo(maxAttemptCounter);
            assertThat(result)
                    .isCompletedWithValueMatching(
                            jobResult -> jobResult.getSerializedThrowable().isPresent());

        } finally {
            ExecutorUtils.gracefulShutdown(5, TimeUnit.SECONDS, executor);
        }
    }

    /**
     * A {@link JobStatus} {@link Supplier supplier} that waits until {@code maxAttempts} before it
     * returns that the job was done.
     */
    private static final class CallCountingJobStatusSupplier
            implements Supplier<CompletableFuture<JobStatus>> {

        private final int maxAttempts;

        private int attemptCounter;

        public CallCountingJobStatusSupplier(int maxAttempts) {
            this.maxAttempts = maxAttempts;
        }

        public int getAttemptCounter() {
            return attemptCounter;
        }

        @Override
        public CompletableFuture<JobStatus> get() {
            if (++attemptCounter < maxAttempts) {
                return CompletableFuture.completedFuture(JobStatus.RUNNING);
            }
            return CompletableFuture.completedFuture(JobStatus.FINISHED);
        }
    }

    private static JobResult createFailedJobResult(final JobID jobId) {
        return new JobResult.Builder()
                .jobId(jobId)
                .netRuntime(2L)
                .applicationStatus(ApplicationStatus.FAILED)
                .serializedThrowable(new SerializedThrowable(new Exception("bla bla bla")))
                .build();
    }

    private static JobResult createSuccessfulJobResult(final JobID jobId) {
        return new JobResult.Builder()
                .jobId(jobId)
                .netRuntime(2L)
                .applicationStatus(ApplicationStatus.SUCCEEDED)
                .build();
    }
}
