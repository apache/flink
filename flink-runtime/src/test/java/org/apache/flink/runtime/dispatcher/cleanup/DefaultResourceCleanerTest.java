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
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.Executors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/** {@code DefaultResourceCleanerTest} tests {@link DefaultResourceCleaner}. */
public class DefaultResourceCleanerTest {

    private static final Executor EXECUTOR = Executors.directExecutor();
    private static final JobID JOB_ID = new JobID();

    private DefaultResourceCleaner<CleanupCallback> testInstance;
    private CleanupCallback cleanup0;
    private CleanupCallback cleanup1;

    @BeforeEach
    public void setup() {
        cleanup0 = CleanupCallback.withoutCompletionOnCleanup();
        cleanup1 = CleanupCallback.withoutCompletionOnCleanup();

        testInstance =
                createTestInstanceBuilder()
                        .withRegularCleanup(cleanup0)
                        .withRegularCleanup(cleanup1)
                        .build();
    }

    @Test
    public void testSuccessfulConcurrentCleanup() {
        CompletableFuture<Void> cleanupResult = testInstance.cleanupAsync(JOB_ID);

        assertThat(cleanupResult).isNotCompleted();
        assertThat(cleanup0).extracting(CleanupCallback::getProcessedJobId).isEqualTo(JOB_ID);
        assertThat(cleanup1).extracting(CleanupCallback::getProcessedJobId).isEqualTo(JOB_ID);

        cleanup0.completeCleanup();
        assertThat(cleanupResult).isNotCompleted();

        cleanup1.completeCleanup();
        assertThat(cleanupResult).isCompleted();
    }

    @Test
    public void testConcurrentCleanupWithExceptionFirst() {
        CompletableFuture<Void> cleanupResult = testInstance.cleanupAsync(JOB_ID);

        assertThat(cleanupResult).isNotCompleted();
        assertThat(cleanup0).extracting(CleanupCallback::getProcessedJobId).isEqualTo(JOB_ID);
        assertThat(cleanup1).extracting(CleanupCallback::getProcessedJobId).isEqualTo(JOB_ID);

        final RuntimeException expectedException = new RuntimeException("Expected exception");
        cleanup0.completeCleanupExceptionally(expectedException);
        assertThat(cleanupResult).isNotCompleted();

        cleanup1.completeCleanup();
        assertThat(cleanupResult)
                .failsWithin(Duration.ZERO)
                .withThrowableOfType(ExecutionException.class)
                .withCause(expectedException);
    }

    @Test
    public void testConcurrentCleanupWithExceptionSecond() {
        CompletableFuture<Void> cleanupResult = testInstance.cleanupAsync(JOB_ID);

        assertThat(cleanupResult).isNotCompleted();
        assertThat(cleanup0).extracting(CleanupCallback::getProcessedJobId).isEqualTo(JOB_ID);
        assertThat(cleanup1).extracting(CleanupCallback::getProcessedJobId).isEqualTo(JOB_ID);

        cleanup0.completeCleanup();
        assertThat(cleanupResult).isNotCompleted();

        final RuntimeException expectedException = new RuntimeException("Expected exception");
        cleanup1.completeCleanupExceptionally(expectedException);
        assertThat(cleanupResult)
                .failsWithin(Duration.ZERO)
                .withThrowableOfType(ExecutionException.class)
                .withCause(expectedException);
    }

    @Test
    public void testHighestPriorityCleanupBlocksAllOtherCleanups() {
        final CleanupCallback highPriorityCleanup = CleanupCallback.withoutCompletionOnCleanup();
        final CleanupCallback lowerThanHighPriorityCleanup =
                CleanupCallback.withCompletionOnCleanup();
        final CleanupCallback noPriorityCleanup0 = CleanupCallback.withCompletionOnCleanup();
        final CleanupCallback noPriorityCleanup1 = CleanupCallback.withCompletionOnCleanup();

        final DefaultResourceCleaner<CleanupCallback> testInstance =
                createTestInstanceBuilder()
                        .withPrioritizedCleanup(highPriorityCleanup)
                        .withPrioritizedCleanup(lowerThanHighPriorityCleanup)
                        .withRegularCleanup(noPriorityCleanup0)
                        .withRegularCleanup(noPriorityCleanup1)
                        .build();

        final CompletableFuture<Void> overallCleanupResult = testInstance.cleanupAsync(JOB_ID);

        assertThat(highPriorityCleanup.isDone()).isFalse();
        assertThat(lowerThanHighPriorityCleanup.isDone()).isFalse();
        assertThat(noPriorityCleanup0.isDone()).isFalse();
        assertThat(noPriorityCleanup1.isDone()).isFalse();

        assertThat(overallCleanupResult.isDone()).isFalse();

        highPriorityCleanup.completeCleanup();

        assertThat(overallCleanupResult).succeedsWithin(Duration.ofMillis(100));

        assertThat(highPriorityCleanup.isDone()).isTrue();
        assertThat(lowerThanHighPriorityCleanup.isDone()).isTrue();
        assertThat(noPriorityCleanup0.isDone()).isTrue();
        assertThat(noPriorityCleanup1.isDone()).isTrue();
    }

    @Test
    public void testMediumPriorityCleanupBlocksAllLowerPrioritizedCleanups() {
        final CleanupCallback highPriorityCleanup = CleanupCallback.withCompletionOnCleanup();
        final CleanupCallback lowerThanHighPriorityCleanup =
                CleanupCallback.withoutCompletionOnCleanup();
        final CleanupCallback noPriorityCleanup0 = CleanupCallback.withCompletionOnCleanup();
        final CleanupCallback noPriorityCleanup1 = CleanupCallback.withCompletionOnCleanup();

        final DefaultResourceCleaner<CleanupCallback> testInstance =
                createTestInstanceBuilder()
                        .withPrioritizedCleanup(highPriorityCleanup)
                        .withPrioritizedCleanup(lowerThanHighPriorityCleanup)
                        .withRegularCleanup(noPriorityCleanup0)
                        .withRegularCleanup(noPriorityCleanup1)
                        .build();

        assertThat(highPriorityCleanup.isDone()).isFalse();

        final CompletableFuture<Void> overallCleanupResult = testInstance.cleanupAsync(JOB_ID);

        assertThat(highPriorityCleanup.isDone()).isTrue();
        assertThat(lowerThanHighPriorityCleanup.isDone()).isFalse();
        assertThat(noPriorityCleanup0.isDone()).isFalse();
        assertThat(noPriorityCleanup1.isDone()).isFalse();

        assertThat(overallCleanupResult.isDone()).isFalse();

        lowerThanHighPriorityCleanup.completeCleanup();

        assertThat(overallCleanupResult).succeedsWithin(Duration.ofMillis(100));

        assertThat(highPriorityCleanup.isDone()).isTrue();
        assertThat(lowerThanHighPriorityCleanup.isDone()).isTrue();
        assertThat(noPriorityCleanup0.isDone()).isTrue();
        assertThat(noPriorityCleanup1.isDone()).isTrue();
    }

    private static DefaultResourceCleaner.Builder<CleanupCallback> createTestInstanceBuilder() {
        return DefaultResourceCleaner.forCleanableResources(
                ComponentMainThreadExecutorServiceAdapter.forMainThread(),
                EXECUTOR,
                CleanupCallback::cleanup);
    }

    private static class CleanupCallback {

        private final CompletableFuture<Void> resultFuture = new CompletableFuture<>();
        private JobID jobId;

        private final Consumer<CompletableFuture<Void>> internalFunction;

        public static CleanupCallback withCompletionOnCleanup() {
            return new CleanupCallback(resultFuture -> resultFuture.complete(null));
        }

        public static CleanupCallback withoutCompletionOnCleanup() {
            return new CleanupCallback(ignoredResultFuture -> {});
        }

        private CleanupCallback(Consumer<CompletableFuture<Void>> internalFunction) {
            this.internalFunction = internalFunction;
        }

        public CompletableFuture<Void> cleanup(JobID jobId, Executor executor) {
            Preconditions.checkState(this.jobId == null);
            this.jobId = jobId;

            internalFunction.accept(resultFuture);

            return resultFuture;
        }

        public boolean isDone() {
            return resultFuture.isDone();
        }

        public JobID getProcessedJobId() {
            return jobId;
        }

        public void completeCleanup() {
            this.resultFuture.complete(null);
        }

        public void completeCleanupExceptionally(Throwable expectedException) {
            this.resultFuture.completeExceptionally(expectedException);
        }
    }
}
