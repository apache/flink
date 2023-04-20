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
import org.apache.flink.core.testutils.FlinkAssertions;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TestLoggerExtension;
import org.apache.flink.util.concurrent.Executors;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.RetryStrategy;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import static org.apache.flink.core.testutils.FlinkAssertions.STREAM_THROWABLE;
import static org.apache.flink.core.testutils.FlinkAssertions.assertThatFuture;
import static org.assertj.core.api.Assertions.assertThat;

/** {@code DefaultResourceCleanerTest} tests {@link DefaultResourceCleaner}. */
@ExtendWith(TestLoggerExtension.class)
class DefaultResourceCleanerTest {

    // runs with retry utilizes the ComponentMainThreadExecutor which adds concurrency despite using
    // Executors.directExecutor as the cleanupExecutor.
    private static final Executor EXECUTOR = Executors.directExecutor();
    private static final JobID JOB_ID = new JobID();

    @Test
    void testSuccessfulConcurrentCleanup() {
        final SingleCallCleanup cleanup0 = SingleCallCleanup.withoutCompletionOnCleanup();
        final SingleCallCleanup cleanup1 = SingleCallCleanup.withoutCompletionOnCleanup();

        final CompletableFuture<Void> cleanupResult =
                createTestInstanceBuilder()
                        .withRegularCleanup("Reg #0", cleanup0)
                        .withRegularCleanup("Reg #1", cleanup1)
                        .build()
                        .cleanupAsync(JOB_ID);

        assertThat(cleanupResult).isNotCompleted();
        assertThat(cleanup0).extracting(SingleCallCleanup::getProcessedJobId).isEqualTo(JOB_ID);
        assertThat(cleanup1).extracting(SingleCallCleanup::getProcessedJobId).isEqualTo(JOB_ID);

        cleanup0.completeCleanup();
        assertThat(cleanupResult).isNotCompleted();

        cleanup1.completeCleanup();
        assertThat(cleanupResult).isCompleted();
    }

    @Test
    void testConcurrentCleanupWithExceptionFirst() {
        final SingleCallCleanup cleanup0 = SingleCallCleanup.withoutCompletionOnCleanup();
        final SingleCallCleanup cleanup1 = SingleCallCleanup.withoutCompletionOnCleanup();

        final CompletableFuture<Void> cleanupResult =
                createTestInstanceBuilder()
                        .withRegularCleanup("Reg #0", cleanup0)
                        .withRegularCleanup("Reg #1", cleanup1)
                        .build()
                        .cleanupAsync(JOB_ID);

        assertThat(cleanupResult).isNotCompleted();
        assertThat(cleanup0).extracting(SingleCallCleanup::getProcessedJobId).isEqualTo(JOB_ID);
        assertThat(cleanup1).extracting(SingleCallCleanup::getProcessedJobId).isEqualTo(JOB_ID);

        final RuntimeException expectedException = new RuntimeException("Expected exception");
        cleanup0.completeCleanupExceptionally(expectedException);
        assertThat(cleanupResult).isNotCompleted();

        cleanup1.completeCleanup();
        assertThatFuture(cleanupResult)
                .eventuallyFailsWith(ExecutionException.class)
                .extracting(FlinkAssertions::chainOfCauses, STREAM_THROWABLE)
                .hasExactlyElementsOfTypes(
                        ExecutionException.class,
                        FutureUtils.RetryException.class,
                        CompletionException.class,
                        expectedException.getClass())
                .last()
                .isEqualTo(expectedException);
    }

    @Test
    void testConcurrentCleanupWithExceptionSecond() {
        final SingleCallCleanup cleanup0 = SingleCallCleanup.withoutCompletionOnCleanup();
        final SingleCallCleanup cleanup1 = SingleCallCleanup.withoutCompletionOnCleanup();

        final CompletableFuture<Void> cleanupResult =
                createTestInstanceBuilder()
                        .withRegularCleanup("Reg #0", cleanup0)
                        .withRegularCleanup("Reg #1", cleanup1)
                        .build()
                        .cleanupAsync(JOB_ID);

        assertThat(cleanupResult).isNotCompleted();
        assertThat(cleanup0).extracting(SingleCallCleanup::getProcessedJobId).isEqualTo(JOB_ID);
        assertThat(cleanup1).extracting(SingleCallCleanup::getProcessedJobId).isEqualTo(JOB_ID);

        cleanup0.completeCleanup();
        assertThat(cleanupResult).isNotCompleted();

        final RuntimeException expectedException = new RuntimeException("Expected exception");
        cleanup1.completeCleanupExceptionally(expectedException);
        assertThatFuture(cleanupResult)
                .eventuallyFailsWith(ExecutionException.class)
                .extracting(FlinkAssertions::chainOfCauses, STREAM_THROWABLE)
                .hasExactlyElementsOfTypes(
                        ExecutionException.class,
                        FutureUtils.RetryException.class,
                        CompletionException.class,
                        expectedException.getClass())
                .last()
                .isEqualTo(expectedException);
    }

    @Test
    void testHighestPriorityCleanupBlocksAllOtherCleanups() {
        final SingleCallCleanup highPriorityCleanup =
                SingleCallCleanup.withoutCompletionOnCleanup();
        final SingleCallCleanup lowerThanHighPriorityCleanup =
                SingleCallCleanup.withCompletionOnCleanup();
        final SingleCallCleanup noPriorityCleanup0 = SingleCallCleanup.withCompletionOnCleanup();
        final SingleCallCleanup noPriorityCleanup1 = SingleCallCleanup.withCompletionOnCleanup();

        final DefaultResourceCleaner<CleanupCallback> testInstance =
                createTestInstanceBuilder()
                        .withPrioritizedCleanup("Prio #0", highPriorityCleanup)
                        .withPrioritizedCleanup("Prio #1", lowerThanHighPriorityCleanup)
                        .withRegularCleanup("Reg #0", noPriorityCleanup0)
                        .withRegularCleanup("Reg #1", noPriorityCleanup1)
                        .build();

        final CompletableFuture<Void> overallCleanupResult = testInstance.cleanupAsync(JOB_ID);

        assertThat(highPriorityCleanup.isDone()).isFalse();
        assertThat(lowerThanHighPriorityCleanup.isDone()).isFalse();
        assertThat(noPriorityCleanup0.isDone()).isFalse();
        assertThat(noPriorityCleanup1.isDone()).isFalse();

        assertThat(overallCleanupResult.isDone()).isFalse();

        highPriorityCleanup.completeCleanup();

        assertThat(overallCleanupResult).isCompleted();

        assertThat(highPriorityCleanup.isDone()).isTrue();
        assertThat(lowerThanHighPriorityCleanup.isDone()).isTrue();
        assertThat(noPriorityCleanup0.isDone()).isTrue();
        assertThat(noPriorityCleanup1.isDone()).isTrue();
    }

    @Test
    void testMediumPriorityCleanupBlocksAllLowerPrioritizedCleanups() {
        final SingleCallCleanup highPriorityCleanup = SingleCallCleanup.withCompletionOnCleanup();
        final SingleCallCleanup lowerThanHighPriorityCleanup =
                SingleCallCleanup.withoutCompletionOnCleanup();
        final SingleCallCleanup noPriorityCleanup0 = SingleCallCleanup.withCompletionOnCleanup();
        final SingleCallCleanup noPriorityCleanup1 = SingleCallCleanup.withCompletionOnCleanup();

        final DefaultResourceCleaner<CleanupCallback> testInstance =
                createTestInstanceBuilder()
                        .withPrioritizedCleanup("Prio #0", highPriorityCleanup)
                        .withPrioritizedCleanup("Prio #1", lowerThanHighPriorityCleanup)
                        .withRegularCleanup("Reg #0", noPriorityCleanup0)
                        .withRegularCleanup("Reg #1", noPriorityCleanup1)
                        .build();

        assertThat(highPriorityCleanup.isDone()).isFalse();

        final CompletableFuture<Void> overallCleanupResult = testInstance.cleanupAsync(JOB_ID);

        assertThat(highPriorityCleanup.isDone()).isTrue();
        assertThat(lowerThanHighPriorityCleanup.isDone()).isFalse();
        assertThat(noPriorityCleanup0.isDone()).isFalse();
        assertThat(noPriorityCleanup1.isDone()).isFalse();

        assertThat(overallCleanupResult.isDone()).isFalse();

        lowerThanHighPriorityCleanup.completeCleanup();

        assertThat(overallCleanupResult).isCompleted();

        assertThat(highPriorityCleanup.isDone()).isTrue();
        assertThat(lowerThanHighPriorityCleanup.isDone()).isTrue();
        assertThat(noPriorityCleanup0.isDone()).isTrue();
        assertThat(noPriorityCleanup1.isDone()).isTrue();
    }

    @Test
    void testCleanupWithRetries() {
        final Collection<JobID> actualJobIds = new ArrayList<>();
        final CleanupCallback cleanupWithRetries = cleanupWithInitialFailingRuns(actualJobIds, 2);
        final SingleCallCleanup oneRunCleanup = SingleCallCleanup.withCompletionOnCleanup();

        final CompletableFuture<Void> compositeCleanupResult =
                createTestInstanceBuilder(TestingRetryStrategies.createWithNumberOfRetries(2))
                        .withRegularCleanup("Reg #0", cleanupWithRetries)
                        .withRegularCleanup("Reg #1", oneRunCleanup)
                        .build()
                        .cleanupAsync(JOB_ID);

        assertThatFuture(compositeCleanupResult).eventuallySucceeds();

        assertThat(oneRunCleanup.getProcessedJobId()).isEqualTo(JOB_ID);
        assertThat(oneRunCleanup.isDone()).isTrue();
        assertThat(actualJobIds).containsExactly(JOB_ID, JOB_ID, JOB_ID);
    }

    @Test
    void testCleanupWithSingleRetryInHighPriorityTask() {
        final Collection<JobID> actualJobIds = new ArrayList<>();
        final CleanupCallback cleanupWithRetry = cleanupWithInitialFailingRuns(actualJobIds, 1);
        final CleanupCallback oneRunHigherPriorityCleanup =
                SingleCallCleanup.withCompletionOnCleanup();
        final SingleCallCleanup oneRunCleanup = SingleCallCleanup.withCompletionOnCleanup();

        final CompletableFuture<Void> compositeCleanupResult =
                createTestInstanceBuilder(TestingRetryStrategies.createWithNumberOfRetries(1))
                        .withPrioritizedCleanup("Prio #0", cleanupWithRetry)
                        .withPrioritizedCleanup("Prio #1", oneRunHigherPriorityCleanup)
                        .withRegularCleanup("Reg #0", oneRunCleanup)
                        .build()
                        .cleanupAsync(JOB_ID);

        assertThatFuture(compositeCleanupResult).eventuallySucceeds();

        assertThat(oneRunCleanup.getProcessedJobId()).isEqualTo(JOB_ID);
        assertThat(oneRunCleanup.isDone()).isTrue();
        assertThat(actualJobIds).containsExactly(JOB_ID, JOB_ID);
    }

    private static DefaultResourceCleaner.Builder<CleanupCallback> createTestInstanceBuilder() {
        return createTestInstanceBuilder(TestingRetryStrategies.NO_RETRY_STRATEGY);
    }

    private static DefaultResourceCleaner.Builder<CleanupCallback> createTestInstanceBuilder(
            RetryStrategy retryStrategy) {
        return DefaultResourceCleaner.forCleanableResources(
                ComponentMainThreadExecutorServiceAdapter.forMainThread(),
                EXECUTOR,
                CleanupCallback::apply,
                retryStrategy);
    }

    private static CleanupCallback cleanupWithInitialFailingRuns(
            Collection<JobID> actualJobIds, int numberOfFailureRuns) {
        final AtomicInteger failureRunCount = new AtomicInteger(numberOfFailureRuns);
        return (actualJobId, executor) -> {
            actualJobIds.add(actualJobId);
            if (failureRunCount.getAndDecrement() > 0) {
                return FutureUtils.completedExceptionally(
                        new RuntimeException("Expected RuntimeException"));
            }

            return FutureUtils.completedVoidFuture();
        };
    }

    private interface CleanupCallback extends BiFunction<JobID, Executor, CompletableFuture<Void>> {
        // empty interface to remove necessity use generics all the time
    }

    private static class SingleCallCleanup implements CleanupCallback {

        private final CompletableFuture<Void> resultFuture = new CompletableFuture<>();
        private JobID jobId;

        private final Consumer<CompletableFuture<Void>> internalFunction;

        public static SingleCallCleanup withCompletionOnCleanup() {
            return new SingleCallCleanup(resultFuture -> resultFuture.complete(null));
        }

        public static SingleCallCleanup withoutCompletionOnCleanup() {
            return new SingleCallCleanup(ignoredResultFuture -> {});
        }

        private SingleCallCleanup(Consumer<CompletableFuture<Void>> internalFunction) {
            this.internalFunction = internalFunction;
        }

        public CompletableFuture<Void> apply(JobID jobId, Executor executor) {
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
