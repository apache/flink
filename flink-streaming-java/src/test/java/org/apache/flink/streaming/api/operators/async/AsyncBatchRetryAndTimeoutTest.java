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

package org.apache.flink.streaming.api.operators.async;

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.streaming.api.functions.async.AsyncBatchFunction;
import org.apache.flink.streaming.api.functions.async.AsyncBatchRetryStrategy;
import org.apache.flink.streaming.api.functions.async.AsyncBatchTimeoutPolicy;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.retryable.AsyncBatchRetryStrategies;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for retry and timeout functionality in {@link AsyncBatchWaitOperator}.
 *
 * <p>These tests verify:
 *
 * <ul>
 *   <li>Retry on exception with fixed delay strategy
 *   <li>Retry on exception with exponential backoff strategy
 *   <li>Retry on result predicate
 *   <li>Timeout with fail behavior
 *   <li>Timeout with allow partial behavior
 *   <li>Retry and timeout interaction
 * </ul>
 */
@Timeout(value = 100, unit = TimeUnit.SECONDS)
class AsyncBatchRetryAndTimeoutTest {

    // ================================================================================
    //  Retry Tests
    // ================================================================================

    /**
     * Test that retry works correctly with fixed delay strategy.
     *
     * <p>Scenario: Batch function fails twice then succeeds on third attempt.
     */
    @Test
    void testRetryWithFixedDelay() throws Exception {
        final int maxBatchSize = 2;
        final AtomicInteger attemptCount = new AtomicInteger(0);
        final int failuresBeforeSuccess = 2;

        // Function that fails first 2 times, then succeeds
        AsyncBatchFunction<Integer, Integer> function =
                (inputs, resultFuture) -> {
                    int attempt = attemptCount.incrementAndGet();
                    if (attempt <= failuresBeforeSuccess) {
                        resultFuture.completeExceptionally(
                                new IOException("Simulated failure #" + attempt));
                    } else {
                        resultFuture.complete(
                                inputs.stream().map(i -> i * 2).collect(Collectors.toList()));
                    }
                };

        // Retry strategy: max 3 attempts, 10ms delay, retry on IOException
        AsyncBatchRetryStrategy<Integer> retryStrategy =
                new AsyncBatchRetryStrategies.FixedDelayRetryStrategyBuilder<Integer>(3, 10L)
                        .ifException(e -> e instanceof IOException)
                        .build();

        try (OneInputStreamOperatorTestHarness<Integer, Integer> testHarness =
                createTestHarnessWithRetry(function, maxBatchSize, retryStrategy)) {

            testHarness.open();

            // Process 2 elements to trigger a batch
            testHarness.processElement(new StreamRecord<>(1, 1L));
            testHarness.processElement(new StreamRecord<>(2, 2L));

            testHarness.endInput();

            // Verify retry happened
            assertThat(attemptCount.get()).isEqualTo(3);

            // Verify outputs after successful retry
            List<Integer> outputs =
                    testHarness.getOutput().stream()
                            .filter(e -> e instanceof StreamRecord)
                            .map(e -> ((StreamRecord<Integer>) e).getValue())
                            .collect(Collectors.toList());

            assertThat(outputs).containsExactlyInAnyOrder(2, 4);

            // Verify retry counter metric
            AsyncBatchWaitOperator<Integer, Integer> operator =
                    (AsyncBatchWaitOperator<Integer, Integer>) testHarness.getOperator();
            assertThat(operator.getBatchRetryCounter().getCount()).isEqualTo(2);
        }
    }

    /**
     * Test that retry works correctly with exponential backoff strategy.
     *
     * <p>Scenario: Batch function fails twice then succeeds on third attempt.
     */
    @Test
    void testRetryWithExponentialBackoff() throws Exception {
        final int maxBatchSize = 2;
        final AtomicInteger attemptCount = new AtomicInteger(0);
        final List<Long> attemptTimes = new CopyOnWriteArrayList<>();

        // Function that fails first 2 times, then succeeds
        AsyncBatchFunction<Integer, Integer> function =
                (inputs, resultFuture) -> {
                    attemptTimes.add(System.currentTimeMillis());
                    int attempt = attemptCount.incrementAndGet();
                    if (attempt <= 2) {
                        resultFuture.completeExceptionally(
                                new RuntimeException("Simulated failure"));
                    } else {
                        resultFuture.complete(inputs);
                    }
                };

        // Exponential backoff: max 3 attempts, initial 10ms, max 100ms, multiplier 2.0
        AsyncBatchRetryStrategy<Integer> retryStrategy =
                new AsyncBatchRetryStrategies.ExponentialBackoffDelayRetryStrategyBuilder<Integer>(
                                3, 10L, 100L, 2.0)
                        .ifException(e -> e instanceof RuntimeException)
                        .build();

        try (OneInputStreamOperatorTestHarness<Integer, Integer> testHarness =
                createTestHarnessWithRetry(function, maxBatchSize, retryStrategy)) {

            testHarness.open();

            testHarness.processElement(new StreamRecord<>(1, 1L));
            testHarness.processElement(new StreamRecord<>(2, 2L));

            testHarness.endInput();

            // Verify 3 attempts were made
            assertThat(attemptCount.get()).isEqualTo(3);
            assertThat(attemptTimes).hasSize(3);
        }
    }

    /**
     * Test that retry is triggered based on result predicate.
     *
     * <p>Scenario: Batch function returns empty result, triggers retry.
     */
    @Test
    void testRetryOnResultPredicate() throws Exception {
        final int maxBatchSize = 2;
        final AtomicInteger attemptCount = new AtomicInteger(0);

        // Function that returns empty result on first attempt
        AsyncBatchFunction<Integer, Integer> function =
                (inputs, resultFuture) -> {
                    int attempt = attemptCount.incrementAndGet();
                    if (attempt == 1) {
                        // Return empty result - should trigger retry
                        resultFuture.complete(Collections.emptyList());
                    } else {
                        resultFuture.complete(inputs);
                    }
                };

        // Retry strategy: retry if result is empty
        AsyncBatchRetryStrategy<Integer> retryStrategy =
                new AsyncBatchRetryStrategies.FixedDelayRetryStrategyBuilder<Integer>(2, 10L)
                        .ifResult(results -> results.isEmpty())
                        .build();

        try (OneInputStreamOperatorTestHarness<Integer, Integer> testHarness =
                createTestHarnessWithRetry(function, maxBatchSize, retryStrategy)) {

            testHarness.open();

            testHarness.processElement(new StreamRecord<>(1, 1L));
            testHarness.processElement(new StreamRecord<>(2, 2L));

            testHarness.endInput();

            // Verify retry happened
            assertThat(attemptCount.get()).isEqualTo(2);

            // Verify outputs after retry
            List<Integer> outputs =
                    testHarness.getOutput().stream()
                            .filter(e -> e instanceof StreamRecord)
                            .map(e -> ((StreamRecord<Integer>) e).getValue())
                            .collect(Collectors.toList());

            assertThat(outputs).containsExactlyInAnyOrder(1, 2);
        }
    }

    /**
     * Test that retry fails after max attempts exhausted.
     *
     * <p>Scenario: All retry attempts fail.
     */
    @Test
    void testRetryExhausted() throws Exception {
        final int maxBatchSize = 2;
        final AtomicInteger attemptCount = new AtomicInteger(0);

        // Function that always fails
        AsyncBatchFunction<Integer, Integer> function =
                (inputs, resultFuture) -> {
                    attemptCount.incrementAndGet();
                    resultFuture.completeExceptionally(new IOException("Always fails"));
                };

        // Retry strategy: max 2 attempts
        AsyncBatchRetryStrategy<Integer> retryStrategy =
                new AsyncBatchRetryStrategies.FixedDelayRetryStrategyBuilder<Integer>(2, 5L)
                        .ifException(e -> e instanceof IOException)
                        .build();

        try (OneInputStreamOperatorTestHarness<Integer, Integer> testHarness =
                createTestHarnessWithRetry(function, maxBatchSize, retryStrategy)) {

            testHarness.open();

            testHarness.processElement(new StreamRecord<>(1, 1L));
            testHarness.processElement(new StreamRecord<>(2, 2L));

            testHarness.endInput();

            // Verify all retry attempts were made (1 initial + 2 retries = 3)
            assertThat(attemptCount.get()).isEqualTo(3);

            // Verify failure is propagated
            assertThat(testHarness.getEnvironment().getActualExternalFailureCause())
                    .isPresent()
                    .get()
                    .satisfies(t -> assertThat(t.getCause()).isInstanceOf(IOException.class));

            // Verify failure counter
            AsyncBatchWaitOperator<Integer, Integer> operator =
                    (AsyncBatchWaitOperator<Integer, Integer>) testHarness.getOperator();
            assertThat(operator.getAsyncCallFailuresCounter().getCount()).isEqualTo(1);
        }
    }

    /**
     * Test that non-matching exceptions are not retried.
     *
     * <p>Scenario: Function throws exception that doesn't match retry predicate.
     */
    @Test
    void testNoRetryForNonMatchingException() throws Exception {
        final int maxBatchSize = 2;
        final AtomicInteger attemptCount = new AtomicInteger(0);

        // Function that throws IllegalStateException
        AsyncBatchFunction<Integer, Integer> function =
                (inputs, resultFuture) -> {
                    attemptCount.incrementAndGet();
                    resultFuture.completeExceptionally(new IllegalStateException("Not retryable"));
                };

        // Retry strategy: only retry on IOException
        AsyncBatchRetryStrategy<Integer> retryStrategy =
                new AsyncBatchRetryStrategies.FixedDelayRetryStrategyBuilder<Integer>(3, 10L)
                        .ifException(e -> e instanceof IOException)
                        .build();

        try (OneInputStreamOperatorTestHarness<Integer, Integer> testHarness =
                createTestHarnessWithRetry(function, maxBatchSize, retryStrategy)) {

            testHarness.open();

            testHarness.processElement(new StreamRecord<>(1, 1L));
            testHarness.processElement(new StreamRecord<>(2, 2L));

            testHarness.endInput();

            // Verify no retry (only 1 attempt)
            assertThat(attemptCount.get()).isEqualTo(1);

            // Verify retry counter is 0
            AsyncBatchWaitOperator<Integer, Integer> operator =
                    (AsyncBatchWaitOperator<Integer, Integer>) testHarness.getOperator();
            assertThat(operator.getBatchRetryCounter().getCount()).isEqualTo(0);
        }
    }

    // ================================================================================
    //  Timeout Tests
    // ================================================================================

    /**
     * Test timeout with fail behavior.
     *
     * <p>Scenario: Async operation takes too long, timeout triggers failure.
     */
    @Test
    void testTimeoutWithFailBehavior() throws Exception {
        final int maxBatchSize = 2;
        final CompletableFuture<Void> blockingFuture = new CompletableFuture<>();

        // Function that blocks indefinitely
        AsyncBatchFunction<Integer, Integer> function =
                (inputs, resultFuture) -> {
                    // Never completes - should timeout
                    blockingFuture.thenRun(() -> resultFuture.complete(inputs));
                };

        // Timeout policy: fail after 50ms
        AsyncBatchTimeoutPolicy timeoutPolicy = AsyncBatchTimeoutPolicy.failOnTimeout(50L);

        try (OneInputStreamOperatorTestHarness<Integer, Integer> testHarness =
                createTestHarnessWithTimeout(function, maxBatchSize, timeoutPolicy)) {

            testHarness.open();

            testHarness.processElement(new StreamRecord<>(1, 1L));
            testHarness.processElement(new StreamRecord<>(2, 2L));

            // Wait for timeout to occur
            Thread.sleep(100);

            testHarness.endInput();

            // Verify timeout failure
            assertThat(testHarness.getEnvironment().getActualExternalFailureCause())
                    .isPresent()
                    .get()
                    .satisfies(t -> assertThat(t).isInstanceOf(TimeoutException.class));

            // Verify timeout counter
            AsyncBatchWaitOperator<Integer, Integer> operator =
                    (AsyncBatchWaitOperator<Integer, Integer>) testHarness.getOperator();
            assertThat(operator.getBatchTimeoutCounter().getCount()).isEqualTo(1);
        }
    }

    /**
     * Test timeout with allow partial behavior.
     *
     * <p>Scenario: Async operation takes too long, timeout allows partial results.
     */
    @Test
    void testTimeoutWithAllowPartialBehavior() throws Exception {
        final int maxBatchSize = 2;
        final CompletableFuture<Void> blockingFuture = new CompletableFuture<>();

        // Function that blocks indefinitely
        AsyncBatchFunction<Integer, Integer> function =
                (inputs, resultFuture) -> {
                    // Never completes - should timeout
                    blockingFuture.thenRun(() -> resultFuture.complete(inputs));
                };

        // Timeout policy: allow partial after 50ms
        AsyncBatchTimeoutPolicy timeoutPolicy = AsyncBatchTimeoutPolicy.allowPartialOnTimeout(50L);

        try (OneInputStreamOperatorTestHarness<Integer, Integer> testHarness =
                createTestHarnessWithTimeout(function, maxBatchSize, timeoutPolicy)) {

            testHarness.open();

            testHarness.processElement(new StreamRecord<>(1, 1L));
            testHarness.processElement(new StreamRecord<>(2, 2L));

            // Wait for timeout to occur
            Thread.sleep(100);

            testHarness.endInput();

            // Verify no failure (partial results allowed)
            assertThat(testHarness.getEnvironment().getActualExternalFailureCause()).isEmpty();

            // Verify timeout counter
            AsyncBatchWaitOperator<Integer, Integer> operator =
                    (AsyncBatchWaitOperator<Integer, Integer>) testHarness.getOperator();
            assertThat(operator.getBatchTimeoutCounter().getCount()).isEqualTo(1);

            // No outputs since function never completed
            assertThat(testHarness.getOutput()).isEmpty();
        }
    }

    /**
     * Test that completion before timeout succeeds normally.
     *
     * <p>Scenario: Async operation completes before timeout.
     */
    @Test
    void testCompletionBeforeTimeout() throws Exception {
        final int maxBatchSize = 2;

        // Function that completes immediately
        AsyncBatchFunction<Integer, Integer> function =
                (inputs, resultFuture) -> {
                    resultFuture.complete(
                            inputs.stream().map(i -> i * 2).collect(Collectors.toList()));
                };

        // Long timeout - should never trigger
        AsyncBatchTimeoutPolicy timeoutPolicy = AsyncBatchTimeoutPolicy.failOnTimeout(10000L);

        try (OneInputStreamOperatorTestHarness<Integer, Integer> testHarness =
                createTestHarnessWithTimeout(function, maxBatchSize, timeoutPolicy)) {

            testHarness.open();

            testHarness.processElement(new StreamRecord<>(1, 1L));
            testHarness.processElement(new StreamRecord<>(2, 2L));

            testHarness.endInput();

            // Verify no failure
            assertThat(testHarness.getEnvironment().getActualExternalFailureCause()).isEmpty();

            // Verify no timeout
            AsyncBatchWaitOperator<Integer, Integer> operator =
                    (AsyncBatchWaitOperator<Integer, Integer>) testHarness.getOperator();
            assertThat(operator.getBatchTimeoutCounter().getCount()).isEqualTo(0);

            // Verify outputs
            List<Integer> outputs =
                    testHarness.getOutput().stream()
                            .filter(e -> e instanceof StreamRecord)
                            .map(e -> ((StreamRecord<Integer>) e).getValue())
                            .collect(Collectors.toList());

            assertThat(outputs).containsExactlyInAnyOrder(2, 4);
        }
    }

    // ================================================================================
    //  Combined Retry and Timeout Tests
    // ================================================================================

    /**
     * Test that timeout cancels pending retry.
     *
     * <p>Scenario: Retrying when timeout occurs.
     */
    @Test
    void testTimeoutCancelsRetry() throws Exception {
        final int maxBatchSize = 2;
        final AtomicInteger attemptCount = new AtomicInteger(0);
        final List<CompletableFuture<Void>> pendingFutures = new ArrayList<>();

        // Function that fails and hangs on retry
        AsyncBatchFunction<Integer, Integer> function =
                (inputs, resultFuture) -> {
                    int attempt = attemptCount.incrementAndGet();
                    if (attempt == 1) {
                        resultFuture.completeExceptionally(new IOException("First attempt fails"));
                    } else {
                        // Hang on retry - should timeout
                        CompletableFuture<Void> future = new CompletableFuture<>();
                        pendingFutures.add(future);
                        future.thenRun(() -> resultFuture.complete(inputs));
                    }
                };

        // Retry with timeout
        AsyncBatchRetryStrategy<Integer> retryStrategy =
                new AsyncBatchRetryStrategies.FixedDelayRetryStrategyBuilder<Integer>(3, 5L)
                        .ifException(e -> e instanceof IOException)
                        .build();
        AsyncBatchTimeoutPolicy timeoutPolicy = AsyncBatchTimeoutPolicy.failOnTimeout(100L);

        try (OneInputStreamOperatorTestHarness<Integer, Integer> testHarness =
                createTestHarnessWithRetryAndTimeout(
                        function, maxBatchSize, retryStrategy, timeoutPolicy)) {

            testHarness.open();

            testHarness.processElement(new StreamRecord<>(1, 1L));
            testHarness.processElement(new StreamRecord<>(2, 2L));

            // Wait for timeout to occur
            Thread.sleep(200);

            testHarness.endInput();

            // Verify timeout occurred
            AsyncBatchWaitOperator<Integer, Integer> operator =
                    (AsyncBatchWaitOperator<Integer, Integer>) testHarness.getOperator();
            assertThat(operator.getBatchTimeoutCounter().getCount()).isEqualTo(1);
        }
    }

    /**
     * Test successful retry before timeout.
     *
     * <p>Scenario: Retry succeeds before timeout expires.
     */
    @Test
    void testRetrySucceedsBeforeTimeout() throws Exception {
        final int maxBatchSize = 2;
        final AtomicInteger attemptCount = new AtomicInteger(0);

        // Function that fails once then succeeds quickly
        AsyncBatchFunction<Integer, Integer> function =
                (inputs, resultFuture) -> {
                    int attempt = attemptCount.incrementAndGet();
                    if (attempt == 1) {
                        resultFuture.completeExceptionally(new IOException("First attempt fails"));
                    } else {
                        resultFuture.complete(
                                inputs.stream().map(i -> i * 2).collect(Collectors.toList()));
                    }
                };

        // Retry with long timeout
        AsyncBatchRetryStrategy<Integer> retryStrategy =
                new AsyncBatchRetryStrategies.FixedDelayRetryStrategyBuilder<Integer>(3, 5L)
                        .ifException(e -> e instanceof IOException)
                        .build();
        AsyncBatchTimeoutPolicy timeoutPolicy = AsyncBatchTimeoutPolicy.failOnTimeout(5000L);

        try (OneInputStreamOperatorTestHarness<Integer, Integer> testHarness =
                createTestHarnessWithRetryAndTimeout(
                        function, maxBatchSize, retryStrategy, timeoutPolicy)) {

            testHarness.open();

            testHarness.processElement(new StreamRecord<>(1, 1L));
            testHarness.processElement(new StreamRecord<>(2, 2L));

            testHarness.endInput();

            // Verify retry happened
            assertThat(attemptCount.get()).isEqualTo(2);

            // Verify no timeout
            AsyncBatchWaitOperator<Integer, Integer> operator =
                    (AsyncBatchWaitOperator<Integer, Integer>) testHarness.getOperator();
            assertThat(operator.getBatchTimeoutCounter().getCount()).isEqualTo(0);

            // Verify outputs
            List<Integer> outputs =
                    testHarness.getOutput().stream()
                            .filter(e -> e instanceof StreamRecord)
                            .map(e -> ((StreamRecord<Integer>) e).getValue())
                            .collect(Collectors.toList());

            assertThat(outputs).containsExactlyInAnyOrder(2, 4);
        }
    }

    // ================================================================================
    //  Test Harness Helpers
    // ================================================================================

    private static OneInputStreamOperatorTestHarness<Integer, Integer> createTestHarnessWithRetry(
            AsyncBatchFunction<Integer, Integer> function,
            int maxBatchSize,
            AsyncBatchRetryStrategy<Integer> retryStrategy)
            throws Exception {
        return new OneInputStreamOperatorTestHarness<>(
                new AsyncBatchWaitOperatorFactory<>(
                        function,
                        maxBatchSize,
                        0L,
                        retryStrategy,
                        AsyncBatchTimeoutPolicy.NO_TIMEOUT_POLICY),
                IntSerializer.INSTANCE);
    }

    @SuppressWarnings("unchecked")
    private static OneInputStreamOperatorTestHarness<Integer, Integer> createTestHarnessWithTimeout(
            AsyncBatchFunction<Integer, Integer> function,
            int maxBatchSize,
            AsyncBatchTimeoutPolicy timeoutPolicy)
            throws Exception {
        return new OneInputStreamOperatorTestHarness<>(
                new AsyncBatchWaitOperatorFactory<>(
                        function,
                        maxBatchSize,
                        0L,
                        AsyncBatchRetryStrategies.noRetry(),
                        timeoutPolicy),
                IntSerializer.INSTANCE);
    }

    private static OneInputStreamOperatorTestHarness<Integer, Integer>
            createTestHarnessWithRetryAndTimeout(
                    AsyncBatchFunction<Integer, Integer> function,
                    int maxBatchSize,
                    AsyncBatchRetryStrategy<Integer> retryStrategy,
                    AsyncBatchTimeoutPolicy timeoutPolicy)
                    throws Exception {
        return new OneInputStreamOperatorTestHarness<>(
                new AsyncBatchWaitOperatorFactory<>(
                        function, maxBatchSize, 0L, retryStrategy, timeoutPolicy),
                IntSerializer.INSTANCE);
    }
}
