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
import org.apache.flink.runtime.operators.testutils.ExpectedTestException;
import org.apache.flink.streaming.api.functions.async.AsyncBatchFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link OrderedAsyncBatchWaitOperator}.
 *
 * <p>These tests verify:
 *
 * <ul>
 *   <li>Strict ordering guarantee - output order matches input order
 *   <li>Batch + time trigger interaction with ordering
 *   <li>Exception propagation
 * </ul>
 */
@Timeout(value = 100, unit = TimeUnit.SECONDS)
class OrderedAsyncBatchWaitOperatorTest {

    /**
     * Test strict ordering guarantee even when async results complete out of order.
     *
     * <p>Inputs: [1, 2, 3, 4, 5]
     *
     * <p>Async batches complete in reverse order (second batch completes before first)
     *
     * <p>Output MUST be: [1, 2, 3, 4, 5] (same as input order)
     */
    @Test
    void testStrictOrderingGuarantee() throws Exception {
        final int maxBatchSize = 3;
        final List<CompletableFuture<Void>> batchFutures = new CopyOnWriteArrayList<>();
        final AtomicInteger batchIndex = new AtomicInteger(0);

        AsyncBatchFunction<Integer, Integer> function =
                (inputs, resultFuture) -> {
                    int currentBatch = batchIndex.getAndIncrement();
                    CompletableFuture<Void> future = new CompletableFuture<>();
                    batchFutures.add(future);

                    // Store input for later completion
                    List<Integer> inputCopy = new ArrayList<>(inputs);

                    // Complete asynchronously when future is completed externally
                    future.thenRun(() -> resultFuture.complete(inputCopy));
                };

        try (OneInputStreamOperatorTestHarness<Integer, Integer> testHarness =
                createTestHarness(function, maxBatchSize)) {

            testHarness.open();

            // Process 5 elements: batch 0 = [1,2,3], batch 1 = [4,5]
            for (int i = 1; i <= 5; i++) {
                testHarness.processElement(new StreamRecord<>(i, i));
            }

            // Trigger end of input to flush remaining elements
            // This creates batch 1 with [4, 5]
            // At this point we have 2 batches pending

            // Wait for batches to be created
            while (batchFutures.size() < 2) {
                Thread.sleep(10);
            }

            // Complete batches in REVERSE order (batch 1 first, then batch 0)
            // This tests that output is still in original order
            batchFutures.get(1).complete(null); // Complete batch [4, 5] first
            Thread.sleep(50); // Give time for async processing

            batchFutures.get(0).complete(null); // Then complete batch [1, 2, 3]

            testHarness.endInput();

            // Verify outputs are in strict input order: [1, 2, 3, 4, 5]
            List<Integer> outputs =
                    testHarness.getOutput().stream()
                            .filter(e -> e instanceof StreamRecord)
                            .map(e -> ((StreamRecord<Integer>) e).getValue())
                            .collect(Collectors.toList());

            // MUST be in exact input order, not completion order
            assertThat(outputs).containsExactly(1, 2, 3, 4, 5);
        }
    }

    /** Test ordering with synchronous completions - simple case to verify basic ordering. */
    @Test
    void testOrderingWithSynchronousCompletion() throws Exception {
        final int maxBatchSize = 2;

        // Function that immediately completes with input values
        AsyncBatchFunction<Integer, Integer> function =
                (inputs, resultFuture) -> {
                    resultFuture.complete(inputs);
                };

        try (OneInputStreamOperatorTestHarness<Integer, Integer> testHarness =
                createTestHarness(function, maxBatchSize)) {

            testHarness.open();

            // Process 6 elements: 3 batches of 2
            for (int i = 1; i <= 6; i++) {
                testHarness.processElement(new StreamRecord<>(i, i));
            }

            testHarness.endInput();

            // Verify outputs are in strict input order
            List<Integer> outputs =
                    testHarness.getOutput().stream()
                            .filter(e -> e instanceof StreamRecord)
                            .map(e -> ((StreamRecord<Integer>) e).getValue())
                            .collect(Collectors.toList());

            assertThat(outputs).containsExactly(1, 2, 3, 4, 5, 6);
        }
    }

    /**
     * Test batch + time trigger interaction with ordering preserved.
     *
     * <p>Small batch size with timeout, verify ordering across multiple batches.
     */
    @Test
    void testBatchAndTimeoutTriggerWithOrdering() throws Exception {
        final int maxBatchSize = 3;
        final long batchTimeoutMs = 100L;

        AsyncBatchFunction<Integer, Integer> function =
                (inputs, resultFuture) -> {
                    resultFuture.complete(inputs);
                };

        try (OneInputStreamOperatorTestHarness<Integer, Integer> testHarness =
                createTestHarnessWithTimeout(function, maxBatchSize, batchTimeoutMs)) {

            testHarness.open();
            testHarness.setProcessingTime(0L);

            // First batch: size-triggered (3 elements)
            testHarness.processElement(new StreamRecord<>(1, 1L));
            testHarness.processElement(new StreamRecord<>(2, 2L));
            testHarness.processElement(new StreamRecord<>(3, 3L));

            // Second batch: timeout-triggered (1 element)
            testHarness.setProcessingTime(200L);
            testHarness.processElement(new StreamRecord<>(4, 4L));
            testHarness.setProcessingTime(301L); // Trigger timeout

            // Third batch: size-triggered (3 elements)
            testHarness.setProcessingTime(400L);
            testHarness.processElement(new StreamRecord<>(5, 5L));
            testHarness.processElement(new StreamRecord<>(6, 6L));
            testHarness.processElement(new StreamRecord<>(7, 7L));

            testHarness.endInput();

            // Verify outputs are in strict input order
            List<Integer> outputs =
                    testHarness.getOutput().stream()
                            .filter(e -> e instanceof StreamRecord)
                            .map(e -> ((StreamRecord<Integer>) e).getValue())
                            .collect(Collectors.toList());

            assertThat(outputs).containsExactly(1, 2, 3, 4, 5, 6, 7);
        }
    }

    /** Test exception propagation - exception in batch invocation fails fast. */
    @Test
    void testExceptionPropagation() throws Exception {
        final int maxBatchSize = 2;

        AsyncBatchFunction<Integer, Integer> function =
                (inputs, resultFuture) -> {
                    resultFuture.completeExceptionally(new ExpectedTestException());
                };

        try (OneInputStreamOperatorTestHarness<Integer, Integer> testHarness =
                createTestHarness(function, maxBatchSize)) {

            testHarness.open();

            // Process 2 elements to trigger a batch
            testHarness.processElement(new StreamRecord<>(1, 1L));
            testHarness.processElement(new StreamRecord<>(2, 2L));

            testHarness.endInput();

            // Verify that the task environment received the exception
            assertThat(testHarness.getEnvironment().getActualExternalFailureCause())
                    .isPresent()
                    .get()
                    .satisfies(
                            t ->
                                    assertThat(t.getCause())
                                            .isInstanceOf(ExpectedTestException.class));
        }
    }

    /** Test ordering with delayed async completions simulating real async I/O. */
    @Test
    void testOrderingWithDelayedAsyncCompletion() throws Exception {
        final int maxBatchSize = 2;
        final List<Integer> completionOrder = new CopyOnWriteArrayList<>();

        AsyncBatchFunction<Integer, Integer> function =
                (inputs, resultFuture) -> {
                    List<Integer> inputCopy = new ArrayList<>(inputs);
                    int firstElement = inputCopy.get(0);

                    // Simulate varying async delays - earlier batches take longer
                    CompletableFuture.runAsync(
                            () -> {
                                try {
                                    // Batch starting with 1 delays more than batch starting with 3
                                    int delay = (5 - firstElement) * 20;
                                    Thread.sleep(delay);
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                }
                                completionOrder.add(firstElement);
                                resultFuture.complete(inputCopy);
                            });
                };

        try (OneInputStreamOperatorTestHarness<Integer, Integer> testHarness =
                createTestHarness(function, maxBatchSize)) {

            testHarness.open();

            // Process 4 elements: batch 0 = [1,2], batch 1 = [3,4]
            for (int i = 1; i <= 4; i++) {
                testHarness.processElement(new StreamRecord<>(i, i));
            }

            testHarness.endInput();

            // Verify outputs are in strict input order regardless of completion order
            List<Integer> outputs =
                    testHarness.getOutput().stream()
                            .filter(e -> e instanceof StreamRecord)
                            .map(e -> ((StreamRecord<Integer>) e).getValue())
                            .collect(Collectors.toList());

            assertThat(outputs).containsExactly(1, 2, 3, 4);
        }
    }

    /** Test that empty batches are not triggered. */
    @Test
    void testEmptyInput() throws Exception {
        final int maxBatchSize = 3;
        final AtomicInteger invocationCount = new AtomicInteger(0);

        AsyncBatchFunction<Integer, Integer> function =
                (inputs, resultFuture) -> {
                    invocationCount.incrementAndGet();
                    resultFuture.complete(Collections.emptyList());
                };

        try (OneInputStreamOperatorTestHarness<Integer, Integer> testHarness =
                createTestHarness(function, maxBatchSize)) {

            testHarness.open();
            testHarness.endInput();

            // No invocations should happen for empty input
            assertThat(invocationCount.get()).isEqualTo(0);
            assertThat(testHarness.getOutput()).isEmpty();
        }
    }

    /** Test single element batch with ordering. */
    @Test
    void testSingleElementBatchOrdering() throws Exception {
        final int maxBatchSize = 1;
        final List<Integer> batchSizes = new CopyOnWriteArrayList<>();

        AsyncBatchFunction<Integer, Integer> function =
                (inputs, resultFuture) -> {
                    batchSizes.add(inputs.size());
                    resultFuture.complete(inputs);
                };

        try (OneInputStreamOperatorTestHarness<Integer, Integer> testHarness =
                createTestHarness(function, maxBatchSize)) {

            testHarness.open();

            testHarness.processElement(new StreamRecord<>(3, 1L));
            testHarness.processElement(new StreamRecord<>(1, 2L));
            testHarness.processElement(new StreamRecord<>(2, 3L));

            testHarness.endInput();

            // Each element is its own batch
            assertThat(batchSizes).containsExactly(1, 1, 1);

            // Verify outputs maintain input order (not value order)
            List<Integer> outputs =
                    testHarness.getOutput().stream()
                            .filter(e -> e instanceof StreamRecord)
                            .map(e -> ((StreamRecord<Integer>) e).getValue())
                            .collect(Collectors.toList());

            // Output order should be [3, 1, 2] - same as input order
            assertThat(outputs).containsExactly(3, 1, 2);
        }
    }

    /** Test that batch function can return different number of outputs while maintaining order. */
    @Test
    void testVariableOutputSizeWithOrdering() throws Exception {
        final int maxBatchSize = 2;

        // Function that returns sum of batch (one output per batch)
        AsyncBatchFunction<Integer, Integer> function =
                (inputs, resultFuture) -> {
                    int sum = inputs.stream().mapToInt(Integer::intValue).sum();
                    resultFuture.complete(Collections.singletonList(sum));
                };

        try (OneInputStreamOperatorTestHarness<Integer, Integer> testHarness =
                createTestHarness(function, maxBatchSize)) {

            testHarness.open();

            // Process 4 elements: batch 0 = [1,2] -> 3, batch 1 = [3,4] -> 7
            for (int i = 1; i <= 4; i++) {
                testHarness.processElement(new StreamRecord<>(i, i));
            }

            testHarness.endInput();

            // First batch outputs first (3), then second batch (7)
            List<Integer> outputs =
                    testHarness.getOutput().stream()
                            .filter(e -> e instanceof StreamRecord)
                            .map(e -> ((StreamRecord<Integer>) e).getValue())
                            .collect(Collectors.toList());

            // Order should be [3, 7] - first batch result, then second batch result
            assertThat(outputs).containsExactly(3, 7);
        }
    }

    /** Test ordering with many batches to verify sequence number handling. */
    @Test
    void testManyBatchesOrdering() throws Exception {
        final int maxBatchSize = 2;
        final int totalElements = 20;

        AsyncBatchFunction<Integer, Integer> function =
                (inputs, resultFuture) -> {
                    resultFuture.complete(inputs);
                };

        try (OneInputStreamOperatorTestHarness<Integer, Integer> testHarness =
                createTestHarness(function, maxBatchSize)) {

            testHarness.open();

            // Process many elements
            for (int i = 1; i <= totalElements; i++) {
                testHarness.processElement(new StreamRecord<>(i, i));
            }

            testHarness.endInput();

            // Verify all outputs are in strict order
            List<Integer> outputs =
                    testHarness.getOutput().stream()
                            .filter(e -> e instanceof StreamRecord)
                            .map(e -> ((StreamRecord<Integer>) e).getValue())
                            .collect(Collectors.toList());

            List<Integer> expected = new ArrayList<>();
            for (int i = 1; i <= totalElements; i++) {
                expected.add(i);
            }

            assertThat(outputs).containsExactlyElementsOf(expected);
        }
    }

    private static OneInputStreamOperatorTestHarness<Integer, Integer> createTestHarness(
            AsyncBatchFunction<Integer, Integer> function, int maxBatchSize) throws Exception {
        return new OneInputStreamOperatorTestHarness<>(
                new OrderedAsyncBatchWaitOperatorFactory<>(function, maxBatchSize),
                IntSerializer.INSTANCE);
    }

    private static OneInputStreamOperatorTestHarness<Integer, Integer> createTestHarnessWithTimeout(
            AsyncBatchFunction<Integer, Integer> function, int maxBatchSize, long batchTimeoutMs)
            throws Exception {
        return new OneInputStreamOperatorTestHarness<>(
                new OrderedAsyncBatchWaitOperatorFactory<>(function, maxBatchSize, batchTimeoutMs),
                IntSerializer.INSTANCE);
    }
}
