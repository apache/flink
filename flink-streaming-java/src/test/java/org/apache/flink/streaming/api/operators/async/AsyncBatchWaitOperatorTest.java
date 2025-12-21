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

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link AsyncBatchWaitOperator}.
 *
 * <p>These tests verify:
 *
 * <ul>
 *   <li>Batch size trigger - elements are batched correctly
 *   <li>Correct result emission - all outputs are emitted downstream
 *   <li>Exception propagation - errors fail the operator
 * </ul>
 */
@Timeout(value = 100, unit = TimeUnit.SECONDS)
class AsyncBatchWaitOperatorTest {

    /**
     * Test that the operator correctly batches elements based on maxBatchSize.
     *
     * <p>Input: 5 records with maxBatchSize = 3
     *
     * <p>Expected: 2 batch invocations with sizes [3, 2]
     */
    @Test
    void testBatchSizeTrigger() throws Exception {
        final int maxBatchSize = 3;
        final List<Integer> batchSizes = new CopyOnWriteArrayList<>();

        AsyncBatchFunction<Integer, Integer> function =
                (inputs, resultFuture) -> {
                    batchSizes.add(inputs.size());
                    // Return input * 2 for each element
                    List<Integer> results =
                            inputs.stream().map(i -> i * 2).collect(Collectors.toList());
                    resultFuture.complete(results);
                };

        try (OneInputStreamOperatorTestHarness<Integer, Integer> testHarness =
                createTestHarness(function, maxBatchSize)) {

            testHarness.open();

            // Process 5 elements
            testHarness.processElement(new StreamRecord<>(1, 1L));
            testHarness.processElement(new StreamRecord<>(2, 2L));
            testHarness.processElement(new StreamRecord<>(3, 3L));
            // First batch of 3 should be triggered here

            testHarness.processElement(new StreamRecord<>(4, 4L));
            testHarness.processElement(new StreamRecord<>(5, 5L));
            // Remaining 2 elements in buffer

            testHarness.endInput();
            // Second batch of 2 should be triggered on endInput

            // Verify batch sizes
            assertThat(batchSizes).containsExactly(3, 2);
        }
    }

    /** Test that all results from the batch function are correctly emitted downstream. */
    @Test
    void testCorrectResultEmission() throws Exception {
        final int maxBatchSize = 3;

        // Function that doubles each input
        AsyncBatchFunction<Integer, Integer> function =
                (inputs, resultFuture) -> {
                    List<Integer> results =
                            inputs.stream().map(i -> i * 2).collect(Collectors.toList());
                    resultFuture.complete(results);
                };

        try (OneInputStreamOperatorTestHarness<Integer, Integer> testHarness =
                createTestHarness(function, maxBatchSize)) {

            testHarness.open();

            // Process 5 elements: 1, 2, 3, 4, 5
            for (int i = 1; i <= 5; i++) {
                testHarness.processElement(new StreamRecord<>(i, i));
            }

            testHarness.endInput();

            // Verify outputs: should be 2, 4, 6, 8, 10
            List<Integer> outputs =
                    testHarness.getOutput().stream()
                            .filter(e -> e instanceof StreamRecord)
                            .map(e -> ((StreamRecord<Integer>) e).getValue())
                            .collect(Collectors.toList());

            assertThat(outputs).containsExactlyInAnyOrder(2, 4, 6, 8, 10);
        }
    }

    /** Test that exceptions from the batch function are properly propagated. */
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

            // The exception should be propagated - we need to yield to process the async result
            // In the test harness, the exception is recorded in the environment
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

    /** Test async completion using CompletableFuture. */
    @Test
    void testAsyncCompletion() throws Exception {
        final int maxBatchSize = 2;
        final AtomicInteger invocationCount = new AtomicInteger(0);

        AsyncBatchFunction<Integer, Integer> function =
                (inputs, resultFuture) -> {
                    invocationCount.incrementAndGet();
                    // Simulate async processing
                    CompletableFuture.supplyAsync(
                                    () ->
                                            inputs.stream()
                                                    .map(i -> i * 3)
                                                    .collect(Collectors.toList()))
                            .thenAccept(resultFuture::complete);
                };

        try (OneInputStreamOperatorTestHarness<Integer, Integer> testHarness =
                createTestHarness(function, maxBatchSize)) {

            testHarness.open();

            // Process 4 elements: should trigger 2 batches
            for (int i = 1; i <= 4; i++) {
                testHarness.processElement(new StreamRecord<>(i, i));
            }

            testHarness.endInput();

            // Verify invocation count
            assertThat(invocationCount.get()).isEqualTo(2);

            // Verify outputs: should be 3, 6, 9, 12
            List<Integer> outputs =
                    testHarness.getOutput().stream()
                            .filter(e -> e instanceof StreamRecord)
                            .map(e -> ((StreamRecord<Integer>) e).getValue())
                            .collect(Collectors.toList());

            assertThat(outputs).containsExactlyInAnyOrder(3, 6, 9, 12);
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

    /** Test that batch function can return fewer or more outputs than inputs. */
    @Test
    void testVariableOutputSize() throws Exception {
        final int maxBatchSize = 3;

        // Function that returns only one output per batch (aggregation-style)
        AsyncBatchFunction<Integer, Integer> function =
                (inputs, resultFuture) -> {
                    int sum = inputs.stream().mapToInt(Integer::intValue).sum();
                    resultFuture.complete(Collections.singletonList(sum));
                };

        try (OneInputStreamOperatorTestHarness<Integer, Integer> testHarness =
                createTestHarness(function, maxBatchSize)) {

            testHarness.open();

            // Process 5 elements: 1, 2, 3, 4, 5
            for (int i = 1; i <= 5; i++) {
                testHarness.processElement(new StreamRecord<>(i, i));
            }

            testHarness.endInput();

            // First batch: 1+2+3 = 6, Second batch: 4+5 = 9
            List<Integer> outputs =
                    testHarness.getOutput().stream()
                            .filter(e -> e instanceof StreamRecord)
                            .map(e -> ((StreamRecord<Integer>) e).getValue())
                            .collect(Collectors.toList());

            assertThat(outputs).containsExactlyInAnyOrder(6, 9);
        }
    }

    /** Test single element batch (maxBatchSize = 1). */
    @Test
    void testSingleElementBatch() throws Exception {
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

            testHarness.processElement(new StreamRecord<>(1, 1L));
            testHarness.processElement(new StreamRecord<>(2, 2L));
            testHarness.processElement(new StreamRecord<>(3, 3L));

            testHarness.endInput();

            // Each element should trigger its own batch
            assertThat(batchSizes).containsExactly(1, 1, 1);
        }
    }

    private static OneInputStreamOperatorTestHarness<Integer, Integer> createTestHarness(
            AsyncBatchFunction<Integer, Integer> function, int maxBatchSize) throws Exception {
        return new OneInputStreamOperatorTestHarness<>(
                new AsyncBatchWaitOperatorFactory<>(function, maxBatchSize),
                IntSerializer.INSTANCE);
    }
}
