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

package org.apache.flink.table.runtime.operators.join;

import org.apache.flink.streaming.api.functions.async.AsyncRetryStrategy;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.apache.flink.streaming.util.MockStreamingRuntimeContext;
import org.apache.flink.streaming.util.retryable.AsyncRetryStrategies;
import org.apache.flink.streaming.util.retryable.RetryPredicates;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncLookupFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.runtime.operators.join.lookup.LookupRetryTestUtils;
import org.apache.flink.table.runtime.operators.join.lookup.ResultRetryStrategy;
import org.apache.flink.table.runtime.operators.join.lookup.RetryableAsyncLookupFunctionDelegator;
import org.apache.flink.table.runtime.util.RowDataHarnessAssertor;
import org.apache.flink.table.types.logical.LogicalType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.flink.table.data.StringData.fromString;
import static org.assertj.core.api.Assertions.assertThat;

/** Harness tests for {@link RetryableAsyncLookupFunctionDelegator}. */
class RetryableAsyncLookupFunctionDelegatorTest {

    private final AsyncLookupFunction userLookupFunc = new TestingAsyncLookupFunction();

    private final ResultRetryStrategy retryStrategy =
            ResultRetryStrategy.fixedDelayRetry(3, 10, RetryPredicates.EMPTY_RESULT_PREDICATE);

    private static final Map<RowData, Collection<RowData>> data = new HashMap<>();

    static {
        data.put(
                GenericRowData.of(1),
                Collections.singletonList(GenericRowData.of(1, fromString("Julian"))));
        data.put(
                GenericRowData.of(3),
                Arrays.asList(
                        GenericRowData.of(3, fromString("Jark")),
                        GenericRowData.of(3, fromString("Jackson"))));
        data.put(
                GenericRowData.of(4),
                Collections.singletonList(GenericRowData.of(4, fromString("Fabian"))));
    }

    private RetryableAsyncLookupFunctionDelegator createDelegator(
            ResultRetryStrategy retryStrategy) {
        RetryableAsyncLookupFunctionDelegator delegator =
                new RetryableAsyncLookupFunctionDelegator(userLookupFunc, retryStrategy);
        LookupRetryTestUtils.setProcessingTimeService(delegator, new TestProcessingTimeService());
        return delegator;
    }

    private final RowDataHarnessAssertor assertor =
            new RowDataHarnessAssertor(
                    new LogicalType[] {
                        DataTypes.INT().getLogicalType(), DataTypes.STRING().getLogicalType()
                    });

    @Test
    void testLookupWithRetry() throws Exception {
        final RetryableAsyncLookupFunctionDelegator delegator = createDelegator(retryStrategy);
        delegator.open(new FunctionContext(new MockStreamingRuntimeContext(1, 0)));
        for (int i = 1; i <= 5; i++) {
            RowData key = GenericRowData.of(i);
            assertor.assertOutputEquals(
                    "output wrong",
                    Collections.singleton(data.get(key)),
                    Collections.singleton(delegator.asyncLookup(key)));
        }
        delegator.close();
    }

    @Test
    void testLookupWithRetryDisabled() throws Exception {
        final RetryableAsyncLookupFunctionDelegator delegator =
                createDelegator(ResultRetryStrategy.NO_RETRY_STRATEGY);
        delegator.open(new FunctionContext(new MockStreamingRuntimeContext(1, 0)));
        for (int i = 1; i <= 5; i++) {
            RowData key = GenericRowData.of(i);
            assertor.assertOutputEquals(
                    "output wrong",
                    Collections.singleton(data.get(key)),
                    Collections.singleton(delegator.asyncLookup(key)));
        }
        delegator.close();
    }

    @Test
    void testLookupWithCustomRetry() throws Exception {
        AsyncRetryStrategy retryStrategy =
                new AsyncRetryStrategies.ExponentialBackoffDelayRetryStrategyBuilder<>(
                                3, 1, 100, 1.1d)
                        .build();
        final RetryableAsyncLookupFunctionDelegator delegator =
                createDelegator(new ResultRetryStrategy(retryStrategy));
        delegator.open(new FunctionContext(new MockStreamingRuntimeContext(1, 0)));
        for (int i = 1; i <= 5; i++) {
            RowData key = GenericRowData.of(i);
            assertor.assertOutputEquals(
                    "output wrong",
                    Collections.singleton(data.get(key)),
                    Collections.singleton(delegator.asyncLookup(key)));
        }
        delegator.close();
    }

    @Test
    void testRetryBackoffDoesNotBlockLookupCompletionExecutor() throws Exception {
        try (LookupTestContext test = new LookupTestContext()) {
            long backoff = MINUTES.toMillis(1);
            RowData key = GenericRowData.of(1);
            AtomicInteger submissions = new AtomicInteger();
            CompletableFuture<Collection<RowData>> firstAttempt = test.newLookupFuture();
            RetryableAsyncLookupFunctionDelegator delegator =
                    test.open(
                            backoff,
                            ignored ->
                                    submissions.getAndIncrement() == 0
                                            ? firstAttempt
                                            : CompletableFuture.completedFuture(data.get(key)));
            CompletableFuture<Collection<RowData>> result = delegator.asyncLookup(key);

            firstAttempt.complete(Collections.emptyList());
            test.awaitCompletionExecutor();
            assertThat(submissions).hasValue(1);
            assertThat(result).isNotDone();

            test.processingTimeService.advance(backoff);
            assertThat(result.get(10, SECONDS)).isEqualTo(data.get(key));
        }
    }

    @Test
    void testRetryDoesNotOverlapNextLookupSubmission() throws Exception {
        try (LookupTestContext test = new LookupTestContext()) {
            RowData firstKey = GenericRowData.of(1);
            RowData secondKey = GenericRowData.of(3);
            CompletableFuture<Collection<RowData>> firstAttempt = test.newLookupFuture();
            CompletableFuture<Collection<RowData>> secondAttempt = new CompletableFuture<>();
            AtomicBoolean retryStarted = new AtomicBoolean();
            RetryableAsyncLookupFunctionDelegator delegator =
                    test.open(
                            1,
                            key -> {
                                if (key.equals(secondKey)) {
                                    // Process the miss while the next call is still submitting.
                                    firstAttempt.complete(Collections.emptyList());
                                    test.awaitCompletionExecutor();
                                    assertThat(retryStarted)
                                            .as("retry started during the next lookup submission")
                                            .isFalse();
                                    return secondAttempt;
                                }
                                if (!firstAttempt.isDone()) {
                                    return firstAttempt;
                                }
                                retryStarted.set(true);
                                return CompletableFuture.completedFuture(data.get(key));
                            });
            CompletableFuture<Collection<RowData>> firstResult = delegator.asyncLookup(firstKey);
            CompletableFuture<Collection<RowData>> secondResult = delegator.asyncLookup(secondKey);

            test.processingTimeService.advance(1);
            assertThat(firstResult.get(10, SECONDS)).isEqualTo(data.get(firstKey));

            secondAttempt.complete(data.get(secondKey));
            assertThat(secondResult.get(10, SECONDS)).isEqualTo(data.get(secondKey));
        }
    }

    private static final class LookupTestContext implements AutoCloseable {
        private final ExecutorService completionExecutor = Executors.newSingleThreadExecutor();
        private final TestProcessingTimeService processingTimeService =
                new TestProcessingTimeService();
        private RetryableAsyncLookupFunctionDelegator delegator;

        CompletableFuture<Collection<RowData>> newLookupFuture() {
            return new CompletableFuture<>() {
                @Override
                public Executor defaultExecutor() {
                    return completionExecutor;
                }
            };
        }

        void awaitCompletionExecutor() {
            try {
                completionExecutor.submit(() -> {}).get(10, SECONDS);
            } catch (Exception e) {
                throw new AssertionError("The lookup completion executor did not make progress", e);
            }
        }

        RetryableAsyncLookupFunctionDelegator open(
                long backoff, Function<RowData, CompletableFuture<Collection<RowData>>> lookup)
                throws Exception {
            AsyncLookupFunction lookupFunction =
                    new AsyncLookupFunction() {
                        @Override
                        public CompletableFuture<Collection<RowData>> asyncLookup(RowData keyRow) {
                            return lookup.apply(keyRow);
                        }
                    };
            delegator =
                    new RetryableAsyncLookupFunctionDelegator(
                            lookupFunction,
                            ResultRetryStrategy.fixedDelayRetry(
                                    1, backoff, RetryPredicates.EMPTY_RESULT_PREDICATE));
            processingTimeService.setCurrentTime(0);
            LookupRetryTestUtils.setProcessingTimeService(delegator, processingTimeService);
            delegator.open(new FunctionContext(null));
            return delegator;
        }

        @Override
        public void close() throws Exception {
            try {
                if (delegator != null) {
                    delegator.close();
                }
            } finally {
                completionExecutor.shutdownNow();
            }
        }
    }

    /** The {@link TestingAsyncLookupFunction} is a {@link AsyncLookupFunction} for testing. */
    private static final class TestingAsyncLookupFunction extends AsyncLookupFunction {

        private static final long serialVersionUID = 1L;

        private final Random random = new Random();
        private transient ExecutorService executor;

        @Override
        public void open(FunctionContext context) throws Exception {
            super.open(context);
            this.executor = Executors.newFixedThreadPool(2);
        }

        @Override
        public CompletableFuture<Collection<RowData>> asyncLookup(RowData keyRow) {
            return CompletableFuture.supplyAsync(
                    () -> {
                        try {
                            Thread.sleep(random.nextInt(5));
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                        return data.get(keyRow);
                    },
                    executor);
        }

        @Override
        public void close() throws Exception {
            if (null != executor && !executor.isShutdown()) {
                executor.shutdown();
            }
            super.close();
        }
    }
}
