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
import org.apache.flink.streaming.util.MockStreamingRuntimeContext;
import org.apache.flink.streaming.util.retryable.AsyncRetryStrategies;
import org.apache.flink.streaming.util.retryable.RetryPredicates;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncLookupFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.runtime.operators.join.lookup.ResultRetryStrategy;
import org.apache.flink.table.runtime.operators.join.lookup.RetryableAsyncLookupFunctionDelegator;
import org.apache.flink.table.runtime.util.RowDataHarnessAssertor;
import org.apache.flink.table.types.logical.LogicalType;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.flink.table.data.StringData.fromString;

/** Harness tests for {@link RetryableAsyncLookupFunctionDelegator}. */
public class RetryableAsyncLookupFunctionDelegatorTest {

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
        return new RetryableAsyncLookupFunctionDelegator(userLookupFunc, retryStrategy);
    }

    private final RowDataHarnessAssertor assertor =
            new RowDataHarnessAssertor(
                    new LogicalType[] {
                        DataTypes.INT().getLogicalType(), DataTypes.STRING().getLogicalType()
                    });

    @Test
    public void testLookupWithRetry() throws Exception {
        final RetryableAsyncLookupFunctionDelegator delegator = createDelegator(retryStrategy);
        delegator.open(new FunctionContext(new MockStreamingRuntimeContext(false, 1, 1)));
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
    public void testLookupWithRetryDisabled() throws Exception {
        final RetryableAsyncLookupFunctionDelegator delegator =
                createDelegator(ResultRetryStrategy.NO_RETRY_STRATEGY);
        delegator.open(new FunctionContext(new MockStreamingRuntimeContext(false, 1, 1)));
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
    public void testLookupWithCustomRetry() throws Exception {
        AsyncRetryStrategy retryStrategy =
                new AsyncRetryStrategies.ExponentialBackoffDelayRetryStrategyBuilder<>(
                                3, 1, 100, 1.1d)
                        .build();
        final RetryableAsyncLookupFunctionDelegator delegator =
                createDelegator(new ResultRetryStrategy(retryStrategy));
        delegator.open(new FunctionContext(new MockStreamingRuntimeContext(false, 1, 1)));
        for (int i = 1; i <= 5; i++) {
            RowData key = GenericRowData.of(i);
            assertor.assertOutputEquals(
                    "output wrong",
                    Collections.singleton(data.get(key)),
                    Collections.singleton(delegator.asyncLookup(key)));
        }
        delegator.close();
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
