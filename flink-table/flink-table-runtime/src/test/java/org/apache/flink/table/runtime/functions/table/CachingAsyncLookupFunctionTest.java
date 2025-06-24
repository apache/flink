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

package org.apache.flink.table.runtime.functions.table;

import org.apache.flink.streaming.util.MockStreamingRuntimeContext;
import org.apache.flink.table.connector.source.lookup.cache.DefaultLookupCache;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncLookupFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.runtime.functions.table.lookup.CachingAsyncLookupFunction;
import org.apache.flink.util.concurrent.FutureUtils;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit test for {@link CachingAsyncLookupFunction}. */
class CachingAsyncLookupFunctionTest {

    private static final RowData KEY_1 = GenericRowData.of(1);
    private static final Collection<RowData> VALUE_1 =
            Collections.singletonList(GenericRowData.of(1, "Alice", 18L));
    private static final RowData KEY_2 = GenericRowData.of(2);
    private static final Collection<RowData> VALUE_2 =
            Arrays.asList(GenericRowData.of(2, "Bob", 20L), GenericRowData.of(2, "Charlie", 22L));
    private static final RowData NON_EXIST_KEY = GenericRowData.of(3);

    @Test
    void testCaching() throws Exception {
        TestingAsyncLookupFunction delegate = new TestingAsyncLookupFunction();
        CachingAsyncLookupFunction function = createCachingFunction(delegate);

        // All cache miss
        FutureUtils.completeAll(
                        Arrays.asList(
                                function.asyncLookup(KEY_1),
                                function.asyncLookup(KEY_2),
                                function.asyncLookup(NON_EXIST_KEY)))
                .get();

        // All cache hit
        FutureUtils.completeAll(
                        Arrays.asList(
                                function.asyncLookup(KEY_1),
                                function.asyncLookup(KEY_2),
                                function.asyncLookup(NON_EXIST_KEY)))
                .get();

        assertThat(delegate.getLookupCount()).hasValue(3);
        assertThat(function.getCache().getIfPresent(KEY_1))
                .containsExactlyInAnyOrderElementsOf(VALUE_1);
        assertThat(function.getCache().getIfPresent(KEY_2))
                .containsExactlyInAnyOrderElementsOf(VALUE_2);
        assertThat(function.getCache().getIfPresent(NON_EXIST_KEY)).isEmpty();
    }

    private CachingAsyncLookupFunction createCachingFunction(AsyncLookupFunction delegate)
            throws Exception {
        CachingAsyncLookupFunction function =
                new CachingAsyncLookupFunction(
                        DefaultLookupCache.newBuilder().maximumSize(Long.MAX_VALUE).build(),
                        delegate);
        function.open(new FunctionContext(new MockStreamingRuntimeContext(false, 1, 0)));
        return function;
    }

    private static final class TestingAsyncLookupFunction extends AsyncLookupFunction {
        private final transient ConcurrentMap<RowData, Collection<RowData>> data =
                new ConcurrentHashMap<>();
        private transient AtomicInteger lookupCount;
        private transient ExecutorService executor;

        @Override
        public void open(FunctionContext context) throws Exception {
            data.put(KEY_1, VALUE_1);
            data.put(KEY_2, VALUE_2);
            lookupCount = new AtomicInteger(0);
            executor = Executors.newFixedThreadPool(3);
        }

        @Override
        public CompletableFuture<Collection<RowData>> asyncLookup(RowData keyRow) {
            return CompletableFuture.supplyAsync(
                    () -> {
                        try {
                            Thread.sleep(ThreadLocalRandom.current().nextInt(0, 10));
                            Collection<RowData> values = data.get(keyRow);
                            lookupCount.incrementAndGet();
                            return values;
                        } catch (Exception e) {
                            throw new RuntimeException("Failed to lookup value", e);
                        }
                    },
                    executor);
        }

        public AtomicInteger getLookupCount() {
            return lookupCount;
        }
    }
}
