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

package org.apache.flink.table.runtime.functions.table.fullcache;

import org.apache.flink.metrics.groups.CacheMetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.table.connector.source.lookup.cache.InterceptingCacheMetricGroup;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.functions.table.lookup.fullcache.LookupFullCache;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.ThrowingRunnable;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit test for {@link LookupFullCache}. */
public class LookupFullCacheTest {

    private final TestManualCacheReloadTrigger reloadTrigger = new TestManualCacheReloadTrigger();

    @Test
    void testAddDataAfterLoad() throws Exception {
        RowData key = row(2);
        List<RowData> newResult = Collections.singletonList(row(2, "Alex"));
        TestCacheLoader cacheLoader = new TestCacheLoader(cache -> cache.put(key, newResult));
        try (LookupFullCache fullCache = createAndLoadCache(cacheLoader)) {
            Collection<RowData> result = fullCache.getIfPresent(key);
            assertThat(result).isNotNull();
            assertThat(result.size()).isEqualTo(0);
            reloadTrigger.trigger();
            assertThat(cacheLoader.getNumLoads()).isEqualTo(2);
            result = fullCache.getIfPresent(key);
            assertThat(result).isEqualTo(newResult);
        }
        assertThat(reloadTrigger.isClosed()).isTrue();
        assertThat(cacheLoader.isStopped()).isTrue();
    }

    @Test
    void testUpdateDataAfterLoad() throws Exception {
        RowData key = row(4);
        List<RowData> newResult = Collections.singletonList(row(4, "Frank"));
        TestCacheLoader cacheLoader = new TestCacheLoader(cache -> cache.put(key, newResult));
        try (LookupFullCache fullCache = createAndLoadCache(cacheLoader)) {
            Collection<RowData> result = fullCache.getIfPresent(key);
            assertThat(result).isEqualTo(TestCacheLoader.DATA.get(key));
            reloadTrigger.trigger();
            assertThat(cacheLoader.getNumLoads()).isEqualTo(2);
            result = fullCache.getIfPresent(key);
            assertThat(result).isEqualTo(newResult);
        }
        assertThat(reloadTrigger.isClosed()).isTrue();
        assertThat(cacheLoader.isStopped()).isTrue();
    }

    @Test
    void testRemoveDataAfterLoad() throws Exception {
        RowData key = row(1);
        TestCacheLoader cacheLoader = new TestCacheLoader(cache -> cache.remove(key));
        try (LookupFullCache fullCache = createAndLoadCache(cacheLoader)) {
            Collection<RowData> result = fullCache.getIfPresent(key);
            assertThat(result).isEqualTo(TestCacheLoader.DATA.get(key));
            reloadTrigger.trigger();
            assertThat(cacheLoader.getNumLoads()).isEqualTo(2);
            result = fullCache.getIfPresent(key);
            assertThat(result).isNotNull();
            assertThat(result.size()).isEqualTo(0);
        }
        assertThat(reloadTrigger.isClosed()).isTrue();
        assertThat(cacheLoader.isStopped()).isTrue();
    }

    @Test
    void testExceptionDuringReload() throws Exception {
        RuntimeException exception = new RuntimeException("Reload failed.");
        TestCacheLoader cacheLoader =
                new TestCacheLoader(
                        cache -> {
                            throw exception;
                        });
        try (LookupFullCache fullCache = createAndLoadCache(cacheLoader)) {
            reloadTrigger.trigger();
            assertThat(cacheLoader.isStopped()).isTrue();
            assertThat(cacheLoader.getNumLoads()).isEqualTo(2);
            assertThatThrownBy(() -> fullCache.getIfPresent(row(1))).hasRootCause(exception);
            reloadTrigger.trigger();
            assertThat(cacheLoader.getNumLoads()).isEqualTo(2); // no reload after fail
        }
        assertThat(reloadTrigger.isClosed()).isTrue();
        assertThat(cacheLoader.isStopped()).isTrue();
    }

    @Test
    void testUnsupportedOperations() {
        TestCacheLoader cacheLoader = new TestCacheLoader(cache -> {});
        LookupFullCache fullCache = new LookupFullCache(cacheLoader, reloadTrigger);
        assertThatThrownBy(() -> fullCache.invalidate(row(1)))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("invalidate");
        assertThatThrownBy(() -> fullCache.put(row(1), Collections.singletonList(row(1, "Julian"))))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("put");
    }

    @Test
    void testCacheMetrics() throws Exception {
        TestCacheLoader cacheLoader = new TestCacheLoader(cache -> {});
        InterceptingCacheMetricGroup metricGroup = new InterceptingCacheMetricGroup();
        LookupFullCache fullCache = createAndLoadCache(cacheLoader, metricGroup);
        assertThat(metricGroup.hitCounter).isNotNull();
        assertThat(metricGroup.hitCounter.getCount()).isEqualTo(0);
        assertThat(metricGroup.missCounter).isNotNull();
        assertThat(metricGroup.missCounter.getCount()).isEqualTo(0);
        assertThat(fullCache.getIfPresent(row(1))).isNotEmpty(); // key '1' exists
        assertThat(metricGroup.hitCounter.getCount()).isEqualTo(1);
        assertThat(metricGroup.missCounter.getCount()).isEqualTo(0);
        assertThat(fullCache.getIfPresent(row(2))).isEmpty(); // key '2' doesn't exist
        assertThat(metricGroup.hitCounter.getCount()).isEqualTo(2);
        assertThat(metricGroup.missCounter.getCount()).isEqualTo(0);
    }

    @Test
    void testConcurrentAccess() throws Exception {
        int concurrency = 4;
        ExecutorService executor = Executors.newFixedThreadPool(concurrency);
        TestCacheLoader cacheLoader = new TestCacheLoader(cache -> {});
        InterceptingCacheMetricGroup metricGroup = new InterceptingCacheMetricGroup();
        try (LookupFullCache cache = new LookupFullCache(cacheLoader, reloadTrigger)) {
            List<CompletableFuture<?>> futures = new ArrayList<>();

            // Concurrently loads the cache
            for (int i = 0; i < concurrency; i++) {
                CompletableFuture<Void> future =
                        runAsync(
                                ThrowingRunnable.unchecked(
                                        () -> {
                                            cache.setUserCodeClassLoader(
                                                    Thread.currentThread().getContextClassLoader());
                                            cache.open(metricGroup);
                                        }),
                                executor);
                futures.add(future);
            }
            FutureUtils.waitForAll(futures).get();
            futures.clear();

            // Concurrently get entries from the cache
            for (int i = 0; i < concurrency; i++) {
                CompletableFuture<Void> hitFuture =
                        runAsync(
                                () -> {
                                    RowData key = row(1);
                                    assertThat(cache.getIfPresent(key))
                                            .isEqualTo(TestCacheLoader.DATA.get(key));
                                },
                                executor);
                futures.add(hitFuture);
            }
            FutureUtils.waitForAll(futures).get();

            // Validate metrics after concurrent accesses
            assertThat(metricGroup.hitCounter.getCount()).isEqualTo(concurrency);
            assertThat(metricGroup.missCounter.getCount()).isZero();
        } finally {
            executor.shutdownNow();
        }
    }

    // ----------------------- Helper functions ----------------------

    private LookupFullCache createAndLoadCache(TestCacheLoader cacheLoader) throws Exception {
        return createAndLoadCache(cacheLoader, UnregisteredMetricsGroup.createCacheMetricGroup());
    }

    private LookupFullCache createAndLoadCache(
            TestCacheLoader cacheLoader, CacheMetricGroup metricGroup) throws Exception {
        LookupFullCache fullCache = new LookupFullCache(cacheLoader, reloadTrigger);
        assertThat(cacheLoader.isAwaitTriggered()).isFalse();
        assertThat(cacheLoader.getNumLoads()).isZero();
        fullCache.setUserCodeClassLoader(Thread.currentThread().getContextClassLoader());
        fullCache.open(metricGroup);
        assertThat(cacheLoader.isAwaitTriggered()).isTrue();
        assertThat(cacheLoader.getNumLoads()).isEqualTo(1);
        assertThat(cacheLoader.getCache()).isEqualTo(TestCacheLoader.DATA);
        return fullCache;
    }

    private CompletableFuture<Void> runAsync(Runnable runnable, ExecutorService executor) {
        return CompletableFuture.runAsync(
                ThrowingRunnable.unchecked(
                        () -> {
                            Thread.sleep(ThreadLocalRandom.current().nextLong(0, 10));
                            runnable.run();
                        }),
                executor);
    }
}
