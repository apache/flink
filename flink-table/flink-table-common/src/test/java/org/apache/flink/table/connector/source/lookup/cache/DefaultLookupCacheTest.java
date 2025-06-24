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

package org.apache.flink.table.connector.source.lookup.cache;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.metrics.groups.CacheMetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.table.connector.source.lookup.LookupOptions;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.clock.ManualClock;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.ThrowingRunnable;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit test for {@link DefaultLookupCache}. */
class DefaultLookupCacheTest {

    private static final RowData KEY = GenericRowData.of("foo", "lookup", "key");
    private static final RowData NON_EXIST_KEY = GenericRowData.of("non-exist");
    private static final Collection<RowData> VALUE =
            Arrays.asList(
                    GenericRowData.of("bar", "lookup", "value", 0),
                    GenericRowData.of("bar", "lookup", "value", 1),
                    GenericRowData.of("bar", "lookup", "value", 2),
                    GenericRowData.of("bar", "lookup", "value", 3));

    @Test
    void testBasicReadWriteInCache() throws Exception {
        try (DefaultLookupCache cache =
                createCache(DefaultLookupCache.newBuilder().maximumSize(Long.MAX_VALUE))) {
            cache.put(KEY, VALUE);
            assertThat(cache.getIfPresent(NON_EXIST_KEY)).isNull();
            assertThat(cache.getIfPresent(KEY)).containsExactlyElementsOf(VALUE);
        }
    }

    @Test
    void testExpireAfterAccess() throws Exception {
        Duration expireDuration = Duration.ofSeconds(15213L);
        Duration margin = Duration.ofSeconds(10);
        ManualClock clock = new ManualClock();
        try (DefaultLookupCache cache =
                createCache(
                        DefaultLookupCache.newBuilder().expireAfterAccess(expireDuration), clock)) {
            cache.put(KEY, VALUE);
            // Advance the clock with (expireDuration - margin) and the key should not expire
            clock.advanceTime(expireDuration.minus(margin));
            // This access should succeed, also renews the entry
            assertThat(cache.getIfPresent(KEY)).containsExactlyElementsOf(VALUE);

            // Advance the clock with (expireDuration - margin) again to validate that the key is
            // not evicted because of the access above
            clock.advanceTime(expireDuration.minus(margin));
            assertThat(cache.getIfPresent(KEY)).containsExactlyElementsOf(VALUE);

            // Advance the clock with expireDuration + margins to expire the key
            clock.advanceTime(expireDuration.plus(margin));
            assertThat(cache.getIfPresent(KEY)).isNull();
        }
    }

    @Test
    void testExpireAfterWrite() throws Exception {
        Duration expireDuration = Duration.ofSeconds(15213L);
        Duration margin = Duration.ofSeconds(10);
        ManualClock clock = new ManualClock();
        try (DefaultLookupCache cache =
                createCache(
                        DefaultLookupCache.newBuilder().expireAfterWrite(expireDuration), clock)) {
            cache.put(KEY, VALUE);
            assertThat(cache.getIfPresent(KEY)).containsAll(VALUE);
            // Advance the clock to expire the key
            clock.advanceTime(expireDuration.plus(margin));
            assertThat(cache.getIfPresent(KEY)).isNull();
        }
    }

    @Test
    void testSizeBasedEviction() throws Exception {
        int cacheSize = 10;
        try (DefaultLookupCache cache =
                createCache(DefaultLookupCache.newBuilder().maximumSize(cacheSize))) {
            for (int i = 0; i < cacheSize; i++) {
                cache.put(GenericRowData.of("lookup", "key", i), VALUE);
            }
            // Put one more entry into the cache, and the first one should be evicted
            cache.put(GenericRowData.of("lookup", "key", cacheSize), VALUE);
            assertThat(cache.getIfPresent(GenericRowData.of("lookup", "key", 0))).isNull();
            // Other entries and the newly put one should stay in the cache
            for (int i = 1; i < cacheSize + 1; i++) {
                assertThat(cache.getIfPresent(GenericRowData.of("lookup", "key", i))).isNotNull();
            }
        }
    }

    @Test
    void testCacheMissingKey() throws Exception {
        try (DefaultLookupCache cache =
                createCache(DefaultLookupCache.newBuilder().maximumSize(Long.MAX_VALUE))) {
            // Caching null key and value is not allowed
            assertThatThrownBy(() -> cache.put(null, VALUE))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessage("Cannot put an entry with null key into the cache");
            assertThatThrownBy(() -> cache.put(KEY, null))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessage("Cannot put an entry with null value into the cache");
            // The default behaviour should be caching missing key
            cache.put(KEY, Collections.emptyList());
            assertThat(cache.getIfPresent(KEY)).isNotNull().isEmpty();
        }

        // Explicitly disable caching missing key
        try (DefaultLookupCache cache =
                createCache(
                        DefaultLookupCache.newBuilder()
                                .cacheMissingKey(false)
                                .maximumSize(Long.MAX_VALUE))) {
            cache.put(KEY, Collections.emptyList());
            assertThat(cache.getIfPresent(KEY)).isNull();
        }
    }

    @Test
    void testCacheMetrics() throws Exception {
        InterceptingCacheMetricGroup metricGroup = new InterceptingCacheMetricGroup();
        try (DefaultLookupCache cache =
                createCache(
                        DefaultLookupCache.newBuilder()
                                .maximumSize(Long.MAX_VALUE)
                                .maximumSize(Long.MAX_VALUE),
                        null,
                        metricGroup)) {
            // These metrics are registered
            assertThat(metricGroup.hitCounter).isNotNull();
            assertThat(metricGroup.hitCounter.getCount()).isEqualTo(0);
            assertThat(metricGroup.missCounter).isNotNull();
            assertThat(metricGroup.missCounter.getCount()).isEqualTo(0);
            assertThat(metricGroup.numCachedRecordsGauge).isNotNull();
            assertThat(metricGroup.numCachedRecordsGauge.getValue()).isEqualTo(0);

            // These metrics are left blank
            assertThat(metricGroup.loadCounter).isNull();
            assertThat(metricGroup.numLoadFailuresCounter).isNull();
            assertThat(metricGroup.latestLoadTimeGauge).isNull();
            assertThat(metricGroup.numCachedBytesGauge).isNull();

            cache.put(KEY, VALUE);
            assertThat(metricGroup.numCachedRecordsGauge.getValue()).isEqualTo(1);
            cache.getIfPresent(KEY);
            assertThat(metricGroup.hitCounter.getCount()).isEqualTo(1);
            cache.getIfPresent(NON_EXIST_KEY);
            assertThat(metricGroup.missCounter.getCount()).isEqualTo(1);
        }
    }

    @Test
    void testCacheSerialization() throws Exception {
        DefaultLookupCache cache =
                DefaultLookupCache.newBuilder()
                        .cacheMissingKey(true)
                        .maximumSize(15213L)
                        .expireAfterWrite(Duration.ofMillis(18213L))
                        .expireAfterAccess(Duration.ofMillis(15513L))
                        .build();

        // Serialize and deserialize the cache
        DefaultLookupCache cacheCopy = CommonTestUtils.createCopySerializable(cache);

        // Validate configurations are kept in the copy as expected
        assertThat(cacheCopy.isCacheMissingKey()).isEqualTo(true);
        assertThat(cacheCopy.getMaximumSize()).isEqualTo(15213L);
        assertThat(cacheCopy.getExpireAfterWriteDuration()).isEqualTo(Duration.ofMillis(18213L));
        assertThat(cacheCopy.getExpireAfterAccessDuration()).isEqualTo(Duration.ofMillis(15513L));
    }

    @Test
    void testConcurrentAccess() throws Exception {
        int concurrency = 4;
        ExecutorService executor = Executors.newFixedThreadPool(concurrency * 2);
        InterceptingCacheMetricGroup metricGroup = new InterceptingCacheMetricGroup();
        try (DefaultLookupCache cache =
                createCache(DefaultLookupCache.newBuilder().maximumSize(Long.MAX_VALUE))) {
            List<CompletableFuture<?>> futures = new ArrayList<>();

            // Concurrently put entries into the cache
            for (int i = 0; i < concurrency; i++) {
                String key = "key-" + i;
                String value = "value-" + i;
                CompletableFuture<Void> future =
                        runAsync(
                                () -> {
                                    cache.open(metricGroup);
                                    cache.put(
                                            GenericRowData.of(key),
                                            Collections.singleton(GenericRowData.of(value)));
                                },
                                executor);
                futures.add(future);
            }
            FutureUtils.waitForAll(futures).get();
            futures.clear();

            // Concurrently get entries from the cache
            for (int i = 0; i < concurrency; i++) {
                String key = "key-" + i;
                String value = "value-" + i;
                // cache hit
                CompletableFuture<Void> hitFuture =
                        runAsync(
                                () -> {
                                    cache.open(metricGroup);
                                    assertThat(cache.getIfPresent(GenericRowData.of(key)))
                                            .isEqualTo(
                                                    Collections.singleton(
                                                            GenericRowData.of(value)));
                                },
                                executor);
                futures.add(hitFuture);
                // Cache miss
                CompletableFuture<Void> missFuture =
                        runAsync(
                                () -> {
                                    cache.open(metricGroup);
                                    assertThat(cache.getIfPresent(NON_EXIST_KEY)).isNull();
                                },
                                executor);
                futures.add(missFuture);
            }
            FutureUtils.waitForAll(futures).get();

            // Validate metrics after concurrent accesses
            assertThat(metricGroup.hitCounter.getCount()).isEqualTo(concurrency);
            assertThat(metricGroup.missCounter.getCount()).isEqualTo(concurrency);
            assertThat(metricGroup.numCachedRecordsGauge.getValue()).isEqualTo(concurrency);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void testBuildFromConfig() {
        // Happy path
        Configuration config = new Configuration();
        config.set(LookupOptions.CACHE_TYPE, LookupOptions.LookupCacheType.PARTIAL);
        config.set(LookupOptions.PARTIAL_CACHE_MAX_ROWS, 15213L);
        config.set(LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_WRITE, Duration.ofMillis(18213L));
        config.set(LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_ACCESS, Duration.ofMillis(15513L));
        config.set(LookupOptions.PARTIAL_CACHE_CACHE_MISSING_KEY, false);
        DefaultLookupCache cache = DefaultLookupCache.fromConfig(config);
        assertThat(cache.getMaximumSize()).isEqualTo(15213L);
        assertThat(cache.getExpireAfterWriteDuration()).isEqualTo(Duration.ofMillis(18213L));
        assertThat(cache.getExpireAfterAccessDuration()).isEqualTo(Duration.ofMillis(15513L));
        assertThat(cache.isCacheMissingKey()).isFalse();

        // Illegal configurations
        Configuration configWithIllegalCacheType = new Configuration();
        configWithIllegalCacheType.set(
                LookupOptions.CACHE_TYPE, LookupOptions.LookupCacheType.NONE);
        assertThatThrownBy(() -> DefaultLookupCache.fromConfig(configWithIllegalCacheType))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "'lookup.cache' should be 'PARTIAL' in order to build a default lookup cache");

        Configuration configWithoutEviction = new Configuration();
        configWithoutEviction.set(LookupOptions.CACHE_TYPE, LookupOptions.LookupCacheType.PARTIAL);
        assertThatThrownBy(() -> DefaultLookupCache.fromConfig(configWithoutEviction))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Missing 'lookup.partial-cache.expire-after-access', "
                                + "'lookup.partial-cache.expire-after-write' "
                                + "or 'lookup.partial-cache.max-rows' in the configuration. "
                                + "The cache will not have evictions under this configuration "
                                + "and could lead to potential memory issues "
                                + "as the cache size may grow indefinitely.");
    }

    @Test
    void testBuilder() {
        // Happy path
        DefaultLookupCache cache =
                DefaultLookupCache.newBuilder()
                        .cacheMissingKey(true)
                        .maximumSize(15213L)
                        .expireAfterWrite(Duration.ofMillis(18213L))
                        .expireAfterAccess(Duration.ofMillis(15513L))
                        .build();
        assertThat(cache.isCacheMissingKey()).isEqualTo(true);
        assertThat(cache.getMaximumSize()).isEqualTo(15213L);
        assertThat(cache.getExpireAfterWriteDuration()).isEqualTo(Duration.ofMillis(18213L));
        assertThat(cache.getExpireAfterAccessDuration()).isEqualTo(Duration.ofMillis(15513L));

        // Test illegal usage without eviction policy
        assertThatThrownBy(() -> DefaultLookupCache.newBuilder().build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Expiration duration and maximum size are not set for the cache. "
                                + "The cache will not have any eviction and could lead to "
                                + "potential memory issues as the cache size may grow infinitely.");
    }

    // ----------------------- Helper functions ----------------------

    private DefaultLookupCache createCache(DefaultLookupCache.Builder builder) throws Exception {
        return createCache(builder, null, null);
    }

    private DefaultLookupCache createCache(DefaultLookupCache.Builder builder, Clock clock)
            throws Exception {
        return createCache(builder, clock, null);
    }

    private DefaultLookupCache createCache(
            DefaultLookupCache.Builder builder, Clock clock, CacheMetricGroup metricGroup)
            throws Exception {
        // We use a serializable copy here to make sure that all functionalities work as expected
        // after the cache being serialized and deserialized.
        DefaultLookupCache copiedCache = CommonTestUtils.createCopySerializable(builder.build());
        if (clock != null) {
            copiedCache.withClock(clock);
        }
        if (metricGroup == null) {
            copiedCache.open(UnregisteredMetricsGroup.createCacheMetricGroup());
        } else {
            copiedCache.open(metricGroup);
        }
        return copiedCache;
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
