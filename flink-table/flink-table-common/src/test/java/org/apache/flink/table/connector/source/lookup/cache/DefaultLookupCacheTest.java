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

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.groups.CacheMetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.clock.ManualClock;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

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
        try (DefaultLookupCache cache = createCache(DefaultLookupCache.newBuilder())) {
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
        try (DefaultLookupCache cache = createCache(DefaultLookupCache.newBuilder())) {
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
                createCache(DefaultLookupCache.newBuilder().cacheMissingKey(false))) {
            cache.put(KEY, Collections.emptyList());
            assertThat(cache.getIfPresent(KEY)).isNull();
        }
    }

    @Test
    void testCacheMetrics() throws Exception {
        InterceptingCacheMetricGroup metricGroup = new InterceptingCacheMetricGroup();
        try (DefaultLookupCache cache =
                createCache(DefaultLookupCache.newBuilder(), null, metricGroup)) {
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

    // ----------------------- Helper functions ----------------------

    private DefaultLookupCache createCache(DefaultLookupCache.Builder builder) {
        return createCache(builder, null, null);
    }

    private DefaultLookupCache createCache(DefaultLookupCache.Builder builder, Clock clock) {
        return createCache(builder, clock, null);
    }

    private DefaultLookupCache createCache(
            DefaultLookupCache.Builder builder, Clock clock, CacheMetricGroup metricGroup) {
        DefaultLookupCache cache = builder.build();
        if (clock != null) {
            cache.withClock(clock);
        }
        if (metricGroup == null) {
            cache.open(UnregisteredMetricsGroup.createCacheMetricGroup());
        } else {
            cache.open(metricGroup);
        }
        return cache;
    }

    // -------------------------- Helper classes ------------------------
    private static class InterceptingCacheMetricGroup extends UnregisteredMetricsGroup
            implements CacheMetricGroup {
        private Counter hitCounter;
        private Counter missCounter;
        private Counter loadCounter;
        private Counter numLoadFailuresCounter;
        private Gauge<Long> latestLoadTimeGauge;
        private Gauge<Long> numCachedRecordsGauge;
        private Gauge<Long> numCachedBytesGauge;

        @Override
        public void hitCounter(Counter hitCounter) {
            this.hitCounter = hitCounter;
        }

        @Override
        public void missCounter(Counter missCounter) {
            this.missCounter = missCounter;
        }

        @Override
        public void loadCounter(Counter loadCounter) {
            this.loadCounter = loadCounter;
        }

        @Override
        public void numLoadFailuresCounter(Counter numLoadFailuresCounter) {
            this.numLoadFailuresCounter = numLoadFailuresCounter;
        }

        @Override
        public void latestLoadTimeGauge(Gauge<Long> latestLoadTimeGauge) {
            this.latestLoadTimeGauge = latestLoadTimeGauge;
        }

        @Override
        public void numCachedRecordsGauge(Gauge<Long> numCachedRecordsGauge) {
            this.numCachedRecordsGauge = numCachedRecordsGauge;
        }

        @Override
        public void numCachedBytesGauge(Gauge<Long> numCachedBytesGauge) {
            this.numCachedBytesGauge = numCachedBytesGauge;
        }
    }
}
