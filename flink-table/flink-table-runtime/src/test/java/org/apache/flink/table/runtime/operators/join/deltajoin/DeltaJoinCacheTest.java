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

package org.apache.flink.table.runtime.operators.join.deltajoin;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Metric;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.table.data.RowData;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.util.function.TriConsumer;

import org.apache.flink.shaded.guava33.com.google.common.collect.Maps;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import static org.apache.flink.table.runtime.operators.join.deltajoin.DeltaJoinCache.LEFT_CACHE_METRIC_PREFIX;
import static org.apache.flink.table.runtime.operators.join.deltajoin.DeltaJoinCache.METRIC_HIT_COUNT;
import static org.apache.flink.table.runtime.operators.join.deltajoin.DeltaJoinCache.METRIC_HIT_RATE;
import static org.apache.flink.table.runtime.operators.join.deltajoin.DeltaJoinCache.METRIC_KEY_SIZE;
import static org.apache.flink.table.runtime.operators.join.deltajoin.DeltaJoinCache.METRIC_REQUEST_COUNT;
import static org.apache.flink.table.runtime.operators.join.deltajoin.DeltaJoinCache.METRIC_TOTAL_NON_EMPTY_VALUE_SIZE;
import static org.apache.flink.table.runtime.operators.join.deltajoin.DeltaJoinCache.RIGHT_CACHE_METRIC_PREFIX;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link DeltaJoinCache}. */
@ExtendWith(ParameterizedTestExtension.class)
class DeltaJoinCacheTest {

    private static final Long LEFT_CACHE_SIZE = 3L;
    private static final Long RIGHT_CACHE_SIZE = 2L;

    @Parameters(name = "testRightCache = {0}")
    private static List<Boolean> parameters() {
        return Arrays.asList(false, true);
    }

    @Parameter private boolean testRightCache;

    private DeltaJoinCache cache;
    private Runnable requestCacheFunc;
    private Runnable hitCacheFunc;
    private BiConsumer<RowData, LinkedHashMap<RowData, Object>> buildCacheFunc;
    private TriConsumer<RowData, RowData, Object> upsertCacheFunc;

    @BeforeEach
    void before() {
        cache = new DeltaJoinCache(LEFT_CACHE_SIZE, RIGHT_CACHE_SIZE);

        requestCacheFunc =
                () -> {
                    if (testRightCache) {
                        cache.requestRightCache();
                    } else {
                        cache.requestLeftCache();
                    }
                };
        hitCacheFunc =
                () -> {
                    if (testRightCache) {
                        cache.hitRightCache();
                    } else {
                        cache.hitLeftCache();
                    }
                };
        buildCacheFunc = (key, ukDataMap) -> cache.buildCache(key, ukDataMap, testRightCache);
        upsertCacheFunc = (key, uk, data) -> cache.upsertCache(key, uk, data, testRightCache);
    }

    @TestTemplate
    void testReportMetrics() {
        Map<String, Metric> allMetrics = new HashMap<>();
        cache.registerMetrics(
                new UnregisteredMetricGroups.UnregisteredOperatorMetricGroup() {
                    @Override
                    protected void addMetric(String name, Metric metric) {
                        allMetrics.put(name, metric);
                        super.addMetric(name, metric);
                    }
                });

        assertReportMetricsInternal(allMetrics, 0, 0, 0.0, 0, 0);
        requestCacheFunc.run();
        assertReportMetricsInternal(allMetrics, 1, 0, 0.0, 0, 0);
        hitCacheFunc.run();
        assertReportMetricsInternal(allMetrics, 1, 1, 1.0, 0, 0);
        requestCacheFunc.run();
        assertReportMetricsInternal(allMetrics, 2, 1, 0.5, 0, 0);

        buildCacheFunc.accept(row("ck1"), Maps.newLinkedHashMap());
        assertReportMetricsInternal(allMetrics, 2, 1, 0.5, 1, 0);
        buildCacheFunc.accept(
                row("ck2"),
                Maps.newLinkedHashMap(Map.of(row("pk1"), 1, row("pk2"), 2, row("pk3"), 3)));
        assertReportMetricsInternal(allMetrics, 2, 1, 0.5, 2, 3);
        upsertCacheFunc.accept(row("ck1"), row("pk4"), 4);
        assertReportMetricsInternal(allMetrics, 2, 1, 0.5, 2, 4);
    }

    @SuppressWarnings("unchecked")
    private void assertReportMetricsInternal(
            Map<String, Metric> actualAllMetrics,
            long expectedRequestCount,
            long expectedHitCount,
            double expectedHitRate,
            long expectedKeySize,
            long expectedNonEmptyValueSize) {
        String prefix = testRightCache ? RIGHT_CACHE_METRIC_PREFIX : LEFT_CACHE_METRIC_PREFIX;

        String hitRate = prefix + METRIC_HIT_RATE;
        assertThat(((Gauge<Double>) actualAllMetrics.get(hitRate)).getValue())
                .isEqualTo(expectedHitRate);

        String requestCount = prefix + METRIC_REQUEST_COUNT;
        assertThat(((Gauge<Long>) actualAllMetrics.get(requestCount)).getValue())
                .isEqualTo(expectedRequestCount);

        String hitCount = prefix + METRIC_HIT_COUNT;
        assertThat(((Gauge<Long>) actualAllMetrics.get(hitCount)).getValue())
                .isEqualTo(expectedHitCount);

        String keySize = prefix + METRIC_KEY_SIZE;
        assertThat(((Gauge<Long>) actualAllMetrics.get(keySize)).getValue())
                .isEqualTo(expectedKeySize);

        String totalNonEmptyValueSize = prefix + METRIC_TOTAL_NON_EMPTY_VALUE_SIZE;
        assertThat(((Gauge<Long>) actualAllMetrics.get(totalNonEmptyValueSize)).getValue())
                .isEqualTo(expectedNonEmptyValueSize);
    }
}
