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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava33.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava33.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.guava33.com.google.common.cache.RemovalListener;
import org.apache.flink.shaded.guava33.com.google.common.cache.RemovalNotification;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.util.LinkedHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Cache for both sides in delta join.
 *
 * <p>Note: This cache is not thread-safe although its inner {@link Cache} is thread-safe.
 */
@NotThreadSafe
public class DeltaJoinCache {

    private static final String LEFT_CACHE_METRIC_PREFIX = "deltaJoin.leftCache.";
    private static final String RIGHT_CACHE_METRIC_PREFIX = "deltaJoin.rightCache.";

    private static final String METRIC_HIT_RATE = "hitRate";
    private static final String METRIC_REQUEST_COUNT = "requestCount";
    private static final String METRIC_HIT_COUNT = "hitCount";
    private static final String METRIC_KEY_SIZE = "keySize";
    private static final String METRIC_TOTAL_NON_EMPTY_VALUE_SIZE = "totalNonEmptyValues";

    // use LinkedHashMap to keep order
    private final Cache<RowData, LinkedHashMap<RowData, Object>> leftCache;
    private final Cache<RowData, LinkedHashMap<RowData, Object>> rightCache;

    // metrics
    private final AtomicLong leftTotalSize = new AtomicLong(0L);
    private final AtomicLong rightTotalSize = new AtomicLong(0L);
    private final AtomicLong leftHitCount = new AtomicLong(0L);
    private final AtomicLong leftRequestCount = new AtomicLong(0L);
    private final AtomicLong rightHitCount = new AtomicLong(0L);
    private final AtomicLong rightRequestCount = new AtomicLong(0L);

    public DeltaJoinCache(long leftCacheMaxSize, long rightCacheMaxSize) {
        this.leftCache =
                CacheBuilder.newBuilder()
                        .maximumSize(leftCacheMaxSize)
                        .removalListener(new DeltaJoinCacheRemovalListener(true))
                        .build();
        this.rightCache =
                CacheBuilder.newBuilder()
                        .maximumSize(rightCacheMaxSize)
                        .removalListener(new DeltaJoinCacheRemovalListener(false))
                        .build();
    }

    public void registerMetrics(MetricGroup metricGroup) {
        // left cache metric
        metricGroup.<Double, Gauge<Double>>gauge(
                LEFT_CACHE_METRIC_PREFIX + METRIC_HIT_RATE,
                () ->
                        leftRequestCount.get() == 0
                                ? 0.0
                                : Long.valueOf(leftHitCount.get()).doubleValue()
                                        / leftRequestCount.get());
        metricGroup.<Long, Gauge<Long>>gauge(
                LEFT_CACHE_METRIC_PREFIX + METRIC_REQUEST_COUNT, rightRequestCount::get);
        metricGroup.<Long, Gauge<Long>>gauge(
                LEFT_CACHE_METRIC_PREFIX + METRIC_HIT_COUNT, leftHitCount::get);
        metricGroup.<Long, Gauge<Long>>gauge(
                LEFT_CACHE_METRIC_PREFIX + METRIC_KEY_SIZE, leftCache::size);

        metricGroup.<Long, Gauge<Long>>gauge(
                LEFT_CACHE_METRIC_PREFIX + METRIC_TOTAL_NON_EMPTY_VALUE_SIZE, leftTotalSize::get);

        // right cache metric
        metricGroup.<Double, Gauge<Double>>gauge(
                RIGHT_CACHE_METRIC_PREFIX + METRIC_HIT_RATE,
                () ->
                        rightRequestCount.get() == 0
                                ? 0.0
                                : Long.valueOf(rightHitCount.get()).doubleValue()
                                        / rightRequestCount.get());
        metricGroup.<Long, Gauge<Long>>gauge(
                RIGHT_CACHE_METRIC_PREFIX + METRIC_REQUEST_COUNT, rightRequestCount::get);
        metricGroup.<Long, Gauge<Long>>gauge(
                RIGHT_CACHE_METRIC_PREFIX + METRIC_HIT_COUNT, rightHitCount::get);
        metricGroup.<Long, Gauge<Long>>gauge(
                RIGHT_CACHE_METRIC_PREFIX + METRIC_KEY_SIZE, rightCache::size);
        metricGroup.<Long, Gauge<Long>>gauge(
                RIGHT_CACHE_METRIC_PREFIX + METRIC_TOTAL_NON_EMPTY_VALUE_SIZE, rightTotalSize::get);
    }

    @Nullable
    public LinkedHashMap<RowData, Object> getData(RowData key, boolean requestRightCache) {
        return requestRightCache ? rightCache.getIfPresent(key) : leftCache.getIfPresent(key);
    }

    public void buildCache(
            RowData key, LinkedHashMap<RowData, Object> ukDataMap, boolean buildRightCache) {
        Preconditions.checkState(getData(key, buildRightCache) == null);
        if (buildRightCache) {
            rightCache.put(key, ukDataMap);
            rightTotalSize.addAndGet(ukDataMap.size());
        } else {
            leftCache.put(key, ukDataMap);
            leftTotalSize.addAndGet(ukDataMap.size());
        }
    }

    public void upsertCache(RowData key, RowData uk, Object data, boolean upsertRightCache) {
        if (upsertRightCache) {
            upsert(rightCache, key, uk, data, rightTotalSize);
        } else {
            upsert(leftCache, key, uk, data, leftTotalSize);
        }
    }

    private void upsert(
            Cache<RowData, LinkedHashMap<RowData, Object>> cache,
            RowData key,
            RowData uk,
            Object data,
            AtomicLong cacheTotalSize) {
        cache.asMap()
                .computeIfPresent(
                        key,
                        (k, v) -> {
                            Object oldData = v.put(uk, data);
                            if (oldData == null) {
                                cacheTotalSize.incrementAndGet();
                            }
                            return v;
                        });
    }

    public void requestLeftCache() {
        leftRequestCount.incrementAndGet();
    }

    public void requestRightCache() {
        rightRequestCount.incrementAndGet();
    }

    public void hitLeftCache() {
        leftHitCount.incrementAndGet();
    }

    public void hitRightCache() {
        rightHitCount.incrementAndGet();
    }

    private class DeltaJoinCacheRemovalListener
            implements RemovalListener<RowData, LinkedHashMap<RowData, Object>> {

        private final boolean isLeftCache;

        public DeltaJoinCacheRemovalListener(boolean isLeftCache) {
            this.isLeftCache = isLeftCache;
        }

        @Override
        public void onRemoval(
                RemovalNotification<RowData, LinkedHashMap<RowData, Object>> removalNotification) {
            if (removalNotification.getValue() == null) {
                return;
            }

            if (isLeftCache) {
                leftTotalSize.addAndGet(-removalNotification.getValue().size());
            } else {
                rightTotalSize.addAndGet(-removalNotification.getValue().size());
            }
        }
    }

    // ===== visible for test =====

    @VisibleForTesting
    public Cache<RowData, LinkedHashMap<RowData, Object>> getLeftCache() {
        return leftCache;
    }

    @VisibleForTesting
    public Cache<RowData, LinkedHashMap<RowData, Object>> getRightCache() {
        return rightCache;
    }

    @VisibleForTesting
    public AtomicLong getLeftTotalSize() {
        return leftTotalSize;
    }

    @VisibleForTesting
    public AtomicLong getRightTotalSize() {
        return rightTotalSize;
    }

    @VisibleForTesting
    public AtomicLong getLeftHitCount() {
        return leftHitCount;
    }

    @VisibleForTesting
    public AtomicLong getLeftRequestCount() {
        return leftRequestCount;
    }

    @VisibleForTesting
    public AtomicLong getRightHitCount() {
        return rightHitCount;
    }

    @VisibleForTesting
    public AtomicLong getRightRequestCount() {
        return rightRequestCount;
    }
}
