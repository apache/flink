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

package org.apache.flink.runtime.metrics.groups;

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.CacheMetricGroup;

import static org.apache.flink.runtime.metrics.MetricNames.HIT_COUNT;
import static org.apache.flink.runtime.metrics.MetricNames.LATEST_LOAD_TIME;
import static org.apache.flink.runtime.metrics.MetricNames.LOAD_COUNT;
import static org.apache.flink.runtime.metrics.MetricNames.MISS_COUNT;
import static org.apache.flink.runtime.metrics.MetricNames.NUM_CACHED_BYTES;
import static org.apache.flink.runtime.metrics.MetricNames.NUM_CACHED_RECORDS;
import static org.apache.flink.runtime.metrics.MetricNames.NUM_LOAD_FAILURES;

/**
 * A {@link CacheMetricGroup} which register all cache related metrics under a subgroup of the
 * parent metric group.
 */
@Internal
public class InternalCacheMetricGroup extends ProxyMetricGroup<MetricGroup>
        implements CacheMetricGroup {

    public static final long UNINITIALIZED = -1;

    /**
     * Creates a subgroup with the specified subgroup name under the parent group. Metrics will be
     * registered under the new created subgroup.
     *
     * <p>For example the hit counter will be registered as "root.cache.hitCount", with {@code
     * parentMetricGroup = root} and {@code subGroupName = "cache"}.
     *
     * @param parentMetricGroup parent metric group of the subgroup
     * @param subGroupName name of the subgroup
     */
    public InternalCacheMetricGroup(MetricGroup parentMetricGroup, String subGroupName) {
        super(parentMetricGroup.addGroup(subGroupName));
    }

    @Override
    public void hitCounter(Counter hitCounter) {
        counter(HIT_COUNT, hitCounter);
    }

    @Override
    public void missCounter(Counter missCounter) {
        counter(MISS_COUNT, missCounter);
    }

    @Override
    public void loadCounter(Counter loadCounter) {
        counter(LOAD_COUNT, loadCounter);
    }

    @Override
    public void numLoadFailuresCounter(Counter numLoadFailuresCounter) {
        counter(NUM_LOAD_FAILURES, numLoadFailuresCounter);
    }

    @Override
    public void latestLoadTimeGauge(Gauge<Long> latestLoadTimeGauge) {
        gauge(LATEST_LOAD_TIME, latestLoadTimeGauge);
    }

    @Override
    public void numCachedRecordsGauge(Gauge<Long> numCachedRecordsGauge) {
        gauge(NUM_CACHED_RECORDS, numCachedRecordsGauge);
    }

    @Override
    public void numCachedBytesGauge(Gauge<Long> numCachedBytesGauge) {
        gauge(NUM_CACHED_BYTES, numCachedBytesGauge);
    }
}
