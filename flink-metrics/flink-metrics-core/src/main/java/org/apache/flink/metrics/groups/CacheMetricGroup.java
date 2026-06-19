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

package org.apache.flink.metrics.groups;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;

/**
 * Pre-defined metrics for cache.
 *
 * <p>Please note that these methods should only be invoked once. Registering a metric with same
 * name for multiple times would lead to an undefined behavior.
 */
@PublicEvolving
public interface CacheMetricGroup extends MetricGroup {
    /** The number of cache hits. */
    void hitCounter(Counter hitCounter);

    /** The number of cache misses. */
    void missCounter(Counter missCounter);

    /** The number of times to load data into cache from external system. */
    void loadCounter(Counter loadCounter);

    /** The number of load failures. */
    void numLoadFailuresCounter(Counter numLoadFailuresCounter);

    /** The time spent for the latest load operation. */
    void latestLoadTimeGauge(Gauge<Long> latestLoadTimeGauge);

    /** The number of records in cache. */
    void numCachedRecordsGauge(Gauge<Long> numCachedRecordsGauge);

    /** The number of bytes used by cache. */
    void numCachedBytesGauge(Gauge<Long> numCachedBytesGauge);
}
