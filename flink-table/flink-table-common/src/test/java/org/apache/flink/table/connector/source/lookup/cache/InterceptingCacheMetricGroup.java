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

/** {@link CacheMetricGroup} that intercepts all registered metrics. */
public class InterceptingCacheMetricGroup extends UnregisteredMetricsGroup
        implements CacheMetricGroup {
    public Counter hitCounter;
    public Counter missCounter;
    public Counter loadCounter;
    public Counter numLoadFailuresCounter;
    public Gauge<Long> latestLoadTimeGauge;
    public Gauge<Long> numCachedRecordsGauge;
    public Gauge<Long> numCachedBytesGauge;

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
