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

package org.apache.flink.runtime.scheduler.metrics;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.clock.SystemClock;

import java.util.function.Supplier;

/**
 * Defines metrics that capture different aspects of a job execution (time spent in deployment, time
 * spent in initialization etc). Specifics are passed in through the constructor and then.
 */
public class SubStateTimeMetrics implements StateTimeMetric, MetricsRegistrar {
    private static final long NOT_STARTED = -1L;

    private final String metricName;
    private final MetricOptions.JobStatusMetricsSettings stateTimeMetricSettings;

    private final Supplier<Boolean> isSubStateActiveSupplier;

    private final Clock clock;

    // metrics state
    private long metricStart = NOT_STARTED;
    private long metricTimeTotal = 0L;

    public SubStateTimeMetrics(
            MetricOptions.JobStatusMetricsSettings stateTimeMetricsSettings,
            String metricName,
            Supplier<Boolean> subStatePredicate) {
        this(stateTimeMetricsSettings, metricName, subStatePredicate, SystemClock.getInstance());
    }

    @VisibleForTesting
    SubStateTimeMetrics(
            MetricOptions.JobStatusMetricsSettings stateTimeMetricsSettings,
            String metricName,
            Supplier<Boolean> subStatePredicate,
            Clock clock) {
        this.stateTimeMetricSettings = stateTimeMetricsSettings;
        this.metricName = metricName;
        this.isSubStateActiveSupplier = subStatePredicate;
        this.clock = clock;
    }

    @Override
    public long getCurrentTime() {
        return metricStart == NOT_STARTED
                ? 0L
                : Math.max(0, clock.absoluteTimeMillis() - metricStart);
    }

    @Override
    public long getTotalTime() {
        return getCurrentTime() + metricTimeTotal;
    }

    @Override
    public long getBinary() {
        return metricStart == NOT_STARTED ? 0L : 1L;
    }

    @Override
    public void registerMetrics(MetricGroup metricGroup) {
        StateTimeMetric.register(stateTimeMetricSettings, metricGroup, this, metricName);
    }

    public void onStateUpdate() {
        final boolean subStateActive = isSubStateActiveSupplier.get();
        if (metricStart == NOT_STARTED) {
            if (subStateActive) {
                markMetricStart();
            }
        } else {
            if (!subStateActive) {
                markMetricEnd();
            }
        }
    }

    private void markMetricStart() {
        metricStart = clock.absoluteTimeMillis();
    }

    private void markMetricEnd() {
        metricTimeTotal += Math.max(0, clock.absoluteTimeMillis() - metricStart);
        metricStart = NOT_STARTED;
    }

    @VisibleForTesting
    boolean hasCleanState() {
        return metricStart == NOT_STARTED;
    }
}
