/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.metrics;

import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/** Base class of state latency metric which counts and histogram the state metric. */
class StateMetricBase implements AutoCloseable {
    protected static final String STATE_NAME_KEY = "state_name";
    protected static final String STATE_CLEAR_LATENCY = "stateClearLatency";
    private final MetricGroup metricGroup;
    private final int sampleInterval;
    private final Map<String, Histogram> histogramMetrics;
    private final Supplier<Histogram> histogramSupplier;
    private int clearCount = 0;

    StateMetricBase(
            String stateName,
            MetricGroup metricGroup,
            int sampleInterval,
            int historySize,
            boolean stateNameAsVariable) {
        this.metricGroup =
                stateNameAsVariable
                        ? metricGroup.addGroup(STATE_NAME_KEY, stateName)
                        : metricGroup.addGroup(stateName);
        this.sampleInterval = sampleInterval;
        this.histogramMetrics = new HashMap<>();
        this.histogramSupplier = () -> new DescriptiveStatisticsHistogram(historySize);
    }

    int getClearCount() {
        return clearCount;
    }

    protected boolean trackMetricsOnClear() {
        clearCount = loopUpdateCounter(clearCount);
        return clearCount == 1;
    }

    protected int loopUpdateCounter(int counter) {
        return (counter + 1 < sampleInterval) ? counter + 1 : 0;
    }

    protected void updateMetrics(String latencyLabel, long value) {
        updateHistogram(latencyLabel, value);
    }

    private void updateHistogram(final String metricName, final long value) {
        this.histogramMetrics
                .computeIfAbsent(
                        metricName,
                        (k) -> {
                            Histogram histogram = histogramSupplier.get();
                            metricGroup.histogram(metricName, histogram);
                            return histogram;
                        })
                .update(value);
    }

    @Override
    public void close() throws Exception {
        histogramMetrics.clear();
    }
}
