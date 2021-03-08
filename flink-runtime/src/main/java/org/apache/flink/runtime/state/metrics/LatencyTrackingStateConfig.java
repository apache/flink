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

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;

/** Config to create latency tracking state metric. */
@Internal
public class LatencyTrackingStateConfig {

    private final MetricGroup metricGroup;

    private final boolean enabled;
    private final int sampleInterval;
    private final int historySize;

    LatencyTrackingStateConfig(
            MetricGroup metricGroup, boolean enabled, int sampleInterval, int historySize) {
        if (enabled) {
            Preconditions.checkNotNull(
                    metricGroup, "Metric group cannot be null if latency tracking is enabled.");
            Preconditions.checkArgument(sampleInterval >= 1);
        }
        this.metricGroup = metricGroup;
        this.enabled = enabled;
        this.sampleInterval = sampleInterval;
        this.historySize = historySize;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public MetricGroup getMetricGroup() {
        return metricGroup;
    }

    public int getHistorySize() {
        return historySize;
    }

    public int getSampleInterval() {
        return sampleInterval;
    }

    public static LatencyTrackingStateConfig disabled() {
        return newBuilder().setEnabled(false).build();
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder implements Serializable {
        private static final long serialVersionUID = 1L;

        private boolean enabled = StateBackendOptions.LATENCY_TRACK_ENABLED.defaultValue();
        private int sampleInterval =
                StateBackendOptions.LATENCY_TRACK_SAMPLE_INTERVAL.defaultValue();
        private int historySize = StateBackendOptions.LATENCY_TRACK_HISTORY_SIZE.defaultValue();
        private MetricGroup metricGroup;

        public Builder setEnabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        public Builder setSampleInterval(int sampleInterval) {
            this.sampleInterval = sampleInterval;
            return this;
        }

        public Builder setHistorySize(int historySize) {
            this.historySize = historySize;
            return this;
        }

        public Builder setMetricGroup(MetricGroup metricGroup) {
            this.metricGroup = metricGroup;
            return this;
        }

        public Builder configure(ReadableConfig config) {
            this.setEnabled(config.get(StateBackendOptions.LATENCY_TRACK_ENABLED))
                    .setSampleInterval(
                            config.get(StateBackendOptions.LATENCY_TRACK_SAMPLE_INTERVAL))
                    .setHistorySize(config.get(StateBackendOptions.LATENCY_TRACK_HISTORY_SIZE));
            return this;
        }

        public LatencyTrackingStateConfig build() {
            return new LatencyTrackingStateConfig(
                    metricGroup, enabled, sampleInterval, historySize);
        }
    }
}
