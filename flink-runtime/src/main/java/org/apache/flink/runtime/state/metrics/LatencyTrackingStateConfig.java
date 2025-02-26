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
import org.apache.flink.configuration.StateLatencyTrackOptions;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.Preconditions;

/** Config to create latency tracking state metric. */
@Internal
public class LatencyTrackingStateConfig extends MetricsTrackingStateConfig {

    LatencyTrackingStateConfig(
            MetricGroup metricGroup,
            boolean enabled,
            int sampleInterval,
            int historySize,
            boolean stateNameAsVariable) {
        super(metricGroup, enabled, sampleInterval, historySize, stateNameAsVariable);
        if (enabled) {
            Preconditions.checkNotNull(
                    metricGroup, "Metric group cannot be null if latency tracking is enabled.");
            Preconditions.checkArgument(sampleInterval >= 1);
        }
    }

    public static LatencyTrackingStateConfig disabled() {
        return newBuilder().setEnabled(false).build();
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder
            extends MetricsTrackingStateConfig.Builder<
                    LatencyTrackingStateConfig, LatencyTrackingStateConfig.Builder> {
        private static final long serialVersionUID = 1L;

        public Builder() {
            this.enabled = StateLatencyTrackOptions.LATENCY_TRACK_ENABLED.defaultValue();
            this.sampleInterval =
                    StateLatencyTrackOptions.LATENCY_TRACK_SAMPLE_INTERVAL.defaultValue();
            this.historySize = StateLatencyTrackOptions.LATENCY_TRACK_HISTORY_SIZE.defaultValue();
            this.stateNameAsVariable =
                    StateLatencyTrackOptions.LATENCY_TRACK_STATE_NAME_AS_VARIABLE.defaultValue();
        }

        public Builder configure(ReadableConfig config) {
            this.setEnabled(config.get(StateLatencyTrackOptions.LATENCY_TRACK_ENABLED))
                    .setSampleInterval(
                            config.get(StateLatencyTrackOptions.LATENCY_TRACK_SAMPLE_INTERVAL))
                    .setHistorySize(config.get(StateLatencyTrackOptions.LATENCY_TRACK_HISTORY_SIZE))
                    .setStateNameAsVariable(
                            config.get(
                                    StateLatencyTrackOptions.LATENCY_TRACK_STATE_NAME_AS_VARIABLE));
            return this;
        }

        public LatencyTrackingStateConfig build() {
            return new LatencyTrackingStateConfig(
                    metricGroup, enabled, sampleInterval, historySize, stateNameAsVariable);
        }
    }
}
