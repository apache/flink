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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link LatencyTrackingStateConfig}. */
class LatencyTrackingStateConfigTest {

    @Test
    void testDefaultDisabledLatencyTrackingStateConfig() {
        LatencyTrackingStateConfig latencyTrackingStateConfig =
                LatencyTrackingStateConfig.newBuilder().build();
        assertThat(latencyTrackingStateConfig.isEnabled()).isFalse();
    }

    @Test
    void testDefaultEnabledLatencyTrackingStateConfig() {
        UnregisteredMetricsGroup metricsGroup = new UnregisteredMetricsGroup();
        LatencyTrackingStateConfig latencyTrackingStateConfig =
                LatencyTrackingStateConfig.newBuilder()
                        .setEnabled(true)
                        .setMetricGroup(metricsGroup)
                        .build();
        assertThat(latencyTrackingStateConfig.isEnabled()).isTrue();
        assertThat(latencyTrackingStateConfig.getSampleInterval())
                .isEqualTo((int) StateBackendOptions.LATENCY_TRACK_SAMPLE_INTERVAL.defaultValue());
        assertThat(latencyTrackingStateConfig.getHistorySize())
                .isEqualTo((long) StateBackendOptions.LATENCY_TRACK_HISTORY_SIZE.defaultValue());
        assertThat(latencyTrackingStateConfig.isStateNameAsVariable())
                .isEqualTo(StateBackendOptions.LATENCY_TRACK_STATE_NAME_AS_VARIABLE.defaultValue());
    }

    @Test
    void testSetLatencyTrackingStateConfig() {
        UnregisteredMetricsGroup metricsGroup = new UnregisteredMetricsGroup();
        LatencyTrackingStateConfig latencyTrackingStateConfig =
                LatencyTrackingStateConfig.newBuilder()
                        .setMetricGroup(metricsGroup)
                        .setEnabled(true)
                        .setSampleInterval(10)
                        .setHistorySize(500)
                        .build();
        assertThat(latencyTrackingStateConfig.isEnabled()).isTrue();
        assertThat(latencyTrackingStateConfig.getSampleInterval()).isEqualTo(10);
        assertThat(latencyTrackingStateConfig.getHistorySize()).isEqualTo(500);
    }

    @Test
    void testConfigureFromReadableConfig() {
        LatencyTrackingStateConfig.Builder builder = LatencyTrackingStateConfig.newBuilder();
        Configuration configuration = new Configuration();
        configuration.setBoolean(StateBackendOptions.LATENCY_TRACK_ENABLED, true);
        configuration.setInteger(StateBackendOptions.LATENCY_TRACK_SAMPLE_INTERVAL, 10);
        configuration.setInteger(StateBackendOptions.LATENCY_TRACK_HISTORY_SIZE, 500);
        LatencyTrackingStateConfig latencyTrackingStateConfig =
                builder.configure(configuration)
                        .setMetricGroup(new UnregisteredMetricsGroup())
                        .build();
        assertThat(latencyTrackingStateConfig.isEnabled()).isTrue();
        assertThat(latencyTrackingStateConfig.getSampleInterval()).isEqualTo(10);
        assertThat(latencyTrackingStateConfig.getHistorySize()).isEqualTo(500);
    }
}
