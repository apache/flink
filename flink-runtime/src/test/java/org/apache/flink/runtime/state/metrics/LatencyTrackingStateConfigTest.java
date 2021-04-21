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

import org.junit.Assert;
import org.junit.Test;

/** Tests for {@link LatencyTrackingStateConfig}. */
public class LatencyTrackingStateConfigTest {

    @Test
    public void testDefaultDisabledLatencyTrackingStateConfig() {
        LatencyTrackingStateConfig latencyTrackingStateConfig =
                LatencyTrackingStateConfig.newBuilder().build();
        Assert.assertFalse(latencyTrackingStateConfig.isEnabled());
    }

    @Test
    public void testDefaultEnabledLatencyTrackingStateConfig() {
        UnregisteredMetricsGroup metricsGroup = new UnregisteredMetricsGroup();
        LatencyTrackingStateConfig latencyTrackingStateConfig =
                LatencyTrackingStateConfig.newBuilder()
                        .setEnabled(true)
                        .setMetricGroup(metricsGroup)
                        .build();
        Assert.assertTrue(latencyTrackingStateConfig.isEnabled());
        Assert.assertEquals(
                (int) StateBackendOptions.LATENCY_TRACK_SAMPLE_INTERVAL.defaultValue(),
                latencyTrackingStateConfig.getSampleInterval());
        Assert.assertEquals(
                (long) StateBackendOptions.LATENCY_TRACK_HISTORY_SIZE.defaultValue(),
                latencyTrackingStateConfig.getHistorySize());
    }

    @Test
    public void testSetLatencyTrackingStateConfig() {
        UnregisteredMetricsGroup metricsGroup = new UnregisteredMetricsGroup();
        LatencyTrackingStateConfig latencyTrackingStateConfig =
                LatencyTrackingStateConfig.newBuilder()
                        .setMetricGroup(metricsGroup)
                        .setEnabled(true)
                        .setSampleInterval(10)
                        .setHistorySize(500)
                        .build();
        Assert.assertTrue(latencyTrackingStateConfig.isEnabled());
        Assert.assertEquals(10, latencyTrackingStateConfig.getSampleInterval());
        Assert.assertEquals(500L, latencyTrackingStateConfig.getHistorySize());
    }

    @Test
    public void testConfigureFromReadableConfig() {
        LatencyTrackingStateConfig.Builder builder = LatencyTrackingStateConfig.newBuilder();
        Configuration configuration = new Configuration();
        configuration.setBoolean(StateBackendOptions.LATENCY_TRACK_ENABLED, true);
        configuration.setInteger(StateBackendOptions.LATENCY_TRACK_SAMPLE_INTERVAL, 10);
        configuration.setInteger(StateBackendOptions.LATENCY_TRACK_HISTORY_SIZE, 500);
        LatencyTrackingStateConfig latencyTrackingStateConfig =
                builder.configure(configuration)
                        .setMetricGroup(new UnregisteredMetricsGroup())
                        .build();
        Assert.assertTrue(latencyTrackingStateConfig.isEnabled());
        Assert.assertEquals(10, latencyTrackingStateConfig.getSampleInterval());
        Assert.assertEquals(500, latencyTrackingStateConfig.getHistorySize());
    }
}
