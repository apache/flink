/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler.metrics;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.metrics.util.InterceptingOperatorMetricGroup;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class StateTimeMetricTest {

    @Test
    void testEnableStateMetrics() {
        testMetricSelection(MetricOptions.JobStatusMetrics.STATE);
    }

    @Test
    void testEnableCurrentTimeMetrics() {
        testMetricSelection(MetricOptions.JobStatusMetrics.CURRENT_TIME);
    }

    @Test
    void testEnableTotalTimeMetrics() {
        testMetricSelection(MetricOptions.JobStatusMetrics.TOTAL_TIME);
    }

    @Test
    void testEnableMultipleMetrics() {
        testMetricSelection(
                MetricOptions.JobStatusMetrics.CURRENT_TIME,
                MetricOptions.JobStatusMetrics.TOTAL_TIME);
    }

    private static void testMetricSelection(MetricOptions.JobStatusMetrics... selectedMetrics) {
        final EnumSet<MetricOptions.JobStatusMetrics> selectedMetricsSet =
                EnumSet.noneOf(MetricOptions.JobStatusMetrics.class);
        Arrays.stream(selectedMetrics).forEach(selectedMetricsSet::add);

        final InterceptingOperatorMetricGroup metricGroup = new InterceptingOperatorMetricGroup();

        StateTimeMetric.register(
                enable(selectedMetrics), metricGroup, new TestStateTimeMetric(), "test");
        final Map<JobStatus, StatusMetricSet> registeredMetrics = extractMetrics(metricGroup);

        for (StatusMetricSet metrics : registeredMetrics.values()) {
            assertThat(metrics.getState().isPresent())
                    .isEqualTo(selectedMetricsSet.contains(MetricOptions.JobStatusMetrics.STATE));
            assertThat(metrics.getCurrentTime().isPresent())
                    .isEqualTo(
                            selectedMetricsSet.contains(
                                    MetricOptions.JobStatusMetrics.CURRENT_TIME));
            assertThat(metrics.getTotalTime().isPresent())
                    .isEqualTo(
                            selectedMetricsSet.contains(MetricOptions.JobStatusMetrics.TOTAL_TIME));
        }
    }

    static MetricOptions.JobStatusMetricsSettings enable(
            MetricOptions.JobStatusMetrics... enabledMetrics) {
        final Configuration configuration = new Configuration();

        configuration.set(MetricOptions.JOB_STATUS_METRICS, Arrays.asList(enabledMetrics));

        return MetricOptions.JobStatusMetricsSettings.fromConfiguration(configuration);
    }

    static Map<JobStatus, StatusMetricSet> extractMetrics(InterceptingOperatorMetricGroup metrics) {
        final Map<JobStatus, StatusMetricSet> extractedMetrics = new EnumMap<>(JobStatus.class);

        for (JobStatus jobStatus : JobStatus.values()) {
            final String baseMetricName = JobStatusMetrics.getBaseMetricName(jobStatus);
            final StatusMetricSet statusMetricSet =
                    new StatusMetricSet(
                            (Gauge<Long>)
                                    metrics.get(StateTimeMetric.getStateMetricName(baseMetricName)),
                            (Gauge<Long>)
                                    metrics.get(
                                            StateTimeMetric.getCurrentTimeMetricName(
                                                    baseMetricName)),
                            (Gauge<Long>)
                                    metrics.get(
                                            StateTimeMetric.getTotalTimeMetricName(
                                                    baseMetricName)));
            if (statusMetricSet.getState().isPresent()
                    || statusMetricSet.getCurrentTime().isPresent()
                    || statusMetricSet.getTotalTime().isPresent()) {
                extractedMetrics.put(jobStatus, statusMetricSet);
            }
        }

        return extractedMetrics;
    }

    private static class TestStateTimeMetric implements StateTimeMetric {

        @Override
        public long getCurrentTime() {
            return 2;
        }

        @Override
        public long getTotalTime() {
            return 3;
        }

        @Override
        public long getBinary() {
            return 1;
        }
    }

    static class StatusMetricSet {

        @Nullable private final Gauge<Long> state;
        @Nullable private final Gauge<Long> currentTime;
        @Nullable private final Gauge<Long> totalTime;

        private StatusMetricSet(
                @Nullable Gauge<Long> state,
                @Nullable Gauge<Long> currentTime,
                @Nullable Gauge<Long> totalTime) {
            this.state = state;
            this.currentTime = currentTime;
            this.totalTime = totalTime;
        }

        @Nullable
        public Optional<Gauge<Long>> getState() {
            return Optional.ofNullable(state);
        }

        @Nullable
        public Optional<Gauge<Long>> getCurrentTime() {
            return Optional.ofNullable(currentTime);
        }

        @Nullable
        public Optional<Gauge<Long>> getTotalTime() {
            return Optional.ofNullable(totalTime);
        }
    }
}
