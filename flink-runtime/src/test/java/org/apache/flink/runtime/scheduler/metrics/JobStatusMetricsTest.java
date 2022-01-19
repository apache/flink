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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.runtime.metrics.util.InterceptingOperatorMetricGroup;
import org.apache.flink.util.clock.ManualClock;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;

import static org.apache.flink.runtime.scheduler.metrics.StateTimeMetricTest.enable;
import static org.apache.flink.runtime.scheduler.metrics.StateTimeMetricTest.extractMetrics;
import static org.assertj.core.api.Assertions.assertThat;

class JobStatusMetricsTest {

    @Test
    void testStateMetric() {
        final JobStatusMetrics jobStatusMetrics =
                new JobStatusMetrics(
                        0L,
                        enable(
                                MetricOptions.JobStatusMetrics.STATE,
                                MetricOptions.JobStatusMetrics.CURRENT_TIME,
                                MetricOptions.JobStatusMetrics.TOTAL_TIME));

        final StateTimeMetric metric = jobStatusMetrics.createTimeMetric(JobStatus.RUNNING);

        assertThat(metric.getBinary()).isEqualTo(0L);
        jobStatusMetrics.jobStatusChanges(new JobID(), JobStatus.RUNNING, 1L);
        assertThat(metric.getBinary()).isEqualTo(1L);
        jobStatusMetrics.jobStatusChanges(new JobID(), JobStatus.RESTARTING, 2L);
        assertThat(metric.getBinary()).isEqualTo(0L);
    }

    @Test
    void testCurrentTimeMetric() {
        final ManualClock clock = new ManualClock();
        final JobStatusMetrics jobStatusMetrics =
                new JobStatusMetrics(
                        0L,
                        enable(
                                MetricOptions.JobStatusMetrics.STATE,
                                MetricOptions.JobStatusMetrics.CURRENT_TIME,
                                MetricOptions.JobStatusMetrics.TOTAL_TIME),
                        clock);
        final StateTimeMetric metric = jobStatusMetrics.createTimeMetric(JobStatus.RUNNING);

        assertThat(metric.getCurrentTime()).isEqualTo(0L);
        jobStatusMetrics.jobStatusChanges(new JobID(), JobStatus.RUNNING, 1L);
        clock.advanceTime(Duration.ofMillis(11));
        assertThat(metric.getCurrentTime()).isEqualTo(10L);
        jobStatusMetrics.jobStatusChanges(new JobID(), JobStatus.RESTARTING, 15L);
        assertThat(metric.getCurrentTime()).isEqualTo(0L);
    }

    @Test
    void testTotalTimeMetric() {
        final ManualClock clock = new ManualClock(0);
        final JobStatusMetrics jobStatusMetrics =
                new JobStatusMetrics(
                        0L,
                        enable(
                                MetricOptions.JobStatusMetrics.STATE,
                                MetricOptions.JobStatusMetrics.CURRENT_TIME,
                                MetricOptions.JobStatusMetrics.TOTAL_TIME),
                        clock);

        final StateTimeMetric metric = jobStatusMetrics.createTimeMetric(JobStatus.RUNNING);

        assertThat(metric.getTotalTime()).isEqualTo(0L);

        jobStatusMetrics.jobStatusChanges(
                new JobID(), JobStatus.RUNNING, clock.absoluteTimeMillis());

        clock.advanceTime(Duration.ofMillis(10));
        assertThat(metric.getTotalTime()).isEqualTo(10L);

        jobStatusMetrics.jobStatusChanges(
                new JobID(), JobStatus.RESTARTING, clock.absoluteTimeMillis());

        clock.advanceTime(Duration.ofMillis(4));
        assertThat(metric.getTotalTime()).isEqualTo(10L);

        jobStatusMetrics.jobStatusChanges(
                new JobID(), JobStatus.RUNNING, clock.absoluteTimeMillis());

        clock.advanceTime(Duration.ofMillis(1));
        assertThat(metric.getTotalTime()).isEqualTo(11L);
    }

    @Test
    void testStatusSelection() {
        final InterceptingOperatorMetricGroup metricGroup = new InterceptingOperatorMetricGroup();

        final JobStatusMetrics jobStatusMetrics =
                new JobStatusMetrics(0L, enable(MetricOptions.JobStatusMetrics.STATE));
        jobStatusMetrics.registerMetrics(metricGroup);
        final Map<JobStatus, StateTimeMetricTest.StatusMetricSet> registeredMetrics =
                extractMetrics(metricGroup);

        for (JobStatus value : JobStatus.values()) {
            if (value.isTerminalState() || value == JobStatus.RECONCILING) {
                assertThat(registeredMetrics).doesNotContainKey(value);
            } else {
                assertThat(registeredMetrics).containsKey(value);
            }
        }
    }
}
