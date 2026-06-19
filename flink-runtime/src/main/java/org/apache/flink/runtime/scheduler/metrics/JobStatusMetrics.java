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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.executiongraph.JobStatusListener;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.clock.SystemClock;

import java.util.Locale;

/** Metrics that capture the time that a job spends in each {@link JobStatus}. */
public class JobStatusMetrics implements JobStatusListener, MetricsRegistrar {

    private final MetricOptions.JobStatusMetricsSettings jobStatusMetricsSettings;
    private JobStatus currentStatus = JobStatus.INITIALIZING;
    private long currentStatusTimestamp;
    private final long[] cumulativeStatusTimes;
    private final Clock clock;

    public JobStatusMetrics(
            long initializationTimestamp,
            MetricOptions.JobStatusMetricsSettings jobStatusMetricsSettings) {
        this(initializationTimestamp, jobStatusMetricsSettings, SystemClock.getInstance());
    }

    @VisibleForTesting
    JobStatusMetrics(
            long initializationTimestamp,
            MetricOptions.JobStatusMetricsSettings jobStatusMetricsSettings,
            Clock clock) {
        this.jobStatusMetricsSettings = jobStatusMetricsSettings;
        this.clock = clock;

        currentStatus = JobStatus.INITIALIZING;
        currentStatusTimestamp = initializationTimestamp;
        cumulativeStatusTimes = new long[JobStatus.values().length];
    }

    @Override
    public void registerMetrics(MetricGroup metricGroup) {
        for (JobStatus jobStatus : JobStatus.values()) {
            if (!jobStatus.isTerminalState() && jobStatus != JobStatus.RECONCILING) {

                final StateTimeMetric stateTimeMetric = createTimeMetric(jobStatus);

                StateTimeMetric.register(
                        jobStatusMetricsSettings,
                        metricGroup,
                        stateTimeMetric,
                        getBaseMetricName(jobStatus));
            }
        }
    }

    @VisibleForTesting
    StateTimeMetric createTimeMetric(JobStatus jobStatus) {
        return new StateTimeMetric() {
            @Override
            public long getCurrentTime() {
                return currentStatus == jobStatus
                        ? Math.max(clock.absoluteTimeMillis() - currentStatusTimestamp, 0)
                        : 0;
            }

            @Override
            public long getTotalTime() {
                return currentStatus == jobStatus
                        ? cumulativeStatusTimes[jobStatus.ordinal()]
                                + Math.max(clock.absoluteTimeMillis() - currentStatusTimestamp, 0)
                        : cumulativeStatusTimes[jobStatus.ordinal()];
            }

            @Override
            public long getBinary() {
                return currentStatus == jobStatus ? 1L : 0L;
            }
        };
    }

    @VisibleForTesting
    static String getBaseMetricName(JobStatus jobStatus) {
        return jobStatus.name().toLowerCase(Locale.ROOT);
    }

    @Override
    public void jobStatusChanges(JobID jobId, JobStatus newJobStatus, long timestamp) {
        cumulativeStatusTimes[currentStatus.ordinal()] += timestamp - currentStatusTimestamp;

        currentStatus = newJobStatus;
        currentStatusTimestamp = timestamp;
    }
}
