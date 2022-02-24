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
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.metrics.MetricGroup;

/** Utility to define metrics that capture the time that some component spends in a state. */
public interface StateTimeMetric {

    /**
     * Returns the time, in milliseconds, that have elapsed since we transitioned to the targeted
     * state. Returns 0 if we are not in the targeted state.
     */
    long getCurrentTime();

    /** Returns the total time, in milliseconds, that we have spent in the targeted state. */
    long getTotalTime();

    /** Returns 1 if we are in the targeted state, otherwise 0. */
    long getBinary();

    static void register(
            MetricOptions.JobStatusMetricsSettings jobStatusMetricsSettings,
            MetricGroup metricGroup,
            StateTimeMetric stateTimeMetric,
            String baseName) {

        if (jobStatusMetricsSettings.isStateMetricsEnabled()) {
            metricGroup.gauge(getStateMetricName(baseName), stateTimeMetric::getBinary);
        }

        if (jobStatusMetricsSettings.isCurrentTimeMetricsEnabled()) {
            metricGroup.gauge(getCurrentTimeMetricName(baseName), stateTimeMetric::getCurrentTime);
        }

        if (jobStatusMetricsSettings.isTotalTimeMetricsEnabled()) {
            metricGroup.gauge(getTotalTimeMetricName(baseName), stateTimeMetric::getTotalTime);
        }
    }

    @VisibleForTesting
    static String getStateMetricName(String baseName) {
        return baseName + "State";
    }

    @VisibleForTesting
    static String getCurrentTimeMetricName(String baseName) {
        return baseName + "Time";
    }

    @VisibleForTesting
    static String getTotalTimeMetricName(String baseName) {
        return baseName + "TimeTotal";
    }
}
