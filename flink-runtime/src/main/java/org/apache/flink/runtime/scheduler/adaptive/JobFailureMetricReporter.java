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

package org.apache.flink.runtime.scheduler.adaptive;

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.executiongraph.failover.FailureHandlingResult;
import org.apache.flink.traces.Span;
import org.apache.flink.traces.SpanBuilder;
import org.apache.flink.util.Preconditions;

import java.util.Map;

/** Helper class to simplify job failure reporting through a metric group. */
public class JobFailureMetricReporter {

    public static final String FAILURE_LABEL_ATTRIBUTE_PREFIX = "failureLabel.";

    private final MetricGroup metricGroup;

    public JobFailureMetricReporter(MetricGroup metricGroup) {
        this.metricGroup = Preconditions.checkNotNull(metricGroup);
    }

    public void reportJobFailure(
            FailureHandlingResult failureHandlingResult, Map<String, String> failureLabels) {
        reportJobFailure(
                failureHandlingResult.getTimestamp(),
                failureHandlingResult.canRestart(),
                failureHandlingResult.isGlobalFailure(),
                failureLabels);
    }

    public void reportJobFailure(
            FailureResult failureHandlingResult, Map<String, String> failureLabels) {
        reportJobFailure(
                System.currentTimeMillis(),
                failureHandlingResult.canRestart(),
                null,
                failureLabels);
    }

    private void reportJobFailure(
            long timestamp,
            Boolean canRestart,
            Boolean isGlobal,
            Map<String, String> failureLabels) {
        // Add base attributes
        SpanBuilder spanBuilder =
                Span.builder(JobFailureMetricReporter.class, "JobFailure")
                        .setStartTsMillis(timestamp)
                        .setEndTsMillis(timestamp);

        if (canRestart != null) {
            spanBuilder.setAttribute("canRestart", String.valueOf(canRestart));
        }

        if (isGlobal != null) {
            spanBuilder.setAttribute("isGlobalFailure", String.valueOf(isGlobal));
        }

        // Add all failure labels
        for (Map.Entry<String, String> entry : failureLabels.entrySet()) {
            spanBuilder.setAttribute(
                    FAILURE_LABEL_ATTRIBUTE_PREFIX + entry.getKey(), entry.getValue());
        }
        metricGroup.addSpan(spanBuilder);
    }
}
