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

package org.apache.flink.core.fs.metrics;

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SlidingWindowHistogram;
import org.apache.flink.metrics.ThreadSafeSimpleCounter;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** Registers normalized filesystem operation metrics supplied by cloud-specific adapters. */
@Internal
public final class FileSystemMetricRecorder {

    public static final String API_CALL_COUNT = "api_call_count";
    public static final String API_CALL_DURATION_MS = "api_call_duration_ms";
    public static final String THROTTLE_COUNT = "throttle_count";
    public static final String RETRY_COUNT = "retry_count";
    public static final String IOPS = "iops";

    public static final List<String> DEFAULT_ALLOWLIST =
            Collections.unmodifiableList(
                    Arrays.asList(
                            API_CALL_COUNT,
                            API_CALL_DURATION_MS,
                            THROTTLE_COUNT,
                            RETRY_COUNT,
                            IOPS));

    private static final String WILDCARD = "*";
    private static final String LABEL_OPERATION = "op";
    private static final String LABEL_STATUS_CLASS = "status_class";
    private static final String LABEL_REASON = "reason";

    private final MetricGroup metricGroup;
    private final int histogramWindowSize;
    private final boolean allowAll;
    private final Set<String> allowlist;
    private final Map<String, Counter> counters = new ConcurrentHashMap<>();
    private final Map<String, Histogram> histograms = new ConcurrentHashMap<>();

    public FileSystemMetricRecorder(
            MetricGroup metricGroup,
            @Nullable Collection<String> configuredAllowlist,
            int histogramWindowSize) {
        this.metricGroup = Preconditions.checkNotNull(metricGroup);
        Preconditions.checkArgument(
                histogramWindowSize > 0, "histogramWindowSize must be positive");
        this.histogramWindowSize = histogramWindowSize;

        final Set<String> normalizedAllowlist = normalizeAllowlist(configuredAllowlist);
        Preconditions.checkArgument(
                !normalizedAllowlist.isEmpty(), "Filesystem metrics allowlist must not be empty");
        if (normalizedAllowlist.contains(WILDCARD)) {
            this.allowAll = true;
            this.allowlist = Collections.emptySet();
        } else {
            if (normalizedAllowlist.contains(IOPS)) {
                normalizedAllowlist.add(API_CALL_COUNT);
            }
            this.allowAll = false;
            this.allowlist = normalizedAllowlist;
        }
    }

    public void recordApiCall(String operation, StatusClass statusClass) {
        if (!isMetricEnabled(API_CALL_COUNT)) {
            return;
        }
        final String normalizedOperation = Preconditions.checkNotNull(operation);
        final String status = Preconditions.checkNotNull(statusClass).labelValue;
        counters.computeIfAbsent(
                        "api-call|" + normalizedOperation + '|' + status,
                        ignored ->
                                metricGroup
                                        .addGroup(LABEL_OPERATION, normalizedOperation)
                                        .addGroup(LABEL_STATUS_CLASS, status)
                                        .counter(API_CALL_COUNT, new ThreadSafeSimpleCounter()))
                .inc();
    }

    public void recordDuration(String operation, long durationMillis) {
        if (!isMetricEnabled(API_CALL_DURATION_MS)) {
            return;
        }
        final String normalizedOperation = Preconditions.checkNotNull(operation);
        histograms
                .computeIfAbsent(
                        normalizedOperation,
                        ignored ->
                                metricGroup
                                        .addGroup(LABEL_OPERATION, normalizedOperation)
                                        .histogram(
                                                API_CALL_DURATION_MS,
                                                new SlidingWindowHistogram(histogramWindowSize)))
                .update(durationMillis);
    }

    public void recordThrottles(String operation, long count) {
        if (count <= 0 || !isMetricEnabled(THROTTLE_COUNT)) {
            return;
        }
        final String normalizedOperation = Preconditions.checkNotNull(operation);
        counters.computeIfAbsent(
                        "throttle|" + normalizedOperation,
                        ignored ->
                                metricGroup
                                        .addGroup(LABEL_OPERATION, normalizedOperation)
                                        .counter(THROTTLE_COUNT, new ThreadSafeSimpleCounter()))
                .inc(count);
    }

    public void recordRetries(String operation, long count, RetryReason reason) {
        if (count <= 0 || !isMetricEnabled(RETRY_COUNT)) {
            return;
        }
        final String normalizedOperation = Preconditions.checkNotNull(operation);
        final String retryReason = Preconditions.checkNotNull(reason).labelValue;
        counters.computeIfAbsent(
                        "retry|" + normalizedOperation + '|' + retryReason,
                        ignored ->
                                metricGroup
                                        .addGroup(LABEL_OPERATION, normalizedOperation)
                                        .addGroup(LABEL_REASON, retryReason)
                                        .counter(RETRY_COUNT, new ThreadSafeSimpleCounter()))
                .inc(count);
    }

    boolean isMetricEnabled(String metricName) {
        return allowAll || allowlist.contains(metricName);
    }

    private static Set<String> normalizeAllowlist(
            @Nullable Collection<String> configuredAllowlist) {
        final Set<String> normalized = new HashSet<>();
        if (configuredAllowlist == null) {
            return normalized;
        }
        for (String metric : configuredAllowlist) {
            if (metric != null && !metric.trim().isEmpty()) {
                normalized.add(metric.trim());
            }
        }
        return normalized;
    }

    public enum StatusClass {
        SUCCESS("2xx"),
        CLIENT_ERROR("4xx"),
        SERVER_ERROR("5xx"),
        THROTTLED("throttled"),
        OTHER("other"),
        ERROR("error"),
        UNKNOWN("unknown");

        private final String labelValue;

        StatusClass(String labelValue) {
            this.labelValue = labelValue;
        }
    }

    public enum RetryReason {
        THROTTLED("throttled"),
        SERVER_ERROR("5xx"),
        OTHER("other");

        private final String labelValue;

        RetryReason(String labelValue) {
            this.labelValue = labelValue;
        }
    }
}
