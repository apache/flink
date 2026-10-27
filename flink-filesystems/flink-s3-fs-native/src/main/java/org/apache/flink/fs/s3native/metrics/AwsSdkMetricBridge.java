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

package org.apache.flink.fs.s3native.metrics;

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SlidingWindowHistogram;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.metrics.CoreMetric;
import software.amazon.awssdk.http.HttpMetric;
import software.amazon.awssdk.metrics.MetricCollection;
import software.amazon.awssdk.metrics.MetricPublisher;
import software.amazon.awssdk.metrics.SdkMetric;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/** Maps AWS SDK metric records to native S3 filesystem metrics. */
@Internal
public final class AwsSdkMetricBridge implements MetricPublisher {

    private static final Logger LOG = LoggerFactory.getLogger(AwsSdkMetricBridge.class);
    private static final String UNKNOWN_OPERATION = "Unknown";

    @Nullable private final Collection<String> configuredAllowlist;
    private final int histogramWindowSize;
    private final AtomicReference<S3MetricRecorder> metrics = new AtomicReference<>();

    public AwsSdkMetricBridge(MetricGroup metricGroup) {
        this(
                metricGroup,
                S3MetricRecorder.DEFAULT_ALLOWLIST,
                SlidingWindowHistogram.DEFAULT_WINDOW_SIZE);
    }

    public AwsSdkMetricBridge(
            MetricGroup metricGroup,
            @Nullable Collection<String> allowlist,
            int histogramWindowSize) {
        this(allowlist, histogramWindowSize);
        setMetricGroup(metricGroup);
    }

    public AwsSdkMetricBridge(@Nullable Collection<String> allowlist, int histogramWindowSize) {
        this.configuredAllowlist = allowlist == null ? null : new ArrayList<>(allowlist);
        this.histogramWindowSize = histogramWindowSize;
    }

    /** Starts publishing subsequent SDK metrics to the given Flink metric group. */
    public void setMetricGroup(MetricGroup metricGroup) {
        metrics.set(new S3MetricRecorder(metricGroup, configuredAllowlist, histogramWindowSize));
    }

    @Override
    public void publish(MetricCollection apiCall) {
        final S3MetricRecorder currentMetrics = metrics.get();
        if (currentMetrics == null) {
            return;
        }
        try {
            translate(apiCall, currentMetrics);
        } catch (Exception e) {
            LOG.warn("Failed to publish S3 SDK metrics", e);
        }
    }

    private void translate(MetricCollection apiCall, S3MetricRecorder currentMetrics) {
        final String operation = first(apiCall, CoreMetric.OPERATION_NAME, UNKNOWN_OPERATION);
        final Duration duration = first(apiCall, CoreMetric.API_CALL_DURATION, null);
        if (duration != null) {
            currentMetrics.recordDuration(operation, duration.toMillis());
        }

        int throttleResponses = 0;
        boolean sawServerError = false;
        Integer lastStatus = null;
        for (MetricCollection attempt : apiCall.children()) {
            for (Integer status : attempt.metricValues(HttpMetric.HTTP_STATUS_CODE)) {
                if (status != null) {
                    lastStatus = status;
                    if (isThrottle(status)) {
                        throttleResponses++;
                    } else if (status >= 500) {
                        sawServerError = true;
                    }
                }
            }
        }

        currentMetrics.recordApiCall(
                operation,
                statusClass(lastStatus, first(apiCall, CoreMetric.API_CALL_SUCCESSFUL, null)));
        currentMetrics.recordThrottles(operation, throttleResponses);

        final Integer retries = first(apiCall, CoreMetric.RETRY_COUNT, 0);
        if (retries != null) {
            currentMetrics.recordRetries(
                    operation, retries, retryReason(throttleResponses > 0, sawServerError));
        }
    }

    private static boolean isThrottle(int status) {
        return status == 429 || status == 503;
    }

    private static String statusClass(Integer status, Boolean successful) {
        if (status == null) {
            if (Boolean.TRUE.equals(successful)) {
                return "2xx";
            }
            return Boolean.FALSE.equals(successful) ? "error" : "unknown";
        }
        if (isThrottle(status)) {
            return "throttled";
        }
        if (status >= 200 && status < 300) {
            return "2xx";
        }
        if (status >= 400 && status < 500) {
            return "4xx";
        }
        if (status >= 500) {
            return "5xx";
        }
        return "other";
    }

    private static String retryReason(boolean sawThrottle, boolean sawServerError) {
        if (sawThrottle) {
            return "throttled";
        }
        return sawServerError ? "5xx" : "other";
    }

    private static <T> T first(MetricCollection collection, SdkMetric<T> metric, T defaultValue) {
        final List<T> values = collection.metricValues(metric);
        return values.isEmpty() ? defaultValue : values.get(0);
    }

    @Override
    public void close() {}
}
