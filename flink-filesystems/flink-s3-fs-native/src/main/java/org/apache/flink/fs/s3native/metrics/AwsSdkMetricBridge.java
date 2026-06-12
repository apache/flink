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
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.ThreadSafeSimpleCounter;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.metrics.CoreMetric;
import software.amazon.awssdk.http.HttpMetric;
import software.amazon.awssdk.metrics.MetricCollection;
import software.amazon.awssdk.metrics.MetricPublisher;
import software.amazon.awssdk.metrics.SdkMetric;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Bridges AWS SDK v2's {@link MetricPublisher} into Flink metrics for {@code flink-s3-fs-native}.
 *
 * <p>The SDK invokes {@link #publish(MetricCollection)} asynchronously after every completed API
 * call, on its internal completion executor. The bridge reads a small, fixed set of fields and
 * emits the default metric surface of FLIP-576 against the {@code filesystem_type}-labelled scope
 * it is handed at construction:
 *
 * <ul>
 *   <li>{@code api_call_count} (Counter) — labels {@code op}, {@code status_class}
 *   <li>{@code api_call_duration_ms} (Histogram) — label {@code op}
 *   <li>{@code throttle_count} (Counter) — label {@code op}
 *   <li>{@code retry_count} (Counter) — labels {@code op}, {@code reason}
 * </ul>
 *
 * <p>({@code iops} from the default allowlist is derived at reporter time as the rate of {@code
 * api_call_count}, so it is not a separately registered metric.)
 *
 * <p><b>Allowlist.</b> Only metrics whose name is in the allowlist passed at construction are
 * registered; the rest are skipped on the hot path. A {@code "*"} entry registers everything. The
 * default set is the five FLIP-576 metrics ({@code api_call_count}, {@code api_call_duration_ms},
 * {@code throttle_count}, {@code retry_count}, {@code iops}).
 *
 * <p><b>Cardinality.</b> {@code op} comes from the SDK operation name (the closed set of S3
 * operation names), {@code status_class} is a closed classifier ({@code 2xx}, {@code 4xx}, {@code
 * 5xx}, {@code throttled}, {@code other}, {@code error}, {@code unknown}), and {@code reason} is a
 * closed enum ({@code throttled}, {@code 5xx}, {@code other}). Metric handles are cached in bounded
 * maps, so {@link #publish} is a map lookup plus a counter increment with no per-record allocation.
 *
 * <p><b>Thread-safety.</b> Counters use {@link ThreadSafeSimpleCounter} and the histogram is
 * synchronized, so concurrent publishes from the SDK completion executor are safe. The bridge is
 * also safe to share across multiple S3 clients of the same plugin instance.
 */
@Internal
public final class AwsSdkMetricBridge implements MetricPublisher {

    private static final Logger LOG = LoggerFactory.getLogger(AwsSdkMetricBridge.class);

    static final String API_CALL_COUNT = "api_call_count";
    static final String API_CALL_DURATION_MS = "api_call_duration_ms";
    static final String THROTTLE_COUNT = "throttle_count";
    static final String RETRY_COUNT = "retry_count";
    static final String IOPS = "iops";

    /** The default-on metric set from FLIP-576. {@code iops} is derived, not registered. */
    static final List<String> DEFAULT_ALLOWLIST =
            Arrays.asList(API_CALL_COUNT, API_CALL_DURATION_MS, THROTTLE_COUNT, RETRY_COUNT, IOPS);

    private static final String WILDCARD = "*";

    private static final String LABEL_OP = "op";
    private static final String LABEL_STATUS_CLASS = "status_class";
    private static final String LABEL_REASON = "reason";

    private static final String UNKNOWN_OP = "Unknown";

    private final MetricGroup fsScope;
    private final int histogramWindowSize;

    private final boolean allowAll;
    private final Set<String> allowlist;

    // op and label sets are closed, so these maps are bounded by construction.
    private final Map<String, Counter> counters = new ConcurrentHashMap<>();
    private final Map<String, S3MetricHistogram> histograms = new ConcurrentHashMap<>();

    public AwsSdkMetricBridge(MetricGroup fsScope) {
        this(fsScope, DEFAULT_ALLOWLIST, S3MetricHistogram.DEFAULT_WINDOW_SIZE);
    }

    public AwsSdkMetricBridge(MetricGroup fsScope, int histogramWindowSize) {
        this(fsScope, DEFAULT_ALLOWLIST, histogramWindowSize);
    }

    public AwsSdkMetricBridge(
            MetricGroup fsScope, @Nullable Collection<String> allowlist, int histogramWindowSize) {
        this.fsScope = Preconditions.checkNotNull(fsScope, "fsScope must not be null");
        Preconditions.checkArgument(
                histogramWindowSize > 0, "histogramWindowSize must be positive");
        this.histogramWindowSize = histogramWindowSize;

        final Set<String> normalizedAllowlist = normalizeAllowlist(allowlist);
        if (normalizedAllowlist.isEmpty()) {
            throw new IllegalArgumentException(
                    "S3 metrics allowlist must not be empty. Disable metrics with "
                            + "s3.metrics.enabled=false instead.");
        } else if (normalizedAllowlist.contains(WILDCARD)) {
            this.allowAll = true;
            this.allowlist = new HashSet<>();
        } else {
            if (normalizedAllowlist.contains(IOPS)) {
                normalizedAllowlist.add(API_CALL_COUNT);
            }
            this.allowAll = false;
            this.allowlist = normalizedAllowlist;
        }
    }

    private static Set<String> normalizeAllowlist(@Nullable Collection<String> configured) {
        final Set<String> normalized = new HashSet<>();
        if (configured == null) {
            return normalized;
        }
        for (String metric : configured) {
            if (metric != null) {
                final String trimmed = metric.trim();
                if (!trimmed.isEmpty()) {
                    normalized.add(trimmed);
                }
            }
        }
        return normalized;
    }

    private boolean allowed(String metricName) {
        return allowAll || allowlist.contains(metricName);
    }

    @Override
    public void publish(MetricCollection apiCall) {
        try {
            translate(apiCall);
        } catch (Exception e) {
            LOG.warn("Failed to publish S3 SDK metrics", e);
        }
    }

    private void translate(MetricCollection apiCall) {
        final String op = first(apiCall, CoreMetric.OPERATION_NAME, UNKNOWN_OP);

        final Duration duration = first(apiCall, CoreMetric.API_CALL_DURATION, null);
        if (duration != null && allowed(API_CALL_DURATION_MS)) {
            histogram(op).update(duration.toMillis());
        }

        // HTTP_STATUS_CODE lives on the per-attempt children, not on the top-level ApiCall record.
        // status_class reflects the overall outcome (last attempt); the retry reason reflects the
        // failures that triggered the retries (any attempt), so they are tracked separately.
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

        final Boolean successful = first(apiCall, CoreMetric.API_CALL_SUCCESSFUL, null);
        if (allowed(API_CALL_COUNT)) {
            apiCallCount(op, statusClass(lastStatus, successful)).inc();
        }

        if (throttleResponses > 0 && allowed(THROTTLE_COUNT)) {
            throttleCount(op).inc(throttleResponses);
        }

        final Integer retries = first(apiCall, CoreMetric.RETRY_COUNT, 0);
        if (retries != null && retries > 0 && allowed(RETRY_COUNT)) {
            retryCount(op, retryReason(throttleResponses > 0, sawServerError)).inc(retries);
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
        if (sawServerError) {
            return "5xx";
        }
        return "other";
    }

    private Counter apiCallCount(String op, String statusClass) {
        return counters.computeIfAbsent(
                "ac|" + op + "|" + statusClass,
                k ->
                        fsScope.addGroup(LABEL_OP, op)
                                .addGroup(LABEL_STATUS_CLASS, statusClass)
                                .counter(API_CALL_COUNT, new ThreadSafeSimpleCounter()));
    }

    private Counter throttleCount(String op) {
        return counters.computeIfAbsent(
                "th|" + op,
                k ->
                        fsScope.addGroup(LABEL_OP, op)
                                .counter(THROTTLE_COUNT, new ThreadSafeSimpleCounter()));
    }

    private Counter retryCount(String op, String reason) {
        return counters.computeIfAbsent(
                "rc|" + op + "|" + reason,
                k ->
                        fsScope.addGroup(LABEL_OP, op)
                                .addGroup(LABEL_REASON, reason)
                                .counter(RETRY_COUNT, new ThreadSafeSimpleCounter()));
    }

    private S3MetricHistogram histogram(String op) {
        return histograms.computeIfAbsent(
                op,
                k ->
                        fsScope.addGroup(LABEL_OP, op)
                                .histogram(
                                        API_CALL_DURATION_MS,
                                        new S3MetricHistogram(histogramWindowSize)));
    }

    private static <T> T first(MetricCollection collection, SdkMetric<T> metric, T defaultValue) {
        final List<T> values = collection.metricValues(metric);
        return (values == null || values.isEmpty()) ? defaultValue : values.get(0);
    }

    @Override
    public void close() {
        // The MetricGroup is owned by the runtime (see MetricsAware); the bridge holds no resources
        // of its own, so close() is a no-op. This also keeps the bridge safe to share across S3
        // clients of the same plugin instance.
    }
}
