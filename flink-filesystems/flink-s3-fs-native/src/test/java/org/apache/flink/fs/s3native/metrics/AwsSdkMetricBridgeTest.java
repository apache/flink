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

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import software.amazon.awssdk.core.metrics.CoreMetric;
import software.amazon.awssdk.http.HttpMetric;
import software.amazon.awssdk.metrics.MetricCollection;
import software.amazon.awssdk.metrics.MetricCollector;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link AwsSdkMetricBridge}'s translation of SDK metric records into Flink metrics. */
class AwsSdkMetricBridgeTest {

    @Test
    void successfulCallIncrementsApiCallCountAndRecordsDuration() {
        CapturingMetricGroup root = new CapturingMetricGroup();
        AwsSdkMetricBridge bridge = new AwsSdkMetricBridge(root);

        bridge.publish(apiCall("PutObject", Duration.ofMillis(120), true, 0, 200));

        assertThat(root.count("op=PutObject/status_class=2xx/api_call_count")).isEqualTo(1L);
        Histogram histogram = root.histograms.get("op=PutObject/api_call_duration_ms");
        assertThat(histogram).isNotNull();
        assertThat(histogram.getCount()).isEqualTo(1L);
        assertThat(histogram.getStatistics().getMax()).isEqualTo(120L);
        assertThat(root.counters).doesNotContainKey("op=PutObject/throttle_count");
    }

    @Test
    void throttledCallIncrementsThrottleAndRetryCounts() {
        CapturingMetricGroup root = new CapturingMetricGroup();
        AwsSdkMetricBridge bridge = new AwsSdkMetricBridge(root);

        // Two throttled attempts (503) followed by a successful one (200); RETRY_COUNT = 2.
        bridge.publish(apiCall("UploadPart", Duration.ofMillis(900), true, 2, 503, 503, 200));

        assertThat(root.count("op=UploadPart/throttle_count")).isEqualTo(2L);
        assertThat(root.count("op=UploadPart/reason=throttled/retry_count")).isEqualTo(2L);
        // The final attempt succeeded, so the overall call is classified 2xx.
        assertThat(root.count("op=UploadPart/status_class=2xx/api_call_count")).isEqualTo(1L);
    }

    @ParameterizedTest(name = "HTTP {0} (successful={1}) -> status_class {2}")
    @CsvSource({
        "200, true, 2xx",
        "404, false, 4xx",
        "500, false, 5xx",
        "503, false, throttled",
        "429, false, throttled",
    })
    void statusClassIsDerivedFromHttpStatus(int status, boolean successful, String expectedClass) {
        CapturingMetricGroup root = new CapturingMetricGroup();
        AwsSdkMetricBridge bridge = new AwsSdkMetricBridge(root);

        bridge.publish(apiCall("GetObject", Duration.ofMillis(15), successful, 0, status));

        assertThat(root.count("op=GetObject/status_class=" + expectedClass + "/api_call_count"))
                .isEqualTo(1L);
    }

    @Test
    void allowlistRegistersOnlyTheListedMetrics() {
        CapturingMetricGroup root = new CapturingMetricGroup();
        // Only api_call_count is allowed; duration, throttle and retry must be skipped.
        AwsSdkMetricBridge bridge =
                new AwsSdkMetricBridge(
                        root,
                        Collections.singletonList(AwsSdkMetricBridge.API_CALL_COUNT),
                        S3MetricHistogram.DEFAULT_WINDOW_SIZE);

        bridge.publish(apiCall("UploadPart", Duration.ofMillis(900), true, 2, 503, 503, 200));

        assertThat(root.count("op=UploadPart/status_class=2xx/api_call_count")).isEqualTo(1L);
        assertThat(root.histograms).doesNotContainKey("op=UploadPart/api_call_duration_ms");
        assertThat(root.counters).doesNotContainKey("op=UploadPart/throttle_count");
        assertThat(root.counters).doesNotContainKey("op=UploadPart/reason=throttled/retry_count");
    }

    @Test
    void wildcardAllowlistRegistersTheFullMetricSet() {
        CapturingMetricGroup root = new CapturingMetricGroup();
        AwsSdkMetricBridge bridge =
                new AwsSdkMetricBridge(
                        root,
                        Collections.singletonList("*"),
                        S3MetricHistogram.DEFAULT_WINDOW_SIZE);

        bridge.publish(apiCall("UploadPart", Duration.ofMillis(900), true, 2, 503, 503, 200));

        // No metric currently sits outside DEFAULT_ALLOWLIST, so "*" and the default list emit the
        // same set. The discriminating case (a restricted list that omits metrics) is covered by
        // allowlistRegistersOnlyTheListedMetrics; this pins that "*" is accepted and emits all.
        assertThat(root.count("op=UploadPart/status_class=2xx/api_call_count")).isEqualTo(1L);
        assertThat(root.histograms.get("op=UploadPart/api_call_duration_ms")).isNotNull();
        assertThat(root.count("op=UploadPart/throttle_count")).isEqualTo(2L);
        assertThat(root.count("op=UploadPart/reason=throttled/retry_count")).isEqualTo(2L);
    }

    @Test
    void emptyAllowlistIsRejected() {
        CapturingMetricGroup root = new CapturingMetricGroup();

        assertThatThrownBy(
                        () ->
                                new AwsSdkMetricBridge(
                                        root,
                                        Collections.emptyList(),
                                        S3MetricHistogram.DEFAULT_WINDOW_SIZE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("allowlist must not be empty");
    }

    @Test
    void iopsAllowlistRegistersApiCallCountForDerivedRate() {
        CapturingMetricGroup root = new CapturingMetricGroup();
        AwsSdkMetricBridge bridge =
                new AwsSdkMetricBridge(
                        root,
                        Collections.singletonList(AwsSdkMetricBridge.IOPS),
                        S3MetricHistogram.DEFAULT_WINDOW_SIZE);

        bridge.publish(apiCall("PutObject", Duration.ofMillis(120), true, 0, 200));

        assertThat(root.count("op=PutObject/status_class=2xx/api_call_count")).isEqualTo(1L);
        assertThat(root.histograms).doesNotContainKey("op=PutObject/api_call_duration_ms");
    }

    @Test
    void serverErrorRetryIsClassifiedAs5xx() {
        CapturingMetricGroup root = new CapturingMetricGroup();
        AwsSdkMetricBridge bridge = new AwsSdkMetricBridge(root);

        bridge.publish(apiCall("GetObject", Duration.ofMillis(50), true, 1, 500, 200));

        assertThat(root.count("op=GetObject/reason=5xx/retry_count")).isEqualTo(1L);
        assertThat(root.counters).doesNotContainKey("op=GetObject/throttle_count");
    }

    @Test
    void accumulatesAcrossMultipleCalls() {
        CapturingMetricGroup root = new CapturingMetricGroup();
        AwsSdkMetricBridge bridge = new AwsSdkMetricBridge(root);

        bridge.publish(apiCall("GetObject", Duration.ofMillis(10), true, 0, 200));
        bridge.publish(apiCall("GetObject", Duration.ofMillis(30), true, 0, 200));

        assertThat(root.count("op=GetObject/status_class=2xx/api_call_count")).isEqualTo(2L);
        Histogram histogram = root.histograms.get("op=GetObject/api_call_duration_ms");
        assertThat(histogram.getCount()).isEqualTo(2L);
        assertThat(histogram.getStatistics().getMin()).isEqualTo(10L);
        assertThat(histogram.getStatistics().getMax()).isEqualTo(30L);
    }

    @Test
    void publishOfEmptyCollectionDoesNotThrowAndUsesUnknownOp() {
        CapturingMetricGroup root = new CapturingMetricGroup();
        AwsSdkMetricBridge bridge = new AwsSdkMetricBridge(root);

        bridge.publish(MetricCollector.create("ApiCall").collect());

        assertThat(root.counters.keySet()).anyMatch(key -> key.contains("op=Unknown"));
    }

    private static MetricCollection apiCall(
            String op, Duration duration, boolean successful, int retries, int... attemptStatuses) {
        MetricCollector apiCall = MetricCollector.create("ApiCall");
        apiCall.reportMetric(CoreMetric.OPERATION_NAME, op);
        apiCall.reportMetric(CoreMetric.API_CALL_DURATION, duration);
        apiCall.reportMetric(CoreMetric.API_CALL_SUCCESSFUL, successful);
        apiCall.reportMetric(CoreMetric.RETRY_COUNT, retries);
        for (int status : attemptStatuses) {
            MetricCollector attempt = apiCall.createChild("ApiCallAttempt");
            attempt.reportMetric(HttpMetric.HTTP_STATUS_CODE, status);
        }
        return apiCall.collect();
    }

    /** A {@link MetricGroup} that captures registered metrics keyed by their full label path. */
    private static final class CapturingMetricGroup extends UnregisteredMetricsGroup {

        final Map<String, Counter> counters;
        final Map<String, Histogram> histograms;
        private final String path;

        CapturingMetricGroup() {
            this("", new HashMap<>(), new HashMap<>());
        }

        private CapturingMetricGroup(
                String path, Map<String, Counter> counters, Map<String, Histogram> histograms) {
            this.path = path;
            this.counters = counters;
            this.histograms = histograms;
        }

        long count(String key) {
            Counter counter = counters.get(key);
            assertThat(counter).as("counter %s", key).isNotNull();
            return counter.getCount();
        }

        @Override
        public MetricGroup addGroup(String name) {
            return new CapturingMetricGroup(child(name), counters, histograms);
        }

        @Override
        public MetricGroup addGroup(String key, String value) {
            return new CapturingMetricGroup(child(key + "=" + value), counters, histograms);
        }

        @Override
        public <C extends Counter> C counter(String name, C counter) {
            counters.put(child(name), counter);
            return counter;
        }

        @Override
        public <H extends Histogram> H histogram(String name, H histogram) {
            histograms.put(child(name), histogram);
            return histogram;
        }

        private String child(String segment) {
            return path.isEmpty() ? segment : path + "/" + segment;
        }
    }
}
