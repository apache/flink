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
import org.apache.flink.metrics.testutils.MetricListener;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import software.amazon.awssdk.core.metrics.CoreMetric;
import software.amazon.awssdk.http.HttpMetric;
import software.amazon.awssdk.metrics.MetricCollection;
import software.amazon.awssdk.metrics.MetricCollector;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class AwsSdkMetricBridgeTest {

    @Test
    void mapsSuccessfulCallAndDuration() {
        final MetricListener listener = new MetricListener();
        final AwsSdkMetricBridge bridge = new AwsSdkMetricBridge(listener.getMetricGroup());

        bridge.publish(apiCall("PutObject", Duration.ofMillis(120), true, 0, 200));

        assertThat(count(listener, "op", "PutObject", "status_class", "2xx", "api_call_count"))
                .isEqualTo(1L);
        final Histogram histogram = histogram(listener, "op", "PutObject", "api_call_duration_ms");
        assertThat(histogram.getCount()).isEqualTo(1L);
        assertThat(histogram.getStatistics().getMax()).isEqualTo(120L);
    }

    @Test
    void mapsAwsThrottlingAndRetries() {
        final MetricListener listener = new MetricListener();
        final AwsSdkMetricBridge bridge = new AwsSdkMetricBridge(listener.getMetricGroup());

        bridge.publish(apiCall("UploadPart", Duration.ofMillis(900), true, 2, 503, 503, 200));

        assertThat(count(listener, "op", "UploadPart", "throttle_count")).isEqualTo(2L);
        assertThat(count(listener, "op", "UploadPart", "reason", "throttled", "retry_count"))
                .isEqualTo(2L);
        assertThat(count(listener, "op", "UploadPart", "status_class", "2xx", "api_call_count"))
                .isEqualTo(1L);
    }

    @ParameterizedTest(name = "HTTP {0} (successful={1}) -> status_class {2}")
    @CsvSource({
        "200, true, 2xx",
        "404, false, 4xx",
        "500, false, 5xx",
        "503, false, throttled",
        "429, false, throttled",
        "302, false, other"
    })
    void mapsAwsHttpStatus(int status, boolean successful, String expectedClass) {
        final MetricListener listener = new MetricListener();
        final AwsSdkMetricBridge bridge = new AwsSdkMetricBridge(listener.getMetricGroup());

        bridge.publish(apiCall("GetObject", Duration.ofMillis(15), successful, 0, status));

        assertThat(
                        count(
                                listener,
                                "op",
                                "GetObject",
                                "status_class",
                                expectedClass,
                                "api_call_count"))
                .isEqualTo(1L);
    }

    @Test
    void mapsAwsServerErrorRetry() {
        final MetricListener listener = new MetricListener();
        final AwsSdkMetricBridge bridge = new AwsSdkMetricBridge(listener.getMetricGroup());

        bridge.publish(apiCall("GetObject", Duration.ofMillis(50), true, 1, 500, 200));

        assertThat(count(listener, "op", "GetObject", "reason", "5xx", "retry_count"))
                .isEqualTo(1L);
        assertThat(listener.getCounter("op", "GetObject", "throttle_count")).isEmpty();
    }

    @Test
    void mapsMissingSdkFieldsToUnknown() {
        final MetricListener listener = new MetricListener();
        final AwsSdkMetricBridge bridge = new AwsSdkMetricBridge(listener.getMetricGroup());

        bridge.publish(MetricCollector.create("ApiCall").collect());

        assertThat(count(listener, "op", "Unknown", "status_class", "unknown", "api_call_count"))
                .isEqualTo(1L);
    }

    private static MetricCollection apiCall(
            String operation,
            Duration duration,
            boolean successful,
            int retries,
            int... attemptStatuses) {
        final MetricCollector apiCall = MetricCollector.create("ApiCall");
        apiCall.reportMetric(CoreMetric.OPERATION_NAME, operation);
        apiCall.reportMetric(CoreMetric.API_CALL_DURATION, duration);
        apiCall.reportMetric(CoreMetric.API_CALL_SUCCESSFUL, successful);
        apiCall.reportMetric(CoreMetric.RETRY_COUNT, retries);
        for (int status : attemptStatuses) {
            final MetricCollector attempt = apiCall.createChild("ApiCallAttempt");
            attempt.reportMetric(HttpMetric.HTTP_STATUS_CODE, status);
        }
        return apiCall.collect();
    }

    private static long count(MetricListener listener, String... identifier) {
        return listener.getCounter(identifier)
                .map(Counter::getCount)
                .orElseThrow(() -> new AssertionError("Missing counter"));
    }

    private static Histogram histogram(MetricListener listener, String... identifier) {
        return listener.getHistogram(identifier)
                .orElseThrow(() -> new AssertionError("Missing histogram"));
    }
}
