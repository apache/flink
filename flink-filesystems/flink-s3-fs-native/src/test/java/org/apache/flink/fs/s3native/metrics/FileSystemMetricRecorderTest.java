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

import org.apache.flink.core.fs.metrics.FileSystemMetricRecorder;
import org.apache.flink.core.fs.metrics.FileSystemMetricRecorder.RetryReason;
import org.apache.flink.core.fs.metrics.FileSystemMetricRecorder.StatusClass;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SlidingWindowHistogram;
import org.apache.flink.metrics.testutils.MetricListener;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FileSystemMetricRecorderTest {

    @Test
    void restrictedAllowlistRegistersOnlySelectedMetrics() {
        final MetricListener listener = new MetricListener();
        final FileSystemMetricRecorder recorder =
                recorder(
                        listener,
                        Collections.singletonList(FileSystemMetricRecorder.API_CALL_COUNT));

        recordAllMetrics(recorder);

        assertThat(count(listener, "op", "Write", "status_class", "2xx", "api_call_count"))
                .isEqualTo(1L);
        assertThat(listener.getHistogram("op", "Write", "api_call_duration_ms")).isEmpty();
        assertThat(listener.getCounter("op", "Write", "throttle_count")).isEmpty();
        assertThat(listener.getCounter("op", "Write", "reason", "throttled", "retry_count"))
                .isEmpty();
    }

    @Test
    void iopsEnablesSourceCounter() {
        final MetricListener listener = new MetricListener();
        final FileSystemMetricRecorder recorder =
                recorder(listener, Collections.singletonList(FileSystemMetricRecorder.IOPS));

        recorder.recordApiCall("Read", StatusClass.SUCCESS);

        assertThat(count(listener, "op", "Read", "status_class", "2xx", "api_call_count"))
                .isEqualTo(1L);
    }

    @Test
    void accumulatesMetricsAcrossCalls() {
        final MetricListener listener = new MetricListener();
        final FileSystemMetricRecorder recorder =
                recorder(listener, FileSystemMetricRecorder.DEFAULT_ALLOWLIST);

        recorder.recordApiCall("Read", StatusClass.SUCCESS);
        recorder.recordApiCall("Read", StatusClass.SUCCESS);
        recorder.recordDuration("Read", 10L);
        recorder.recordDuration("Read", 30L);

        assertThat(count(listener, "op", "Read", "status_class", "2xx", "api_call_count"))
                .isEqualTo(2L);
        assertThat(
                        listener.getHistogram("op", "Read", "api_call_duration_ms")
                                .orElseThrow(AssertionError::new)
                                .getStatistics()
                                .getValues())
                .containsExactly(10L, 30L);
    }

    @Test
    void rejectsEmptyAllowlist() {
        final MetricListener listener = new MetricListener();

        assertThatThrownBy(() -> recorder(listener, Collections.emptyList()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("allowlist must not be empty");
    }

    private static FileSystemMetricRecorder recorder(
            MetricListener listener, java.util.Collection<String> allowlist) {
        return new FileSystemMetricRecorder(
                listener.getMetricGroup(), allowlist, SlidingWindowHistogram.DEFAULT_WINDOW_SIZE);
    }

    private static void recordAllMetrics(FileSystemMetricRecorder recorder) {
        recorder.recordApiCall("Write", StatusClass.SUCCESS);
        recorder.recordDuration("Write", 10L);
        recorder.recordThrottles("Write", 1L);
        recorder.recordRetries("Write", 1L, RetryReason.THROTTLED);
    }

    private static long count(MetricListener listener, String... identifier) {
        return listener.getCounter(identifier)
                .map(Counter::getCount)
                .orElseThrow(() -> new AssertionError("Missing counter"));
    }
}
