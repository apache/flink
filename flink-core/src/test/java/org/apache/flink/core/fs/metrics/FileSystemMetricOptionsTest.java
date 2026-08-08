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

import org.apache.flink.metrics.SlidingWindowHistogram;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FileSystemMetricOptionsTest {

    @Test
    void createsOptionsUnderConfiguredPrefix() {
        assertThat(FileSystemMetricOptions.metricsEnabled("gcs").key())
                .isEqualTo("gcs.metrics.enabled");
        assertThat(FileSystemMetricOptions.metricsAllowlist("gcs").key())
                .isEqualTo("gcs.metrics.allowlist");
        assertThat(FileSystemMetricOptions.metricsHistogramWindowSize("gcs").key())
                .isEqualTo("gcs.metrics.histogram.window-size");
    }

    @Test
    void usesCloudNeutralDefaults() {
        assertThat(FileSystemMetricOptions.metricsAllowlist("s3").defaultValue())
                .containsExactlyElementsOf(FileSystemMetricRecorder.DEFAULT_ALLOWLIST);
        assertThat(FileSystemMetricOptions.metricsHistogramWindowSize("s3").defaultValue())
                .isEqualTo(SlidingWindowHistogram.DEFAULT_WINDOW_SIZE);
    }

    @Test
    void rejectsEmptyPrefix() {
        assertThatThrownBy(() -> FileSystemMetricOptions.metricsEnabled(" "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("configurationPrefix must not be empty");
    }
}
