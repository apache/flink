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
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

class FileSystemMetricRecorderTest {

    @Test
    void wildcardIncludesMetricsOutsideDefaultAllowlist() {
        final FileSystemMetricRecorder wildcardRecorder = recorder(Collections.singletonList("*"));
        final FileSystemMetricRecorder defaultRecorder =
                recorder(FileSystemMetricRecorder.DEFAULT_ALLOWLIST);

        assertThat(wildcardRecorder.isMetricEnabled("future_metric")).isTrue();
        assertThat(defaultRecorder.isMetricEnabled("future_metric")).isFalse();
    }

    private static FileSystemMetricRecorder recorder(java.util.Collection<String> allowlist) {
        return new FileSystemMetricRecorder(
                new UnregisteredMetricsGroup(),
                allowlist,
                SlidingWindowHistogram.DEFAULT_WINDOW_SIZE);
    }
}
