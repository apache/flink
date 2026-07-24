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
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.metrics.SlidingWindowHistogram;
import org.apache.flink.util.Preconditions;

import java.util.List;

/** Creates filesystem metric options under a cloud-specific configuration prefix. */
@Internal
public final class FileSystemMetricOptions {

    private FileSystemMetricOptions() {}

    public static ConfigOption<Boolean> metricsEnabled(String configurationPrefix) {
        final String prefix = validatePrefix(configurationPrefix);
        return ConfigOptions.key(prefix + ".metrics.enabled")
                .booleanType()
                .defaultValue(true)
                .withDescription(
                        "Master switch for publishing "
                                + prefix
                                + " filesystem operation metrics to Flink's metric system.");
    }

    public static ConfigOption<List<String>> metricsAllowlist(String configurationPrefix) {
        final String prefix = validatePrefix(configurationPrefix);
        return ConfigOptions.key(prefix + ".metrics.allowlist")
                .stringType()
                .asList()
                .defaultValues(FileSystemMetricRecorder.DEFAULT_ALLOWLIST.toArray(new String[0]))
                .withDescription(
                        "Names of "
                                + prefix
                                + " filesystem metrics to register. Replaces the default list; use "
                                + "\"*\" to register every emitted metric. An empty list is invalid. "
                                + "The iops metric is derived from api_call_count.");
    }

    public static ConfigOption<Integer> metricsHistogramWindowSize(String configurationPrefix) {
        final String prefix = validatePrefix(configurationPrefix);
        return ConfigOptions.key(prefix + ".metrics.histogram.window-size")
                .intType()
                .defaultValue(SlidingWindowHistogram.DEFAULT_WINDOW_SIZE)
                .withDescription(
                        "Number of recent values retained by "
                                + prefix
                                + " filesystem latency histograms. Must be positive.");
    }

    private static String validatePrefix(String configurationPrefix) {
        final String prefix = Preconditions.checkNotNull(configurationPrefix).trim();
        Preconditions.checkArgument(!prefix.isEmpty(), "configurationPrefix must not be empty");
        return prefix;
    }
}
