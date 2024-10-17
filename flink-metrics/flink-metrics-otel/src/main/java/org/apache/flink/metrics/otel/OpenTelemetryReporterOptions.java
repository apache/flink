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

package org.apache.flink.metrics.otel;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.util.TimeUtils;

import java.time.Duration;
import java.util.function.Consumer;

import static org.apache.flink.util.Preconditions.checkArgument;

/** Config options for subclasses of {@link OpenTelemetryReporterBase}. */
@PublicEvolving
@Documentation.SuffixOption(ConfigConstants.METRICS_REPORTER_PREFIX + "OpenTelemetry")
public final class OpenTelemetryReporterOptions {

    private OpenTelemetryReporterOptions() {}

    public static final ConfigOption<String> EXPORTER_ENDPOINT =
            ConfigOptions.key("exporter.endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text("Endpoint for the OpenTelemetry Reporters.")
                                    .build());

    public static final ConfigOption<String> EXPORTER_TIMEOUT =
            ConfigOptions.key("exporter.timeout")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Timeout for OpenTelemetry Reporters, as Duration string. Example: 10s for 10 seconds")
                                    .build());

    public static final ConfigOption<String> SERVICE_NAME =
            ConfigOptions.key("service.name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text("service.name passed to OpenTelemetry Reporters.")
                                    .build());

    @PublicEvolving
    public static final ConfigOption<String> SERVICE_VERSION =
            ConfigOptions.key("service.version")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text("service.version passed to OpenTelemetry Reporters.")
                                    .build());

    @Internal
    public static void tryConfigureTimeout(MetricConfig metricConfig, Consumer<Duration> builder) {
        final String timeoutConfKey = EXPORTER_TIMEOUT.key();
        if (metricConfig.containsKey(timeoutConfKey)) {
            builder.accept(TimeUtils.parseDuration(metricConfig.getProperty(timeoutConfKey)));
        }
    }

    @Internal
    public static void tryConfigureEndpoint(MetricConfig metricConfig, Consumer<String> builder) {
        final String endpointConfKey = EXPORTER_ENDPOINT.key();
        checkArgument(
                metricConfig.containsKey(endpointConfKey), "Must set " + EXPORTER_ENDPOINT.key());
        builder.accept(metricConfig.getProperty(endpointConfKey));
    }
}
