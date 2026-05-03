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

    public enum Protocol {
        gRPC,
        HTTP
    }

    public static final String COMPRESSION_NONE = "none";
    public static final String COMPRESSION_GZIP = "gzip";

    private OpenTelemetryReporterOptions() {}

    public static final ConfigOption<Protocol> EXPORTER_PROTOCOL =
            ConfigOptions.key("exporter.protocol")
                    .enumType(Protocol.class)
                    .defaultValue(Protocol.gRPC)
                    .withDescription(
                            Description.builder()
                                    .text("Protocol for the OpenTelemetry Reporters.")
                                    .build());

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

    public static final ConfigOption<String> EXPORTER_COMPRESSION =
            ConfigOptions.key("exporter.compression")
                    .stringType()
                    .defaultValue(COMPRESSION_NONE)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            String.format(
                                                    "Compression method for OTel Reporter only '%s' or '%s'. Default is '%s'.",
                                                    COMPRESSION_GZIP,
                                                    COMPRESSION_NONE,
                                                    COMPRESSION_NONE))
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

    @PublicEvolving
    public static final ConfigOption<Integer> BATCH_SIZE =
            ConfigOptions.key("batch.size")
                    .intType()
                    .defaultValue(0)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Number of metrics per export batch. "
                                                    + "Values <= 0 disable batching and all metrics are exported in a single request.")
                                    .build());

    @PublicEvolving
    public static final ConfigOption<Long> EXPORT_COMPLETION_TIMEOUT_MILLIS =
            ConfigOptions.key("export-completion-timeout-millis")
                    .longType()
                    .defaultValue(300_000L)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Timeout in milliseconds for waiting on async export completion.")
                                    .build());

    /** Prefix key used to identify attribute value length limit configuration entries. */
    public static final String ATTRIBUTE_VALUE_LENGTH_LIMITS_PREFIX_KEY =
            "transform.attribute-value-length-limits.";

    /**
     * Config option for attribute value length limits. Only used for documentation purposes — the
     * actual option is prefix-based and parsed by {@link MetricAttributeTransformer}.
     *
     * <p>For example, to limit the {@code task_name} attribute to 60 characters set {@code
     * metrics.reporter.otel.transform.attribute-value-length-limits.task_name: 60} in the Flink
     * configuration. The special key {@code *} defines a global limit for all attributes not
     * explicitly listed. {@code 0} drops an attribute; negative values disable the limit for that
     * attribute (useful to override a global cap).
     */
    @PublicEvolving
    public static final ConfigOption<Integer> ATTRIBUTE_VALUE_LENGTH_LIMITS =
            ConfigOptions.key(ATTRIBUTE_VALUE_LENGTH_LIMITS_PREFIX_KEY + "<attribute-name>")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Limits of the exported attribute values length. Only applies to the metric "
                                                    + "reporter; ignored by the trace and event reporters. "
                                                    + "Configuration is prefix based, "
                                                    + "for example to limit `task_name` attribute set "
                                                    + "`metrics.reporter.otel.transform.attribute-value-length-limits.task_name: 60` "
                                                    + "in the config for OTel reporter. "
                                                    + "A special key '*' can be used to define a global limit for all attributes not "
                                                    + "explicitly listed. "
                                                    + "For example `metrics.reporter.otel.transform.attribute-value-length-limits.*: 1024` "
                                                    + "will limit all attributes to attributeValue.substring(0, 1024). "
                                                    + "Global limit defaults to Integer.MAX_VALUE if not set. Individual attribute "
                                                    + "limits always override the global limit and are verified by exact match on the "
                                                    + "attribute name. "
                                                    + "0 can be used to drop an attribute. Negative values are interpreted as no limit "
                                                    + "for the attribute (can be used for global limit overrides).")
                                    .build());

    /** Config key for the collision tracker's maximum size. */
    public static final String COLLISION_TRACKING_MAX_SLOTS_KEY =
            "transform.collision-tracking-max-slots";

    @PublicEvolving
    public static final ConfigOption<Integer> COLLISION_TRACKING_MAX_SLOTS =
            ConfigOptions.key(COLLISION_TRACKING_MAX_SLOTS_KEY)
                    .intType()
                    .defaultValue(50_000)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Maximum number of distinct (metric name, transformed attributes) "
                                                    + "slots the truncation-collision tracker retains. Only applies to "
                                                    + "the metric reporter; ignored by the trace and event reporters. "
                                                    + "When the cap is "
                                                    + "reached the least-recently-touched slot is evicted (LRU); a "
                                                    + "previously warned slot that later gets evicted may fire its warning "
                                                    + "again on re-entry. "
                                                    + "Only consulted when attribute-value length limits are configured — "
                                                    + "if no truncation is happening, there is nothing to track and this "
                                                    + "option has no effect. "
                                                    + "Set to 0 to disable collision tracking entirely. Malformed or "
                                                    + "negative values fall back to the default with a warning in the logs.")
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

    @Internal
    public static void tryConfigureCompression(
            MetricConfig metricConfig, Consumer<String> builder) {
        final String compressionConfKey = EXPORTER_COMPRESSION.key();
        if (metricConfig.containsKey(compressionConfKey)) {
            String compression = metricConfig.getProperty(compressionConfKey);
            checkArgument(
                    COMPRESSION_NONE.equals(compression) || COMPRESSION_GZIP.equals(compression),
                    "Unsupported compression method: '%s'. Supported values are '%s' and '%s'.",
                    compression,
                    COMPRESSION_NONE,
                    COMPRESSION_GZIP);
            builder.accept(compression);
        }
    }
}
