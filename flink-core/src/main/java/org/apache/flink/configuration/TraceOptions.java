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

package org.apache.flink.configuration;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.description.Description;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.configuration.description.LineBreakElement.linebreak;
import static org.apache.flink.configuration.description.TextElement.code;
import static org.apache.flink.configuration.description.TextElement.text;

/** Configuration options for traces and trace reporters. */
@Experimental
public class TraceOptions {

    /** Enum for the detail level of checkpointing spans. */
    public enum CheckpointSpanDetailLevel {
        /** Sum/Max for sub-metrics per checkpoint. */
        SPAN_PER_CHECKPOINT,
        /** Sum/Max for sub-metrics per checkpoint and arrays of task aggregates. */
        SPAN_PER_CHECKPOINT_WITH_TASKS,
        /** Sub/Max for sub-metrics of checkpoint and tasks (tasks as child spans). */
        CHILDREN_SPANS_PER_TASK,
        /**
         * Sub/Max for sub-metrics of checkpoint, tasks, and subtasks (tasks as child spans,
         * subtasks as grand-child spans).
         */
        CHILDREN_SPANS_PER_SUBTASK;
    }

    private static final String NAMED_REPORTER_CONFIG_PREFIX =
            ConfigConstants.TRACES_REPORTER_PREFIX + "<name>";

    @Experimental
    public static Configuration forReporter(Configuration configuration, String reporterName) {
        return new DelegatingConfiguration(
                configuration, ConfigConstants.TRACES_REPORTER_PREFIX + reporterName + ".");
    }

    /**
     * An optional list of reporter names. If configured, only reporters whose name matches any of
     * the names in the list will be started. Otherwise, all reporters that could be found in the
     * configuration will be started.
     *
     * <p>Example:
     *
     * <pre>{@code
     * traces.reporters = foo,bar
     *
     * traces.reporter.foo.class = org.apache.flink.traces.reporter.OpenTelemetryTraceReporter
     * traces.reporter.foo.endpoint = 127.0.0.1:4137
     * }</pre>
     */
    public static final ConfigOption<String> TRACE_REPORTERS_LIST =
            key("traces.reporters")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "An optional list of trace reporter names. If configured, only reporters whose name matches"
                                    + " any of the names in the list will be started. Otherwise, all reporters that could be found in"
                                    + " the configuration will be started.");

    /** The detail level for reporting checkpoint spans. */
    public static final ConfigOption<TraceOptions.CheckpointSpanDetailLevel>
            CHECKPOINT_SPAN_DETAIL_LEVEL =
                    key("traces.checkpoint.span-detail-level")
                            .enumType(CheckpointSpanDetailLevel.class)
                            .defaultValue(CheckpointSpanDetailLevel.SPAN_PER_CHECKPOINT)
                            .withDescription(
                                    Description.builder()
                                            .text(
                                                    "Detail level for reporting checkpoint spans. Possible values:\n")
                                            .list(
                                                    text(
                                                            "'%s' (default): Single span per checkpoint. "
                                                                    + "Aggregated sum/max for sub-metrics from all tasks and subtasks per checkpoint.",
                                                            code(
                                                                    CheckpointSpanDetailLevel
                                                                            .SPAN_PER_CHECKPOINT
                                                                            .name())),
                                                    text(
                                                            "'%s': Single span per checkpoint. "
                                                                    + "Same as '%s', plus arrays of aggregated values per task.",
                                                            code(
                                                                    CheckpointSpanDetailLevel
                                                                            .SPAN_PER_CHECKPOINT_WITH_TASKS
                                                                            .name()),
                                                            code(
                                                                    CheckpointSpanDetailLevel
                                                                            .SPAN_PER_CHECKPOINT
                                                                            .name())),
                                                    text(
                                                            "'%s': Same as '%s' plus children spans per each task. "
                                                                    + "Each task span with aggregated sum/max sub-metrics from subtasks.",
                                                            code(
                                                                    CheckpointSpanDetailLevel
                                                                            .CHILDREN_SPANS_PER_TASK
                                                                            .name()),
                                                            code(
                                                                    CheckpointSpanDetailLevel
                                                                            .SPAN_PER_CHECKPOINT
                                                                            .name())),
                                                    text(
                                                            "'%s': Same as '%s' plus children spans per each subtask. "
                                                                    + "Child spans for tasks and grand-child spans for subtasks.",
                                                            code(
                                                                    CheckpointSpanDetailLevel
                                                                            .CHILDREN_SPANS_PER_SUBTASK
                                                                            .name()),
                                                            code(
                                                                    CheckpointSpanDetailLevel
                                                                            .CHILDREN_SPANS_PER_TASK
                                                                            .name())))
                                            .build());

    /**
     * Returns a view over the given configuration via which options can be set/retrieved for the
     * given reporter.
     *
     * <pre>
     *     Configuration config = ...
     *     MetricOptions.forReporter(config, "my_reporter")
     *         .set(MetricOptions.REPORTER_INTERVAL, Duration.ofSeconds(10))
     *         ...
     * </pre>
     *
     * @param configuration backing configuration
     * @param reporterName reporter name
     * @return view over configuration
     */
    @Experimental
    public static Configuration forTraceReporter(Configuration configuration, String reporterName) {
        return new DelegatingConfiguration(
                configuration, ConfigConstants.TRACES_REPORTER_PREFIX + reporterName + ".");
    }

    @Documentation.SuffixOption(NAMED_REPORTER_CONFIG_PREFIX)
    @Documentation.Section(value = Documentation.Sections.TRACE_REPORTERS, position = 1)
    public static final ConfigOption<String> REPORTER_FACTORY_CLASS =
            key("factory.class")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The reporter factory class to use for the reporter named <name>.");

    @Documentation.SuffixOption(NAMED_REPORTER_CONFIG_PREFIX)
    @Documentation.Section(value = Documentation.Sections.TRACE_REPORTERS, position = 5)
    public static final ConfigOption<String> REPORTER_CONFIG_PARAMETER =
            key("<parameter>")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Configures the parameter <parameter> for the reporter named <name>.");

    @Documentation.SuffixOption(NAMED_REPORTER_CONFIG_PREFIX)
    @Documentation.Section(value = Documentation.Sections.TRACE_REPORTERS, position = 4)
    public static final ConfigOption<Map<String, String>> REPORTER_ADDITIONAL_VARIABLES =
            key("scope.variables.additional")
                    .mapType()
                    .defaultValue(Collections.emptyMap())
                    .withDescription(
                            "The map of additional variables that should be included for the reporter named <name>.");

    @Documentation.SuffixOption(NAMED_REPORTER_CONFIG_PREFIX)
    @Documentation.Section(value = Documentation.Sections.TRACE_REPORTERS, position = 2)
    public static final ConfigOption<String> REPORTER_SCOPE_DELIMITER =
            key("scope.delimiter")
                    .stringType()
                    .defaultValue(".")
                    .withDescription(
                            "The delimiter used to assemble the metric identifier for the reporter named <name>.");

    @Documentation.SuffixOption(NAMED_REPORTER_CONFIG_PREFIX)
    @Documentation.Section(value = Documentation.Sections.TRACE_REPORTERS, position = 3)
    public static final ConfigOption<String> REPORTER_EXCLUDED_VARIABLES =
            key("scope.variables.excludes")
                    .stringType()
                    .defaultValue(".")
                    .withDescription(
                            "The set of variables that should be excluded for the reporter named <name>. Only applicable to tag-based reporters.");

    @Documentation.SuffixOption(NAMED_REPORTER_CONFIG_PREFIX)
    @Documentation.Section(value = Documentation.Sections.TRACE_REPORTERS, position = 6)
    public static final ConfigOption<List<String>> REPORTER_INCLUDES =
            key("filter.includes")
                    .stringType()
                    .asList()
                    .defaultValues("*:*:*")
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The spans that should be included for the reporter named <name>."
                                                    + " Filters are specified as a list, with each filter following this format:")
                                    .linebreak()
                                    .text("%s", code("<scope>[:<name>[,<name>]]"))
                                    .linebreak()
                                    .text(
                                            "A span matches a filter if the scope pattern and at least one of the name patterns match.")
                                    .linebreak()
                                    .list(
                                            text(
                                                    "scope: Filters based on the logical scope.%s"
                                                            + "Specified as a pattern where %s matches any sequence of characters and %s separates scope components.%s%s"
                                                            + "For example:%s"
                                                            + " \"%s\" matches any job-related spans on the JobManager,%s"
                                                            + " \"%s\" matches all job-related spans and%s"
                                                            + " \"%s\" matches all spans below the job-level (i.e., task/operator spans etc.).%s%s",
                                                    linebreak(),
                                                    code("*"),
                                                    code("."),
                                                    linebreak(),
                                                    linebreak(),
                                                    linebreak(),
                                                    code("jobmanager.job"),
                                                    linebreak(),
                                                    code("*.job"),
                                                    linebreak(),
                                                    code("*.job.*"),
                                                    linebreak(),
                                                    linebreak()),
                                            text(
                                                    "name: Filters based on the span name.%s"
                                                            + "Specified as a comma-separate list of patterns where %s matches any sequence of characters.%s%s"
                                                            + "For example, \"%s\" matches any span where the name contains %s.",
                                                    linebreak(),
                                                    code("*"),
                                                    linebreak(),
                                                    linebreak(),
                                                    code("*Records*,*Bytes*"),
                                                    code("\"Records\" or \"Bytes\"")))
                                    .build());

    @Documentation.SuffixOption(NAMED_REPORTER_CONFIG_PREFIX)
    @Documentation.Section(value = Documentation.Sections.TRACE_REPORTERS, position = 7)
    public static final ConfigOption<List<String>> REPORTER_EXCLUDES =
            key("filter.excludes")
                    .stringType()
                    .asList()
                    .defaultValues()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The spans that should be excluded for the reporter named <name>. The format is identical to %s",
                                            code(REPORTER_INCLUDES.key()))
                                    .linebreak()
                                    .build());

    private TraceOptions() {}
}
