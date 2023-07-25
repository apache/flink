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
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.configuration.description.InlineElement;
import org.apache.flink.configuration.description.TextElement;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.configuration.description.LineBreakElement.linebreak;
import static org.apache.flink.configuration.description.TextElement.code;
import static org.apache.flink.configuration.description.TextElement.text;

/** Configuration options for metrics and metric reporters. */
@PublicEvolving
public class MetricOptions {

    private static final String NAMED_REPORTER_CONFIG_PREFIX =
            ConfigConstants.METRICS_REPORTER_PREFIX + "<name>";

    /**
     * An optional list of reporter names. If configured, only reporters whose name matches any of
     * the names in the list will be started. Otherwise, all reporters that could be found in the
     * configuration will be started.
     *
     * <p>Example:
     *
     * <pre>{@code
     * metrics.reporters = foo,bar
     *
     * metrics.reporter.foo.class = org.apache.flink.metrics.reporter.JMXReporter
     * metrics.reporter.foo.interval = 10
     *
     * metrics.reporter.bar.class = org.apache.flink.metrics.graphite.GraphiteReporter
     * metrics.reporter.bar.port = 1337
     * }</pre>
     */
    public static final ConfigOption<String> REPORTERS_LIST =
            key("metrics.reporters")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "An optional list of reporter names. If configured, only reporters whose name matches"
                                    + " any of the names in the list will be started. Otherwise, all reporters that could be found in"
                                    + " the configuration will be started.");

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
    public static Configuration forReporter(Configuration configuration, String reporterName) {
        return new DelegatingConfiguration(
                configuration, ConfigConstants.METRICS_REPORTER_PREFIX + reporterName + ".");
    }

    /** @deprecated use {@link MetricOptions#REPORTER_FACTORY_CLASS} instead. */
    @Deprecated
    public static final ConfigOption<String> REPORTER_CLASS =
            key("class")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The reporter class to use for the reporter named <name>.");

    @Documentation.SuffixOption(NAMED_REPORTER_CONFIG_PREFIX)
    @Documentation.Section(value = Documentation.Sections.METRIC_REPORTERS, position = 1)
    public static final ConfigOption<String> REPORTER_FACTORY_CLASS =
            key("factory.class")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The reporter factory class to use for the reporter named <name>.");

    @Documentation.SuffixOption(NAMED_REPORTER_CONFIG_PREFIX)
    @Documentation.Section(value = Documentation.Sections.METRIC_REPORTERS, position = 2)
    public static final ConfigOption<Duration> REPORTER_INTERVAL =
            key("interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(10))
                    .withDescription(
                            "The reporter interval to use for the reporter named <name>. Only applicable to push-based reporters.");

    @Documentation.SuffixOption(NAMED_REPORTER_CONFIG_PREFIX)
    @Documentation.Section(value = Documentation.Sections.METRIC_REPORTERS, position = 2)
    public static final ConfigOption<String> REPORTER_SCOPE_DELIMITER =
            key("scope.delimiter")
                    .stringType()
                    .defaultValue(".")
                    .withDescription(
                            "The delimiter used to assemble the metric identifier for the reporter named <name>.");

    @Documentation.SuffixOption(NAMED_REPORTER_CONFIG_PREFIX)
    @Documentation.Section(value = Documentation.Sections.METRIC_REPORTERS, position = 3)
    public static final ConfigOption<Map<String, String>> REPORTER_ADDITIONAL_VARIABLES =
            key("scope.variables.additional")
                    .mapType()
                    .defaultValue(Collections.emptyMap())
                    .withDescription(
                            "The map of additional variables that should be included for the reporter named <name>. Only applicable to tag-based reporters.");

    @Documentation.SuffixOption(NAMED_REPORTER_CONFIG_PREFIX)
    @Documentation.Section(value = Documentation.Sections.METRIC_REPORTERS, position = 3)
    public static final ConfigOption<String> REPORTER_EXCLUDED_VARIABLES =
            key("scope.variables.excludes")
                    .stringType()
                    .defaultValue(".")
                    .withDescription(
                            "The set of variables that should be excluded for the reporter named <name>. Only applicable to tag-based reporters.");

    @Documentation.SuffixOption(NAMED_REPORTER_CONFIG_PREFIX)
    @Documentation.Section(value = Documentation.Sections.METRIC_REPORTERS, position = 4)
    public static final ConfigOption<List<String>> REPORTER_INCLUDES =
            key("filter.includes")
                    .stringType()
                    .asList()
                    .defaultValues("*:*:*")
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The metrics that should be included for the reporter named <name>."
                                                    + " Filters are specified as a list, with each filter following this format:")
                                    .linebreak()
                                    .text("%s", code("<scope>[:<name>[,<name>][:<type>[,<type>]]]"))
                                    .linebreak()
                                    .text(
                                            "A metric matches a filter if the scope pattern and at least one of the name patterns and at least one of the types match.")
                                    .linebreak()
                                    .list(
                                            text(
                                                    "scope: Filters based on the logical scope.%s"
                                                            + "Specified as a pattern where %s matches any sequence of characters and %s separates scope components.%s%s"
                                                            + "For example:%s"
                                                            + " \"%s\" matches any job-related metrics on the JobManager,%s"
                                                            + " \"%s\" matches all job-related metrics and%s"
                                                            + " \"%s\" matches all metrics below the job-level (i.e., task/operator metrics etc.).%s%s",
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
                                                    "name: Filters based on the metric name.%s"
                                                            + "Specified as a comma-separate list of patterns where %s matches any sequence of characters.%s%s"
                                                            + "For example, \"%s\" matches any metrics where the name contains %s.%s%s",
                                                    linebreak(),
                                                    code("*"),
                                                    linebreak(),
                                                    linebreak(),
                                                    code("*Records*,*Bytes*"),
                                                    code("\"Records\" or \"Bytes\""),
                                                    linebreak(),
                                                    linebreak()),
                                            text(
                                                    "type: Filters based on the metric type. Specified as a comma-separated list of metric types: %s",
                                                    code("[counter, meter, gauge, histogram]")))
                                    .text("Examples:")
                                    .list(
                                            text(
                                                    "\"%s\" Matches metrics like %s.",
                                                    code("*:numRecords*"), code("numRecordsIn")),
                                            text(
                                                    "\"%s\" Matches metrics like %s on the operator level.",
                                                    code("*.job.task.operator:numRecords*"),
                                                    code("numRecordsIn")),
                                            text(
                                                    "\"%s\" Matches meter metrics like %s on the operator level.",
                                                    code("*.job.task.operator:numRecords*:meter"),
                                                    code("numRecordsInPerSecond")),
                                            text(
                                                    "\"%s\" Matches all counter/meter metrics like or %s.",
                                                    code("*:numRecords*,numBytes*:counter,meter"),
                                                    code("numRecordsInPerSecond"),
                                                    code("numBytesOut")))
                                    .build());

    @Documentation.SuffixOption(NAMED_REPORTER_CONFIG_PREFIX)
    @Documentation.Section(value = Documentation.Sections.METRIC_REPORTERS, position = 5)
    public static final ConfigOption<List<String>> REPORTER_EXCLUDES =
            key("filter.excludes")
                    .stringType()
                    .asList()
                    .defaultValues()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The metrics that should be excluded for the reporter named <name>. The format is identical to %s",
                                            code(REPORTER_INCLUDES.key()))
                                    .linebreak()
                                    .build());

    @Documentation.SuffixOption(NAMED_REPORTER_CONFIG_PREFIX)
    @Documentation.Section(value = Documentation.Sections.METRIC_REPORTERS, position = 6)
    public static final ConfigOption<String> REPORTER_CONFIG_PARAMETER =
            key("<parameter>")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Configures the parameter <parameter> for the reporter named <name>.");

    /** The delimiter used to assemble the metric identifier. */
    public static final ConfigOption<String> SCOPE_DELIMITER =
            key("metrics.scope.delimiter")
                    .stringType()
                    .defaultValue(".")
                    .withDescription("Delimiter used to assemble the metric identifier.");

    /** The scope format string that is applied to all metrics scoped to a JobManager. */
    public static final ConfigOption<String> SCOPE_NAMING_JM =
            key("metrics.scope.jm")
                    .stringType()
                    .defaultValue("<host>.jobmanager")
                    .withDescription(
                            "Defines the scope format string that is applied to all metrics scoped to a JobManager. Only effective when a identifier-based reporter is configured.");

    /** The scope format string that is applied to all metrics scoped to a TaskManager. */
    public static final ConfigOption<String> SCOPE_NAMING_TM =
            key("metrics.scope.tm")
                    .stringType()
                    .defaultValue("<host>.taskmanager.<tm_id>")
                    .withDescription(
                            "Defines the scope format string that is applied to all metrics scoped to a TaskManager. Only effective when a identifier-based reporter is configured");

    /** The scope format string that is applied to all metrics scoped to a job on a JobManager. */
    public static final ConfigOption<String> SCOPE_NAMING_JM_JOB =
            key("metrics.scope.jm-job")
                    .stringType()
                    .defaultValue("<host>.jobmanager.<job_name>")
                    .withDeprecatedKeys("metrics.scope.jm.job")
                    .withDescription(
                            "Defines the scope format string that is applied to all metrics scoped to a job on a JobManager. Only effective when a identifier-based reporter is configured");

    /**
     * The scope format string that is applied to all metrics scoped to the components running on a
     * JobManager of an operator.
     */
    public static final ConfigOption<String> SCOPE_NAMING_JM_OPERATOR =
            key("metrics.scope.jm-operator")
                    .stringType()
                    .defaultValue("<host>.jobmanager.<job_name>.<operator_name>")
                    .withDescription(
                            "Defines the scope format string that is applied to all metrics scoped to the components running on a JobManager of an Operator, like OperatorCoordinator.");

    /** The scope format string that is applied to all metrics scoped to a job on a TaskManager. */
    public static final ConfigOption<String> SCOPE_NAMING_TM_JOB =
            key("metrics.scope.tm-job")
                    .stringType()
                    .defaultValue("<host>.taskmanager.<tm_id>.<job_name>")
                    .withDeprecatedKeys("metrics.scope.tm.job")
                    .withDescription(
                            "Defines the scope format string that is applied to all metrics scoped to a job on a TaskManager. Only effective when a identifier-based reporter is configured");

    /** The scope format string that is applied to all metrics scoped to a task. */
    public static final ConfigOption<String> SCOPE_NAMING_TASK =
            key("metrics.scope.task")
                    .stringType()
                    .defaultValue(
                            "<host>.taskmanager.<tm_id>.<job_name>.<task_name>.<subtask_index>")
                    .withDescription(
                            "Defines the scope format string that is applied to all metrics scoped to a task. Only effective when a identifier-based reporter is configured");

    /** The scope format string that is applied to all metrics scoped to an operator. */
    public static final ConfigOption<String> SCOPE_NAMING_OPERATOR =
            key("metrics.scope.operator")
                    .stringType()
                    .defaultValue(
                            "<host>.taskmanager.<tm_id>.<job_name>.<operator_name>.<subtask_index>")
                    .withDescription(
                            "Defines the scope format string that is applied to all metrics scoped to an operator. Only effective when a identifier-based reporter is configured");

    public static final ConfigOption<Long> LATENCY_INTERVAL =
            key("metrics.latency.interval")
                    .longType()
                    .defaultValue(0L)
                    .withDescription(
                            "Defines the interval at which latency tracking marks are emitted from the sources."
                                    + " Disables latency tracking if set to 0 or a negative value. Enabling this feature can significantly"
                                    + " impact the performance of the cluster.");

    public static final ConfigOption<String> LATENCY_SOURCE_GRANULARITY =
            key("metrics.latency.granularity")
                    .stringType()
                    .defaultValue("operator")
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Defines the granularity of latency metrics. Accepted values are:")
                                    .list(
                                            text(
                                                    "single - Track latency without differentiating between sources and subtasks."),
                                            text(
                                                    "operator - Track latency while differentiating between sources, but not subtasks."),
                                            text(
                                                    "subtask - Track latency while differentiating between sources and subtasks."))
                                    .build());

    /** The number of measured latencies to maintain at each operator. */
    public static final ConfigOption<Integer> LATENCY_HISTORY_SIZE =
            key("metrics.latency.history-size")
                    .intType()
                    .defaultValue(128)
                    .withDescription(
                            "Defines the number of measured latencies to maintain at each operator.");

    /**
     * Whether Flink should report system resource metrics such as machine's CPU, memory or network
     * usage.
     */
    public static final ConfigOption<Boolean> SYSTEM_RESOURCE_METRICS =
            key("metrics.system-resource")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Flag indicating whether Flink should report system resource metrics such as machine's CPU,"
                                    + " memory or network usage.");
    /**
     * Interval between probing of system resource metrics specified in milliseconds. Has an effect
     * only when {@link #SYSTEM_RESOURCE_METRICS} is enabled.
     */
    public static final ConfigOption<Long> SYSTEM_RESOURCE_METRICS_PROBING_INTERVAL =
            key("metrics.system-resource-probing-interval")
                    .longType()
                    .defaultValue(5000L)
                    .withDescription(
                            "Interval between probing of system resource metrics specified in milliseconds. Has an effect"
                                    + " only when '"
                                    + SYSTEM_RESOURCE_METRICS.key()
                                    + "' is enabled.");

    /**
     * The default network port range for Flink's internal metric query service. The {@code "0"}
     * means that Flink searches for a free port.
     */
    @Documentation.Section(Documentation.Sections.COMMON_HOST_PORT)
    public static final ConfigOption<String> QUERY_SERVICE_PORT =
            key("metrics.internal.query-service.port")
                    .stringType()
                    .defaultValue("0")
                    .withDescription(
                            "The port range used for Flink's internal metric query service. Accepts a list of ports "
                                    + "(“50100,50101”), ranges(“50100-50200”) or a combination of both. It is recommended to set a range of "
                                    + "ports to avoid collisions when multiple Flink components are running on the same machine. Per default "
                                    + "Flink will pick a random port.");

    /**
     * The thread priority for Flink's internal metric query service. The {@code 1} means the min
     * priority and the {@code 10} means the max priority.
     */
    public static final ConfigOption<Integer> QUERY_SERVICE_THREAD_PRIORITY =
            key("metrics.internal.query-service.thread-priority")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            "The thread priority used for Flink's internal metric query service. The thread is created"
                                    + " by Pekko's thread pool executor. "
                                    + "The range of the priority is from 1 (MIN_PRIORITY) to 10 (MAX_PRIORITY). "
                                    + "Warning, increasing this value may bring the main Flink components down.");
    /**
     * The config parameter defining the update interval for the metric fetcher used by the web UI
     * in milliseconds.
     */
    public static final ConfigOption<Long> METRIC_FETCHER_UPDATE_INTERVAL =
            key("metrics.fetcher.update-interval")
                    .longType()
                    .defaultValue(10000L)
                    .withDescription(
                            "Update interval for the metric fetcher used by the web UI in milliseconds. Decrease this value for "
                                    + "faster updating metrics. Increase this value if the metric fetcher causes too much load. Setting this value to 0 "
                                    + "disables the metric fetching completely.");

    /** Controls which job status metrics will be exposed. */
    public static final ConfigOption<List<JobStatusMetrics>> JOB_STATUS_METRICS =
            key("metrics.job.status.enable")
                    .enumType(JobStatusMetrics.class)
                    .asList()
                    .defaultValues(JobStatusMetrics.CURRENT_TIME)
                    .withDescription(
                            "The selection of job status metrics that should be reported.");

    /** Enum describing the different kinds of job status metrics. */
    public enum JobStatusMetrics implements DescribedEnum {
        STATE(
                "For a given state, return 1 if the job is currently in that state, otherwise return 0."),
        CURRENT_TIME(
                "For a given state, if the job is currently in that state, return the time since the job transitioned into that state, otherwise return 0."),
        TOTAL_TIME(
                "For a given state, return how much time the job has spent in that state in total."),
        ;

        private final String description;

        JobStatusMetrics(String description) {
            this.description = description;
        }

        @Override
        public InlineElement getDescription() {
            return TextElement.text(description);
        }
    }

    /** Describes which job status metrics have been enabled. */
    public static final class JobStatusMetricsSettings {

        private final boolean stateMetricsEnabled;
        private final boolean currentTimeMetricsEnabled;
        private final boolean totalTimeMetricsEnabled;

        private JobStatusMetricsSettings(
                boolean stateMetricsEnabled,
                boolean currentTimeMetricsEnabled,
                boolean totalTimeMetricsEnabled) {
            this.stateMetricsEnabled = stateMetricsEnabled;
            this.currentTimeMetricsEnabled = currentTimeMetricsEnabled;
            this.totalTimeMetricsEnabled = totalTimeMetricsEnabled;
        }

        public boolean isStateMetricsEnabled() {
            return stateMetricsEnabled;
        }

        public boolean isCurrentTimeMetricsEnabled() {
            return currentTimeMetricsEnabled;
        }

        public boolean isTotalTimeMetricsEnabled() {
            return totalTimeMetricsEnabled;
        }

        public static JobStatusMetricsSettings fromConfiguration(Configuration configuration) {
            final List<JobStatusMetrics> jobStatusMetrics = configuration.get(JOB_STATUS_METRICS);
            boolean stateMetricsEnabled = false;
            boolean currentTimeMetricsEnabled = false;
            boolean totalTimeMetricsEnabled = false;

            for (JobStatusMetrics jobStatusMetric : jobStatusMetrics) {
                switch (jobStatusMetric) {
                    case STATE:
                        stateMetricsEnabled = true;
                        break;
                    case CURRENT_TIME:
                        currentTimeMetricsEnabled = true;
                        break;
                    case TOTAL_TIME:
                        totalTimeMetricsEnabled = true;
                        break;
                }
            }

            return new JobStatusMetricsSettings(
                    stateMetricsEnabled, currentTimeMetricsEnabled, totalTimeMetricsEnabled);
        }
    }

    private MetricOptions() {}
}
