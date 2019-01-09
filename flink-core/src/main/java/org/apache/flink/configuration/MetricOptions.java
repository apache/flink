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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.description.Description;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.configuration.description.TextElement.text;

/**
 * Configuration options for metrics and metric reporters.
 */
@PublicEvolving
public class MetricOptions {

	/**
	 * An optional list of reporter names. If configured, only reporters whose name matches any of the names in the list
	 * will be started. Otherwise, all reporters that could be found in the configuration will be started.
	 *
	 * <p>Example:
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
			.noDefaultValue();

	public static final ConfigOption<String> REPORTER_CLASS =
		key("metrics.reporter.<name>.class")
			.noDefaultValue()
			.withDescription("The reporter class to use for the reporter named <name>.");

	public static final ConfigOption<String> REPORTER_INTERVAL =
		key("metrics.reporter.<name>.interval")
			.noDefaultValue()
			.withDescription("The reporter interval to use for the reporter named <name>.");

	public static final ConfigOption<String> REPORTER_CONFIG_PARAMETER =
		key("metrics.reporter.<name>.<parameter>")
			.noDefaultValue()
			.withDescription("Configures the parameter <parameter> for the reporter named <name>.");


	/** The delimiter used to assemble the metric identifier. */
	public static final ConfigOption<String> SCOPE_DELIMITER =
		key("metrics.scope.delimiter")
			.defaultValue(".");

	/** The scope format string that is applied to all metrics scoped to a JobManager. */
	public static final ConfigOption<String> SCOPE_NAMING_JM =
		key("metrics.scope.jm")
			.defaultValue("<host>.jobmanager")
			.withDescription("Defines the scope format string that is applied to all metrics scoped to a JobManager.");

	/** The scope format string that is applied to all metrics scoped to a TaskManager. */
	public static final ConfigOption<String> SCOPE_NAMING_TM =
		key("metrics.scope.tm")
			.defaultValue("<host>.taskmanager.<tm_id>")
			.withDescription("Defines the scope format string that is applied to all metrics scoped to a TaskManager.");

	/** The scope format string that is applied to all metrics scoped to a job on a JobManager. */
	public static final ConfigOption<String> SCOPE_NAMING_JM_JOB =
		key("metrics.scope.jm.job")
			.defaultValue("<host>.jobmanager.<job_name>")
			.withDescription("Defines the scope format string that is applied to all metrics scoped to a job on a JobManager.");

	/** The scope format string that is applied to all metrics scoped to a job on a TaskManager. */
	public static final ConfigOption<String> SCOPE_NAMING_TM_JOB =
		key("metrics.scope.tm.job")
			.defaultValue("<host>.taskmanager.<tm_id>.<job_name>")
			.withDescription("Defines the scope format string that is applied to all metrics scoped to a job on a TaskManager.");

	/** The scope format string that is applied to all metrics scoped to a task. */
	public static final ConfigOption<String> SCOPE_NAMING_TASK =
		key("metrics.scope.task")
			.defaultValue("<host>.taskmanager.<tm_id>.<job_name>.<task_name>.<subtask_index>")
			.withDescription("Defines the scope format string that is applied to all metrics scoped to a task.");

	/** The scope format string that is applied to all metrics scoped to an operator. */
	public static final ConfigOption<String> SCOPE_NAMING_OPERATOR =
		key("metrics.scope.operator")
			.defaultValue("<host>.taskmanager.<tm_id>.<job_name>.<operator_name>.<subtask_index>")
			.withDescription("Defines the scope format string that is applied to all metrics scoped to an operator.");

	public static final ConfigOption<Long> LATENCY_INTERVAL =
		key("metrics.latency.interval")
			.defaultValue(0L)
			.withDescription("Defines the interval at which latency tracking marks are emitted from the sources." +
				" Disables latency tracking if set to 0 or a negative value. Enabling this feature can significantly" +
				" impact the performance of the cluster.");

	public static final ConfigOption<String> LATENCY_SOURCE_GRANULARITY =
		key("metrics.latency.granularity")
			.defaultValue("operator")
			.withDescription(Description.builder()
				.text("Defines the granularity of latency metrics. Accepted values are:")
				.list(
					text("single - Track latency without differentiating between sources and subtasks."),
					text("operator - Track latency while differentiating between sources, but not subtasks."),
					text("subtask - Track latency while differentiating between sources and subtasks."))
				.build());

	/** The number of measured latencies to maintain at each operator. */
	public static final ConfigOption<Integer> LATENCY_HISTORY_SIZE =
		key("metrics.latency.history-size")
			.defaultValue(128)
			.withDescription("Defines the number of measured latencies to maintain at each operator.");

	/**
	 * Whether Flink should report system resource metrics such as machine's CPU, memory or network usage.
	 */
	public static final ConfigOption<Boolean> SYSTEM_RESOURCE_METRICS =
		key("metrics.system-resource")
			.defaultValue(false);
	/**
	 * Interval between probing of system resource metrics specified in milliseconds. Has an effect only when
	 * {@link #SYSTEM_RESOURCE_METRICS} is enabled.
	 */
	public static final ConfigOption<Long> SYSTEM_RESOURCE_METRICS_PROBING_INTERVAL =
		key("metrics.system-resource-probing-interval")
			.defaultValue(5000L);

	/**
	 * The default network port range for Flink's internal metric query service. The {@code "0"} means that
	 * Flink searches for a free port.
	 */
	public static final ConfigOption<String> QUERY_SERVICE_PORT =
		key("metrics.internal.query-service.port")
		.defaultValue("0")
		.withDescription("The port range used for Flink's internal metric query service. Accepts a list of ports " +
			"(“50100,50101”), ranges(“50100-50200”) or a combination of both. It is recommended to set a range of " +
			"ports to avoid collisions when multiple Flink components are running on the same machine. Per default " +
			"Flink will pick a random port.");

	/**
	 * The thread priority for Flink's internal metric query service. The {@code 1} means the min priority and the
	 * {@code 10} means the max priority.
	 */
	public static final ConfigOption<Integer> QUERY_SERVICE_THREAD_PRIORITY =
		key("metrics.internal.query-service.thread-priority")
		.defaultValue(1)
		.withDescription("The thread priority used for Flink's internal metric query service. The thread is created" +
			" by Akka's thread pool executor. " +
			"The range of the priority is from 1 (MIN_PRIORITY) to 10 (MAX_PRIORITY). " +
			"Warning, increasing this value may bring the main Flink components down.");

	private MetricOptions() {
	}
}
