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

import static org.apache.flink.configuration.ConfigOptions.key;

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

	/** The delimiter used to assemble the metric identifier. */
	public static final ConfigOption<String> SCOPE_DELIMITER =
		key("metrics.scope.delimiter")
			.defaultValue(".");

	/** The scope format string that is applied to all metrics scoped to a JobManager. */
	public static final ConfigOption<String> SCOPE_NAMING_JM =
		key("metrics.scope.jm")
			.defaultValue("<host>.jobmanager");

	/** The scope format string that is applied to all metrics scoped to a TaskManager. */
	public static final ConfigOption<String> SCOPE_NAMING_TM =
		key("metrics.scope.tm")
			.defaultValue("<host>.taskmanager.<tm_id>");

	/** The scope format string that is applied to all metrics scoped to a job on a JobManager. */
	public static final ConfigOption<String> SCOPE_NAMING_JM_JOB =
		key("metrics.scope.jm.job")
			.defaultValue("<host>.jobmanager.<job_name>");

	/** The scope format string that is applied to all metrics scoped to a job on a TaskManager. */
	public static final ConfigOption<String> SCOPE_NAMING_TM_JOB =
		key("metrics.scope.tm.job")
			.defaultValue("<host>.taskmanager.<tm_id>.<job_name>");

	/** The scope format string that is applied to all metrics scoped to a task. */
	public static final ConfigOption<String> SCOPE_NAMING_TASK =
		key("metrics.scope.task")
			.defaultValue("<host>.taskmanager.<tm_id>.<job_name>.<task_name>.<subtask_index>");

	/** The scope format string that is applied to all metrics scoped to an operator. */
	public static final ConfigOption<String> SCOPE_NAMING_OPERATOR =
		key("metrics.scope.operator")
			.defaultValue("<host>.taskmanager.<tm_id>.<job_name>.<operator_name>.<subtask_index>");

	/** The number of measured latencies to maintain at each operator */
	public static final ConfigOption<Integer> LATENCY_HISTORY_SIZE =
		key("metrics.latency.history-size")
			.defaultValue(128);

	private MetricOptions() {
	}
}
