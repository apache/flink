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

import java.util.List;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * The {@link ConfigOption configuration options} relevant for all Executors.
 */
@PublicEvolving
public class DeploymentOptions {

	public static final ConfigOption<String> TARGET =
			key("execution.target")
					.stringType()
					.noDefaultValue()
					.withDescription("The deployment target for the execution, e.g. \"local\" for local execution.");

	public static final ConfigOption<Boolean> ATTACHED =
			key("execution.attached")
					.booleanType()
					.defaultValue(false)
					.withDescription("Specifies if the pipeline is submitted in attached or detached mode.");

	public static final ConfigOption<Boolean> SHUTDOWN_IF_ATTACHED =
			key("execution.shutdown-on-attached-exit")
					.booleanType()
					.defaultValue(false)
					.withDescription("If the job is submitted in attached mode, perform a best-effort cluster shutdown " +
							"when the CLI is terminated abruptly, e.g., in response to a user interrupt, such as typing Ctrl + C.");

	public static final ConfigOption<List<String>> JOB_LISTENERS =
			key("execution.job-listeners")
					.stringType()
					.asList()
					.noDefaultValue()
					.withDescription("Custom JobListeners to be registered with the execution environment." +
							" The registered listeners cannot have constructors with arguments.");
}
