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

/**
 * The {@link ConfigOption configuration options} relevant for all Executors.
 */
@PublicEvolving
public class DeploymentOptions {

	/**
	 * A flag indicating if we want the job to be executed in detached or attached mode.
	 */
	public static final ConfigOption<Boolean> ATTACHED =
			key("execution.attached")
					.defaultValue(false)
					.withDescription("A flag indicating if we want the job to be executed in detached mode." +
							"If set to true, the client will wait for the result of the execution of the pipeline.");

	/**
	 * If the job is submitted in attached mode and this is set to true, then Flink will try to perform a best-effort
	 * cluster shutdown when the CLI is terminated abruptly.
	 */
	public static final ConfigOption<Boolean> SHUTDOWN_IF_ATTACHED =
			key("execution.shutdown-on-attached-exit")
					.defaultValue(false)
					.withDescription("If the job is submitted in attached mode, perform a best-effort cluster shutdown " +
							"when the CLI is terminated abruptly, e.g., in response to a user interrupt, such as typing Ctrl + C.");

	/**
	 * The desired parallelism for the job. By default, this is set to 1.
	 */
	public static final ConfigOption<Integer> PARALLELISM =
			key("execution.parallelism")
					.defaultValue(1)
					.withDescription("The desired parallelism for the job. By default, this is set to 1.");
}
