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

package org.apache.flink.metrics.prometheus;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * Config options for the {@link PrometheusPushGatewayReporter}.
 */
public class PrometheusPushGatewayReporterOptions {

	public static final ConfigOption<String> HOST = ConfigOptions
		.key("host")
		.noDefaultValue()
		.withDescription("The PushGateway server host.");

	public static final ConfigOption<Integer> PORT = ConfigOptions
		.key("port")
		.defaultValue(-1)
		.withDescription("The PushGateway server port.");

	public static final ConfigOption<String> JOB_NAME = ConfigOptions
		.key("jobName")
		.defaultValue("")
		.withDescription("The job name under which metrics will be pushed");

	public static final ConfigOption<Boolean> RANDOM_JOB_NAME_SUFFIX = ConfigOptions
		.key("randomJobNameSuffix")
		.defaultValue(true)
		.withDescription("Specifies whether a random suffix should be appended to the job name.");

	public static final ConfigOption<Boolean> DELETE_ON_SHUTDOWN = ConfigOptions
		.key("deleteOnShutdown")
		.defaultValue(true)
		.withDescription("Specifies whether to delete metrics from the PushGateway on shutdown.");
}
