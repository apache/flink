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

package org.apache.flink.connectors.test.common.environment;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * Pre-defined configurations for the test environment.
 */
public class TestEnvironmentConfigs {

	public static final ConfigOption<String> RECORD_FILE_PATH_FOR_JOB =
		ConfigOptions.key("controllable.source.record.file.path.job")
			.stringType()
			.noDefaultValue()
			.withDescription("File path of the record file for ControllableSource in the Flink job. "
					+ "For FlinkContainer, this is usually a file in the workspace INSIDE the container; "
					+ "For other local environments, this should be a local path accessible by Flink. ");

	public static final ConfigOption<String> RECORD_FILE_PATH_FOR_VALIDATION =
		ConfigOptions.key("controllable.source.record.file.path.validation")
			.stringType()
			.noDefaultValue()
			.withDescription("File path of the record file framework validation. "
					+ "For FlinkContainer, this is usually the record file in the workspace OUTSIDE the container; "
					+ "For other local environments, this should be a local path accessible by framework. ");

	public static final ConfigOption<String> OUTPUT_FILE_PATH_FOR_JOB =
		ConfigOptions.key("output.file.path.job")
			.stringType()
			.noDefaultValue()
			.withDescription("File path of the record file for record sinking in the Flink job. "
					+ "For FlinkContainer, this is usually a file in the workspace INSIDE the container; "
					+ "For other local environments, this should be a local path accessible by Flink. ");

	public static final ConfigOption<String> OUTPUT_FILE_PATH_FOR_VALIDATION =
		ConfigOptions.key("output.file.path.validation")
			.stringType()
			.noDefaultValue()
			.withDescription("File path of the record file for record sinking in the Flink job. "
					+ "For FlinkContainer, this is usually the record file in the workspace OUTSIDE the container; "
					+ "For other local environments, this should be a local path accessible by framework. ");

	public static final ConfigOption<String> RMI_HOST =
			ConfigOptions.key("controllable.source.rmi.host")
					.stringType()
					.noDefaultValue()
					.withDescription("Host of Java RMI for ControllableSource.");

	public static final ConfigOption<String> RMI_POTENTIAL_PORTS =
		ConfigOptions.key("controllable.source.rmi.potential.ports")
			.stringType()
			.noDefaultValue()
			.withDescription("Potential port numbers of Java RMI for ControllableSource. These "
					+ "port numbers are wrapped as a comma-separated string, such as '15213,18213,18600'");
}
