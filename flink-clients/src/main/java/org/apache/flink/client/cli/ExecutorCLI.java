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

package org.apache.flink.client.cli;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.UnmodifiableConfiguration;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A generic implementation of the {@link CustomCommandLine} that only expects
 * the execution.target parameter to be explicitly specified and simply forwards the
 * rest of the options specified with -D to the corresponding {@link org.apache.flink.core.execution.Executor}
 * for further parsing.
 */
@Internal
public class ExecutorCLI implements CustomCommandLine {

	private static final Logger LOG = LoggerFactory.getLogger(ExecutorCLI.class);

	private static final String ID = "Executor-CLI";

	private final Option executorOption = new Option("e", "executor", true,
			"The name of the executor to be used for executing the given job, e.g. \"local\"." +
					" This is equivalent to the \"" + DeploymentOptions.TARGET.key() + "\" config option.");

	/**
	 * Dynamic properties allow the user to specify additional configuration values with -D, such as
	 * <tt> -Dfs.overwrite-files=true  -Dtaskmanager.memory.shuffle.min=536346624</tt>.
	 */
	private final Option dynamicProperties = Option.builder("D")
			.argName("property=value")
			.numberOfArgs(2)
			.valueSeparator('=')
			.desc("use value for given property")
			.build();

	private final Configuration baseConfiguration;

	public ExecutorCLI(final Configuration configuration) {
		this.baseConfiguration = new UnmodifiableConfiguration(checkNotNull(configuration));
	}

	@Override
	public boolean isActive(CommandLine commandLine) {
		return baseConfiguration.getOptional(DeploymentOptions.TARGET).isPresent()
				|| commandLine.hasOption(executorOption.getOpt());
	}

	@Override
	public String getId() {
		return ID;
	}

	@Override
	public void addRunOptions(Options baseOptions) {
		// nothing to add here
	}

	@Override
	public void addGeneralOptions(Options baseOptions) {
		baseOptions.addOption(executorOption);
		baseOptions.addOption(dynamicProperties);
	}

	@Override
	public Configuration applyCommandLineOptionsToConfiguration(final CommandLine commandLine) {
		final Configuration effectiveConfiguration = new Configuration(baseConfiguration);

		final String executorName = commandLine.getOptionValue(executorOption.getOpt());
		if (executorName != null) {
			effectiveConfiguration.setString(DeploymentOptions.TARGET, executorName);
		}

		encodeDynamicProperties(commandLine, effectiveConfiguration);

		if (LOG.isDebugEnabled()) {
			LOG.debug("Effective Configuration: {}", effectiveConfiguration);
		}

		return effectiveConfiguration;
	}

	private void encodeDynamicProperties(final CommandLine commandLine, final Configuration effectiveConfiguration) {
		final Properties properties = commandLine.getOptionProperties(dynamicProperties.getOpt());
		properties.stringPropertyNames()
				.forEach(key -> {
					final String value = properties.getProperty(key);
					if (value != null) {
						effectiveConfiguration.setString(key, value);
					} else {
						effectiveConfiguration.setString(key, "true");
					}
				});
	}
}
