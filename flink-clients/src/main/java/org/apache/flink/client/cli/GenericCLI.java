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
import org.apache.flink.configuration.DeploymentOptionsInternal;
import org.apache.flink.configuration.UnmodifiableConfiguration;
import org.apache.flink.core.execution.DefaultExecutorServiceLoader;
import org.apache.flink.core.execution.PipelineExecutor;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A generic implementation of the {@link CustomCommandLine} that only expects
 * the execution.target parameter to be explicitly specified and simply forwards the
 * rest of the options specified with -D to the corresponding {@link PipelineExecutor}
 * for further parsing.
 */
@Internal
public class GenericCLI implements CustomCommandLine {

	private static final String ID = "Generic CLI";

	private final Option executorOption = new Option("e", "executor", true,
			"DEPRECATED: Please use the -t option instead which is also available with the \"Application Mode\".\n" +
					"The name of the executor to be used for executing the given job, which is equivalent " +
					"to the \"" + DeploymentOptions.TARGET.key() + "\" config option. The " +
					"currently available executors are: " + getExecutorFactoryNames() + ".");

	private final Option targetOption = new Option("t", "target", true,
			"The deployment target for the given application, which is equivalent " +
					"to the \"" + DeploymentOptions.TARGET.key() + "\" config option. The " +
					"currently available targets are: " + getExecutorFactoryNames() +
					", \"yarn-application\" and \"kubernetes-application\".");

	/**
	 * Dynamic properties allow the user to specify additional configuration values with -D, such as
	 * <tt> -Dfs.overwrite-files=true  -Dtaskmanager.memory.network.min=536346624</tt>.
	 */
	private final Option dynamicProperties = Option.builder("D")
			.argName("property=value")
			.numberOfArgs(2)
			.valueSeparator('=')
			.desc("Generic configuration options for execution/deployment and for the configured " +
					"executor. The available options can be found at " +
					"https://ci.apache.org/projects/flink/flink-docs-stable/ops/config.html")
			.build();

	private final Configuration configuration;

	private final String configurationDir;

	public GenericCLI(final Configuration configuration, final String configDir) {
		this.configuration = new UnmodifiableConfiguration(checkNotNull(configuration));
		this.configurationDir =  checkNotNull(configDir);
	}

	@Override
	public boolean isActive(CommandLine commandLine) {
		return configuration.getOptional(DeploymentOptions.TARGET).isPresent()
				|| commandLine.hasOption(executorOption.getOpt())
				|| commandLine.hasOption(targetOption.getOpt());
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
		baseOptions.addOption(targetOption);
		baseOptions.addOption(dynamicProperties);
	}

	@Override
	public Configuration toConfiguration(final CommandLine commandLine) {
		final Configuration resultConfiguration = new Configuration();

		final String executorName = commandLine.getOptionValue(executorOption.getOpt());
		if (executorName != null) {
			resultConfiguration.setString(DeploymentOptions.TARGET, executorName);
		}

		final String targetName = commandLine.getOptionValue(targetOption.getOpt());
		if (targetName != null) {
			resultConfiguration.setString(DeploymentOptions.TARGET, targetName);
		}

		encodeDynamicProperties(commandLine, resultConfiguration);
		resultConfiguration.set(DeploymentOptionsInternal.CONF_DIR, configurationDir);

		return resultConfiguration;
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

	private static String getExecutorFactoryNames() {
		return new DefaultExecutorServiceLoader().getExecutorNames()
				.map(name -> String.format("\"%s\"", name))
				.collect(Collectors.joining(", "));
	}
}
