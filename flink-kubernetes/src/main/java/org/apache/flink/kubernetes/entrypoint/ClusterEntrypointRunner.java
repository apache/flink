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

package org.apache.flink.kubernetes.entrypoint;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.kubernetes.FlinkKubernetesOptions;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint;
import org.apache.flink.runtime.entrypoint.FlinkParseException;
import org.apache.flink.runtime.entrypoint.parser.CommandLineParser;
import org.apache.flink.runtime.entrypoint.parser.ParserResultFactory;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.SignalHandler;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.util.Properties;

import static org.apache.flink.kubernetes.FlinkKubernetesOptions.CLUSTERID_OPTION;
import static org.apache.flink.kubernetes.FlinkKubernetesOptions.IMAGE_OPTION;
import static org.apache.flink.runtime.entrypoint.parser.CommandLineOptions.CONFIG_DIR_OPTION;
import static org.apache.flink.runtime.entrypoint.parser.CommandLineOptions.DYNAMIC_PROPERTY_OPTION;

/**
 * Base class for the run {@link ClusterEntrypoint}.
 * */
public abstract class ClusterEntrypointRunner implements ParserResultFactory<FlinkKubernetesOptions> {
	protected static final Logger LOG = LoggerFactory.getLogger(ClusterEntrypointRunner.class);

	protected abstract ClusterEntrypoint createClusterEntrypoint(FlinkKubernetesOptions options);

	@Override
	public Options getOptions() {
		final Options options = new Options();
		options.addOption(CONFIG_DIR_OPTION);
		options.addOption(DYNAMIC_PROPERTY_OPTION);
		options.addOption(IMAGE_OPTION);
		options.addOption(CLUSTERID_OPTION);
		return options;
	}

	@Override
	public FlinkKubernetesOptions createResult(@Nonnull CommandLine commandLine) {
		final String configDir = commandLine.getOptionValue(CONFIG_DIR_OPTION.getOpt());
		final Properties dynamicProperties = commandLine.getOptionProperties(DYNAMIC_PROPERTY_OPTION.getOpt());

		Configuration configuration = GlobalConfiguration
			.loadConfiguration(configDir, ConfigurationUtils.createConfiguration(dynamicProperties));

		FlinkKubernetesOptions flinkKubernetesOptions = new FlinkKubernetesOptions(configuration);

		final String imageName = commandLine.getOptionValue(FlinkKubernetesOptions.IMAGE_OPTION.getOpt());
		final String clusterId = commandLine.getOptionValue(FlinkKubernetesOptions.CLUSTERID_OPTION.getOpt());

		flinkKubernetesOptions.setClusterId(clusterId);
		flinkKubernetesOptions.setImageName(imageName);

		return flinkKubernetesOptions;
	}

	public void run(String[] args) {

		Class clazz = this.getClass();

		// startup checks and logging
		EnvironmentInformation.logEnvironmentInfo(LOG, clazz.getSimpleName(), args);
		SignalHandler.register(LOG);
		JvmShutdownSafeguard.installAsShutdownHook(LOG);

		final CommandLineParser<FlinkKubernetesOptions> commandLineParser = new CommandLineParser<>(this);

		try {
			FlinkKubernetesOptions options = commandLineParser.parse(args);
			ClusterEntrypoint entrypoint = this.createClusterEntrypoint(options);
			ClusterEntrypoint.runClusterEntrypoint(entrypoint);

		} catch (FlinkParseException e) {
			LOG.error("Could not parse command line arguments: {}.", args, e.getCause().getMessage());
			commandLineParser.printHelp(clazz.getSimpleName());
			System.exit(1);
		}
	}
}
