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

package org.apache.flink.container.entrypoint;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint;
import org.apache.flink.runtime.entrypoint.FlinkParseException;
import org.apache.flink.runtime.entrypoint.JobClusterEntrypoint;
import org.apache.flink.runtime.entrypoint.component.DispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.entrypoint.component.JobDispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.entrypoint.parser.CommandLineParser;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.resourcemanager.StandaloneResourceManagerFactory;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.SignalHandler;

import javax.annotation.Nonnull;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link JobClusterEntrypoint} which is started with a job in a predefined
 * location.
 */
public final class StandaloneJobClusterEntryPoint extends JobClusterEntrypoint {

	private final String[] programArguments;

	@Nonnull
	private final String jobClassName;

	@Nonnull
	private final SavepointRestoreSettings savepointRestoreSettings;

	StandaloneJobClusterEntryPoint(
			Configuration configuration,
			@Nonnull String jobClassName,
			@Nonnull SavepointRestoreSettings savepointRestoreSettings,
			@Nonnull String[] programArguments) {
		super(configuration);
		this.programArguments = checkNotNull(programArguments);
		this.jobClassName = checkNotNull(jobClassName);
		this.savepointRestoreSettings = savepointRestoreSettings;
	}

	@Override
	protected DispatcherResourceManagerComponentFactory<?> createDispatcherResourceManagerComponentFactory(Configuration configuration) {
		return new JobDispatcherResourceManagerComponentFactory(
			StandaloneResourceManagerFactory.INSTANCE,
			new ClassPathJobGraphRetriever(jobClassName, savepointRestoreSettings, programArguments));
	}

	public static void main(String[] args) {
		// startup checks and logging
		EnvironmentInformation.logEnvironmentInfo(LOG, StandaloneJobClusterEntryPoint.class.getSimpleName(), args);
		SignalHandler.register(LOG);
		JvmShutdownSafeguard.installAsShutdownHook(LOG);

		final CommandLineParser<StandaloneJobClusterConfiguration> commandLineParser = new CommandLineParser<>(new StandaloneJobClusterConfigurationParserFactory());
		StandaloneJobClusterConfiguration clusterConfiguration = null;

		try {
			clusterConfiguration = commandLineParser.parse(args);
		} catch (FlinkParseException e) {
			LOG.error("Could not parse command line arguments {}.", args, e);
			commandLineParser.printHelp(StandaloneJobClusterEntryPoint.class.getSimpleName());
			System.exit(1);
		}

		Configuration configuration = loadConfiguration(clusterConfiguration);

		configuration.setString(ClusterEntrypoint.EXECUTION_MODE, ExecutionMode.DETACHED.toString());

		StandaloneJobClusterEntryPoint entrypoint = new StandaloneJobClusterEntryPoint(
			configuration,
			clusterConfiguration.getJobClassName(),
			clusterConfiguration.getSavepointRestoreSettings(),
			clusterConfiguration.getArgs());

		ClusterEntrypoint.runClusterEntrypoint(entrypoint);
	}
}
