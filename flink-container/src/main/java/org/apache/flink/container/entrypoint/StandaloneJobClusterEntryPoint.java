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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint;
import org.apache.flink.runtime.entrypoint.JobClusterEntrypoint;
import org.apache.flink.runtime.entrypoint.component.DefaultDispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.entrypoint.component.DispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.entrypoint.parser.CommandLineParser;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.runtime.resourcemanager.StandaloneResourceManagerFactory;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.SignalHandler;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Optional;

import static java.util.Objects.requireNonNull;
import static org.apache.flink.runtime.util.ClusterEntrypointUtils.tryFindUserLibDirectory;

/**
 * {@link JobClusterEntrypoint} which is started with a job in a predefined
 * location.
 */
public final class StandaloneJobClusterEntryPoint extends JobClusterEntrypoint {

	public static final JobID ZERO_JOB_ID = new JobID(0, 0);

	@Nonnull
	private final JobID jobId;

	@Nonnull
	private final SavepointRestoreSettings savepointRestoreSettings;

	@Nonnull
	private final String[] programArguments;

	@Nullable
	private final String jobClassName;

	private StandaloneJobClusterEntryPoint(
			Configuration configuration,
			@Nonnull JobID jobId,
			@Nonnull SavepointRestoreSettings savepointRestoreSettings,
			@Nonnull String[] programArguments,
			@Nullable String jobClassName) {
		super(configuration);
		this.jobId = requireNonNull(jobId, "jobId");
		this.savepointRestoreSettings = requireNonNull(savepointRestoreSettings, "savepointRestoreSettings");
		this.programArguments = requireNonNull(programArguments, "programArguments");
		this.jobClassName = jobClassName;
	}

	@Override
	protected DispatcherResourceManagerComponentFactory createDispatcherResourceManagerComponentFactory(Configuration configuration) throws IOException {
		final ClassPathJobGraphRetriever.Builder classPathJobGraphRetrieverBuilder = ClassPathJobGraphRetriever.newBuilder(jobId, savepointRestoreSettings, programArguments)
			.setJobClassName(jobClassName);
		tryFindUserLibDirectory().ifPresent(classPathJobGraphRetrieverBuilder::setUserLibDirectory);

		return DefaultDispatcherResourceManagerComponentFactory.createJobComponentFactory(
			StandaloneResourceManagerFactory.INSTANCE,
			classPathJobGraphRetrieverBuilder.build());
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
		} catch (Exception e) {
			LOG.error("Could not parse command line arguments {}.", args, e);
			commandLineParser.printHelp(StandaloneJobClusterEntryPoint.class.getSimpleName());
			System.exit(1);
		}

		Configuration configuration = loadConfiguration(clusterConfiguration);
		setDefaultExecutionModeIfNotConfigured(configuration);

		StandaloneJobClusterEntryPoint entrypoint = new StandaloneJobClusterEntryPoint(
			configuration,
			resolveJobIdForCluster(Optional.ofNullable(clusterConfiguration.getJobId()), configuration),
			clusterConfiguration.getSavepointRestoreSettings(),
			clusterConfiguration.getArgs(),
			clusterConfiguration.getJobClassName());

		ClusterEntrypoint.runClusterEntrypoint(entrypoint);
	}

	@VisibleForTesting
	@Nonnull
	static JobID resolveJobIdForCluster(Optional<JobID> optionalJobID, Configuration configuration) {
		return optionalJobID.orElseGet(() -> createJobIdForCluster(configuration));
	}

	@Nonnull
	private static JobID createJobIdForCluster(Configuration globalConfiguration) {
		if (HighAvailabilityMode.isHighAvailabilityModeActivated(globalConfiguration)) {
			return ZERO_JOB_ID;
		} else {
			return JobID.generate();
		}
	}

	@VisibleForTesting
	static void setDefaultExecutionModeIfNotConfigured(Configuration configuration) {
		if (isNoExecutionModeConfigured(configuration)) {
			// In contrast to other places, the default for standalone job clusters is ExecutionMode.DETACHED
			configuration.setString(ClusterEntrypoint.EXECUTION_MODE, ExecutionMode.DETACHED.toString());
		}
	}

	private static boolean isNoExecutionModeConfigured(Configuration configuration) {
		return configuration.getString(ClusterEntrypoint.EXECUTION_MODE, null) == null;
	}
}
