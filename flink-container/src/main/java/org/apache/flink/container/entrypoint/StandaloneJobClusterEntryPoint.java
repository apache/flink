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
import org.apache.flink.client.deployment.application.ApplicationDispatcherLeaderProcessFactoryFactory;
import org.apache.flink.client.deployment.application.executors.EmbeddedExecutor;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramRetriever;
import org.apache.flink.configuration.ConfigUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.PipelineOptionsInternal;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.dispatcher.ArchivedExecutionGraphStore;
import org.apache.flink.runtime.dispatcher.MemoryArchivedExecutionGraphStore;
import org.apache.flink.runtime.dispatcher.SessionDispatcherFactory;
import org.apache.flink.runtime.dispatcher.runner.DefaultDispatcherRunnerFactory;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint;
import org.apache.flink.runtime.entrypoint.JobClusterEntrypoint;
import org.apache.flink.runtime.entrypoint.component.DefaultDispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.entrypoint.component.DispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.entrypoint.parser.CommandLineParser;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.runtime.resourcemanager.StandaloneResourceManagerFactory;
import org.apache.flink.runtime.rest.JobRestEndpointFactory;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.SignalHandler;
import org.apache.flink.util.FlinkException;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.URL;
import java.util.Optional;

import static java.util.Objects.requireNonNull;
import static org.apache.flink.runtime.util.ClusterEntrypointUtils.tryFindUserLibDirectory;

/**
 * {@link JobClusterEntrypoint} which is started with a job in a predefined
 * location.
 */
public final class StandaloneJobClusterEntryPoint extends ClusterEntrypoint {

	public static final JobID ZERO_JOB_ID = new JobID(0, 0);

	private final PackagedProgram program;

	private StandaloneJobClusterEntryPoint(
			final Configuration configuration,
			final PackagedProgram program) {
		super(configuration);
		this.program = requireNonNull(program);
	}

	@Override
	protected DispatcherResourceManagerComponentFactory createDispatcherResourceManagerComponentFactory(Configuration configuration) {
		return new DefaultDispatcherResourceManagerComponentFactory(
				new DefaultDispatcherRunnerFactory(
						ApplicationDispatcherLeaderProcessFactoryFactory
								.create(configuration, SessionDispatcherFactory.INSTANCE, program)),
				StandaloneResourceManagerFactory.getInstance(),
				JobRestEndpointFactory.INSTANCE);
	}

	@Override
	protected ArchivedExecutionGraphStore createSerializableExecutionGraphStore(
			Configuration configuration,
			ScheduledExecutor scheduledExecutor) {
		return new MemoryArchivedExecutionGraphStore();
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

		PackagedProgram program = null;
		try {
			program = getPackagedProgram(clusterConfiguration);
		} catch (Exception e) {
			LOG.error("Could not create application program.", e);
			System.exit(1);
		}

		Configuration configuration = loadConfigurationFromClusterConfig(clusterConfiguration);
		configuration.set(DeploymentOptions.TARGET, EmbeddedExecutor.NAME);
		ConfigUtils.encodeCollectionToConfig(configuration, PipelineOptions.JARS, program.getJobJarAndDependencies(), URL::toString);
		ConfigUtils.encodeCollectionToConfig(configuration, PipelineOptions.CLASSPATHS, program.getClasspaths(), URL::toString);

		StandaloneJobClusterEntryPoint entrypoint = new StandaloneJobClusterEntryPoint(configuration, program);

		ClusterEntrypoint.runClusterEntrypoint(entrypoint);
	}

	@VisibleForTesting
	static Configuration loadConfigurationFromClusterConfig(StandaloneJobClusterConfiguration clusterConfiguration) {
		Configuration configuration = loadConfiguration(clusterConfiguration);
		setStaticJobId(clusterConfiguration, configuration);
		SavepointRestoreSettings.toConfiguration(clusterConfiguration.getSavepointRestoreSettings(), configuration);
		return configuration;
	}

	private static PackagedProgram getPackagedProgram(
			final StandaloneJobClusterConfiguration clusterConfiguration) throws IOException, FlinkException {
		final PackagedProgramRetriever programRetriever = getPackagedProgramRetriever(
				clusterConfiguration.getArgs(),
				clusterConfiguration.getJobClassName());
		return programRetriever.getPackagedProgram();
	}

	private static PackagedProgramRetriever getPackagedProgramRetriever(
			final String[] programArguments,
			@Nullable final String jobClassName) throws IOException {
		final ClassPathPackagedProgramRetriever.Builder retrieverBuilder =
				ClassPathPackagedProgramRetriever
						.newBuilder(programArguments)
						.setJobClassName(jobClassName);
		tryFindUserLibDirectory().ifPresent(retrieverBuilder::setUserLibDirectory);
		return retrieverBuilder.build();
	}

	private static void setStaticJobId(StandaloneJobClusterConfiguration clusterConfiguration, Configuration configuration) {
		final JobID jobId = resolveJobIdForCluster(Optional.ofNullable(clusterConfiguration.getJobId()), configuration);
		configuration.set(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID, jobId.toHexString());
	}

	@VisibleForTesting
	static JobID resolveJobIdForCluster(Optional<JobID> optionalJobID, Configuration configuration) {
		return optionalJobID.orElseGet(() -> createJobIdForCluster(configuration));
	}

	private static JobID createJobIdForCluster(Configuration globalConfiguration) {
		if (HighAvailabilityMode.isHighAvailabilityModeActivated(globalConfiguration)) {
			return ZERO_JOB_ID;
		} else {
			return JobID.generate();
		}
	}
}
