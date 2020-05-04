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

import org.apache.flink.annotation.Internal;
import org.apache.flink.client.deployment.application.ApplicationClusterEntryPoint;
import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.client.deployment.application.ClassPathPackagedProgramRetriever;
import org.apache.flink.client.deployment.application.executors.EmbeddedExecutor;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramRetriever;
import org.apache.flink.configuration.ConfigUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.SignalHandler;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.List;

import static org.apache.flink.runtime.util.ClusterEntrypointUtils.tryFindUserLibDirectory;

/**
 * An {@link ApplicationClusterEntryPoint} for Kubernetes.
 */
@Internal
public final class KubernetesApplicationClusterEntrypoint extends ApplicationClusterEntryPoint {

	private KubernetesApplicationClusterEntrypoint(
			final Configuration configuration,
			final PackagedProgram program) {
		super(configuration, program, KubernetesResourceManagerFactory.getInstance());
	}

	public static void main(final String[] args) {
		// startup checks and logging
		EnvironmentInformation.logEnvironmentInfo(LOG, KubernetesApplicationClusterEntrypoint.class.getSimpleName(), args);
		SignalHandler.register(LOG);
		JvmShutdownSafeguard.installAsShutdownHook(LOG);

		final Configuration configuration = KubernetesEntrypointUtils.loadConfiguration();

		PackagedProgram program = null;
		try {
			program = getPackagedProgram(configuration);
		} catch (Exception e) {
			LOG.error("Could not create application program.", e);
			System.exit(1);
		}

		configuration.set(DeploymentOptions.TARGET, EmbeddedExecutor.NAME);
		ConfigUtils.encodeCollectionToConfig(configuration, PipelineOptions.JARS, program.getJobJarAndDependencies(), URL::toString);
		ConfigUtils.encodeCollectionToConfig(configuration, PipelineOptions.CLASSPATHS, program.getClasspaths(), URL::toString);

		final KubernetesApplicationClusterEntrypoint kubernetesApplicationClusterEntrypoint =
			new KubernetesApplicationClusterEntrypoint(configuration, program);

		ClusterEntrypoint.runClusterEntrypoint(kubernetesApplicationClusterEntrypoint);
	}

	private static PackagedProgram getPackagedProgram(final Configuration configuration) throws IOException, FlinkException {

		final ApplicationConfiguration applicationConfiguration =
			ApplicationConfiguration.fromConfiguration(configuration);

		final PackagedProgramRetriever programRetriever = getPackagedProgramRetriever(
			configuration,
			applicationConfiguration.getProgramArguments(),
			applicationConfiguration.getApplicationClassName());
		return programRetriever.getPackagedProgram();
	}

	private static PackagedProgramRetriever getPackagedProgramRetriever(
			final Configuration configuration,
			final String[] programArguments,
			@Nullable final String jobClassName) throws IOException {

		final List<File> pipelineJars = KubernetesUtils.checkJarFileForApplicationMode(configuration);
		Preconditions.checkArgument(pipelineJars.size() == 1, "Should only have one jar");

		final ClassPathPackagedProgramRetriever.Builder retrieverBuilder =
			ClassPathPackagedProgramRetriever
				.newBuilder(programArguments)
				.setJarFile(pipelineJars.get(0))
				.setJobClassName(jobClassName);
		tryFindUserLibDirectory().ifPresent(retrieverBuilder::setUserLibDirectory);
		return retrieverBuilder.build();
	}
}
