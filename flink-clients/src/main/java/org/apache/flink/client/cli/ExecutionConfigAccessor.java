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
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Accessor that exposes config settings that are relevant for execution from an underlying {@link Configuration}.
 */
@Internal
public class ExecutionConfigAccessor {

	private final Configuration configuration;

	private ExecutionConfigAccessor(final Configuration configuration) {
		this.configuration = checkNotNull(configuration);
	}

	/**
	 * Creates an {@link ExecutionConfigAccessor} based on the provided {@link Configuration}.
	 */
	public static ExecutionConfigAccessor fromConfiguration(final Configuration configuration) {
		return new ExecutionConfigAccessor(checkNotNull(configuration));
	}

	/**
	 * Creates an {@link ExecutionConfigAccessor} based on the provided {@link ProgramOptions} as provided by the user through the CLI.
	 */
	public static ExecutionConfigAccessor fromProgramOptions(final ProgramOptions options) {
		checkNotNull(options);

		final Configuration configuration = new Configuration();
		configuration.setInteger(CoreOptions.DEFAULT_PARALLELISM, options.getParallelism());
		configuration.setBoolean(DeploymentOptions.ATTACHED, !options.getDetachedMode());
		configuration.setBoolean(DeploymentOptions.SHUTDOWN_IF_ATTACHED, options.isShutdownOnAttachedExit());

		parseClasspathURLsToConfig(options.getClasspaths(), configuration);
		parseJarURLToConfig(options.getJarFilePath(), configuration);

		SavepointRestoreSettings.toConfiguration(options.getSavepointRestoreSettings(), configuration);

		return new ExecutionConfigAccessor(configuration);
	}

	private static void parseClasspathURLsToConfig(final List<URL> classpathURLs, final Configuration configuration) {
		ExecutionConfigurationUtils.urlListToConfig(
				classpathURLs,
				configuration,
				PipelineOptions.CLASSPATHS);
	}

	private static void parseJarURLToConfig(final String jarFile, final Configuration configuration) {
		if (jarFile == null) {
			return;
		}

		try {
			final URL jarUrl = new File(jarFile).getAbsoluteFile().toURI().toURL();
			final List<URL> jarUrlSingleton = Collections.singletonList(jarUrl);
			ExecutionConfigurationUtils.urlListToConfig(jarUrlSingleton, configuration, PipelineOptions.JARS);
		} catch (MalformedURLException e) {
			throw new IllegalArgumentException("JAR file path invalid", e);
		}
	}

	public Configuration getConfiguration() {
		return configuration;
	}

	public String getJarFilePath() {
		final List<URL> jarURL = ExecutionConfigurationUtils.urlListFromConfig(configuration, PipelineOptions.JARS);
		if (jarURL != null && !jarURL.isEmpty()) {
			return jarURL.get(0).getPath();
		}
		return null;
	}

	public List<URL> getClasspaths() {
		return ExecutionConfigurationUtils.urlListFromConfig(configuration, PipelineOptions.CLASSPATHS);
	}

	public int getParallelism() {
		return  configuration.getInteger(CoreOptions.DEFAULT_PARALLELISM);
	}

	public boolean getDetachedMode() {
		return !configuration.getBoolean(DeploymentOptions.ATTACHED);
	}

	public SavepointRestoreSettings getSavepointRestoreSettings() {
		return SavepointRestoreSettings.fromConfiguration(configuration);
	}

	public boolean isShutdownOnAttachedExit() {
		return configuration.getBoolean(DeploymentOptions.SHUTDOWN_IF_ATTACHED);
	}
}
