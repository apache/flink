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
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;

import java.net.MalformedURLException;
import java.net.URL;
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
	public static ExecutionConfigAccessor fromProgramOptions(final ProgramOptions options, final List<URL> jobJars) {
		checkNotNull(options);
		checkNotNull(jobJars);

		final Configuration configuration = new Configuration();

		if (options.getParallelism() != ExecutionConfig.PARALLELISM_DEFAULT) {
			configuration.setInteger(CoreOptions.DEFAULT_PARALLELISM, options.getParallelism());
		}

		configuration.setBoolean(DeploymentOptions.ATTACHED, !options.getDetachedMode());
		configuration.setBoolean(DeploymentOptions.SHUTDOWN_IF_ATTACHED, options.isShutdownOnAttachedExit());

		ConfigUtils.encodeCollectionToConfig(configuration, PipelineOptions.CLASSPATHS, options.getClasspaths(), URL::toString);
		ConfigUtils.encodeCollectionToConfig(configuration, PipelineOptions.JARS, jobJars, URL::toString);

		SavepointRestoreSettings.toConfiguration(options.getSavepointRestoreSettings(), configuration);

		return new ExecutionConfigAccessor(configuration);
	}

	public Configuration applyToConfiguration(final Configuration baseConfiguration) {
		baseConfiguration.addAll(configuration);
		return baseConfiguration;
	}

	public List<URL> getJars() {
		return decodeUrlList(configuration, PipelineOptions.JARS);
	}

	public List<URL> getClasspaths() {
		return decodeUrlList(configuration, PipelineOptions.CLASSPATHS);
	}

	private List<URL> decodeUrlList(final Configuration configuration, final ConfigOption<List<String>> configOption) {
		return ConfigUtils.decodeListFromConfig(configuration, configOption, url -> {
			try {
				return new URL(url);
			} catch (MalformedURLException e) {
				throw new IllegalArgumentException("Invalid URL", e);
			}
		});
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
