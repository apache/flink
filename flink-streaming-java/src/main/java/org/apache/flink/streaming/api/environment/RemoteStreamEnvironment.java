/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.environment;

import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.RemoteEnvironmentConfigUtils;
import org.apache.flink.configuration.ConfigUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.core.execution.DefaultExecutorServiceLoader;
import org.apache.flink.core.execution.PipelineExecutorServiceLoader;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.streaming.api.graph.StreamGraph;

import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * A {@link StreamExecutionEnvironment} for executing on a cluster.
 */
@Public
public class RemoteStreamEnvironment extends StreamExecutionEnvironment {

	/**
	 * Creates a new RemoteStreamEnvironment that points to the master
	 * (JobManager) described by the given host name and port.
	 *
	 * @param host
	 *            The host name or address of the master (JobManager), where the
	 *            program should be executed.
	 * @param port
	 *            The port of the master (JobManager), where the program should
	 *            be executed.
	 * @param jarFiles
	 *            The JAR files with code that needs to be shipped to the
	 *            cluster. If the program uses user-defined functions,
	 *            user-defined input formats, or any libraries, those must be
	 *            provided in the JAR files.
	 */
	public RemoteStreamEnvironment(String host, int port, String... jarFiles) {
		this(host, port, null, jarFiles);
	}

	/**
	 * Creates a new RemoteStreamEnvironment that points to the master
	 * (JobManager) described by the given host name and port.
	 *
	 * @param host
	 *            The host name or address of the master (JobManager), where the
	 *            program should be executed.
	 * @param port
	 *            The port of the master (JobManager), where the program should
	 *            be executed.
	 * @param clientConfiguration
	 *            The configuration used to parametrize the client that connects to the
	 *            remote cluster.
	 * @param jarFiles
	 *            The JAR files with code that needs to be shipped to the
	 *            cluster. If the program uses user-defined functions,
	 *            user-defined input formats, or any libraries, those must be
	 *            provided in the JAR files.
	 */
	public RemoteStreamEnvironment(String host, int port, Configuration clientConfiguration, String... jarFiles) {
		this(host, port, clientConfiguration, jarFiles, null);
	}

	/**
	 * Creates a new RemoteStreamEnvironment that points to the master
	 * (JobManager) described by the given host name and port.
	 *
	 * @param host
	 *            The host name or address of the master (JobManager), where the
	 *            program should be executed.
	 * @param port
	 *            The port of the master (JobManager), where the program should
	 *            be executed.
	 * @param clientConfiguration
	 *            The configuration used to parametrize the client that connects to the
	 *            remote cluster.
	 * @param jarFiles
	 *            The JAR files with code that needs to be shipped to the
	 *            cluster. If the program uses user-defined functions,
	 *            user-defined input formats, or any libraries, those must be
	 *            provided in the JAR files.
	 * @param globalClasspaths
	 *            The paths of directories and JAR files that are added to each user code
	 *            classloader on all nodes in the cluster. Note that the paths must specify a
	 *            protocol (e.g. file://) and be accessible on all nodes (e.g. by means of a NFS share).
	 *            The protocol must be supported by the {@link java.net.URLClassLoader}.
	 */
	public RemoteStreamEnvironment(String host, int port, Configuration clientConfiguration, String[] jarFiles, URL[] globalClasspaths) {
		this(host, port, clientConfiguration, jarFiles, globalClasspaths, null);
	}

	/**
	 * Creates a new RemoteStreamEnvironment that points to the master
	 * (JobManager) described by the given host name and port.
	 *
	 * @param host
	 *            The host name or address of the master (JobManager), where the
	 *            program should be executed.
	 * @param port
	 *            The port of the master (JobManager), where the program should
	 *            be executed.
	 * @param clientConfiguration
	 *            The configuration used to parametrize the client that connects to the
	 *            remote cluster.
	 * @param jarFiles
	 *            The JAR files with code that needs to be shipped to the
	 *            cluster. If the program uses user-defined functions,
	 *            user-defined input formats, or any libraries, those must be
	 *            provided in the JAR files.
	 * @param globalClasspaths
	 *            The paths of directories and JAR files that are added to each user code
	 *            classloader on all nodes in the cluster. Note that the paths must specify a
	 *            protocol (e.g. file://) and be accessible on all nodes (e.g. by means of a NFS share).
	 *            The protocol must be supported by the {@link java.net.URLClassLoader}.
	 * @param savepointRestoreSettings
	 *            Optional savepoint restore settings for job execution.
	 */
	@PublicEvolving
	public RemoteStreamEnvironment(String host, int port, Configuration clientConfiguration, String[] jarFiles, URL[] globalClasspaths, SavepointRestoreSettings savepointRestoreSettings) {
		this(DefaultExecutorServiceLoader.INSTANCE, host, port, clientConfiguration, jarFiles, globalClasspaths, savepointRestoreSettings);
	}

	@PublicEvolving
	public RemoteStreamEnvironment(
			final PipelineExecutorServiceLoader executorServiceLoader,
			final String host,
			final int port,
			final Configuration clientConfiguration,
			final String[] jarFiles,
			final URL[] globalClasspaths,
			final SavepointRestoreSettings savepointRestoreSettings) {
		super(
				executorServiceLoader,
				validateAndGetEffectiveConfiguration(clientConfiguration, host, port, jarFiles, globalClasspaths, savepointRestoreSettings),
				null
		);
	}

	private static Configuration getClientConfiguration(final Configuration configuration) {
		return configuration == null ? new Configuration() : configuration;
	}

	private static List<URL> getClasspathURLs(final URL[] classpaths) {
		return classpaths == null ? Collections.emptyList() : Arrays.asList(classpaths);
	}

	private static Configuration validateAndGetEffectiveConfiguration(
			final Configuration configuration,
			final String host,
			final int port,
			final String[] jarFiles,
			final URL[] classpaths,
			final SavepointRestoreSettings savepointRestoreSettings) {
		RemoteEnvironmentConfigUtils.validate(host, port);
		return getEffectiveConfiguration(
				getClientConfiguration(configuration),
				host,
				port,
				jarFiles,
				getClasspathURLs(classpaths),
				savepointRestoreSettings);
	}

	private static Configuration getEffectiveConfiguration(
			final Configuration baseConfiguration,
			final String host,
			final int port,
			final String[] jars,
			final List<URL> classpaths,
			final SavepointRestoreSettings savepointRestoreSettings) {

		final Configuration effectiveConfiguration = new Configuration(baseConfiguration);

		RemoteEnvironmentConfigUtils.setJobManagerAddressToConfig(host, port, effectiveConfiguration);
		RemoteEnvironmentConfigUtils.setJarURLsToConfig(jars, effectiveConfiguration);
		ConfigUtils.encodeCollectionToConfig(effectiveConfiguration, PipelineOptions.CLASSPATHS, classpaths, URL::toString);

		if (savepointRestoreSettings != null) {
			SavepointRestoreSettings.toConfiguration(savepointRestoreSettings, effectiveConfiguration);
		} else {
			SavepointRestoreSettings.toConfiguration(SavepointRestoreSettings.none(), effectiveConfiguration);
		}

		// these should be set in the end to overwrite any values from the client config provided in the constructor.
		effectiveConfiguration.setString(DeploymentOptions.TARGET, "remote");
		effectiveConfiguration.setBoolean(DeploymentOptions.ATTACHED, true);

		return effectiveConfiguration;
	}

	@Override
	public JobExecutionResult execute(StreamGraph streamGraph) throws Exception {
		return super.execute(streamGraph);
	}

	@Override
	public String toString() {
		final String host = getConfiguration().getString(JobManagerOptions.ADDRESS);
		final int port = getConfiguration().getInteger(JobManagerOptions.PORT);
		final String parallelism = (getParallelism() == -1 ? "default" : "" + getParallelism());

		return "Remote Environment (" + host + ":" + port + " - parallelism = " + parallelism + ").";
	}

	/**
	 * Gets the hostname of the master (JobManager), where the
	 * program will be executed.
	 *
	 * @return The hostname of the master
	 */
	public String getHost() {
		return getConfiguration().getString(JobManagerOptions.ADDRESS);
	}

	/**
	 * Gets the port of the master (JobManager), where the
	 * program will be executed.
	 *
	 * @return The port of the master
	 */
	public int getPort() {
		return getConfiguration().getInteger(JobManagerOptions.PORT);
	}

	/** @deprecated This method is going to be removed in the next releases. */
	@Deprecated
	public Configuration getClientConfiguration() {
		return getConfiguration();
	}
}
