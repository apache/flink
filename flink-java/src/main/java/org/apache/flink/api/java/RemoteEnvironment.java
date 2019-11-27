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

package org.apache.flink.api.java;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.configuration.ConfigUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.util.JarUtils;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An {@link ExecutionEnvironment} that sends programs to a cluster for execution. The environment
 * needs to be created with the address and port of the JobManager of the Flink cluster that
 * should execute the programs.
 *
 * <p>Many programs executed via the remote environment depend on additional classes. Such classes
 * may be the classes of functions (transformation, aggregation, ...) or libraries. Those classes
 * must be attached to the remote environment as JAR files, to allow the environment to ship the
 * classes into the cluster for the distributed execution.
 */
@Public
public class RemoteEnvironment extends ExecutionEnvironment {

	/**
	 * Creates a new RemoteEnvironment that points to the master (JobManager) described by the
	 * given host name and port.
	 *
	 * <p>Each program execution will have all the given JAR files in its classpath.
	 *
	 * @param host The host name or address of the master (JobManager), where the program should be executed.
	 * @param port The port of the master (JobManager), where the program should be executed.
	 * @param jarFiles The JAR files with code that needs to be shipped to the cluster. If the program uses
	 *                 user-defined functions, user-defined input formats, or any libraries, those must be
	 *                 provided in the JAR files.
	 */
	public RemoteEnvironment(String host, int port, String... jarFiles) {
		this(host, port, new Configuration(), jarFiles, null);
	}

	/**
	 * Creates a new RemoteEnvironment that points to the master (JobManager) described by the
	 * given host name and port.
	 *
	 * <p>Each program execution will have all the given JAR files in its classpath.
	 *
	 * @param host The host name or address of the master (JobManager), where the program should be executed.
	 * @param port The port of the master (JobManager), where the program should be executed.
	 * @param clientConfig The configuration used by the client that connects to the cluster.
	 * @param jarFiles The JAR files with code that needs to be shipped to the cluster. If the program uses
	 *                 user-defined functions, user-defined input formats, or any libraries, those must be
	 *                 provided in the JAR files.
	 */
	public RemoteEnvironment(String host, int port, Configuration clientConfig, String[] jarFiles) {
		this(host, port, clientConfig, jarFiles, null);
	}

	/**
	 * Creates a new RemoteEnvironment that points to the master (JobManager) described by the
	 * given host name and port.
	 *
	 * <p>Each program execution will have all the given JAR files in its classpath.
	 *
	 * @param host The host name or address of the master (JobManager), where the program should be executed.
	 * @param port The port of the master (JobManager), where the program should be executed.
	 * @param clientConfig The configuration used by the client that connects to the cluster.
	 * @param jarFiles The JAR files with code that needs to be shipped to the cluster. If the program uses
	 *                 user-defined functions, user-defined input formats, or any libraries, those must be
	 *                 provided in the JAR files.
	 * @param globalClasspaths The paths of directories and JAR files that are added to each user code
	 *                 classloader on all nodes in the cluster. Note that the paths must specify a
	 *                 protocol (e.g. file://) and be accessible on all nodes (e.g. by means of a NFS share).
	 *                 The protocol must be supported by the {@link java.net.URLClassLoader}.
	 */
	public RemoteEnvironment(String host, int port, Configuration clientConfig, String[] jarFiles, URL[] globalClasspaths) {
		super(validateAndGetEffectiveConfiguration(clientConfig, host, port, jarFiles, globalClasspaths));
	}

	private static Configuration validateAndGetEffectiveConfiguration(
			final Configuration configuration,
			final String host,
			final int port,
			final String[] jarFiles,
			final URL[] globalClasspaths) {
		validate(host, port);
		return getEffectiveConfiguration(
				getClientConfiguration(configuration),
				host,
				port,
				getJarFiles(jarFiles),
				getClasspathURLs(globalClasspaths));
	}

	private static void validate(final String host, final int port) {
		if (!ExecutionEnvironment.areExplicitEnvironmentsAllowed()) {
			throw new InvalidProgramException(
					"The RemoteEnvironment cannot be instantiated when running in a pre-defined context " +
							"(such as Command Line Client, Scala Shell, or TestEnvironment)");
		}

		checkNotNull(host);
		checkArgument(port > 0 && port < 0xffff);
	}

	private static Configuration getClientConfiguration(final Configuration configuration) {
		return configuration == null ? new Configuration() : configuration;
	}

	private static List<URL> getClasspathURLs(final URL[] classpaths) {
		return classpaths == null ? Collections.emptyList() : Arrays.asList(classpaths);
	}

	private static List<URL> getJarFiles(final String[] jars) {
		return jars == null
				? Collections.emptyList()
				: Arrays.stream(jars).map(jarPath -> {
					try {
						final URL fileURL = new File(jarPath).getAbsoluteFile().toURI().toURL();
						JarUtils.checkJarFile(fileURL);
						return fileURL;
					} catch (MalformedURLException e) {
						throw new IllegalArgumentException("JAR file path invalid", e);
					} catch (IOException e) {
						throw new RuntimeException("Problem with jar file " + jarPath, e);
					}
				}).collect(Collectors.toList());
	}

	private static Configuration getEffectiveConfiguration(
			final Configuration baseConfiguration,
			final String host,
			final int port,
			final List<URL> jars,
			final List<URL> classpaths) {

		final Configuration effectiveConfiguration = new Configuration(baseConfiguration);

		setJobManagerAddressToConfig(host, port, effectiveConfiguration);
		ConfigUtils.encodeCollectionToConfig(effectiveConfiguration, PipelineOptions.CLASSPATHS, classpaths, URL::toString);
		ConfigUtils.encodeCollectionToConfig(effectiveConfiguration, PipelineOptions.JARS, jars, URL::toString);

		// these should be set in the end to overwrite any values from the client config provided in the constructor.
		effectiveConfiguration.setString(DeploymentOptions.TARGET, "remote-cluster");
		effectiveConfiguration.setBoolean(DeploymentOptions.ATTACHED, true);

		return effectiveConfiguration;
	}

	private static void setJobManagerAddressToConfig(final String host, final int port, final Configuration effectiveConfiguration) {
		final InetSocketAddress address = new InetSocketAddress(host, port);
		effectiveConfiguration.setString(JobManagerOptions.ADDRESS, address.getHostString());
		effectiveConfiguration.setInteger(JobManagerOptions.PORT, address.getPort());
		effectiveConfiguration.setString(RestOptions.ADDRESS, address.getHostString());
		effectiveConfiguration.setInteger(RestOptions.PORT, address.getPort());
	}

	@Override
	public String toString() {
		final String host = getConfiguration().getString(JobManagerOptions.ADDRESS);
		final int port = getConfiguration().getInteger(JobManagerOptions.PORT);
		final String parallelism = (getParallelism() == -1 ? "default" : "" + getParallelism());

		return "Remote Environment (" + host + ":" + port + " - parallelism = " + parallelism + ").";
	}
}
