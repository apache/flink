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
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.JobWithJars;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.streaming.api.graph.StreamGraph;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * A {@link StreamExecutionEnvironment} for executing on a cluster.
 */
@Public
public class RemoteStreamEnvironment extends StreamExecutionEnvironment {

	private static final Logger LOG = LoggerFactory.getLogger(RemoteStreamEnvironment.class);

	/** The hostname of the JobManager. */
	private final String host;

	/** The port of the JobManager main actor system. */
	private final int port;

	/** The configuration used to parametrize the client that connects to the remote cluster. */
	private final Configuration clientConfiguration;

	/** The jar files that need to be attached to each job. */
	private final List<URL> jarFiles;

	/** The classpaths that need to be attached to each job. */
	private final List<URL> globalClasspaths;

	/** The savepoint restore settings for job execution. */
	private final SavepointRestoreSettings savepointRestoreSettings;

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
		this(host, port, clientConfiguration, jarFiles, null, null);
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
		if (!ExecutionEnvironment.areExplicitEnvironmentsAllowed()) {
			throw new InvalidProgramException(
					"The RemoteEnvironment cannot be used when submitting a program through a client, " +
							"or running in a TestEnvironment context.");
		}

		if (host == null) {
			throw new NullPointerException("Host must not be null.");
		}
		if (port < 1 || port >= 0xffff) {
			throw new IllegalArgumentException("Port out of range");
		}

		this.host = host;
		this.port = port;
		this.clientConfiguration = clientConfiguration == null ? new Configuration() : clientConfiguration;
		this.jarFiles = new ArrayList<>(jarFiles.length);
		for (String jarFile : jarFiles) {
			try {
				URL jarFileUrl = new File(jarFile).getAbsoluteFile().toURI().toURL();
				this.jarFiles.add(jarFileUrl);
				JobWithJars.checkJarFile(jarFileUrl);
			} catch (MalformedURLException e) {
				throw new IllegalArgumentException("JAR file path is invalid '" + jarFile + "'", e);
			} catch (IOException e) {
				throw new RuntimeException("Problem with jar file " + jarFile, e);
			}
		}
		if (globalClasspaths == null) {
			this.globalClasspaths = Collections.emptyList();
		}
		else {
			this.globalClasspaths = Arrays.asList(globalClasspaths);
		}
		this.savepointRestoreSettings = savepointRestoreSettings;
	}

	/**
	 * Executes the job remotely.
	 *
	 * <p>This method can be used independent of the {@link StreamExecutionEnvironment} type.
	 * @return The result of the job execution, containing elapsed time and accumulators.
	 */
	@PublicEvolving
	public static JobExecutionResult executeRemotely(StreamExecutionEnvironment streamExecutionEnvironment,
		List<URL> jarFiles,
		String host,
		int port,
		Configuration clientConfiguration,
		List<URL> globalClasspaths,
		String jobName,
		SavepointRestoreSettings savepointRestoreSettings
	) throws ProgramInvocationException {
		StreamGraph streamGraph = streamExecutionEnvironment.getStreamGraph(jobName);
		return executeRemotely(streamGraph,
			streamExecutionEnvironment.getClass().getClassLoader(),
			streamExecutionEnvironment.getConfig(),
			jarFiles,
			host,
			port,
			clientConfiguration,
			globalClasspaths,
			savepointRestoreSettings);
	}

	/**
	 * Execute the given stream graph remotely.
	 *
	 * <p>Method for internal use since it exposes stream graph and other implementation details that are subject to change.
	 * @throws ProgramInvocationException
	 */
	private static JobExecutionResult executeRemotely(StreamGraph streamGraph,
		ClassLoader envClassLoader,
		ExecutionConfig executionConfig,
		List<URL> jarFiles,
		String host,
		int port,
		Configuration clientConfiguration,
		List<URL> globalClasspaths,
		SavepointRestoreSettings savepointRestoreSettings
	) throws ProgramInvocationException {
		if (LOG.isInfoEnabled()) {
			LOG.info("Running remotely at {}:{}", host, port);
		}

		ClassLoader userCodeClassLoader = JobWithJars.buildUserCodeClassLoader(jarFiles, globalClasspaths, envClassLoader);

		Configuration configuration = new Configuration();
		configuration.addAll(clientConfiguration);

		configuration.setString(JobManagerOptions.ADDRESS, host);
		configuration.setInteger(JobManagerOptions.PORT, port);

		configuration.setInteger(RestOptions.PORT, port);

		final ClusterClient<?> client;
		try {
			client = new RestClusterClient<>(configuration, "RemoteStreamEnvironment");
		}
		catch (Exception e) {
			throw new ProgramInvocationException("Cannot establish connection to JobManager: " + e.getMessage(),
				streamGraph.getJobGraph().getJobID(), e);
		}

		client.setPrintStatusDuringExecution(executionConfig.isSysoutLoggingEnabled());

		if (savepointRestoreSettings == null) {
			savepointRestoreSettings = SavepointRestoreSettings.none();
		}

		try {
			return client.run(streamGraph, jarFiles, globalClasspaths, userCodeClassLoader, savepointRestoreSettings)
				.getJobExecutionResult();
		}
		catch (ProgramInvocationException e) {
			throw e;
		}
		catch (Exception e) {
			String term = e.getMessage() == null ? "." : (": " + e.getMessage());
			throw new ProgramInvocationException("The program execution failed" + term,
				streamGraph.getJobGraph().getJobID(), e);
		}
		finally {
			try {
				client.shutdown();
			} catch (Exception e) {
				LOG.warn("Could not properly shut down the cluster client.", e);
			}
		}
	}

	@Override
	public JobExecutionResult execute(StreamGraph streamGraph) throws Exception {
		transformations.clear();
		return executeRemotely(streamGraph, jarFiles);
	}

	/**
	 * Executes the remote job.
	 *
	 * <p>Note: This method exposes stream graph internal in the public API, but cannot be removed for backward compatibility.
	 * @param streamGraph
	 *            Stream Graph to execute
	 * @param jarFiles
	 * 			  List of jar file URLs to ship to the cluster
	 * @return The result of the job execution, containing elapsed time and accumulators.
	 */
	@Deprecated
	protected JobExecutionResult executeRemotely(StreamGraph streamGraph, List<URL> jarFiles) throws ProgramInvocationException {
		return executeRemotely(streamGraph,
			this.getClass().getClassLoader(),
			getConfig(),
			jarFiles,
			host,
			port,
			clientConfiguration,
			globalClasspaths,
			savepointRestoreSettings);
	}

	@Override
	public String toString() {
		return "Remote Environment (" + this.host + ":" + this.port + " - parallelism = "
				+ (getParallelism() == -1 ? "default" : getParallelism()) + ")";
	}

	/**
	 * Gets the hostname of the master (JobManager), where the
	 * program will be executed.
	 *
	 * @return The hostname of the master
	 */
	public String getHost() {
		return host;
	}

	/**
	 * Gets the port of the master (JobManager), where the
	 * program will be executed.
	 *
	 * @return The port of the master
	 */
	public int getPort() {
		return port;
	}

	public Configuration getClientConfiguration() {
		return clientConfiguration;
	}
}
