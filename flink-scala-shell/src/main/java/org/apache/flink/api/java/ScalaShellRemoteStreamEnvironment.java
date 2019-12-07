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

import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.PlanExecutor;
import org.apache.flink.api.scala.FlinkILoop;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironmentFactory;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.util.JarUtils;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A {@link StreamExecutionEnvironment} for the Scala shell.
 */
public class ScalaShellRemoteStreamEnvironment extends StreamExecutionEnvironment {

	/** The hostname of the JobManager. */
	private final String host;

	/** The port of the JobManager main actor system. */
	private final int port;

	/** The configuration used to parametrize the client that connects to the remote cluster. */
	private final Configuration clientConfiguration;

	/** The jar files that need to be attached to each job. */
	private final List<URL> jarFiles;

	/** Reference to Scala Shell, for access to virtual directory. */
	private final FlinkILoop flinkILoop;

	/**
	 * Creates a new RemoteStreamEnvironment that points to the master
	 * (JobManager) described by the given host name and port.
	 *
	 * @param host	 The host name or address of the master (JobManager), where the
	 *				 program should be executed.
	 * @param port	 The port of the master (JobManager), where the program should
	 *				 be executed.
	 * @param configuration The configuration to be used for the environment
	 * @param jarFiles The JAR files with code that needs to be shipped to the
	 *				 cluster. If the program uses user-defined functions,
	 *				 user-defined input formats, or any libraries, those must be
	 */
	public ScalaShellRemoteStreamEnvironment(
		String host,
		int port,
		FlinkILoop flinkILoop,
		Configuration configuration,
		String... jarFiles) {

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
		this.clientConfiguration = configuration == null ? new Configuration() : configuration;
		this.jarFiles = new ArrayList<>(jarFiles.length);
		for (String jarFile : jarFiles) {
			try {
				URL jarFileUrl = new File(jarFile).getAbsoluteFile().toURI().toURL();
				this.jarFiles.add(jarFileUrl);
				JarUtils.checkJarFile(jarFileUrl);
			} catch (MalformedURLException e) {
				throw new IllegalArgumentException("JAR file path is invalid '" + jarFile + "'", e);
			} catch (IOException e) {
				throw new RuntimeException("Problem with jar file " + jarFile, e);
			}
		}
		this.flinkILoop = flinkILoop;
	}

	/**
	 * Executes the remote job.
	 *
	 * @param streamGraph
	 *            Stream Graph to execute
	 * @return The result of the job execution, containing elapsed time and accumulators.
	 */
	@Override
	public JobExecutionResult execute(StreamGraph streamGraph) throws ProgramInvocationException {
		try {
			final List<URL> allJarFiles = getUpdatedJars(jarFiles);
			final PlanExecutor executor = PlanExecutor.createRemoteExecutor(host, port, clientConfiguration);
			return executor.executePlan(streamGraph, allJarFiles, Collections.emptyList()).getJobExecutionResult();
		}
		catch (ProgramInvocationException e) {
			throw e;
		}
		catch (MalformedURLException e) {
			throw new ProgramInvocationException("Could not write the user code classes to disk.",
					streamGraph.getJobGraph().getJobID(), e);
		}
		catch (Exception e) {
			String term = e.getMessage() == null ? "." : (": " + e.getMessage());
			throw new ProgramInvocationException("The program execution failed" + term, e);
		}
	}

	private List<URL> getUpdatedJars(List<URL> jarFiles) throws MalformedURLException {
		final URL jarUrl = flinkILoop.writeFilesToDisk().getAbsoluteFile().toURI().toURL();
		final List<URL> allJarFiles = new ArrayList<>(jarFiles.size() + 1);
		allJarFiles.addAll(jarFiles);
		allJarFiles.add(jarUrl);
		return allJarFiles;
	}

	public Configuration getClientConfiguration() {
		return clientConfiguration;
	}

	public void setAsContext() {
		StreamExecutionEnvironmentFactory factory = new StreamExecutionEnvironmentFactory() {
			@Override
			public StreamExecutionEnvironment createExecutionEnvironment() {
				throw new UnsupportedOperationException("Execution Environment is already defined" +
						" for this shell.");
			}
		};
		initializeContextEnvironment(factory);
	}

	public static void disableAllContextAndOtherEnvironments() {
		// we create a context environment that prevents the instantiation of further
		// context environments. at the same time, setting the context environment prevents manual
		// creation of local and remote environments
		StreamExecutionEnvironmentFactory factory = new StreamExecutionEnvironmentFactory() {
			@Override
			public StreamExecutionEnvironment createExecutionEnvironment() {
				throw new UnsupportedOperationException("Execution Environment is already defined" +
						" for this shell.");
			}
		};
		initializeContextEnvironment(factory);
	}

	public static void resetContextEnvironments() {
		StreamExecutionEnvironment.resetContextEnvironment();
	}
}
