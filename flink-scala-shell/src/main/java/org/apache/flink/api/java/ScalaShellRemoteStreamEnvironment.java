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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.PlanExecutor;
import org.apache.flink.api.scala.FlinkILoop;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.RemoteStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.util.JarUtils;
import org.apache.flink.util.function.TriFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A remote {@link StreamExecutionEnvironment} for the Scala shell.
 */
public class ScalaShellRemoteStreamEnvironment extends StreamExecutionEnvironment {

	private static final Logger LOG = LoggerFactory.getLogger(RemoteStreamEnvironment.class);

	// reference to Scala Shell, for access to virtual directory
	private final FlinkILoop flinkILoop;
	private final String host;
	private final int port;
	private final Configuration configuration;
	private final List<URL> jarFiles;

	private TriFunction<String, Integer, Configuration, PlanExecutor> planExecutorFactory = PlanExecutor::createRemoteExecutor;

	@VisibleForTesting
	void setPlanExecutorFactory(TriFunction<String, Integer, Configuration, PlanExecutor> planExecutorFactory) {
		this.planExecutorFactory = planExecutorFactory;
	}

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
				"The RemoteEnvironment cannot be instantiated when running in a pre-defined context " +
					"(such as Command Line Client, Scala Shell, or TestEnvironment)");
		}

		checkNotNull(host);
		checkArgument(1 <= port && port < 0xffff);

		this.host = host;
		this.port = port;
		this.flinkILoop = flinkILoop;
		this.configuration = configuration != null ? configuration : new Configuration();

		if (jarFiles != null) {
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
		} else {
			this.jarFiles = Collections.emptyList();
		}
	}

	@Override
	public JobExecutionResult execute(StreamGraph streamGraph) throws Exception {
		transformations.clear();

		URL jarUrl;
		try {
			jarUrl = flinkILoop.writeFilesToDisk().getAbsoluteFile().toURI().toURL();
		} catch (MalformedURLException e) {
			throw new ProgramInvocationException("Could not write the user code classes to disk.",
				streamGraph.getJobGraph().getJobID(), e);
		}

		List<URL> allJarFiles = new ArrayList<>(jarFiles.size() + 1);
		allJarFiles.addAll(jarFiles);
		allJarFiles.add(jarUrl);

		try {
			LOG.info("Running remotely at {}:{}", host, port);

			final PlanExecutor executor = planExecutorFactory.apply(host, port, configuration);

			return executor.executePlan(streamGraph, allJarFiles, Collections.emptyList());
		} catch (ProgramInvocationException e) {
			throw e;
		} catch (Exception e) {
			String term = e.getMessage() == null ? "." : (": " + e.getMessage());
			throw new ProgramInvocationException("The program execution failed" + term, e);
		}
	}
}
