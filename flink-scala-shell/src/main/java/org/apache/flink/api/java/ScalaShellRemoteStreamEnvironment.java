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

import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.api.scala.FlinkILoop;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.streaming.api.environment.RemoteStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironmentFactory;
import org.apache.flink.streaming.api.graph.StreamGraph;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * A {@link RemoteStreamEnvironment} for the Scala shell.
 */
public class ScalaShellRemoteStreamEnvironment extends RemoteStreamEnvironment {
	private static final Logger LOG = LoggerFactory.getLogger(ScalaShellRemoteStreamEnvironment.class);

	// reference to Scala Shell, for access to virtual directory
	private FlinkILoop flinkILoop;
	private String host;
	private int port;
	private Configuration configuration;
	private String[] jarFiles;

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

		super(host, port, configuration, jarFiles);
		this.flinkILoop = flinkILoop;
		this.host = host;
		this.port = port;
		this.configuration = configuration;
		this.jarFiles = jarFiles;
	}

	/**
	 * Executes the remote job.
	 *
	 * @param streamGraph
	 *            Stream Graph to execute
	 * @param jarFiles
	 * 			  List of jar file URLs to ship to the cluster
	 * @return The result of the job execution, containing elapsed time and accumulators.
	 */
	@Override
	protected JobSubmissionResult executeRemotely(StreamGraph streamGraph, List<URL> jarFiles, boolean detached, SavepointRestoreSettings savepointRestoreSettings) throws ProgramInvocationException {
		URL jarUrl;
		try {
			jarUrl = flinkILoop.writeFilesToDisk().getAbsoluteFile().toURI().toURL();
		} catch (MalformedURLException e) {
			throw new ProgramInvocationException("Could not write the user code classes to disk.", e);
		}

		List<URL> allJarFiles = new ArrayList<>(jarFiles.size() + 1);
		allJarFiles.addAll(jarFiles);
		allJarFiles.add(jarUrl);

		return super.executeRemotely(streamGraph, allJarFiles, detached, savepointRestoreSettings);
	}

	@Override
	public void cancel(String jobId) throws Exception {
		super.cancel(jobId);
	}

	@Override
	public String cancelWithSavepoint(String jobId, String path) throws Exception {
		return super.cancelWithSavepoint(jobId, path);
	}

	@Override
	public String triggerSavepoint(String jobId, String path) throws Exception {
		return super.triggerSavepoint(jobId, path);
	}

	public void setAsContext() {
		StreamExecutionEnvironmentFactory factory = () -> new RemoteStreamEnvironment(host, port,
			configuration, jarFiles);
		initializeContextEnvironment(factory);
	}

	public static void resetContextEnvironments() {
		StreamExecutionEnvironment.resetContextEnvironment();
	}
}
