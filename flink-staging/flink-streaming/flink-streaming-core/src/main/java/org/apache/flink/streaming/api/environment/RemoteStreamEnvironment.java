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

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.client.program.Client;
import org.apache.flink.client.program.JobWithJars;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RemoteStreamEnvironment extends StreamExecutionEnvironment {
	private static final Logger LOG = LoggerFactory.getLogger(RemoteStreamEnvironment.class);

	private final String host;
	private final int port;
	private final List<File> jarFiles;

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
		if (host == null) {
			throw new NullPointerException("Host must not be null.");
		}

		if (port < 1 || port >= 0xffff) {
			throw new IllegalArgumentException("Port out of range");
		}

		this.host = host;
		this.port = port;
		this.jarFiles = new ArrayList<File>();
		for (String jarFile : jarFiles) {
			File file = new File(jarFile);
			try {
				JobWithJars.checkJarFile(file);
			}
			catch (IOException e) {
				throw new RuntimeException("Problem with jar file " + file.getAbsolutePath(), e);
			}
			this.jarFiles.add(file);
		}
	}

	@Override
	public JobExecutionResult execute() throws ProgramInvocationException {
		JobGraph jobGraph = getStreamGraph().getJobGraph();
		transformations.clear();
		return executeRemotely(jobGraph);
	}

	@Override
	public JobExecutionResult execute(String jobName) throws ProgramInvocationException {
		JobGraph jobGraph = getStreamGraph().getJobGraph(jobName);
		transformations.clear();
		return executeRemotely(jobGraph);
	}

	/**
	 * Executes the remote job.
	 * 
	 * @param jobGraph
	 *            jobGraph to execute
	 * @return The result of the job execution, containing elapsed time and accumulators.
	 */
	private JobExecutionResult executeRemotely(JobGraph jobGraph) throws ProgramInvocationException {
		if (LOG.isInfoEnabled()) {
			LOG.info("Running remotely at {}:{}", host, port);
		}

		for (File file : jarFiles) {
			jobGraph.addJar(new Path(file.getAbsolutePath()));
		}

		Configuration configuration = jobGraph.getJobConfiguration();
		ClassLoader usercodeClassLoader = JobWithJars.buildUserCodeClassLoader(jarFiles, getClass().getClassLoader());

		try {
			Client client = new Client(new InetSocketAddress(host, port), configuration, usercodeClassLoader, -1);
			client.setPrintStatusDuringExecution(getConfig().isSysoutLoggingEnabled());
			
			JobSubmissionResult result = client.run(jobGraph, true);
			if (result instanceof JobExecutionResult) {
				return (JobExecutionResult) result;
			} else {
				LOG.warn("The Client didn't return a JobExecutionResult");
				return new JobExecutionResult(result.getJobID(), -1, null);
			}
		}
		catch (ProgramInvocationException e) {
			throw e;
		}
		catch (UnknownHostException e) {
			throw new ProgramInvocationException(e.getMessage(), e);
		}
		catch (Exception e) {
			String term = e.getMessage() == null ? "." : (": " + e.getMessage());
			throw new ProgramInvocationException("The program execution failed" + term, e);
		}
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
}
