/**
 *
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
 *
 */

package org.apache.flink.streaming.api.environment;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.flink.client.program.Client;
import org.apache.flink.client.program.JobWithJars;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.jobgraph.JobGraph;

public class RemoteStreamEnvironment extends StreamExecutionEnvironment {
	private static final Log LOG = LogFactory.getLog(RemoteStreamEnvironment.class);

	private String host;
	private int port;
	private String[] jarFiles;

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
		this.jarFiles = jarFiles;
	}

	@Override
	public void execute() {
		if (LOG.isInfoEnabled()) {
			LOG.info("Running remotely at " + host + ":" + port);
		}

		JobGraph jobGraph = jobGraphBuilder.getJobGraph();

		for (int i = 0; i < jarFiles.length; i++) {
			File file = new File(jarFiles[i]);
			try {
				JobWithJars.checkJarFile(file);
			} catch (IOException e) {
				throw new RuntimeException("Problem with jar file " + file.getAbsolutePath(), e);
			}
			jobGraph.addJar(new Path(file.getAbsolutePath()));
		}

		Configuration configuration = jobGraph.getJobConfiguration();
		Client client = new Client(new InetSocketAddress(host, port), configuration, getClass().getClassLoader());

		try {
			client.run(jobGraph, true);
		} catch (ProgramInvocationException e) {
			throw new RuntimeException("Cannot execute job due to ProgramInvocationException", e);
		}
	}

	@Override
	public String toString() {
		return "Remote Environment (" + this.host + ":" + this.port + " - DOP = "
				+ (getDegreeOfParallelism() == -1 ? "default" : getDegreeOfParallelism()) + ")";
	}

}
