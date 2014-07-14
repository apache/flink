/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.streaming.api;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.client.program.Client;
import eu.stratosphere.client.program.JobWithJars;
import eu.stratosphere.client.program.ProgramInvocationException;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.core.fs.Path;
import eu.stratosphere.nephele.jobgraph.JobGraph;

public class RemoteStreamEnvironment extends StreamExecutionEnvironment {
	private static final Log log = LogFactory.getLog(RemoteStreamEnvironment.class);

	private String host;
	private int port;
	private String[] jarFiles;

	/**
	 * Creates a new RemoteStreamEnvironment that points to the master (JobManager) described by the
	 * given host name and port.
	 * 
	 * @param host The host name or address of the master (JobManager), where the program should be executed.
	 * @param port The port of the master (JobManager), where the program should be executed. 
	 * @param jarFiles The JAR files with code that needs to be shipped to the cluster. If the program uses
	 *                 user-defined functions, user-defined input formats, or any libraries, those must be
	 *                 provided in the JAR files.
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
		try {
			
			JobGraph jobGraph = jobGraphBuilder.getJobGraph();

			for (int i = 0; i < jarFiles.length; i++) {
				File file = new File(jarFiles[i]);
				JobWithJars.checkJarFile(file);
				jobGraph.addJar(new Path(file.getAbsolutePath()));
			}
			
			Configuration configuration = jobGraph.getJobConfiguration();
			Client client = new Client(new InetSocketAddress(host, port), configuration);

			client.run(jobGraph, true);
			
		} catch (IOException e) {
			if (log.isErrorEnabled()) {
				log.error(e.getMessage());
				e.printStackTrace();
			}
		} catch (ProgramInvocationException e) {
			if (log.isErrorEnabled()) {
				log.error(e.getMessage());
				e.printStackTrace();
			}
		}
	}
	
	@Override
	public String toString() {
		return "Remote Environment (" + this.host + ":" + this.port + " - DOP = " + 
				(getDegreeOfParallelism() == -1 ? "default" : getDegreeOfParallelism()) + ")";
	}

}
