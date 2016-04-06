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

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.scala.FlinkILoop;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.streaming.api.environment.RemoteStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironmentFactory;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;


public class ScalaShellRemoteStreamEnvironment extends RemoteStreamEnvironment {
	private static final Logger LOG = LoggerFactory.getLogger(ScalaShellRemoteStreamEnvironment.class);

	// reference to Scala Shell, for access to virtual directory
	private FlinkILoop flinkILoop;
	/**
	 * Creates a new RemoteStreamEnvironment that points to the master
	 * (JobManager) described by the given host name and port.
	 *
	 * @param host	 The host name or address of the master (JobManager), where the
	 *				 program should be executed.
	 * @param port	 The port of the master (JobManager), where the program should
	 *				 be executed.
	 * @param jarFiles The JAR files with code that needs to be shipped to the
	 *				 cluster. If the program uses user-defined functions,
	 *				 user-defined input formats, or any libraries, those must be
	 */
	public ScalaShellRemoteStreamEnvironment(String host, int port, FlinkILoop flinkILoop, String... jarFiles) {
		super(host, port, jarFiles);
		this.flinkILoop = flinkILoop;
	}
	/**
	 * Executes the remote job.
	 *
	 * @param streamGraph
	 *            Stream Graph to execute
	 * @return The result of the job execution, containing elapsed time and accumulators.
	 */
	protected JobExecutionResult executeRemotely(StreamGraph streamGraph) throws ProgramInvocationException {
		URL jarUrl = null;
		try {
			jarUrl = flinkILoop.writeFilesToDisk().getAbsoluteFile().toURI().toURL();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		}

		jarFiles.add(jarUrl);
		// get external (library) jars
		String[] extJars = this.flinkILoop.getExternalJars();

		for (String extJar : extJars) {
			URL extJarUrl = null;
			try {
				extJarUrl = new File(extJar).getAbsoluteFile().toURI().toURL();
			} catch (MalformedURLException e) {
				e.printStackTrace();
			}
			jarFiles.add(extJarUrl);
		}
		return super.executeRemotely(streamGraph);
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
