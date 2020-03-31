
package org.apache.flink.api.java;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *	 http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.PlanExecutor;
import org.apache.flink.api.scala.FlinkILoop;
import org.apache.flink.configuration.Configuration;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Special version of {@link org.apache.flink.api.java.ExecutionEnvironment} that has a reference
 * to a {@link org.apache.flink.api.scala.FlinkILoop}. When execute is called this will
 * use the reference of the ILoop to write the compiled classes of the current session to
 * a Jar file and submit these with the program.
 */
public class ScalaShellRemoteEnvironment extends ExecutionEnvironment {

	/** The hostname of the JobManager. */
	private final String host;

	/** The port of the JobManager main actor system. */
	private final int port;

	/** The jar files that need to be attached to each job. */
	private final List<URL> jarFiles;

	/** The configuration used by the client that connects to the cluster. */
	private Configuration clientConfiguration;

	/** reference to Scala Shell, for access to virtual directory. */
	private final FlinkILoop flinkILoop;

	/**
	 * Creates new ScalaShellRemoteEnvironment that has a reference to the FlinkILoop.
	 *
	 * @param host	   The host name or address of the master (JobManager), where the program should be executed.
	 * @param port	   The port of the master (JobManager), where the program should be executed.
	 * @param flinkILoop The flink Iloop instance from which the ScalaShellRemoteEnvironment is called.
	 * @param clientConfig The configuration used by the client that connects to the cluster.
	 * @param jarFiles The JAR files with code that needs to be shipped to the cluster. If the program uses
	 *                 user-defined functions, user-defined input formats, or any libraries, those must be
	 *                 provided in the JAR files.
	 */
	public ScalaShellRemoteEnvironment(String host, int port, FlinkILoop flinkILoop, Configuration clientConfig, String... jarFiles) {
		if (!ExecutionEnvironment.areExplicitEnvironmentsAllowed()) {
			throw new InvalidProgramException(
					"The RemoteEnvironment cannot be instantiated when running in a pre-defined context " +
							"(such as Command Line Client, Scala Shell, or TestEnvironment)");
		}
		if (host == null) {
			throw new NullPointerException("Host must not be null.");
		}
		if (port < 1 || port >= 0xffff) {
			throw new IllegalArgumentException("Port out of range");
		}

		this.host = host;
		this.port = port;
		this.clientConfiguration = clientConfig == null ? new Configuration() : clientConfig;
		if (jarFiles != null) {
			this.jarFiles = new ArrayList<>(jarFiles.length);
			for (String jarFile : jarFiles) {
				try {
					this.jarFiles.add(new File(jarFile).getAbsoluteFile().toURI().toURL());
				} catch (MalformedURLException e) {
					throw new IllegalArgumentException("JAR file path invalid", e);
				}
			}
		}
		else {
			this.jarFiles = Collections.emptyList();
		}

		this.flinkILoop = flinkILoop;
	}

	@Override
	public JobExecutionResult execute(String jobName) throws Exception {
		final Plan p = createProgramPlan(jobName);
		final List<URL> allJarFiles = getUpdatedJarFiles();

		final PlanExecutor executor = PlanExecutor.createRemoteExecutor(host, port, clientConfiguration);
		lastJobExecutionResult = executor.executePlan(p, allJarFiles, Collections.emptyList());
		return lastJobExecutionResult;
	}

	private List<URL> getUpdatedJarFiles() throws MalformedURLException {
		// write generated classes to disk so that they can be shipped to the cluster
		URL jarUrl = flinkILoop.writeFilesToDisk().getAbsoluteFile().toURI().toURL();
		List<URL> allJarFiles = new ArrayList<>(jarFiles);
		allJarFiles.add(jarUrl);
		return allJarFiles;
	}

	public static void disableAllContextAndOtherEnvironments() {

		// we create a context environment that prevents the instantiation of further
		// context environments. at the same time, setting the context environment prevents manual
		// creation of local and remote environments
		ExecutionEnvironmentFactory factory = new ExecutionEnvironmentFactory() {
			@Override
			public ExecutionEnvironment createExecutionEnvironment() {
				throw new UnsupportedOperationException("Execution Environment is already defined" +
						" for this shell.");
			}
		};
		initializeContextEnvironment(factory);
	}

	public static void resetContextEnvironments() {
		ExecutionEnvironment.resetContextEnvironment();
	}
}
