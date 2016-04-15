
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

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.PlanExecutor;

import org.apache.flink.api.scala.FlinkILoop;
import org.apache.flink.configuration.Configuration;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * Special version of {@link org.apache.flink.api.java.RemoteEnvironment} that has a reference
 * to a {@link org.apache.flink.api.scala.FlinkILoop}. When execute is called this will
 * use the reference of the ILoop to write the compiled classes of the current session to
 * a Jar file and submit these with the program.
 */
public class ScalaShellRemoteEnvironment extends RemoteEnvironment {

	// reference to Scala Shell, for access to virtual directory
	private FlinkILoop flinkILoop;

	/**
	 * Creates new ScalaShellRemoteEnvironment that has a reference to the FlinkILoop
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
		super(host, port, clientConfig, jarFiles, null);
		this.flinkILoop = flinkILoop;
	}

	/**
	 * compiles jars from files in the shell virtual directory on the fly, sends and executes it in the remote environment
	 *
	 * @param jobName name of the job as string
	 * @return Result of the computation
	 * @throws Exception
	 */
	@Override
	public JobExecutionResult execute(String jobName) throws Exception {
		Plan p = createProgramPlan(jobName);

		URL jarUrl = flinkILoop.writeFilesToDisk().getAbsoluteFile().toURI().toURL();

		// get "external jars, and add the shell command jar, pass to executor
		List<URL> alljars = new ArrayList<>();
		// get external (library) jars
		String[] extJars = this.flinkILoop.getExternalJars();

		for (String extJar : extJars) {
			URL extJarUrl = new File(extJar).getAbsoluteFile().toURI().toURL();
			alljars.add(extJarUrl);
		}

		// add shell commands
		alljars.add(jarUrl);
		PlanExecutor executor = PlanExecutor.createRemoteExecutor(host, port, new Configuration(),
				alljars.toArray(new URL[alljars.size()]), null);

		executor.setPrintStatusDuringExecution(p.getExecutionConfig().isSysoutLoggingEnabled());
		return executor.executePlan(p);
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
