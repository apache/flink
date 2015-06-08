
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

import org.apache.commons.lang.ArrayUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.PlanExecutor;

import org.apache.flink.api.scala.FlinkILoop;

import java.util.ArrayList;
import java.util.Arrays;
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
	 */
	public ScalaShellRemoteEnvironment(String host, int port, FlinkILoop flinkILoop, String... jarFiles) {
		super(host, port, jarFiles);
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

		String jarFile = flinkILoop.writeFilesToDisk().getAbsolutePath();

		// get "external jars, and add the shell command jar, pass to executor
		List<String> alljars = new ArrayList<String>();
		// get external (library) jars
		String[] extJars = this.flinkILoop.getExternalJars();
		
		if(!ArrayUtils.isEmpty(extJars)) {
			alljars.addAll(Arrays.asList(extJars));
		}
		// add shell commands
		alljars.add(jarFile);
		String[] alljarsArr = new String[alljars.size()];
		alljarsArr = alljars.toArray(alljarsArr);
		PlanExecutor executor = PlanExecutor.createRemoteExecutor(host, port, alljarsArr);

		executor.setPrintStatusDuringExecution(p.getExecutionConfig().isSysoutLoggingEnabled());
		return executor.executePlan(p);
	}
}
