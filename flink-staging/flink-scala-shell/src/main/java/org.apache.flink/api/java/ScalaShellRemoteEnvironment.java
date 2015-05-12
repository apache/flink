
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

import java.io.File;


/**
 * Created by Nikolaas Steenbergen on 23-4-15.
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

		// write virtual files to disk first
		JarHelper jh = new JarHelper();

		flinkILoop.writeFilesToDisk();

		// jarr up.
		File inFile = new File( flinkILoop.getTmpDirShell().getAbsolutePath());
		File outFile = new File( flinkILoop.getTmpJarShell().getAbsolutePath());

		jh.jarDir(inFile, outFile);

		String[] jarFiles = {outFile.getAbsolutePath()};

		// call "traditional" execution methods
		PlanExecutor executor = PlanExecutor.createRemoteExecutor(super.getHost(), super.getPort(), jarFiles);
		executor.setPrintStatusDuringExecution(p.getExecutionConfig().isSysoutLoggingEnabled());
		return executor.executePlan(p);
	}
}