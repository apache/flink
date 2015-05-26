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
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.PlanExecutor;

/**
 * An {@link ExecutionEnvironment} that sends programs 
 * to a cluster for execution. Note that all file paths used in the program must be accessible from the
 * cluster. The execution will use the cluster's default parallelism, unless the parallelism is
 * set explicitly via {@link ExecutionEnvironment#setParallelism(int)}.
 */
public class RemoteEnvironment extends ExecutionEnvironment {
	
	private final String host;
	
	private final int port;
	
	private final String[] jarFiles;
	
	/**
	 * Creates a new RemoteEnvironment that points to the master (JobManager) described by the
	 * given host name and port.
	 * 
	 * @param host The host name or address of the master (JobManager), where the program should be executed.
	 * @param port The port of the master (JobManager), where the program should be executed. 
	 * @param jarFiles The JAR files with code that needs to be shipped to the cluster. If the program uses
	 *                 user-defined functions, user-defined input formats, or any libraries, those must be
	 *                 provided in the JAR files.
	 */	
	public RemoteEnvironment(String host, int port, String... jarFiles) {
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
	public JobExecutionResult execute(String jobName) throws Exception {
		Plan p = createProgramPlan(jobName);
		
		PlanExecutor executor = PlanExecutor.createRemoteExecutor(host, port, jarFiles);
		executor.setPrintStatusDuringExecution(p.getExecutionConfig().isSysoutLoggingEnabled());

		this.lastJobExecutionResult = executor.executePlan(p);
		return this.lastJobExecutionResult;
	}
	
	@Override
	public String getExecutionPlan() throws Exception {
		Plan p = createProgramPlan("unnamed", false);
		p.setDefaultParallelism(getParallelism());
		registerCachedFilesWithPlan(p);
		
		PlanExecutor executor = PlanExecutor.createRemoteExecutor(host, port, jarFiles);
		return executor.getOptimizerPlanAsJSON(p);
	}

	@Override
	public String toString() {
		return "Remote Environment (" + this.host + ":" + this.port + " - parallelism = " +
				(getParallelism() == -1 ? "default" : getParallelism()) + ") : " + getIdString();
	}


	// needed to call execute on ScalaShellRemoteEnvironment
	public int getPort() {
		return this.port;
	}

	public String getHost() {
		return this.host;
	}
}
