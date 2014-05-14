/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
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
package eu.stratosphere.api.java;

import eu.stratosphere.api.common.JobExecutionResult;
import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.PlanExecutor;

/**
 * An {@link ExecutionEnvironment} that sends programs 
 * to a cluster for execution. Note that all file paths used in the program must be accessible from the
 * cluster. The execution will use the cluster's default degree of parallelism, unless the parallelism is
 * set explicitly via {@link ExecutionEnvironment#setDegreeOfParallelism(int)}.
 * 
 * @param host The host name or address of the master (JobManager), where the program should be executed.
 * @param port The port of the master (JobManager), where the program should be executed. 
 * @param jarFiles The JAR files with code that needs to be shipped to the cluster. If the program uses
 *                 user-defined functions, user-defined input formats, or any libraries, those must be
 *                 provided in the JAR files.
 * @return A remote environment that executes the program on a cluster.
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
		p.setDefaultParallelism(getDegreeOfParallelism());
		registerCachedFilesWithPlan(p);
		
		PlanExecutor executor = PlanExecutor.createRemoteExecutor(host, port, jarFiles);
		return executor.executePlan(p);
	}
	
	@Override
	public String getExecutionPlan() throws Exception {
		Plan p = createProgramPlan("unnamed");
		p.setDefaultParallelism(getDegreeOfParallelism());
		registerCachedFilesWithPlan(p);
		
		PlanExecutor executor = PlanExecutor.createRemoteExecutor(host, port, jarFiles);
		return executor.getOptimizerPlanAsJSON(p);
	}

	@Override
	public String toString() {
		return "Remote Environment (" + this.host + ":" + this.port + " - DOP = " + 
				(getDegreeOfParallelism() == -1 ? "default" : getDegreeOfParallelism()) + ") : " + getIdString();
	}
}
