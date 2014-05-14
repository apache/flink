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
package eu.stratosphere.client.program;

import java.io.File;
import java.util.List;

import eu.stratosphere.api.common.JobExecutionResult;
import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.compiler.plan.OptimizedPlan;
import eu.stratosphere.compiler.plandump.PlanJSONDumpGenerator;

/**
 *
 */
public class ContextEnvironment extends ExecutionEnvironment {
	
	private final Client client;
	
	private final List<File> jarFilesToAttach;
	
	private final ClassLoader userCodeClassLoader;
	
	
	
	public ContextEnvironment(Client remoteConnection, List<File> jarFiles, ClassLoader userCodeClassLoader) {
		this.client = remoteConnection;
		this.jarFilesToAttach = jarFiles;
		this.userCodeClassLoader = userCodeClassLoader;
	}

	@Override
	public JobExecutionResult execute(String jobName) throws Exception {
		Plan p = createProgramPlan(jobName);
		p.setDefaultParallelism(getDegreeOfParallelism());
		
		JobWithJars toRun = new JobWithJars(p, this.jarFilesToAttach, this.userCodeClassLoader);
		return this.client.run(toRun, true);
	}

	@Override
	public String getExecutionPlan() throws Exception {
		Plan p = createProgramPlan("unnamed job");
		p.setDefaultParallelism(getDegreeOfParallelism());
		
		OptimizedPlan op = this.client.getOptimizedPlan(p);
		
		PlanJSONDumpGenerator gen = new PlanJSONDumpGenerator();
		return gen.getOptimizerPlanAsJSON(op);
	}
	
	
	@Override
	public String toString() {
		return "Context Environment (DOP = " + (getDegreeOfParallelism() == -1 ? "default" : getDegreeOfParallelism())
				+ ") : " + getIdString();
	}
	
	
	public void setAsContext() {
		initializeContextEnvironment(this);
	}
	
	public static void disableLocalExecution() {
		ExecutionEnvironment.disableLocalExecution();
	}
}
