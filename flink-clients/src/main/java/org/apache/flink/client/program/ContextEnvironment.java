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

package org.apache.flink.client.program;

import java.io.File;
import java.util.List;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.ExecutionEnvironmentFactory;
import org.apache.flink.compiler.plan.OptimizedPlan;
import org.apache.flink.compiler.plandump.PlanJSONDumpGenerator;

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
		JobWithJars toRun = new JobWithJars(p, this.jarFilesToAttach, this.userCodeClassLoader);
		
		return this.client.run(toRun, getDegreeOfParallelism(), true);
	}

	@Override
	public String getExecutionPlan() throws Exception {
		Plan p = createProgramPlan("unnamed job");
		
		OptimizedPlan op = this.client.getOptimizedPlan(p, getDegreeOfParallelism());
		
		PlanJSONDumpGenerator gen = new PlanJSONDumpGenerator();
		return gen.getOptimizerPlanAsJSON(op);
	}
	
	
	@Override
	public String toString() {
		return "Context Environment (DOP = " + (getDegreeOfParallelism() == -1 ? "default" : getDegreeOfParallelism())
				+ ") : " + getIdString();
	}
	
	public Client getClient() {
		return this.client;
	}
	
	public List<File> getJars(){
		return jarFilesToAttach;
	}
	
	// --------------------------------------------------------------------------------------------
	
	static void setAsContext(Client client, List<File> jarFilesToAttach, 
				ClassLoader userCodeClassLoader, int defaultParallelism)
	{
		initializeContextEnvironment(new ContextEnvironmentFactory(client, jarFilesToAttach, userCodeClassLoader, defaultParallelism));
	}
	
	protected static void enableLocalExecution(boolean enabled) {
		ExecutionEnvironment.enableLocalExecution(enabled);
	}
	
	// --------------------------------------------------------------------------------------------
	
	public static class ContextEnvironmentFactory implements ExecutionEnvironmentFactory {
		
		private final Client client;
		
		private final List<File> jarFilesToAttach;
		
		private final ClassLoader userCodeClassLoader;
		
		private final int defaultParallelism;
		

		public ContextEnvironmentFactory(Client client, List<File> jarFilesToAttach, 
				ClassLoader userCodeClassLoader, int defaultParallelism)
		{
			this.client = client;
			this.jarFilesToAttach = jarFilesToAttach;
			this.userCodeClassLoader = userCodeClassLoader;
			this.defaultParallelism = defaultParallelism;
		}
		
		@Override
		public ExecutionEnvironment createExecutionEnvironment() {
			ContextEnvironment env = new ContextEnvironment(client, jarFilesToAttach, userCodeClassLoader);
			if (defaultParallelism > 0) {
				env.setDegreeOfParallelism(defaultParallelism);
			}
			return env;
		}
	}
}
