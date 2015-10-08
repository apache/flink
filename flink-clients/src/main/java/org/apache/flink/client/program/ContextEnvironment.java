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

import java.net.URL;
import java.util.List;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.ExecutionEnvironmentFactory;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plandump.PlanJSONDumpGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Execution Environment for remote execution with the Client.
 */
public class ContextEnvironment extends ExecutionEnvironment {

	private static final Logger LOG = LoggerFactory.getLogger(ContextEnvironment.class);

	private final Client client;

	private final List<URL> jarFilesToAttach;

	private final List<URL> classpathsToAttach;
	
	private final ClassLoader userCodeClassLoader;

	private final boolean wait;
	
	
	
	public ContextEnvironment(Client remoteConnection, List<URL> jarFiles, List<URL> classpaths,
			ClassLoader userCodeClassLoader, boolean wait) {
		this.client = remoteConnection;
		this.jarFilesToAttach = jarFiles;
		this.classpathsToAttach = classpaths;
		this.userCodeClassLoader = userCodeClassLoader;
		this.wait = wait;
	}

	@Override
	public JobExecutionResult execute(String jobName) throws Exception {
		Plan p = createProgramPlan(jobName);
		JobWithJars toRun = new JobWithJars(p, this.jarFilesToAttach, this.classpathsToAttach,
				this.userCodeClassLoader);

		if (wait) {
			this.lastJobExecutionResult = client.runBlocking(toRun, getParallelism());
			return this.lastJobExecutionResult;
		}
		else {
			JobSubmissionResult result = client.runDetached(toRun, getParallelism());
			LOG.warn("Job was executed in detached mode, the results will be available on completion.");
			this.lastJobExecutionResult = JobExecutionResult.fromJobSubmissionResult(result);
			return this.lastJobExecutionResult;
		}
	}

	@Override
	public String getExecutionPlan() throws Exception {
		Plan plan = createProgramPlan("unnamed job");

		OptimizedPlan op = Client.getOptimizedPlan(client.compiler, plan, getParallelism());
		PlanJSONDumpGenerator gen = new PlanJSONDumpGenerator();
		return gen.getOptimizerPlanAsJSON(op);
	}

	@Override
	public void startNewSession() throws Exception {
		client.endSession(jobID);
		jobID = JobID.generate();
	}

	public boolean isWait() {
		return wait;
	}

	@Override
	public String toString() {
		return "Context Environment (parallelism = " + (getParallelism() == -1 ? "default" : getParallelism())
				+ ") : " + getIdString();
	}
	
	public Client getClient() {
		return this.client;
	}
	
	public List<URL> getJars(){
		return jarFilesToAttach;
	}

	public List<URL> getClasspaths(){
		return classpathsToAttach;
	}
	
	// --------------------------------------------------------------------------------------------
	
	static void setAsContext(Client client, List<URL> jarFilesToAttach, List<URL> classpathsToAttach,
				ClassLoader userCodeClassLoader, int defaultParallelism, boolean wait)
	{
		ContextEnvironmentFactory factory = new ContextEnvironmentFactory(client, jarFilesToAttach,
				classpathsToAttach, userCodeClassLoader, defaultParallelism, wait);
		initializeContextEnvironment(factory);
	}
	
	static void unsetContext() {
		resetContextEnvironment();
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * The factory that instantiates the environment to be used when running jobs that are
	 * submitted through a pre-configured client connection.
	 * This happens for example when a job is submitted from the command line.
	 */
	public static class ContextEnvironmentFactory implements ExecutionEnvironmentFactory {
		
		private final Client client;
		
		private final List<URL> jarFilesToAttach;

		private final List<URL> classpathsToAttach;
		
		private final ClassLoader userCodeClassLoader;
		
		private final int defaultParallelism;

		private final boolean wait;
		

		public ContextEnvironmentFactory(Client client, List<URL> jarFilesToAttach,
				List<URL> classpathsToAttach, ClassLoader userCodeClassLoader, int defaultParallelism,
				boolean wait)
		{
			this.client = client;
			this.jarFilesToAttach = jarFilesToAttach;
			this.classpathsToAttach = classpathsToAttach;
			this.userCodeClassLoader = userCodeClassLoader;
			this.defaultParallelism = defaultParallelism;
			this.wait = wait;
		}
		
		@Override
		public ExecutionEnvironment createExecutionEnvironment() {
			ContextEnvironment env = new ContextEnvironment(client, jarFilesToAttach, classpathsToAttach,
					userCodeClassLoader, wait);
			if (defaultParallelism > 0) {
				env.setParallelism(defaultParallelism);
			}
			return env;
		}
	}
}
