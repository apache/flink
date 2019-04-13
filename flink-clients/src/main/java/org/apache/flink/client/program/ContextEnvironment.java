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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plandump.PlanJSONDumpGenerator;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;

import java.net.URL;
import java.util.List;

/**
 * Execution Environment for remote execution with the Client.
 */
public class ContextEnvironment extends ExecutionEnvironment {

	protected final ClusterClient<?> client;

	protected final List<URL> jarFilesToAttach;

	protected final List<URL> classpathsToAttach;

	protected final ClassLoader userCodeClassLoader;

	protected final SavepointRestoreSettings savepointSettings;

	public ContextEnvironment(ClusterClient<?> remoteConnection, List<URL> jarFiles, List<URL> classpaths,
				ClassLoader userCodeClassLoader, SavepointRestoreSettings savepointSettings) {
		this.client = remoteConnection;
		this.jarFilesToAttach = jarFiles;
		this.classpathsToAttach = classpaths;
		this.userCodeClassLoader = userCodeClassLoader;
		this.savepointSettings = savepointSettings;
	}

	@Override
	public JobExecutionResult execute(String jobName) throws Exception {
		Plan p = createProgramPlan(jobName);
		JobWithJars toRun = new JobWithJars(p, this.jarFilesToAttach, this.classpathsToAttach,
				this.userCodeClassLoader);
		this.lastJobExecutionResult = client.run(toRun, getParallelism(), savepointSettings).getJobExecutionResult();
		return this.lastJobExecutionResult;
	}

	@Override
	public String getExecutionPlan() throws Exception {
		Plan plan = createProgramPlan("unnamed job");

		OptimizedPlan op = ClusterClient.getOptimizedPlan(client.compiler, plan, getParallelism());
		PlanJSONDumpGenerator gen = new PlanJSONDumpGenerator();
		return gen.getOptimizerPlanAsJSON(op);
	}

	@Override
	public void startNewSession() throws Exception {
		jobID = JobID.generate();
	}

	@Override
	public String toString() {
		return "Context Environment (parallelism = " + (getParallelism() == ExecutionConfig.PARALLELISM_DEFAULT ? "default" : getParallelism())
				+ ") : " + getIdString();
	}

	public ClusterClient<?> getClient() {
		return this.client;
	}

	public List<URL> getJars(){
		return jarFilesToAttach;
	}

	public List<URL> getClasspaths(){
		return classpathsToAttach;
	}

	public ClassLoader getUserCodeClassLoader() {
		return userCodeClassLoader;
	}

	public SavepointRestoreSettings getSavepointRestoreSettings() {
		return savepointSettings;
	}

	// --------------------------------------------------------------------------------------------

	static void setAsContext(ContextEnvironmentFactory factory) {
		initializeContextEnvironment(factory);
	}

	static void unsetContext() {
		resetContextEnvironment();
	}
}
