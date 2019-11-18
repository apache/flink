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
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.client.ClientUtils;
import org.apache.flink.client.FlinkPipelineTranslationUtil;
import org.apache.flink.client.cli.ExecutionConfigAccessor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;

import java.net.URL;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Execution Environment for remote execution with the Client.
 */
public class ContextEnvironment extends ExecutionEnvironment {

	private final ClusterClient<?> client;

	private final boolean detached;

	private final List<URL> jarFilesToAttach;

	private final List<URL> classpathsToAttach;

	private final ClassLoader userCodeClassLoader;

	private final SavepointRestoreSettings savepointSettings;

	private final AtomicReference<JobExecutionResult> jobExecutionResult;

	private boolean alreadyCalled;

	public ContextEnvironment(
			final Configuration configuration,
			final ClusterClient<?> remoteConnection,
			final ClassLoader userCodeClassLoader,
			final AtomicReference<JobExecutionResult> jobExecutionResult) {

		this.client = checkNotNull(remoteConnection);
		this.userCodeClassLoader = checkNotNull(userCodeClassLoader);
		this.jobExecutionResult = checkNotNull(jobExecutionResult);

		final ExecutionConfigAccessor accessor = ExecutionConfigAccessor
				.fromConfiguration(checkNotNull(configuration));

		if (accessor.getParallelism() > 0) {
			setParallelism(accessor.getParallelism());
		}

		this.jarFilesToAttach = accessor.getJars();
		this.classpathsToAttach = accessor.getClasspaths();
		this.savepointSettings = accessor.getSavepointRestoreSettings();
		this.detached = accessor.getDetachedMode();

		this.alreadyCalled = false;
	}

	@Override
	public JobExecutionResult execute(String jobName) throws Exception {
		verifyExecuteIsCalledOnceWhenInDetachedMode();

		Plan plan = createProgramPlan(jobName);

		JobGraph jobGraph = FlinkPipelineTranslationUtil.getJobGraph(
				plan,
				client.getFlinkConfiguration(),
				getParallelism());

		jobGraph.addJars(this.jarFilesToAttach);
		jobGraph.setClasspaths(this.classpathsToAttach);

		if (detached) {
			lastJobExecutionResult = ClientUtils.submitJob(client, jobGraph);
		} else {
			lastJobExecutionResult = ClientUtils.submitJobAndWaitForResult(client, jobGraph, userCodeClassLoader).getJobExecutionResult();
		}

		setJobExecutionResult(lastJobExecutionResult);

		return lastJobExecutionResult;
	}

	private void verifyExecuteIsCalledOnceWhenInDetachedMode() {
		if (alreadyCalled && detached) {
			throw new InvalidProgramException(DetachedJobExecutionResult.DETACHED_MESSAGE + DetachedJobExecutionResult.EXECUTE_TWICE_MESSAGE);
		}
		alreadyCalled = true;
	}

	public void setJobExecutionResult(JobExecutionResult jobExecutionResult) {
		this.jobExecutionResult.set(jobExecutionResult);
	}

	@Override
	public String toString() {
		return "Context Environment (parallelism = " + (getParallelism() == ExecutionConfig.PARALLELISM_DEFAULT ? "default" : getParallelism()) + ")";
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

	public boolean isDetached() {
		return detached;
	}

	// --------------------------------------------------------------------------------------------

	public static void setAsContext(ContextEnvironmentFactory factory) {
		initializeContextEnvironment(factory);
	}

	public static void unsetContext() {
		resetContextEnvironment();
	}
}
