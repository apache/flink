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

import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.api.common.Plan;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;

import java.net.URL;
import java.util.List;

/**
 * Execution Environment for remote execution with the Client in detached mode.
 */
public class DetachedEnvironment extends ContextEnvironment {

	/** Keeps track of the program plan for the Client to access. */
	private boolean alreadyCalled;

	public DetachedEnvironment(
			ClusterClient<?> remoteConnection,
			List<URL> jarFiles,
			List<URL> classpaths,
			ClassLoader userCodeClassLoader,
			SavepointRestoreSettings savepointSettings) {
		super(remoteConnection, jarFiles, classpaths, userCodeClassLoader, savepointSettings);
		alreadyCalled = false;
	}

	@Override
	public JobExecutionResult execute(String jobName) throws Exception {
		verifyExecuteIsCalledOnce();

		Plan p = createProgramPlan(jobName);
		OptimizedPlan optPlan = ClusterClient.getOptimizedPlan(client.compiler, p, getParallelism());

		final JobSubmissionResult result = client.run(optPlan, jarFilesToAttach, classpathsToAttach, userCodeClassLoader, savepointSettings);
		this.lastJobExecutionResult = result.getJobExecutionResult();
		return this.lastJobExecutionResult;
	}

	private void verifyExecuteIsCalledOnce() {
		if (alreadyCalled) {
			throw new InvalidProgramException(DetachedJobExecutionResult.DETACHED_MESSAGE + DetachedJobExecutionResult.EXECUTE_TWICE_MESSAGE);
		}
		alreadyCalled = true;
	}
}
