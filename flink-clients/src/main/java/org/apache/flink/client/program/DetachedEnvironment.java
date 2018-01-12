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
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.api.common.Plan;
import org.apache.flink.optimizer.plan.FlinkPlan;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.List;
import java.util.Map;

/**
 * Execution Environment for remote execution with the Client in detached mode.
 */
public class DetachedEnvironment extends ContextEnvironment {

	/** Keeps track of the program plan for the Client to access. */
	private FlinkPlan detachedPlan;

	private static final Logger LOG = LoggerFactory.getLogger(DetachedEnvironment.class);

	public DetachedEnvironment(
			ClusterClient<?> remoteConnection,
			List<URL> jarFiles,
			List<URL> classpaths,
			ClassLoader userCodeClassLoader,
			SavepointRestoreSettings savepointSettings) {
		super(remoteConnection, jarFiles, classpaths, userCodeClassLoader, savepointSettings);
	}

	@Override
	public JobExecutionResult execute(String jobName) throws Exception {
		Plan p = createProgramPlan(jobName);
		setDetachedPlan(ClusterClient.getOptimizedPlan(client.compiler, p, getParallelism()));
		LOG.warn("Job was executed in detached mode, the results will be available on completion.");
		this.lastJobExecutionResult = DetachedJobExecutionResult.INSTANCE;
		return this.lastJobExecutionResult;
	}

	public void setDetachedPlan(FlinkPlan plan) {
		if (detachedPlan == null) {
			detachedPlan = plan;
		} else {
			throw new InvalidProgramException(DetachedJobExecutionResult.DETACHED_MESSAGE +
					DetachedJobExecutionResult.EXECUTE_TWICE_MESSAGE);
		}
	}

	/**
	 * Finishes this Context Environment's execution by explicitly running the plan constructed.
	 */
	JobSubmissionResult finalizeExecute() throws ProgramInvocationException {
		return client.run(detachedPlan, jarFilesToAttach, classpathsToAttach, userCodeClassLoader, savepointSettings);
	}

	/**
	 * The {@link JobExecutionResult} returned by a {@link DetachedEnvironment}.
	 */
	public static final class DetachedJobExecutionResult extends JobExecutionResult {

		public static final DetachedJobExecutionResult INSTANCE = new DetachedJobExecutionResult();

		static final String DETACHED_MESSAGE = "Job was submitted in detached mode. ";

		static final String EXECUTE_TWICE_MESSAGE = "Only one call to execute is allowed. ";

		static final String EAGER_FUNCTION_MESSAGE = "Please make sure your program doesn't call " +
				"an eager execution function [collect, print, printToErr, count]. ";

		static final String JOB_RESULT_MESSAGE = "Results of job execution, such as accumulators," +
				" runtime, job id etc. are not available. ";

		private DetachedJobExecutionResult() {
			super(null, -1, null);
		}

		@Override
		public long getNetRuntime() {
			throw new InvalidProgramException(DETACHED_MESSAGE + JOB_RESULT_MESSAGE);
		}

		@Override
		public <T> T getAccumulatorResult(String accumulatorName) {
			throw new InvalidProgramException(DETACHED_MESSAGE + JOB_RESULT_MESSAGE + EAGER_FUNCTION_MESSAGE);
		}

		@Override
		public Map<String, Object> getAllAccumulatorResults() {
			throw new InvalidProgramException(DETACHED_MESSAGE + JOB_RESULT_MESSAGE);
		}

		@Override
		public Integer getIntCounterResult(String accumulatorName) {
			throw new InvalidProgramException(DETACHED_MESSAGE + JOB_RESULT_MESSAGE);
		}

		@Override
		public JobID getJobID() {
			throw new InvalidProgramException(DETACHED_MESSAGE + JOB_RESULT_MESSAGE);
		}
	}
}
