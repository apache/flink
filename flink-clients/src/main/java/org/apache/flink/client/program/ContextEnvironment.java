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
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.ExecutionEnvironmentFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.core.execution.DetachedJobExecutionResult;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.PipelineExecutorServiceLoader;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.ShutdownHookUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Execution Environment for remote execution with the Client.
 */
public class ContextEnvironment extends ExecutionEnvironment {

	private static final Logger LOG = LoggerFactory.getLogger(ExecutionEnvironment.class);

	private final boolean suppressSysout;

	private final boolean enforceSingleJobExecution;

	private int jobCounter;

	public ContextEnvironment(
			final PipelineExecutorServiceLoader executorServiceLoader,
			final Configuration configuration,
			final ClassLoader userCodeClassLoader,
			final boolean enforceSingleJobExecution,
			final boolean suppressSysout) {
		super(executorServiceLoader, configuration, userCodeClassLoader);
		this.suppressSysout = suppressSysout;
		this.enforceSingleJobExecution = enforceSingleJobExecution;

		this.jobCounter = 0;
	}

	@Override
	public JobExecutionResult execute(String jobName) throws Exception {
		JobClient jobClient = executeAsync(jobName);

		JobExecutionResult jobExecutionResult;
		if (getConfiguration().getBoolean(DeploymentOptions.ATTACHED)) {
			CompletableFuture<JobExecutionResult> jobExecutionResultFuture =
					jobClient.getJobExecutionResult(getUserCodeClassLoader());

			if (getConfiguration().getBoolean(DeploymentOptions.SHUTDOWN_IF_ATTACHED)) {
				Thread shutdownHook = ShutdownHookUtil.addShutdownHook(
						() -> {
							// wait a smidgen to allow the async request to go through before
							// the jvm exits
							jobClient.cancel().get(1, TimeUnit.SECONDS);
						},
						ContextEnvironment.class.getSimpleName(),
						LOG);
				jobExecutionResultFuture.whenComplete((ignored, throwable) ->
						ShutdownHookUtil.removeShutdownHook(shutdownHook, ContextEnvironment.class.getSimpleName(), LOG));
			}

			jobExecutionResult = jobExecutionResultFuture.get();
			System.out.println(jobExecutionResult);
		} else {
			jobExecutionResult = new DetachedJobExecutionResult(jobClient.getJobID());
		}

		return jobExecutionResult;
	}

	@Override
	public JobClient executeAsync(String jobName) throws Exception {
		validateAllowedExecution();
		final JobClient jobClient = super.executeAsync(jobName);

		if (!suppressSysout) {
			System.out.println("Job has been submitted with JobID " + jobClient.getJobID());
		}

		return jobClient;
	}

	private void validateAllowedExecution() {
		if (enforceSingleJobExecution && jobCounter > 0) {
			throw new FlinkRuntimeException("Cannot have more than one execute() or executeAsync() call in a single environment.");
		}
		jobCounter++;
	}

	@Override
	public String toString() {
		return "Context Environment (parallelism = " + (getParallelism() == ExecutionConfig.PARALLELISM_DEFAULT ? "default" : getParallelism()) + ")";
	}

	// --------------------------------------------------------------------------------------------

	public static void setAsContext(
			final PipelineExecutorServiceLoader executorServiceLoader,
			final Configuration configuration,
			final ClassLoader userCodeClassLoader,
			final boolean enforceSingleJobExecution,
			final boolean suppressSysout) {
		ExecutionEnvironmentFactory factory = () -> new ContextEnvironment(
			executorServiceLoader,
			configuration,
			userCodeClassLoader,
			enforceSingleJobExecution,
			suppressSysout);
		initializeContextEnvironment(factory);
	}

	public static void unsetAsContext() {
		resetContextEnvironment();
	}
}
