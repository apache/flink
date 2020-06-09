/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.client.program;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.core.execution.DetachedJobExecutionResult;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.JobListener;
import org.apache.flink.core.execution.PipelineExecutorServiceLoader;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironmentFactory;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.ShutdownHookUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Special {@link StreamExecutionEnvironment} that will be used in cases where the CLI client or
 * testing utilities create a {@link StreamExecutionEnvironment} that should be used when
 * {@link StreamExecutionEnvironment#getExecutionEnvironment()} is called.
 */
@PublicEvolving
public class StreamContextEnvironment extends StreamExecutionEnvironment {

	private static final Logger LOG = LoggerFactory.getLogger(ExecutionEnvironment.class);

	private final boolean suppressSysout;

	private final boolean enforceSingleJobExecution;

	private int jobCounter;

	public StreamContextEnvironment(
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
	public JobExecutionResult execute(StreamGraph streamGraph) throws Exception {
		final JobClient jobClient = executeAsync(streamGraph);
		final List<JobListener> jobListeners = getJobListeners();

		try {
			final JobExecutionResult  jobExecutionResult = getJobExecutionResult(jobClient);
			jobListeners.forEach(jobListener ->
					jobListener.onJobExecuted(jobExecutionResult, null));
			return jobExecutionResult;
		} catch (Throwable t) {
			jobListeners.forEach(jobListener ->
					jobListener.onJobExecuted(null, ExceptionUtils.stripExecutionException(t)));
			ExceptionUtils.rethrowException(t);

			// never reached, only make javac happy
			return null;
		}
	}

	private JobExecutionResult getJobExecutionResult(final JobClient jobClient) throws Exception {
		checkNotNull(jobClient);

		JobExecutionResult jobExecutionResult;
		if (getConfiguration().getBoolean(DeploymentOptions.ATTACHED)) {
			CompletableFuture<JobExecutionResult> jobExecutionResultFuture =
					jobClient.getJobExecutionResult(getUserClassloader());

			if (getConfiguration().getBoolean(DeploymentOptions.SHUTDOWN_IF_ATTACHED)) {
				Thread shutdownHook = ShutdownHookUtil.addShutdownHook(
						() -> {
							// wait a smidgen to allow the async request to go through before
							// the jvm exits
							jobClient.cancel().get(1, TimeUnit.SECONDS);
						},
						StreamContextEnvironment.class.getSimpleName(),
						LOG);
				jobExecutionResultFuture.whenComplete((ignored, throwable) ->
						ShutdownHookUtil.removeShutdownHook(
							shutdownHook, StreamContextEnvironment.class.getSimpleName(), LOG));
			}

			jobExecutionResult = jobExecutionResultFuture.get();
			System.out.println(jobExecutionResult);
		} else {
			jobExecutionResult = new DetachedJobExecutionResult(jobClient.getJobID());
		}

		return jobExecutionResult;
	}

	@Override
	public JobClient executeAsync(StreamGraph streamGraph) throws Exception {
		validateAllowedExecution();
		final JobClient jobClient = super.executeAsync(streamGraph);

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

	// --------------------------------------------------------------------------------------------

	public static void setAsContext(
			final PipelineExecutorServiceLoader executorServiceLoader,
			final Configuration configuration,
			final ClassLoader userCodeClassLoader,
			final boolean enforceSingleJobExecution,
			final boolean suppressSysout) {
		StreamExecutionEnvironmentFactory factory = () -> new StreamContextEnvironment(
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
