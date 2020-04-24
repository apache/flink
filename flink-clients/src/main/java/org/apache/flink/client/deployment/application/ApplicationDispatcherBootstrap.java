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

package org.apache.flink.client.deployment.application;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.client.ClientUtils;
import org.apache.flink.client.deployment.application.executors.EmbeddedExecutor;
import org.apache.flink.client.deployment.application.executors.EmbeddedExecutorServiceLoader;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.core.execution.PipelineExecutorServiceLoader;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.dispatcher.AbstractDispatcherBootstrap;
import org.apache.flink.runtime.dispatcher.Dispatcher;
import org.apache.flink.runtime.dispatcher.DispatcherBootstrap;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.util.SerializedThrowable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link DispatcherBootstrap} used for running the user's {@code main()} in "Application Mode" (see FLIP-85).
 *
 * <p>This dispatcher bootstrap submits the recovered {@link JobGraph job graphs} for re-execution
 * (in case of recovery from a failure), and then submits the remaining jobs of the application for execution.
 *
 * <p>To achieve this, it works in conjunction with the
 * {@link EmbeddedExecutor EmbeddedExecutor} which decides
 * if it should submit a job for execution (in case of a new job) or the job was already recovered and is running.
 */
@Internal
public class ApplicationDispatcherBootstrap extends AbstractDispatcherBootstrap {

	private static final Logger LOG = LoggerFactory.getLogger(ApplicationDispatcherBootstrap.class);

	private final PackagedProgram application;

	private final Collection<JobGraph> recoveredJobs;

	private final Configuration configuration;

	private CompletableFuture<Void> applicationCompletionFuture;

	private ScheduledFuture<?> applicationExecutionTask;

	public ApplicationDispatcherBootstrap(
			final PackagedProgram application,
			final Collection<JobGraph> recoveredJobs,
			final Configuration configuration) {
		this.configuration = checkNotNull(configuration);
		this.recoveredJobs = checkNotNull(recoveredJobs);
		this.application = checkNotNull(application);
	}

	@Override
	public void initialize(final Dispatcher dispatcher, ScheduledExecutor scheduledExecutor) {
		checkNotNull(dispatcher);
		launchRecoveredJobGraphs(dispatcher, recoveredJobs);

		runApplicationAndShutdownClusterAsync(
				dispatcher,
				scheduledExecutor);
	}

	@Override
	public void stop() {
		if (applicationExecutionTask != null) {
			applicationExecutionTask.cancel(true);
		}

		if (applicationCompletionFuture != null) {
			applicationCompletionFuture.cancel(true);
		}
	}

	@VisibleForTesting
	ScheduledFuture<?> getApplicationExecutionFuture() {
		return applicationExecutionTask;
	}

	/**
	 * Runs the user program entrypoint using {@link #runApplicationAsync(DispatcherGateway,
	 * ScheduledExecutor, boolean)} and shuts down the given dispatcher when the application
	 * completes (either succesfully or in case of failure).
	 */
	@VisibleForTesting
	CompletableFuture<Acknowledge> runApplicationAndShutdownClusterAsync(
			final DispatcherGateway dispatcher,
			final ScheduledExecutor scheduledExecutor) {

		applicationCompletionFuture = runApplicationAsync(dispatcher, scheduledExecutor, false);

		return applicationCompletionFuture
				.handle((r, t) -> {
					if (t != null) {
						LOG.warn("Application FAILED: ", t);
					} else {
						LOG.info("Application completed SUCCESSFULLY");
					}
					return dispatcher.shutDownCluster();
				})
				.thenCompose(Function.identity());
	}

	/**
	 * Runs the user program entrypoint by scheduling a task on the given {@code scheduledExecutor}.
	 * The returned {@link CompletableFuture} completes when all jobs of the user application
	 * succeeded. if any of them fails, or if job submission fails.
	 */
	@VisibleForTesting
	CompletableFuture<Void> runApplicationAsync(
			final DispatcherGateway dispatcher,
			final ScheduledExecutor scheduledExecutor,
			final boolean enforceSingleJobExecution) {
		final CompletableFuture<List<JobID>> applicationExecutionFuture = new CompletableFuture<>();

		// we need to hand in a future as return value because we need to get those JobIs out
		// from the scheduled task that executes the user program
		applicationExecutionTask = scheduledExecutor.schedule(
				() -> runApplicationEntrypoint(
						applicationExecutionFuture,
						dispatcher,
						scheduledExecutor,
						enforceSingleJobExecution),
				0L,
				TimeUnit.MILLISECONDS);

		return applicationExecutionFuture.thenCompose(
				jobIds -> getApplicationResult(dispatcher, jobIds, scheduledExecutor));
	}

	/**
	 * Runs the user program entrypoint and completes the given {@code jobIdsFuture} with the {@link
	 * JobID JobIDs} of the submitted jobs.
	 *
	 * <p>This should be executed in a separate thread (or task).
	 */
	private void runApplicationEntrypoint(
			final CompletableFuture<List<JobID>> jobIdsFuture,
			final DispatcherGateway dispatcher,
			final ScheduledExecutor scheduledExecutor,
			final boolean enforceSingleJobExecution) {
		try {
			final List<JobID> applicationJobIds =
					new ArrayList<>(getRecoveredJobIds(recoveredJobs));

			final PipelineExecutorServiceLoader executorServiceLoader =
					new EmbeddedExecutorServiceLoader(
							applicationJobIds, dispatcher, scheduledExecutor);

			ClientUtils.executeProgram(
					executorServiceLoader,
					configuration,
					application,
					enforceSingleJobExecution,
					true /* suppress sysout */);

			if (applicationJobIds.isEmpty()) {
				jobIdsFuture.completeExceptionally(
						new ApplicationExecutionException(
								"The application contains no execute() calls."));
			} else {
				jobIdsFuture.complete(applicationJobIds);
			}
		} catch (Throwable t) {
			jobIdsFuture.completeExceptionally(
					new ApplicationExecutionException("Could not execute application.", t));
		}
	}

	private CompletableFuture<Void> getApplicationResult(
			final DispatcherGateway dispatcherGateway,
			final Collection<JobID> applicationJobIds,
			final ScheduledExecutor executor) {
		final CompletableFuture<?>[] jobResultFutures = applicationJobIds
				.stream()
				.map(jobId ->
						unwrapJobResultException(getJobResult(dispatcherGateway, jobId, executor)))
				.toArray(CompletableFuture<?>[]::new);

		final CompletableFuture<Void> allStatusFutures = CompletableFuture.allOf(jobResultFutures);
		Stream.of(jobResultFutures)
				.forEach(f -> f.exceptionally(e -> {
					allStatusFutures.completeExceptionally(e);
					return null;
				}));
		return allStatusFutures;
	}

	private CompletableFuture<JobResult> getJobResult(
			final DispatcherGateway dispatcherGateway,
			final JobID jobId,
			final ScheduledExecutor scheduledExecutor) {

		final Time timeout = Time.milliseconds(configuration.get(ExecutionOptions.EMBEDDED_RPC_TIMEOUT).toMillis());
		final Time retryPeriod = Time.milliseconds(configuration.get(ExecutionOptions.EMBEDDED_RPC_RETRY_PERIOD).toMillis());

		return JobStatusPollingUtils.getJobResult(
						dispatcherGateway, jobId, scheduledExecutor, timeout, retryPeriod);
	}

	/**
	 * If the given {@link JobResult} indicates success, this passes through the {@link JobResult}.
	 * Otherwise, this returns a future that is finished exceptionally (potentially with an
	 * exception from the {@link JobResult}.
	 */
	private CompletableFuture<JobResult> unwrapJobResultException(
			CompletableFuture<JobResult> jobResult) {
		return jobResult.thenApply(result -> {
			if (result.isSuccess()) {
				return result;
			}

			Optional<SerializedThrowable> serializedThrowable = result.getSerializedThrowable();
			if (serializedThrowable.isPresent()) {
				Throwable throwable =
						serializedThrowable
								.get()
								.deserializeError(application.getUserCodeClassLoader());
				throw new CompletionException(throwable);
			}
			throw new RuntimeException("Job execution failed for unknown reason.");
		});
	}

	private List<JobID> getRecoveredJobIds(final Collection<JobGraph> recoveredJobs) {
		return recoveredJobs
				.stream()
				.map(JobGraph::getJobID)
				.collect(Collectors.toList());
	}
}
