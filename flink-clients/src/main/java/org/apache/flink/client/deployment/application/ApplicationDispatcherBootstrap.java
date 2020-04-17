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
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.core.execution.PipelineExecutorServiceLoader;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.dispatcher.AbstractDispatcherBootstrap;
import org.apache.flink.runtime.dispatcher.Dispatcher;
import org.apache.flink.runtime.dispatcher.DispatcherBootstrap;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
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

	private final CompletableFuture<Void> applicationStatusFuture;

	private CompletableFuture<Void> applicationCompletionFuture;

	private ScheduledFuture<List<JobID>> applicationExecutionTask;

	public ApplicationDispatcherBootstrap(
			final PackagedProgram application,
			final Collection<JobGraph> recoveredJobs,
			final Configuration configuration) {
		this.configuration = checkNotNull(configuration);
		this.recoveredJobs = checkNotNull(recoveredJobs);
		this.application = checkNotNull(application);

		this.applicationStatusFuture = new CompletableFuture<>();
	}

	public CompletableFuture<Void> getApplicationStatusFuture() {
		return applicationStatusFuture;
	}

	@Override
	public void initialize(final Dispatcher dispatcher) {
		checkNotNull(dispatcher);
		launchRecoveredJobGraphs(dispatcher, recoveredJobs);

		runApplicationAndShutdownClusterAsync(
				dispatcher,
				dispatcher.getRpcService().getScheduledExecutor());
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
	CompletableFuture<Acknowledge> runApplicationAndShutdownClusterAsync(
			final DispatcherGateway dispatcher,
			final ScheduledExecutor executor) {

		applicationCompletionFuture = executeApplicationAsync(dispatcher, executor)
				.thenCompose(applicationIds -> getApplicationResult(dispatcher, applicationIds, executor));

		applicationCompletionFuture
				.whenComplete((r, t) -> {
					if (t != null) {
						LOG.warn("Application FAILED: ", t);
						applicationStatusFuture.completeExceptionally(t);
					} else {
						LOG.info("Application completed SUCCESSFULLY");
						applicationStatusFuture.complete(null);
					}
				});

		return applicationStatusFuture.handle((r, t) -> dispatcher.shutDownCluster().join());
	}

	private CompletableFuture<List<JobID>> executeApplicationAsync(
			final DispatcherGateway dispatcher,
			final ScheduledExecutor executor) {
		final CompletableFuture<List<JobID>> applicationExecutionFuture = new CompletableFuture<>();
		applicationExecutionTask = executor.schedule(
				() -> tryRunApplicationAndGetJobIDs(applicationExecutionFuture, dispatcher, executor, true),
				0L,
				TimeUnit.MILLISECONDS);
		return applicationExecutionFuture;
	}

	@VisibleForTesting
	List<JobID> tryRunApplicationAndGetJobIDs(
			final CompletableFuture<List<JobID>> applicationExecutionFuture,
			final DispatcherGateway dispatcher,
			final ScheduledExecutor executor,
			final boolean enforceSingleJobExecution) {
		try {
			final List<JobID> applicationJobIds =
					runApplicationAndGetJobIDs(dispatcher, executor, enforceSingleJobExecution);
			applicationExecutionFuture.complete(applicationJobIds);
			return applicationJobIds;
		} catch (Throwable t) {
			applicationExecutionFuture.completeExceptionally(t);
		}
		return null;
	}

	private List<JobID> runApplicationAndGetJobIDs(
			final DispatcherGateway dispatcher,
			final ScheduledExecutor executor,
			final boolean enforceSingleJobExecution) throws ApplicationExecutionException {
		final List<JobID> applicationJobIds = runApplication(
				dispatcher, application, configuration, executor, enforceSingleJobExecution);
		if (applicationJobIds.isEmpty()) {
			throw new ApplicationExecutionException("The application contains no execute() calls.");
		}
		return applicationJobIds;
	}

	private List<JobID> runApplication(
			final DispatcherGateway dispatcherGateway,
			final PackagedProgram program,
			final Configuration configuration,
			final ScheduledExecutor executor,
			final boolean enforceSingleJobExecution) {

		final List<JobID> applicationJobIds =
				new ArrayList<>(getRecoveredJobIds(recoveredJobs));

		final PipelineExecutorServiceLoader executorServiceLoader =
				new EmbeddedExecutorServiceLoader(applicationJobIds, dispatcherGateway, executor);

		try {
			ClientUtils.executeProgram(executorServiceLoader, configuration, program, enforceSingleJobExecution, true);
		} catch (ProgramInvocationException e) {
			LOG.warn("Could not execute application: ", e);
			throw new CompletionException("Could not execute application.", e);
		}

		return applicationJobIds;
	}

	@VisibleForTesting
	CompletableFuture<Void> getApplicationResult(
			final DispatcherGateway dispatcherGateway,
			final Collection<JobID> applicationJobIds,
			final ScheduledExecutor executor) {
		final CompletableFuture<?>[] jobResultFutures = applicationJobIds
				.stream()
				.map(jobId -> getJobResult(dispatcherGateway, jobId, executor))
				.toArray(CompletableFuture<?>[]::new);

		final CompletableFuture<Void> allStatusFutures = CompletableFuture.allOf(jobResultFutures);
		Stream.of(jobResultFutures)
				.forEach(f -> f.exceptionally(e -> {
					allStatusFutures.completeExceptionally(e);
					return null;
				}));
		return allStatusFutures;
	}

	private CompletableFuture<Void> getJobResult(
			final DispatcherGateway dispatcherGateway,
			final JobID jobId,
			final ScheduledExecutor scheduledExecutor) {

		final Time timeout = Time.milliseconds(configuration.get(ExecutionOptions.EMBEDDED_RPC_TIMEOUT).toMillis());
		final Time retryPeriod = Time.milliseconds(configuration.get(ExecutionOptions.EMBEDDED_RPC_RETRY_PERIOD).toMillis());

		final CompletableFuture<Void> jobFuture = new CompletableFuture<>();

		JobStatusPollingUtils.getJobResult(dispatcherGateway, jobId, scheduledExecutor, timeout, retryPeriod)
				.whenComplete((result, throwable) -> {
					if (throwable != null) {
						LOG.warn("Job {} FAILED: {}", jobId, throwable);
						jobFuture.completeExceptionally(throwable);
					} else {
						final Optional<SerializedThrowable> optionalThrowable = result.getSerializedThrowable();
						if (optionalThrowable.isPresent()) {
							final SerializedThrowable t = optionalThrowable.get();
							LOG.warn("Job {} FAILED: {}", jobId, t.getFullStringifiedStackTrace());
							jobFuture.completeExceptionally(t);
						} else {
							jobFuture.complete(null);
						}
					}
				});
		return jobFuture;
	}

	private List<JobID> getRecoveredJobIds(final Collection<JobGraph> recoveredJobs) {
		return recoveredJobs
				.stream()
				.map(JobGraph::getJobID)
				.collect(Collectors.toList());
	}
}
