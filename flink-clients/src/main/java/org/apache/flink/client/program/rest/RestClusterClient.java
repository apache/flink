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

package org.apache.flink.client.program.rest;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.api.common.accumulators.AccumulatorHelper;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.NewClusterClient;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.client.program.rest.retry.ExponentialWaitStrategy;
import org.apache.flink.client.program.rest.retry.WaitStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.client.JobSubmissionException;
import org.apache.flink.runtime.clusterframework.messages.GetClusterStatusResponse;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.concurrent.ScheduledExecutorServiceAdapter;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalException;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rest.FileUpload;
import org.apache.flink.runtime.rest.RestClient;
import org.apache.flink.runtime.rest.handler.async.AsynchronousOperationInfo;
import org.apache.flink.runtime.rest.handler.async.TriggerResponse;
import org.apache.flink.runtime.rest.handler.job.rescaling.RescalingStatusHeaders;
import org.apache.flink.runtime.rest.handler.job.rescaling.RescalingStatusMessageParameters;
import org.apache.flink.runtime.rest.handler.job.rescaling.RescalingTriggerHeaders;
import org.apache.flink.runtime.rest.handler.job.rescaling.RescalingTriggerMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.EmptyResponseBody;
import org.apache.flink.runtime.rest.messages.JobAccumulatorsHeaders;
import org.apache.flink.runtime.rest.messages.JobAccumulatorsInfo;
import org.apache.flink.runtime.rest.messages.JobAccumulatorsMessageParameters;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.JobTerminationHeaders;
import org.apache.flink.runtime.rest.messages.JobTerminationMessageParameters;
import org.apache.flink.runtime.rest.messages.JobsOverviewHeaders;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.TerminationModeQueryParameter;
import org.apache.flink.runtime.rest.messages.TriggerId;
import org.apache.flink.runtime.rest.messages.cluster.ShutdownHeaders;
import org.apache.flink.runtime.rest.messages.job.JobDetailsHeaders;
import org.apache.flink.runtime.rest.messages.job.JobDetailsInfo;
import org.apache.flink.runtime.rest.messages.job.JobExecutionResultHeaders;
import org.apache.flink.runtime.rest.messages.job.JobSubmitHeaders;
import org.apache.flink.runtime.rest.messages.job.JobSubmitRequestBody;
import org.apache.flink.runtime.rest.messages.job.JobSubmitResponseBody;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointDisposalRequest;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointDisposalStatusHeaders;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointDisposalStatusMessageParameters;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointDisposalTriggerHeaders;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointInfo;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointStatusHeaders;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointStatusMessageParameters;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointTriggerHeaders;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointTriggerMessageParameters;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointTriggerRequestBody;
import org.apache.flink.runtime.rest.messages.queue.AsynchronouslyCreatedResource;
import org.apache.flink.runtime.rest.messages.queue.QueueStatus;
import org.apache.flink.runtime.rest.util.RestClientException;
import org.apache.flink.runtime.rest.util.RestConstants;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.runtime.util.LeaderConnectionInfo;
import org.apache.flink.runtime.util.LeaderRetrievalUtils;
import org.apache.flink.runtime.webmonitor.retriever.LeaderRetriever;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.OptionalFailure;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.CheckedSupplier;

import org.apache.flink.shaded.netty4.io.netty.channel.ConnectTimeoutException;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * A {@link ClusterClient} implementation that communicates via HTTP REST requests.
 */
public class RestClusterClient<T> extends ClusterClient<T> implements NewClusterClient {

	private final RestClusterClientConfiguration restClusterClientConfiguration;

	private final RestClient restClient;

	private final ExecutorService executorService = Executors.newFixedThreadPool(4, new ExecutorThreadFactory("Flink-RestClusterClient-IO"));

	private final WaitStrategy waitStrategy;

	private final T clusterId;

	private final LeaderRetrievalService webMonitorRetrievalService;

	private final LeaderRetrievalService dispatcherRetrievalService;

	private final LeaderRetriever webMonitorLeaderRetriever = new LeaderRetriever();

	private final LeaderRetriever dispatcherLeaderRetriever = new LeaderRetriever();

	/** ExecutorService to run operations that can be retried on exceptions. */
	private ScheduledExecutorService retryExecutorService;

	public RestClusterClient(Configuration config, T clusterId) throws Exception {
		this(
			config,
			null,
			clusterId,
			new ExponentialWaitStrategy(10L, 2000L),
			null);
	}

	public RestClusterClient(
			Configuration config,
			T clusterId,
			LeaderRetrievalService webMonitorRetrievalService) throws Exception {
		this(
			config,
			null,
			clusterId,
			new ExponentialWaitStrategy(10L, 2000L),
			webMonitorRetrievalService);
	}

	@VisibleForTesting
	RestClusterClient(
			Configuration configuration,
			@Nullable RestClient restClient,
			T clusterId,
			WaitStrategy waitStrategy,
			@Nullable LeaderRetrievalService webMonitorRetrievalService) throws Exception {
		super(configuration);
		this.restClusterClientConfiguration = RestClusterClientConfiguration.fromConfiguration(configuration);

		if (restClient != null) {
			this.restClient = restClient;
		} else {
			this.restClient = new RestClient(restClusterClientConfiguration.getRestClientConfiguration(), executorService);
		}

		this.waitStrategy = Preconditions.checkNotNull(waitStrategy);
		this.clusterId = Preconditions.checkNotNull(clusterId);

		if (webMonitorRetrievalService == null) {
			this.webMonitorRetrievalService = highAvailabilityServices.getWebMonitorLeaderRetriever();
		} else {
			this.webMonitorRetrievalService = webMonitorRetrievalService;
		}
		this.dispatcherRetrievalService = highAvailabilityServices.getDispatcherLeaderRetriever();
		this.retryExecutorService = Executors.newSingleThreadScheduledExecutor(new ExecutorThreadFactory("Flink-RestClusterClient-Retry"));
		startLeaderRetrievers();
	}

	private void startLeaderRetrievers() throws Exception {
		this.webMonitorRetrievalService.start(webMonitorLeaderRetriever);
		this.dispatcherRetrievalService.start(dispatcherLeaderRetriever);
	}

	@Override
	public void shutdown() {
		ExecutorUtils.gracefulShutdown(restClusterClientConfiguration.getRetryDelay(), TimeUnit.MILLISECONDS, retryExecutorService);

		this.restClient.shutdown(Time.seconds(5));
		ExecutorUtils.gracefulShutdown(5, TimeUnit.SECONDS, this.executorService);

		try {
			webMonitorRetrievalService.stop();
		} catch (Exception e) {
			log.error("An error occurred during stopping the webMonitorRetrievalService", e);
		}

		try {
			dispatcherRetrievalService.stop();
		} catch (Exception e) {
			log.error("An error occurred during stopping the dispatcherLeaderRetriever", e);
		}

		try {
			// we only call this for legacy reasons to shutdown components that are started in the ClusterClient constructor
			super.shutdown();
		} catch (Exception e) {
			log.error("An error occurred during the client shutdown.", e);
		}
	}

	@Override
	public JobSubmissionResult submitJob(JobGraph jobGraph, ClassLoader classLoader) throws ProgramInvocationException {
		log.info("Submitting job {} (detached: {}).", jobGraph.getJobID(), isDetached());

		final CompletableFuture<JobSubmissionResult> jobSubmissionFuture = submitJob(jobGraph);

		if (isDetached()) {
			try {
				return jobSubmissionFuture.get();
			} catch (Exception e) {
				throw new ProgramInvocationException("Could not submit job",
					jobGraph.getJobID(), ExceptionUtils.stripExecutionException(e));
			}
		} else {
			final CompletableFuture<JobResult> jobResultFuture = jobSubmissionFuture.thenCompose(
				ignored -> requestJobResult(jobGraph.getJobID()));

			final JobResult jobResult;
			try {
				jobResult = jobResultFuture.get();
			} catch (Exception e) {
				throw new ProgramInvocationException("Could not retrieve the execution result.",
					jobGraph.getJobID(), ExceptionUtils.stripExecutionException(e));
			}

			try {
				this.lastJobExecutionResult = jobResult.toJobExecutionResult(classLoader);
				return lastJobExecutionResult;
			} catch (JobExecutionException e) {
				throw new ProgramInvocationException("Job failed.", jobGraph.getJobID(), e);
			} catch (IOException | ClassNotFoundException e) {
				throw new ProgramInvocationException("Job failed.", jobGraph.getJobID(), e);
			}
		}
	}

	@Override
	public CompletableFuture<JobStatus> getJobStatus(JobID jobId) {
		JobDetailsHeaders detailsHeaders = JobDetailsHeaders.getInstance();
		final JobMessageParameters  params = new JobMessageParameters();
		params.jobPathParameter.resolve(jobId);

		CompletableFuture<JobDetailsInfo> responseFuture = sendRequest(
			detailsHeaders,
			params);

		return responseFuture.thenApply(JobDetailsInfo::getJobStatus);
	}

	/**
	 * Requests the {@link JobResult} for the given {@link JobID}. The method retries multiple
	 * times to poll the {@link JobResult} before giving up.
	 *
	 * @param jobId specifying the job for which to retrieve the {@link JobResult}
	 * @return Future which is completed with the {@link JobResult} once the job has completed or
	 * with a failure if the {@link JobResult} could not be retrieved.
	 */
	@Override
	public CompletableFuture<JobResult> requestJobResult(@Nonnull JobID jobId) {
		return pollResourceAsync(
			() -> {
				final JobMessageParameters messageParameters = new JobMessageParameters();
				messageParameters.jobPathParameter.resolve(jobId);
				return sendRequest(
					JobExecutionResultHeaders.getInstance(),
					messageParameters);
			});
	}

	/**
	 * Submits the given {@link JobGraph} to the dispatcher.
	 *
	 * @param jobGraph to submit
	 * @return Future which is completed with the submission response
	 */
	@Override
	public CompletableFuture<JobSubmissionResult> submitJob(@Nonnull JobGraph jobGraph) {
		// we have to enable queued scheduling because slot will be allocated lazily
		jobGraph.setAllowQueuedScheduling(true);

		CompletableFuture<java.nio.file.Path> jobGraphFileFuture = CompletableFuture.supplyAsync(() -> {
			try {
				final java.nio.file.Path jobGraphFile = Files.createTempFile("flink-jobgraph", ".bin");
				try (ObjectOutputStream objectOut = new ObjectOutputStream(Files.newOutputStream(jobGraphFile))) {
					objectOut.writeObject(jobGraph);
				}
				return jobGraphFile;
			} catch (IOException e) {
				throw new CompletionException(new FlinkException("Failed to serialize JobGraph.", e));
			}
		}, executorService);

		CompletableFuture<Tuple2<JobSubmitRequestBody, Collection<FileUpload>>> requestFuture = jobGraphFileFuture.thenApply(jobGraphFile -> {
			List<String> jarFileNames = new ArrayList<>(8);
			List<JobSubmitRequestBody.DistributedCacheFile> artifactFileNames = new ArrayList<>(8);
			Collection<FileUpload> filesToUpload = new ArrayList<>(8);

			filesToUpload.add(new FileUpload(jobGraphFile, RestConstants.CONTENT_TYPE_BINARY));

			for (Path jar : jobGraph.getUserJars()) {
				jarFileNames.add(jar.getName());
				filesToUpload.add(new FileUpload(Paths.get(jar.toUri()), RestConstants.CONTENT_TYPE_JAR));
			}

			for (Map.Entry<String, DistributedCache.DistributedCacheEntry> artifacts : jobGraph.getUserArtifacts().entrySet()) {
				artifactFileNames.add(new JobSubmitRequestBody.DistributedCacheFile(artifacts.getKey(), new Path(artifacts.getValue().filePath).getName()));
				filesToUpload.add(new FileUpload(Paths.get(artifacts.getValue().filePath), RestConstants.CONTENT_TYPE_BINARY));
			}

			final JobSubmitRequestBody requestBody = new JobSubmitRequestBody(
				jobGraphFile.getFileName().toString(),
				jarFileNames,
				artifactFileNames);

			return Tuple2.of(requestBody, Collections.unmodifiableCollection(filesToUpload));
		});

		final CompletableFuture<JobSubmitResponseBody> submissionFuture = requestFuture.thenCompose(
			requestAndFileUploads -> sendRetriableRequest(
				JobSubmitHeaders.getInstance(),
				EmptyMessageParameters.getInstance(),
				requestAndFileUploads.f0,
				requestAndFileUploads.f1,
				isConnectionProblemOrServiceUnavailable())
		);

		submissionFuture
			.thenCombine(jobGraphFileFuture, (ignored, jobGraphFile) -> jobGraphFile)
			.thenAccept(jobGraphFile -> {
			try {
				Files.delete(jobGraphFile);
			} catch (IOException e) {
				log.warn("Could not delete temporary file {}.", jobGraphFile, e);
			}
		});

		return submissionFuture
			.thenApply(
				(JobSubmitResponseBody jobSubmitResponseBody) -> new JobSubmissionResult(jobGraph.getJobID()))
			.exceptionally(
				(Throwable throwable) -> {
					throw new CompletionException(new JobSubmissionException(jobGraph.getJobID(), "Failed to submit JobGraph.", ExceptionUtils.stripCompletionException(throwable)));
				});
	}

	@Override
	public void stop(JobID jobID) throws Exception {
		JobTerminationMessageParameters params = new JobTerminationMessageParameters();
		params.jobPathParameter.resolve(jobID);
		params.terminationModeQueryParameter.resolve(Collections.singletonList(TerminationModeQueryParameter.TerminationMode.STOP));
		CompletableFuture<EmptyResponseBody> responseFuture = sendRequest(
			JobTerminationHeaders.getInstance(),
			params);
		responseFuture.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
	}

	@Override
	public void cancel(JobID jobID) throws Exception {
		JobTerminationMessageParameters params = new JobTerminationMessageParameters();
		params.jobPathParameter.resolve(jobID);
		params.terminationModeQueryParameter.resolve(Collections.singletonList(TerminationModeQueryParameter.TerminationMode.CANCEL));
		CompletableFuture<EmptyResponseBody> responseFuture = sendRequest(
			JobTerminationHeaders.getInstance(),
			params);
		responseFuture.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
	}

	@Override
	public String cancelWithSavepoint(JobID jobId, @Nullable String savepointDirectory) throws Exception {
		return triggerSavepoint(jobId, savepointDirectory, true).get();
	}

	@Override
	public CompletableFuture<String> triggerSavepoint(
			final JobID jobId,
			final @Nullable String savepointDirectory) {
		return triggerSavepoint(jobId, savepointDirectory, false);
	}

	private CompletableFuture<String> triggerSavepoint(
			final JobID jobId,
			final @Nullable String savepointDirectory,
			final boolean cancelJob) {
		final SavepointTriggerHeaders savepointTriggerHeaders = SavepointTriggerHeaders.getInstance();
		final SavepointTriggerMessageParameters savepointTriggerMessageParameters =
			savepointTriggerHeaders.getUnresolvedMessageParameters();
		savepointTriggerMessageParameters.jobID.resolve(jobId);

		final CompletableFuture<TriggerResponse> responseFuture = sendRequest(
			savepointTriggerHeaders,
			savepointTriggerMessageParameters,
			new SavepointTriggerRequestBody(savepointDirectory, cancelJob));

		return responseFuture.thenCompose(savepointTriggerResponseBody -> {
			final TriggerId savepointTriggerId = savepointTriggerResponseBody.getTriggerId();
			return pollSavepointAsync(jobId, savepointTriggerId);
		}).thenApply(savepointInfo -> {
			if (savepointInfo.getFailureCause() != null) {
				throw new CompletionException(savepointInfo.getFailureCause());
			}
			return savepointInfo.getLocation();
		});
	}

	@Override
	public Map<String, OptionalFailure<Object>> getAccumulators(final JobID jobID, ClassLoader loader) throws Exception {
		final JobAccumulatorsHeaders accumulatorsHeaders = JobAccumulatorsHeaders.getInstance();
		final JobAccumulatorsMessageParameters accMsgParams = accumulatorsHeaders.getUnresolvedMessageParameters();
		accMsgParams.jobPathParameter.resolve(jobID);
		accMsgParams.includeSerializedAccumulatorsParameter.resolve(Collections.singletonList(true));

		CompletableFuture<JobAccumulatorsInfo> responseFuture = sendRequest(
			accumulatorsHeaders,
			accMsgParams);

		Map<String, OptionalFailure<Object>> result = Collections.emptyMap();

		try {
			result = responseFuture.thenApply((JobAccumulatorsInfo accumulatorsInfo) -> {
				try {
					return AccumulatorHelper.deserializeAccumulators(
						accumulatorsInfo.getSerializedUserAccumulators(),
						loader);
				} catch (Exception e) {
					throw new CompletionException(
						new FlinkException(
							String.format("Deserialization of accumulators for job %s failed.", jobID),
							e));
				}
			}).get(timeout.toMillis(), TimeUnit.MILLISECONDS);
		} catch (ExecutionException ee) {
			ExceptionUtils.rethrowException(ExceptionUtils.stripExecutionException(ee));
		}

		return result;
	}

	private CompletableFuture<SavepointInfo> pollSavepointAsync(
			final JobID jobId,
			final TriggerId triggerID) {
		return pollResourceAsync(() -> {
			final SavepointStatusHeaders savepointStatusHeaders = SavepointStatusHeaders.getInstance();
			final SavepointStatusMessageParameters savepointStatusMessageParameters =
				savepointStatusHeaders.getUnresolvedMessageParameters();
			savepointStatusMessageParameters.jobIdPathParameter.resolve(jobId);
			savepointStatusMessageParameters.triggerIdPathParameter.resolve(triggerID);
			return sendRequest(
				savepointStatusHeaders,
				savepointStatusMessageParameters);
		});
	}

	@Override
	public CompletableFuture<Collection<JobStatusMessage>> listJobs() {
		return sendRequest(JobsOverviewHeaders.getInstance())
			.thenApply(
				(multipleJobsDetails) -> multipleJobsDetails
					.getJobs()
					.stream()
					.map(detail -> new JobStatusMessage(
						detail.getJobId(),
						detail.getJobName(),
						detail.getStatus(),
						detail.getStartTime()))
					.collect(Collectors.toList()));
	}

	@Override
	public T getClusterId() {
		return clusterId;
	}

	@Override
	public LeaderConnectionInfo getClusterConnectionInfo() throws LeaderRetrievalException {
		return LeaderRetrievalUtils.retrieveLeaderConnectionInfo(
			highAvailabilityServices.getDispatcherLeaderRetriever(),
			timeout);
	}

	@Override
	public CompletableFuture<Acknowledge> rescaleJob(JobID jobId, int newParallelism) {

		final RescalingTriggerHeaders rescalingTriggerHeaders = RescalingTriggerHeaders.getInstance();
		final RescalingTriggerMessageParameters rescalingTriggerMessageParameters = rescalingTriggerHeaders.getUnresolvedMessageParameters();
		rescalingTriggerMessageParameters.jobPathParameter.resolve(jobId);
		rescalingTriggerMessageParameters.rescalingParallelismQueryParameter.resolve(Collections.singletonList(newParallelism));

		final CompletableFuture<TriggerResponse> rescalingTriggerResponseFuture = sendRequest(
			rescalingTriggerHeaders,
			rescalingTriggerMessageParameters);

		final CompletableFuture<AsynchronousOperationInfo> rescalingOperationFuture = rescalingTriggerResponseFuture.thenCompose(
			(TriggerResponse triggerResponse) -> {
				final TriggerId triggerId = triggerResponse.getTriggerId();
				final RescalingStatusHeaders rescalingStatusHeaders = RescalingStatusHeaders.getInstance();
				final RescalingStatusMessageParameters rescalingStatusMessageParameters = rescalingStatusHeaders.getUnresolvedMessageParameters();

				rescalingStatusMessageParameters.jobPathParameter.resolve(jobId);
				rescalingStatusMessageParameters.triggerIdPathParameter.resolve(triggerId);

				return pollResourceAsync(
					() -> sendRequest(
						rescalingStatusHeaders,
						rescalingStatusMessageParameters));
			});

		return rescalingOperationFuture.thenApply(
			(AsynchronousOperationInfo asynchronousOperationInfo) -> {
				if (asynchronousOperationInfo.getFailureCause() == null) {
					return Acknowledge.get();
				} else {
					throw new CompletionException(asynchronousOperationInfo.getFailureCause());
				}
			});
	}

	@Override
	public CompletableFuture<Acknowledge> disposeSavepoint(String savepointPath) {
		final SavepointDisposalRequest savepointDisposalRequest = new SavepointDisposalRequest(savepointPath);

		final CompletableFuture<TriggerResponse> savepointDisposalTriggerFuture = sendRequest(
			SavepointDisposalTriggerHeaders.getInstance(),
			savepointDisposalRequest);

		final CompletableFuture<AsynchronousOperationInfo> savepointDisposalFuture = savepointDisposalTriggerFuture.thenCompose(
			(TriggerResponse triggerResponse) -> {
				final TriggerId triggerId = triggerResponse.getTriggerId();
				final SavepointDisposalStatusHeaders savepointDisposalStatusHeaders = SavepointDisposalStatusHeaders.getInstance();
				final SavepointDisposalStatusMessageParameters savepointDisposalStatusMessageParameters = savepointDisposalStatusHeaders.getUnresolvedMessageParameters();
				savepointDisposalStatusMessageParameters.triggerIdPathParameter.resolve(triggerId);

				return pollResourceAsync(
					() -> sendRequest(
						savepointDisposalStatusHeaders,
						savepointDisposalStatusMessageParameters));
			});

		return savepointDisposalFuture.thenApply(
			(AsynchronousOperationInfo asynchronousOperationInfo) -> {
				if (asynchronousOperationInfo.getFailureCause() == null) {
					return Acknowledge.get();
				} else {
					throw new CompletionException(asynchronousOperationInfo.getFailureCause());
				}
			});
	}

	@Override
	public void shutDownCluster() {
		try {
			sendRequest(ShutdownHeaders.getInstance()).get();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		} catch (ExecutionException e) {
			log.error("Error while shutting down cluster", e);
		}
	}

	/**
	 * Creates a {@code CompletableFuture} that polls a {@code AsynchronouslyCreatedResource} until
	 * its {@link AsynchronouslyCreatedResource#queueStatus() QueueStatus} becomes
	 * {@link QueueStatus.Id#COMPLETED COMPLETED}. The future completes with the result of
	 * {@link AsynchronouslyCreatedResource#resource()}.
	 *
	 * @param resourceFutureSupplier The operation which polls for the
	 *                               {@code AsynchronouslyCreatedResource}.
	 * @param <R>                    The type of the resource.
	 * @param <A>                    The type of the {@code AsynchronouslyCreatedResource}.
	 * @return A {@code CompletableFuture} delivering the resource.
	 */
	private <R, A extends AsynchronouslyCreatedResource<R>> CompletableFuture<R> pollResourceAsync(
			final Supplier<CompletableFuture<A>> resourceFutureSupplier) {
		return pollResourceAsync(resourceFutureSupplier, new CompletableFuture<>(), 0);
	}

	private <R, A extends AsynchronouslyCreatedResource<R>> CompletableFuture<R> pollResourceAsync(
			final Supplier<CompletableFuture<A>> resourceFutureSupplier,
			final CompletableFuture<R> resultFuture,
			final long attempt) {

		resourceFutureSupplier.get().whenComplete((asynchronouslyCreatedResource, throwable) -> {
			if (throwable != null) {
				resultFuture.completeExceptionally(throwable);
			} else {
				if (asynchronouslyCreatedResource.queueStatus().getId() == QueueStatus.Id.COMPLETED) {
					resultFuture.complete(asynchronouslyCreatedResource.resource());
				} else {
					retryExecutorService.schedule(() -> {
						pollResourceAsync(resourceFutureSupplier, resultFuture, attempt + 1);
					}, waitStrategy.sleepTime(attempt), TimeUnit.MILLISECONDS);
				}
			}
		});

		return resultFuture;
	}

	// ======================================
	// Legacy stuff we actually implement
	// ======================================

	@Override
	public boolean hasUserJarsInClassPath(List<URL> userJarFiles) {
		return false;
	}

	@Override
	public void waitForClusterToBeReady() {
		// no op
	}

	@Override
	public String getWebInterfaceURL() {
		try {
			return getWebMonitorBaseUrl().get().toString();
		} catch (InterruptedException | ExecutionException e) {
			ExceptionUtils.checkInterrupted(e);

			log.warn("Could not retrieve the web interface URL for the cluster.", e);
			return "Unknown address.";
		}
	}

	@Override
	public GetClusterStatusResponse getClusterStatus() {
		return null;
	}

	@Override
	public List<String> getNewMessages() {
		return Collections.emptyList();
	}

	@Override
	public int getMaxSlots() {
		return MAX_SLOTS_UNKNOWN;
	}

	//-------------------------------------------------------------------------
	// RestClient Helper
	//-------------------------------------------------------------------------

	private <M extends MessageHeaders<EmptyRequestBody, P, U>, U extends MessageParameters, P extends ResponseBody> CompletableFuture<P>
			sendRequest(M messageHeaders, U messageParameters) {
		return sendRequest(messageHeaders, messageParameters, EmptyRequestBody.getInstance());
	}

	private <M extends MessageHeaders<R, P, EmptyMessageParameters>, R extends RequestBody, P extends ResponseBody> CompletableFuture<P>
			sendRequest(M messageHeaders, R request) {
		return sendRequest(messageHeaders, EmptyMessageParameters.getInstance(), request);
	}

	@VisibleForTesting
	<M extends MessageHeaders<EmptyRequestBody, P, EmptyMessageParameters>, P extends ResponseBody> CompletableFuture<P>
			sendRequest(M messageHeaders) {
		return sendRequest(messageHeaders, EmptyMessageParameters.getInstance(), EmptyRequestBody.getInstance());
	}

	private <M extends MessageHeaders<R, P, U>, U extends MessageParameters, R extends RequestBody, P extends ResponseBody> CompletableFuture<P>
			sendRequest(M messageHeaders, U messageParameters, R request) {
		return sendRetriableRequest(
			messageHeaders, messageParameters, request, isConnectionProblemOrServiceUnavailable());
	}

	private <M extends MessageHeaders<R, P, U>, U extends MessageParameters, R extends RequestBody, P extends ResponseBody> CompletableFuture<P>
			sendRetriableRequest(M messageHeaders, U messageParameters, R request, Predicate<Throwable> retryPredicate) {
		return sendRetriableRequest(messageHeaders, messageParameters, request, Collections.emptyList(), retryPredicate);
	}

	private <M extends MessageHeaders<R, P, U>, U extends MessageParameters, R extends RequestBody, P extends ResponseBody> CompletableFuture<P>
	sendRetriableRequest(M messageHeaders, U messageParameters, R request, Collection<FileUpload> filesToUpload, Predicate<Throwable> retryPredicate) {
		return retry(() -> getWebMonitorBaseUrl().thenCompose(webMonitorBaseUrl -> {
			try {
				return restClient.sendRequest(webMonitorBaseUrl.getHost(), webMonitorBaseUrl.getPort(), messageHeaders, messageParameters, request, filesToUpload);
			} catch (IOException e) {
				throw new CompletionException(e);
			}
		}), retryPredicate);
	}

	private <C> CompletableFuture<C> retry(
			CheckedSupplier<CompletableFuture<C>> operation,
			Predicate<Throwable> retryPredicate) {
		return FutureUtils.retryWithDelay(
			CheckedSupplier.unchecked(operation),
			restClusterClientConfiguration.getRetryMaxAttempts(),
			Time.milliseconds(restClusterClientConfiguration.getRetryDelay()),
			retryPredicate,
			new ScheduledExecutorServiceAdapter(retryExecutorService));
	}

	private static Predicate<Throwable> isConnectionProblemOrServiceUnavailable() {
		return isConnectionProblemException().or(isServiceUnavailable());
	}

	private static Predicate<Throwable> isConnectionProblemException() {
		return (throwable) ->
			ExceptionUtils.findThrowable(throwable, java.net.ConnectException.class).isPresent() ||
				ExceptionUtils.findThrowable(throwable, java.net.SocketTimeoutException.class).isPresent() ||
				ExceptionUtils.findThrowable(throwable, ConnectTimeoutException.class).isPresent() ||
				ExceptionUtils.findThrowable(throwable, IOException.class).isPresent();
	}

	private static Predicate<Throwable> isServiceUnavailable() {
		return httpExceptionCodePredicate(code -> code == HttpResponseStatus.SERVICE_UNAVAILABLE.code());
	}

	private static Predicate<Throwable> httpExceptionCodePredicate(Predicate<Integer> statusCodePredicate) {
		return (throwable) -> ExceptionUtils.findThrowable(throwable, RestClientException.class)
			.map(restClientException -> {
				final int code = restClientException.getHttpResponseStatus().code();
				return statusCodePredicate.test(code);
			})
			.orElse(false);
	}

	@VisibleForTesting
	CompletableFuture<URL> getWebMonitorBaseUrl() {
		return FutureUtils.orTimeout(
				webMonitorLeaderRetriever.getLeaderFuture(),
				restClusterClientConfiguration.getAwaitLeaderTimeout(),
				TimeUnit.MILLISECONDS)
			.thenApplyAsync(leaderAddressSessionId -> {
				final String url = leaderAddressSessionId.f0;
				try {
					return new URL(url);
				} catch (MalformedURLException e) {
					throw new IllegalArgumentException("Could not parse URL from " + url, e);
				}
			}, executorService);
	}
}
