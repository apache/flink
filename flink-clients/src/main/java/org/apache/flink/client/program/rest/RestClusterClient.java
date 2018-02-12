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
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.api.common.accumulators.AccumulatorHelper;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.client.program.rest.retry.ExponentialWaitStrategy;
import org.apache.flink.client.program.rest.retry.WaitStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobClient;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.client.JobSubmissionException;
import org.apache.flink.runtime.clusterframework.messages.GetClusterStatusResponse;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.concurrent.ScheduledExecutorServiceAdapter;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalException;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.rest.RestClient;
import org.apache.flink.runtime.rest.messages.BlobServerPortHeaders;
import org.apache.flink.runtime.rest.messages.BlobServerPortResponseBody;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.EmptyResponseBody;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.JobTerminationHeaders;
import org.apache.flink.runtime.rest.messages.JobTerminationMessageParameters;
import org.apache.flink.runtime.rest.messages.JobsOverviewHeaders;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.TerminationModeQueryParameter;
import org.apache.flink.runtime.rest.messages.job.JobExecutionResultHeaders;
import org.apache.flink.runtime.rest.messages.job.JobSubmitHeaders;
import org.apache.flink.runtime.rest.messages.job.JobSubmitRequestBody;
import org.apache.flink.runtime.rest.messages.job.JobSubmitResponseBody;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointInfo;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointStatusHeaders;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointStatusMessageParameters;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointTriggerHeaders;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointTriggerId;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointTriggerMessageParameters;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointTriggerRequestBody;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointTriggerResponseBody;
import org.apache.flink.runtime.rest.messages.queue.AsynchronouslyCreatedResource;
import org.apache.flink.runtime.rest.messages.queue.QueueStatus;
import org.apache.flink.runtime.rest.util.RestClientException;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.runtime.util.LeaderConnectionInfo;
import org.apache.flink.runtime.util.LeaderRetrievalUtils;
import org.apache.flink.runtime.webmonitor.retriever.LeaderRetriever;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedThrowable;
import org.apache.flink.util.function.CheckedSupplier;

import org.apache.flink.shaded.netty4.io.netty.channel.ConnectTimeoutException;

import akka.actor.AddressFromURIString;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import scala.Option;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A {@link ClusterClient} implementation that communicates via HTTP REST requests.
 */
public class RestClusterClient<T> extends ClusterClient<T> {

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
			new ExponentialWaitStrategy(10L, 2000L));
	}

	@VisibleForTesting
	RestClusterClient(Configuration configuration, @Nullable RestClient restClient, T clusterId, WaitStrategy waitStrategy) throws Exception {
		super(configuration);
		this.restClusterClientConfiguration = RestClusterClientConfiguration.fromConfiguration(configuration);

		if (restClient != null) {
			this.restClient = restClient;
		} else {
			this.restClient = new RestClient(restClusterClientConfiguration.getRestClientConfiguration(), executorService);
		}

		this.waitStrategy = Preconditions.checkNotNull(waitStrategy);
		this.clusterId = Preconditions.checkNotNull(clusterId);

		this.webMonitorRetrievalService = highAvailabilityServices.getWebMonitorLeaderRetriever();
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
		try {
			// we only call this for legacy reasons to shutdown components that are started in the ClusterClient constructor
			super.shutdown();
		} catch (Exception e) {
			log.error("An error occurred during the client shutdown.", e);
		}

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
	}

	@Override
	protected JobSubmissionResult submitJob(JobGraph jobGraph, ClassLoader classLoader) throws ProgramInvocationException {
		log.info("Submitting job {}.", jobGraph.getJobID());

		final CompletableFuture<JobSubmitResponseBody> jobSubmissionFuture = submitJob(jobGraph);

		if (isDetached()) {
			try {
				jobSubmissionFuture.get();
			} catch (Exception e) {
				throw new ProgramInvocationException("Could not submit job " + jobGraph.getJobID() + '.', ExceptionUtils.stripExecutionException(e));
			}

			return new JobSubmissionResult(jobGraph.getJobID());
		} else {
			final CompletableFuture<JobResult> jobResultFuture = jobSubmissionFuture.thenCompose(
				ignored -> requestJobResult(jobGraph.getJobID()));

			final JobResult jobResult;
			try {
				jobResult = jobResultFuture.get();
			} catch (Exception e) {
				throw new ProgramInvocationException("Could not retrieve the execution result.", ExceptionUtils.stripExecutionException(e));
			}

			if (jobResult.getSerializedThrowable().isPresent()) {
				final SerializedThrowable serializedThrowable = jobResult.getSerializedThrowable().get();
				final Throwable throwable = serializedThrowable.deserializeError(classLoader);
				throw new ProgramInvocationException(throwable);
			}

			try {
				this.lastJobExecutionResult = new JobExecutionResult(
					jobResult.getJobId(),
					jobResult.getNetRuntime(),
					AccumulatorHelper.deserializeAccumulators(
						jobResult.getAccumulatorResults(),
						classLoader));
				return lastJobExecutionResult;
			} catch (IOException | ClassNotFoundException e) {
				throw new ProgramInvocationException(e);
			}
		}
	}

	/**
	 * Requests the {@link JobResult} for the given {@link JobID}. The method retries multiple
	 * times to poll the {@link JobResult} before giving up.
	 *
	 * @param jobId specifying the job for which to retrieve the {@link JobResult}
	 * @return Future which is completed with the {@link JobResult} once the job has completed or
	 * with a failure if the {@link JobResult} could not be retrieved.
	 */
	public CompletableFuture<JobResult> requestJobResult(JobID jobId) {
		return pollResourceAsync(
			() -> {
				final JobMessageParameters messageParameters = new JobMessageParameters();
				messageParameters.jobPathParameter.resolve(jobId);
				return sendRetryableRequest(
					JobExecutionResultHeaders.getInstance(),
					messageParameters,
					EmptyRequestBody.getInstance(),
					isConnectionProblemException().or(isHttpStatusUnsuccessfulException()));
			});
	}

	/**
	 * Submits the given {@link JobGraph} to the dispatcher.
	 *
	 * @param jobGraph to submit
	 * @return Future which is completed with the submission response
	 */
	public CompletableFuture<JobSubmitResponseBody> submitJob(JobGraph jobGraph) {
		// we have to enable queued scheduling because slot will be allocated lazily
		jobGraph.setAllowQueuedScheduling(true);

		log.info("Requesting blob server port.");
		CompletableFuture<BlobServerPortResponseBody> portFuture = sendRequest(
			BlobServerPortHeaders.getInstance());

		CompletableFuture<JobGraph> jobUploadFuture = portFuture.thenCombine(
			getDispatcherAddress(),
			(BlobServerPortResponseBody response, String dispatcherAddress) -> {
				log.info("Uploading jar files.");
				final int blobServerPort = response.port;
				final InetSocketAddress address = new InetSocketAddress(dispatcherAddress, blobServerPort);
				final List<PermanentBlobKey> keys;
				try {
					keys = BlobClient.uploadJarFiles(address, flinkConfig, jobGraph.getJobID(), jobGraph.getUserJars());
				} catch (IOException ioe) {
					throw new CompletionException(new FlinkException("Could not upload job jar files.", ioe));
				}

				for (PermanentBlobKey key : keys) {
					jobGraph.addBlob(key);
				}

				return jobGraph;
			});

		CompletableFuture<JobSubmitResponseBody> submissionFuture = jobUploadFuture.thenCompose(
			(JobGraph jobGraphToSubmit) -> {
				log.info("Submitting job graph.");

				try {
					return sendRequest(
						JobSubmitHeaders.getInstance(),
						new JobSubmitRequestBody(jobGraph));
				} catch (IOException ioe) {
					throw new CompletionException(new FlinkException("Could not create JobSubmitRequestBody.", ioe));
				}
			});

		return submissionFuture.exceptionally(
			(Throwable throwable) -> {
				throw new CompletionException(new JobSubmissionException(jobGraph.getJobID(), "Failed to submit JobGraph.", throwable));
			});
	}

	@Override
	public void stop(JobID jobID) throws Exception {
		JobTerminationMessageParameters params = new JobTerminationMessageParameters();
		params.jobPathParameter.resolve(jobID);
		params.terminationModeQueryParameter.resolve(Collections.singletonList(TerminationModeQueryParameter.TerminationMode.STOP));
		CompletableFuture<EmptyResponseBody> responseFuture = sendRequest(
			JobTerminationHeaders.getInstance(),
			params
		);
		responseFuture.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
	}

	@Override
	public void cancel(JobID jobID) throws Exception {
		JobTerminationMessageParameters params = new JobTerminationMessageParameters();
		params.jobPathParameter.resolve(jobID);
		params.terminationModeQueryParameter.resolve(Collections.singletonList(TerminationModeQueryParameter.TerminationMode.CANCEL));
		CompletableFuture<EmptyResponseBody> responseFuture = sendRequest(
			JobTerminationHeaders.getInstance(),
			params
		);
		responseFuture.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
	}

	@Override
	public String cancelWithSavepoint(JobID jobId, @Nullable String savepointDirectory) throws Exception {
		throw new UnsupportedOperationException("Not implemented yet.");
	}

	@Override
	public CompletableFuture<String> triggerSavepoint(
			final JobID jobId,
			final @Nullable String savepointDirectory) throws FlinkException {
		final SavepointTriggerHeaders savepointTriggerHeaders = SavepointTriggerHeaders.getInstance();
		final SavepointTriggerMessageParameters savepointTriggerMessageParameters =
			savepointTriggerHeaders.getUnresolvedMessageParameters();
		savepointTriggerMessageParameters.jobID.resolve(jobId);

		final CompletableFuture<SavepointTriggerResponseBody> responseFuture;

		responseFuture = sendRequest(
			savepointTriggerHeaders,
			savepointTriggerMessageParameters,
			new SavepointTriggerRequestBody(savepointDirectory));

		return responseFuture.thenCompose(savepointTriggerResponseBody -> {
			final SavepointTriggerId savepointTriggerId = savepointTriggerResponseBody.getSavepointTriggerId();
			return pollSavepointAsync(jobId, savepointTriggerId);
		}).thenApply(savepointInfo -> {
			if (savepointInfo.getFailureCause() != null) {
				throw new CompletionException(savepointInfo.getFailureCause());
			}
			return savepointInfo.getLocation();
		});
	}

	private CompletableFuture<SavepointInfo> pollSavepointAsync(
			final JobID jobId,
			final SavepointTriggerId savepointTriggerId) {
		return pollResourceAsync(() -> {
			final SavepointStatusHeaders savepointStatusHeaders = SavepointStatusHeaders.getInstance();
			final SavepointStatusMessageParameters savepointStatusMessageParameters =
				savepointStatusHeaders.getUnresolvedMessageParameters();
			savepointStatusMessageParameters.jobIdPathParameter.resolve(jobId);
			savepointStatusMessageParameters.savepointTriggerIdPathParameter.resolve(savepointTriggerId);
			return sendRetryableRequest(
				savepointStatusHeaders,
				savepointStatusMessageParameters,
				EmptyRequestBody.getInstance(),
				isConnectionProblemException());
		});
	}

	@Override
	public CompletableFuture<Collection<JobStatusMessage>> listJobs() throws Exception {
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
		return getWebMonitorBaseUrl().toString();
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
		return 0;
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

	private <M extends MessageHeaders<EmptyRequestBody, P, EmptyMessageParameters>, P extends ResponseBody> CompletableFuture<P>
			sendRequest(M messageHeaders) {
		return sendRequest(messageHeaders, EmptyMessageParameters.getInstance(), EmptyRequestBody.getInstance());
	}

	private <M extends MessageHeaders<R, P, U>, U extends MessageParameters, R extends RequestBody, P extends ResponseBody> CompletableFuture<P>
			sendRequest(M messageHeaders, U messageParameters, R request) {
		return getWebMonitorBaseUrl().thenCompose(webMonitorBaseUrl -> {
			try {
				return restClient.sendRequest(webMonitorBaseUrl.getHost(), webMonitorBaseUrl.getPort(), messageHeaders, messageParameters, request);
			} catch (IOException e) {
				throw new CompletionException(e);
			}
		});
	}

	private <M extends MessageHeaders<R, P, U>, U extends MessageParameters, R extends RequestBody, P extends ResponseBody> CompletableFuture<P>
			sendRetryableRequest(M messageHeaders, U messageParameters, R request, Predicate<Throwable> retryPredicate) {
		return retry(() -> getWebMonitorBaseUrl().thenCompose(webMonitorBaseUrl -> {
			try {
				return restClient.sendRequest(webMonitorBaseUrl.getHost(), webMonitorBaseUrl.getPort(), messageHeaders, messageParameters, request);
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

	private static Predicate<Throwable> isConnectionProblemException() {
		return (throwable) ->
			ExceptionUtils.findThrowable(throwable, java.net.ConnectException.class).isPresent() ||
				ExceptionUtils.findThrowable(throwable, java.net.SocketTimeoutException.class).isPresent() ||
				ExceptionUtils.findThrowable(throwable, ConnectTimeoutException.class).isPresent() ||
				ExceptionUtils.findThrowable(throwable, IOException.class).isPresent();
	}

	private static Predicate<Throwable> isHttpStatusUnsuccessfulException() {
		return (throwable) -> ExceptionUtils.findThrowable(throwable, RestClientException.class)
				.map(restClientException -> {
					final int code = restClientException.getHttpResponseStatus().code();
					return code < 200 || code > 299;
				})
				.orElse(false);
	}

	private CompletableFuture<URL> getWebMonitorBaseUrl() {
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

	private CompletableFuture<String> getDispatcherAddress() {
		return FutureUtils.orTimeout(
				dispatcherLeaderRetriever.getLeaderFuture(),
				restClusterClientConfiguration.getAwaitLeaderTimeout(),
				TimeUnit.MILLISECONDS)
			.thenApplyAsync(leaderAddressSessionId -> {
				final String address = leaderAddressSessionId.f0;
				final Option<String> host = AddressFromURIString.parse(address).host();
				checkArgument(host.isDefined(), "Could not parse host from %s", address);
				return host.get();
			}, executorService);
	}

}
