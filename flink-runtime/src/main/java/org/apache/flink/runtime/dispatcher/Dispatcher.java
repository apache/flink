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

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.checkpoint.Checkpoints;
import org.apache.flink.runtime.client.DuplicateJobSubmissionException;
import org.apache.flink.runtime.client.JobSubmissionException;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.RunningJobsRegistry;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmanager.JobGraphWriter;
import org.apache.flink.runtime.jobmaster.JobManagerRunner;
import org.apache.flink.runtime.jobmaster.JobManagerRunnerImpl;
import org.apache.flink.runtime.jobmaster.JobManagerSharedServices;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.jobmaster.JobNotFinishedException;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.jobmaster.factories.DefaultJobManagerJobMetricGroupFactory;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.messages.webmonitor.ClusterOverview;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.messages.webmonitor.JobsOverview;
import org.apache.flink.runtime.messages.webmonitor.MultipleJobsDetails;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.groups.JobManagerMetricGroup;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.ResourceOverview;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.OperatorBackPressureStatsResponse;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.PermanentlyFencedRpcEndpoint;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.akka.AkkaRpcServiceUtils;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.function.BiConsumerWithException;
import org.apache.flink.util.function.CheckedSupplier;
import org.apache.flink.util.function.FunctionUtils;
import org.apache.flink.util.function.FunctionWithException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base class for the Dispatcher component. The Dispatcher component is responsible
 * for receiving job submissions, persisting them, spawning JobManagers to execute
 * the jobs and to recover them in case of a master failure. Furthermore, it knows
 * about the state of the Flink session cluster.
 */
public abstract class Dispatcher extends PermanentlyFencedRpcEndpoint<DispatcherId> implements DispatcherGateway {

	public static final String DISPATCHER_NAME = "dispatcher";

	private final Configuration configuration;

	private final JobGraphWriter jobGraphWriter;
	private final RunningJobsRegistry runningJobsRegistry;

	private final HighAvailabilityServices highAvailabilityServices;
	private final GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever;
	private final JobManagerSharedServices jobManagerSharedServices;
	private final HeartbeatServices heartbeatServices;
	private final BlobServer blobServer;

	private final FatalErrorHandler fatalErrorHandler;

	private final Map<JobID, CompletableFuture<JobManagerRunner>> jobManagerRunnerFutures;

	private final DispatcherBootstrap dispatcherBootstrap;

	private final ArchivedExecutionGraphStore archivedExecutionGraphStore;

	private final JobManagerRunnerFactory jobManagerRunnerFactory;

	private final JobManagerMetricGroup jobManagerMetricGroup;

	private final HistoryServerArchivist historyServerArchivist;

	@Nullable
	private final String metricServiceQueryAddress;

	private final Map<JobID, CompletableFuture<Void>> jobManagerTerminationFutures;

	protected final CompletableFuture<ApplicationStatus> shutDownFuture;

	public Dispatcher(
			RpcService rpcService,
			DispatcherId fencingToken,
			DispatcherBootstrap dispatcherBootstrap,
			DispatcherServices dispatcherServices) throws Exception {
		super(rpcService, AkkaRpcServiceUtils.createRandomName(DISPATCHER_NAME), fencingToken);
		checkNotNull(dispatcherServices);

		this.configuration = dispatcherServices.getConfiguration();
		this.highAvailabilityServices = dispatcherServices.getHighAvailabilityServices();
		this.resourceManagerGatewayRetriever = dispatcherServices.getResourceManagerGatewayRetriever();
		this.heartbeatServices = dispatcherServices.getHeartbeatServices();
		this.blobServer = dispatcherServices.getBlobServer();
		this.fatalErrorHandler = dispatcherServices.getFatalErrorHandler();
		this.jobGraphWriter = dispatcherServices.getJobGraphWriter();
		this.jobManagerMetricGroup = dispatcherServices.getJobManagerMetricGroup();
		this.metricServiceQueryAddress = dispatcherServices.getMetricQueryServiceAddress();

		this.jobManagerSharedServices = JobManagerSharedServices.fromConfiguration(
			configuration,
			blobServer);

		this.runningJobsRegistry = highAvailabilityServices.getRunningJobsRegistry();

		jobManagerRunnerFutures = new HashMap<>(16);

		this.historyServerArchivist = dispatcherServices.getHistoryServerArchivist();

		this.archivedExecutionGraphStore = dispatcherServices.getArchivedExecutionGraphStore();

		this.jobManagerRunnerFactory = dispatcherServices.getJobManagerRunnerFactory();

		this.jobManagerTerminationFutures = new HashMap<>(2);

		this.shutDownFuture = new CompletableFuture<>();

		this.dispatcherBootstrap = checkNotNull(dispatcherBootstrap);
	}

	//------------------------------------------------------
	// Getters
	//------------------------------------------------------

	public CompletableFuture<ApplicationStatus> getShutDownFuture() {
		return shutDownFuture;
	}

	//------------------------------------------------------
	// Lifecycle methods
	//------------------------------------------------------

	@Override
	public void onStart() throws Exception {
		try {
			startDispatcherServices();
		} catch (Exception e) {
			final DispatcherException exception = new DispatcherException(String.format("Could not start the Dispatcher %s", getAddress()), e);
			onFatalError(exception);
			throw exception;
		}

		dispatcherBootstrap.initialize(this, this.getRpcService().getScheduledExecutor());
	}

	private void startDispatcherServices() throws Exception {
		try {
			registerDispatcherMetrics(jobManagerMetricGroup);
		} catch (Exception e) {
			handleStartDispatcherServicesException(e);
		}
	}

	void runRecoveredJob(final JobGraph recoveredJob) {
		checkNotNull(recoveredJob);
		FutureUtils.assertNoException(runJob(recoveredJob)
			.handle(handleRecoveredJobStartError(recoveredJob.getJobID())));
	}

	private BiFunction<Void, Throwable, Void> handleRecoveredJobStartError(JobID jobId) {
		return (ignored, throwable) -> {
			if (throwable != null) {
				onFatalError(new DispatcherException(String.format("Could not start recovered job %s.", jobId), throwable));
			}

			return null;
		};
	}

	private void handleStartDispatcherServicesException(Exception e) throws Exception {
		try {
			stopDispatcherServices();
		} catch (Exception exception) {
			e.addSuppressed(exception);
		}

		throw e;
	}

	@Override
	public CompletableFuture<Void> onStop() {
		log.info("Stopping dispatcher {}.", getAddress());

		final CompletableFuture<Void> allJobManagerRunnersTerminationFuture = terminateJobManagerRunnersAndGetTerminationFuture();

		return FutureUtils.runAfterwards(
			allJobManagerRunnersTerminationFuture,
			() -> {
				dispatcherBootstrap.stop();

				stopDispatcherServices();

				log.info("Stopped dispatcher {}.", getAddress());
			});
	}

	private void stopDispatcherServices() throws Exception {
		Exception exception = null;
		try {
			jobManagerSharedServices.shutdown();
		} catch (Exception e) {
			exception = e;
		}

		jobManagerMetricGroup.close();

		ExceptionUtils.tryRethrowException(exception);
	}

	//------------------------------------------------------
	// RPCs
	//------------------------------------------------------

	@Override
	public CompletableFuture<Acknowledge> submitJob(JobGraph jobGraph, Time timeout) {
		log.info("Received JobGraph submission {} ({}).", jobGraph.getJobID(), jobGraph.getName());

		try {
			if (isDuplicateJob(jobGraph.getJobID())) {
				return FutureUtils.completedExceptionally(
					new DuplicateJobSubmissionException(jobGraph.getJobID()));
			} else if (isPartialResourceConfigured(jobGraph)) {
				return FutureUtils.completedExceptionally(
					new JobSubmissionException(jobGraph.getJobID(), "Currently jobs is not supported if parts of the vertices have " +
							"resources configured. The limitation will be removed in future versions."));
			} else {
				return internalSubmitJob(jobGraph);
			}
		} catch (FlinkException e) {
			return FutureUtils.completedExceptionally(e);
		}
	}

	/**
	 * Checks whether the given job has already been submitted or executed.
	 *
	 * @param jobId identifying the submitted job
	 * @return true if the job has already been submitted (is running) or has been executed
	 * @throws FlinkException if the job scheduling status cannot be retrieved
	 */
	private boolean isDuplicateJob(JobID jobId) throws FlinkException {
		final RunningJobsRegistry.JobSchedulingStatus jobSchedulingStatus;

		try {
			jobSchedulingStatus = runningJobsRegistry.getJobSchedulingStatus(jobId);
		} catch (IOException e) {
			throw new FlinkException(String.format("Failed to retrieve job scheduling status for job %s.", jobId), e);
		}

		return jobSchedulingStatus == RunningJobsRegistry.JobSchedulingStatus.DONE || jobManagerRunnerFutures.containsKey(jobId);
	}

	private boolean isPartialResourceConfigured(JobGraph jobGraph) {
		boolean hasVerticesWithUnknownResource = false;
		boolean hasVerticesWithConfiguredResource = false;

		for (JobVertex jobVertex : jobGraph.getVertices()) {
			if (jobVertex.getMinResources() == ResourceSpec.UNKNOWN) {
				hasVerticesWithUnknownResource = true;
			} else {
				hasVerticesWithConfiguredResource = true;
			}

			if (hasVerticesWithUnknownResource && hasVerticesWithConfiguredResource) {
				return true;
			}
		}

		return false;
	}

	private CompletableFuture<Acknowledge> internalSubmitJob(JobGraph jobGraph) {
		log.info("Submitting job {} ({}).", jobGraph.getJobID(), jobGraph.getName());

		final CompletableFuture<Acknowledge> persistAndRunFuture = waitForTerminatingJobManager(jobGraph.getJobID(), jobGraph, this::persistAndRunJob)
			.thenApply(ignored -> Acknowledge.get());

		return persistAndRunFuture.handleAsync((acknowledge, throwable) -> {
			if (throwable != null) {
				cleanUpJobData(jobGraph.getJobID(), true);

				final Throwable strippedThrowable = ExceptionUtils.stripCompletionException(throwable);
				log.error("Failed to submit job {}.", jobGraph.getJobID(), strippedThrowable);
				throw new CompletionException(
					new JobSubmissionException(jobGraph.getJobID(), "Failed to submit job.", strippedThrowable));
			} else {
				return acknowledge;
			}
		}, getRpcService().getExecutor());
	}

	private CompletableFuture<Void> persistAndRunJob(JobGraph jobGraph) throws Exception {
		jobGraphWriter.putJobGraph(jobGraph);

		final CompletableFuture<Void> runJobFuture = runJob(jobGraph);

		return runJobFuture.whenComplete(BiConsumerWithException.unchecked((Object ignored, Throwable throwable) -> {
			if (throwable != null) {
				jobGraphWriter.removeJobGraph(jobGraph.getJobID());
			}
		}));
	}

	private CompletableFuture<Void> runJob(JobGraph jobGraph) {
		Preconditions.checkState(!jobManagerRunnerFutures.containsKey(jobGraph.getJobID()));

		final CompletableFuture<JobManagerRunner> jobManagerRunnerFuture = createJobManagerRunner(jobGraph);

		jobManagerRunnerFutures.put(jobGraph.getJobID(), jobManagerRunnerFuture);

		return jobManagerRunnerFuture
			.thenApply(FunctionUtils.uncheckedFunction(this::startJobManagerRunner))
			.thenApply(FunctionUtils.nullFn())
			.whenCompleteAsync(
				(ignored, throwable) -> {
					if (throwable != null) {
						jobManagerRunnerFutures.remove(jobGraph.getJobID());
					}
				},
				getMainThreadExecutor());
	}

	private CompletableFuture<JobManagerRunner> createJobManagerRunner(JobGraph jobGraph) {
		final RpcService rpcService = getRpcService();

		return CompletableFuture.supplyAsync(
			CheckedSupplier.unchecked(() ->
				jobManagerRunnerFactory.createJobManagerRunner(
					jobGraph,
					configuration,
					rpcService,
					highAvailabilityServices,
					heartbeatServices,
					jobManagerSharedServices,
					new DefaultJobManagerJobMetricGroupFactory(jobManagerMetricGroup),
					fatalErrorHandler)),
			rpcService.getExecutor());
	}

	private JobManagerRunner startJobManagerRunner(JobManagerRunner jobManagerRunner) throws Exception {
		final JobID jobId = jobManagerRunner.getJobID();

		FutureUtils.assertNoException(
			jobManagerRunner.getResultFuture().handleAsync(
				(ArchivedExecutionGraph archivedExecutionGraph, Throwable throwable) -> {
					// check if we are still the active JobManagerRunner by checking the identity
					final JobManagerRunner currentJobManagerRunner = Optional.ofNullable(jobManagerRunnerFutures.get(jobId))
						.map(future -> future.getNow(null))
						.orElse(null);
					//noinspection ObjectEquality
					if (jobManagerRunner == currentJobManagerRunner) {
						if (archivedExecutionGraph != null) {
							jobReachedGloballyTerminalState(archivedExecutionGraph);
						} else {
							final Throwable strippedThrowable = ExceptionUtils.stripCompletionException(throwable);

							if (strippedThrowable instanceof JobNotFinishedException) {
								jobNotFinished(jobId);
							} else {
								jobMasterFailed(jobId, strippedThrowable);
							}
						}
					} else {
						log.debug("There is a newer JobManagerRunner for the job {}.", jobId);
					}

					return null;
				}, getMainThreadExecutor()));

		jobManagerRunner.start();

		return jobManagerRunner;
	}

	@Override
	public CompletableFuture<Collection<JobID>> listJobs(Time timeout) {
		return CompletableFuture.completedFuture(
			Collections.unmodifiableSet(new HashSet<>(jobManagerRunnerFutures.keySet())));
	}

	@Override
	public CompletableFuture<Acknowledge> disposeSavepoint(String savepointPath, Time timeout) {
		final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

		return CompletableFuture.supplyAsync(
			() -> {
				log.info("Disposing savepoint {}.", savepointPath);

				try {
					Checkpoints.disposeSavepoint(savepointPath, configuration, classLoader, log);
				} catch (IOException | FlinkException e) {
					throw new CompletionException(new FlinkException(String.format("Could not dispose savepoint %s.", savepointPath), e));
				}

				return Acknowledge.get();
			},
			jobManagerSharedServices.getScheduledExecutorService());
	}

	@Override
	public CompletableFuture<Acknowledge> cancelJob(JobID jobId, Time timeout) {
		final CompletableFuture<JobMasterGateway> jobMasterGatewayFuture = getJobMasterGatewayFuture(jobId);

		return jobMasterGatewayFuture.thenCompose((JobMasterGateway jobMasterGateway) -> jobMasterGateway.cancel(timeout));
	}

	@Override
	public CompletableFuture<ClusterOverview> requestClusterOverview(Time timeout) {
		CompletableFuture<ResourceOverview> taskManagerOverviewFuture = runResourceManagerCommand(resourceManagerGateway -> resourceManagerGateway.requestResourceOverview(timeout));

		final List<CompletableFuture<Optional<JobStatus>>> optionalJobInformation = queryJobMastersForInformation(
			(JobMasterGateway jobMasterGateway) -> jobMasterGateway.requestJobStatus(timeout));

		CompletableFuture<Collection<Optional<JobStatus>>> allOptionalJobsFuture = FutureUtils.combineAll(optionalJobInformation);

		CompletableFuture<Collection<JobStatus>> allJobsFuture = allOptionalJobsFuture.thenApply(this::flattenOptionalCollection);

		final JobsOverview completedJobsOverview = archivedExecutionGraphStore.getStoredJobsOverview();

		return allJobsFuture.thenCombine(
			taskManagerOverviewFuture,
			(Collection<JobStatus> runningJobsStatus, ResourceOverview resourceOverview) -> {
				final JobsOverview allJobsOverview = JobsOverview.create(runningJobsStatus).combine(completedJobsOverview);
				return new ClusterOverview(resourceOverview, allJobsOverview);
			});
	}

	@Override
	public CompletableFuture<MultipleJobsDetails> requestMultipleJobDetails(Time timeout) {
		List<CompletableFuture<Optional<JobDetails>>> individualOptionalJobDetails = queryJobMastersForInformation(
			(JobMasterGateway jobMasterGateway) -> jobMasterGateway.requestJobDetails(timeout));

		CompletableFuture<Collection<Optional<JobDetails>>> optionalCombinedJobDetails = FutureUtils.combineAll(
			individualOptionalJobDetails);

		CompletableFuture<Collection<JobDetails>> combinedJobDetails = optionalCombinedJobDetails.thenApply(this::flattenOptionalCollection);

		final Collection<JobDetails> completedJobDetails = archivedExecutionGraphStore.getAvailableJobDetails();

		return combinedJobDetails.thenApply(
			(Collection<JobDetails> runningJobDetails) -> {
				final Collection<JobDetails> allJobDetails = new ArrayList<>(completedJobDetails.size() + runningJobDetails.size());

				allJobDetails.addAll(runningJobDetails);
				allJobDetails.addAll(completedJobDetails);

				return new MultipleJobsDetails(allJobDetails);
			});
	}

	@Override
	public CompletableFuture<JobStatus> requestJobStatus(JobID jobId, Time timeout) {

		final CompletableFuture<JobMasterGateway> jobMasterGatewayFuture = getJobMasterGatewayFuture(jobId);

		final CompletableFuture<JobStatus> jobStatusFuture = jobMasterGatewayFuture.thenCompose(
			(JobMasterGateway jobMasterGateway) -> jobMasterGateway.requestJobStatus(timeout));

		return jobStatusFuture.exceptionally(
			(Throwable throwable) -> {
				final JobDetails jobDetails = archivedExecutionGraphStore.getAvailableJobDetails(jobId);

				// check whether it is a completed job
				if (jobDetails == null) {
					throw new CompletionException(ExceptionUtils.stripCompletionException(throwable));
				} else {
					return jobDetails.getStatus();
				}
			});
	}

	@Override
	public CompletableFuture<OperatorBackPressureStatsResponse> requestOperatorBackPressureStats(
			final JobID jobId,
			final JobVertexID jobVertexId) {
		final CompletableFuture<JobMasterGateway> jobMasterGatewayFuture = getJobMasterGatewayFuture(jobId);

		return jobMasterGatewayFuture.thenCompose((JobMasterGateway jobMasterGateway) -> jobMasterGateway.requestOperatorBackPressureStats(jobVertexId));
	}

	@Override
	public CompletableFuture<ArchivedExecutionGraph> requestJob(JobID jobId, Time timeout) {
		final CompletableFuture<JobMasterGateway> jobMasterGatewayFuture = getJobMasterGatewayFuture(jobId);

		final CompletableFuture<ArchivedExecutionGraph> archivedExecutionGraphFuture = jobMasterGatewayFuture.thenCompose(
			(JobMasterGateway jobMasterGateway) -> jobMasterGateway.requestJob(timeout));

		return archivedExecutionGraphFuture.exceptionally(
			(Throwable throwable) -> {
				final ArchivedExecutionGraph serializableExecutionGraph = archivedExecutionGraphStore.get(jobId);

				// check whether it is a completed job
				if (serializableExecutionGraph == null) {
					throw new CompletionException(ExceptionUtils.stripCompletionException(throwable));
				} else {
					return serializableExecutionGraph;
				}
			});
	}

	@Override
	public CompletableFuture<JobResult> requestJobResult(JobID jobId, Time timeout) {
		final CompletableFuture<JobManagerRunner> jobManagerRunnerFuture = jobManagerRunnerFutures.get(jobId);

		if (jobManagerRunnerFuture == null) {
			final ArchivedExecutionGraph archivedExecutionGraph = archivedExecutionGraphStore.get(jobId);

			if (archivedExecutionGraph == null) {
				return FutureUtils.completedExceptionally(new FlinkJobNotFoundException(jobId));
			} else {
				return CompletableFuture.completedFuture(JobResult.createFrom(archivedExecutionGraph));
			}
		} else {
			return jobManagerRunnerFuture.thenCompose(JobManagerRunner::getResultFuture).thenApply(JobResult::createFrom);
		}
	}

	@Override
	public CompletableFuture<Collection<String>> requestMetricQueryServiceAddresses(Time timeout) {
		if (metricServiceQueryAddress != null) {
			return CompletableFuture.completedFuture(Collections.singleton(metricServiceQueryAddress));
		} else {
			return CompletableFuture.completedFuture(Collections.emptyList());
		}
	}

	@Override
	public CompletableFuture<Collection<Tuple2<ResourceID, String>>> requestTaskManagerMetricQueryServiceAddresses(Time timeout) {
		return runResourceManagerCommand(resourceManagerGateway -> resourceManagerGateway.requestTaskManagerMetricQueryServiceAddresses(timeout));
	}

	@Override
	public CompletableFuture<Integer> getBlobServerPort(Time timeout) {
		return CompletableFuture.completedFuture(blobServer.getPort());
	}

	@Override
	public CompletableFuture<String> triggerSavepoint(
			final JobID jobId,
			final String targetDirectory,
			final boolean cancelJob,
			final Time timeout) {
		final CompletableFuture<JobMasterGateway> jobMasterGatewayFuture = getJobMasterGatewayFuture(jobId);

		return jobMasterGatewayFuture.thenCompose(
			(JobMasterGateway jobMasterGateway) ->
				jobMasterGateway.triggerSavepoint(targetDirectory, cancelJob, timeout));
	}

	@Override
	public CompletableFuture<String> stopWithSavepoint(
			final JobID jobId,
			final String targetDirectory,
			final boolean advanceToEndOfEventTime,
			final Time timeout) {
		final CompletableFuture<JobMasterGateway> jobMasterGatewayFuture = getJobMasterGatewayFuture(jobId);

		return jobMasterGatewayFuture.thenCompose(
				(JobMasterGateway jobMasterGateway) ->
						jobMasterGateway.stopWithSavepoint(targetDirectory, advanceToEndOfEventTime, timeout));
	}

	@Override
	public CompletableFuture<Acknowledge> shutDownCluster() {
		return shutDownCluster(ApplicationStatus.SUCCEEDED);
	}

	@Override
	public CompletableFuture<Acknowledge> shutDownCluster(final ApplicationStatus applicationStatus) {
		shutDownFuture.complete(applicationStatus);
		return CompletableFuture.completedFuture(Acknowledge.get());
	}

	@Override
	public CompletableFuture<CoordinationResponse> deliverCoordinationRequestToCoordinator(
			JobID jobId,
			OperatorID operatorId,
			SerializedValue<CoordinationRequest> serializedRequest,
			Time timeout) {
		final CompletableFuture<JobMasterGateway> jobMasterGatewayFuture = getJobMasterGatewayFuture(jobId);

		return jobMasterGatewayFuture.thenCompose(
			(JobMasterGateway jobMasterGateway) ->
				jobMasterGateway.deliverCoordinationRequestToCoordinator(operatorId, serializedRequest, timeout));
	}

	/**
	 * Cleans up the job related data from the dispatcher. If cleanupHA is true, then
	 * the data will also be removed from HA.
	 *
	 * @param jobId JobID identifying the job to clean up
	 * @param cleanupHA True iff HA data shall also be cleaned up
	 */
	private void removeJobAndRegisterTerminationFuture(JobID jobId, boolean cleanupHA) {
		final CompletableFuture<Void> cleanupFuture = removeJob(jobId, cleanupHA);

		registerJobManagerRunnerTerminationFuture(jobId, cleanupFuture);
	}

	private void registerJobManagerRunnerTerminationFuture(JobID jobId, CompletableFuture<Void> jobManagerRunnerTerminationFuture) {
		Preconditions.checkState(!jobManagerTerminationFutures.containsKey(jobId));

		jobManagerTerminationFutures.put(jobId, jobManagerRunnerTerminationFuture);

		// clean up the pending termination future
		jobManagerRunnerTerminationFuture.thenRunAsync(
			() -> {
				final CompletableFuture<Void> terminationFuture = jobManagerTerminationFutures.remove(jobId);

				//noinspection ObjectEquality
				if (terminationFuture != null && terminationFuture != jobManagerRunnerTerminationFuture) {
					jobManagerTerminationFutures.put(jobId, terminationFuture);
				}
			},
			getMainThreadExecutor());
	}

	private CompletableFuture<Void> removeJob(JobID jobId, boolean cleanupHA) {
		CompletableFuture<JobManagerRunner> jobManagerRunnerFuture = jobManagerRunnerFutures.remove(jobId);

		final CompletableFuture<Void> jobManagerRunnerTerminationFuture;
		if (jobManagerRunnerFuture != null) {
			jobManagerRunnerTerminationFuture = jobManagerRunnerFuture.thenCompose(JobManagerRunner::closeAsync);
		} else {
			jobManagerRunnerTerminationFuture = CompletableFuture.completedFuture(null);
		}

		return jobManagerRunnerTerminationFuture.thenRunAsync(
			() -> cleanUpJobData(jobId, cleanupHA),
			getRpcService().getExecutor());
	}

	private void cleanUpJobData(JobID jobId, boolean cleanupHA) {
		jobManagerMetricGroup.removeJob(jobId);

		boolean cleanupHABlobs = false;
		if (cleanupHA) {
			try {
				jobGraphWriter.removeJobGraph(jobId);

				// only clean up the HA blobs if we could remove the job from HA storage
				cleanupHABlobs = true;
			} catch (Exception e) {
				log.warn("Could not properly remove job {} from submitted job graph store.", jobId, e);
			}

			try {
				runningJobsRegistry.clearJob(jobId);
			} catch (IOException e) {
				log.warn("Could not properly remove job {} from the running jobs registry.", jobId, e);
			}
		} else {
			try {
				jobGraphWriter.releaseJobGraph(jobId);
			} catch (Exception e) {
				log.warn("Could not properly release job {} from submitted job graph store.", jobId, e);
			}
		}

		blobServer.cleanupJob(jobId, cleanupHABlobs);
	}

	/**
	 * Terminate all currently running {@link JobManagerRunnerImpl}.
	 */
	private void terminateJobManagerRunners() {
		log.info("Stopping all currently running jobs of dispatcher {}.", getAddress());

		final HashSet<JobID> jobsToRemove = new HashSet<>(jobManagerRunnerFutures.keySet());

		for (JobID jobId : jobsToRemove) {
			removeJobAndRegisterTerminationFuture(jobId, false);
		}
	}

	private CompletableFuture<Void> terminateJobManagerRunnersAndGetTerminationFuture() {
		terminateJobManagerRunners();
		final Collection<CompletableFuture<Void>> values = jobManagerTerminationFutures.values();
		return FutureUtils.completeAll(values);
	}

	protected void onFatalError(Throwable throwable) {
		fatalErrorHandler.onFatalError(throwable);
	}

	protected void jobReachedGloballyTerminalState(ArchivedExecutionGraph archivedExecutionGraph) {
		Preconditions.checkArgument(
			archivedExecutionGraph.getState().isGloballyTerminalState(),
			"Job %s is in state %s which is not globally terminal.",
			archivedExecutionGraph.getJobID(),
			archivedExecutionGraph.getState());

		log.info("Job {} reached globally terminal state {}.", archivedExecutionGraph.getJobID(), archivedExecutionGraph.getState());

		archiveExecutionGraph(archivedExecutionGraph);

		final JobID jobId = archivedExecutionGraph.getJobID();

		removeJobAndRegisterTerminationFuture(jobId, true);
	}

	private void archiveExecutionGraph(ArchivedExecutionGraph archivedExecutionGraph) {
		try {
			archivedExecutionGraphStore.put(archivedExecutionGraph);
		} catch (IOException e) {
			log.info(
				"Could not store completed job {}({}).",
				archivedExecutionGraph.getJobName(),
				archivedExecutionGraph.getJobID(),
				e);
		}

		final CompletableFuture<Acknowledge> executionGraphFuture = historyServerArchivist.archiveExecutionGraph(archivedExecutionGraph);

		executionGraphFuture.whenComplete(
			(Acknowledge ignored, Throwable throwable) -> {
				if (throwable != null) {
					log.info(
						"Could not archive completed job {}({}) to the history server.",
						archivedExecutionGraph.getJobName(),
						archivedExecutionGraph.getJobID(),
						throwable);
				}
			});
	}

	protected void jobNotFinished(JobID jobId) {
		log.info("Job {} was not finished by JobManager.", jobId);

		removeJobAndRegisterTerminationFuture(jobId, false);
	}

	private void jobMasterFailed(JobID jobId, Throwable cause) {
		// we fail fatally in case of a JobMaster failure in order to restart the
		// dispatcher to recover the jobs again. This only works in HA mode, though
		onFatalError(new FlinkException(String.format("JobMaster for job %s failed.", jobId), cause));
	}

	private CompletableFuture<JobMasterGateway> getJobMasterGatewayFuture(JobID jobId) {
		final CompletableFuture<JobManagerRunner> jobManagerRunnerFuture = jobManagerRunnerFutures.get(jobId);

		if (jobManagerRunnerFuture == null) {
			return FutureUtils.completedExceptionally(new FlinkJobNotFoundException(jobId));
		} else {
			final CompletableFuture<JobMasterGateway> leaderGatewayFuture = jobManagerRunnerFuture.thenCompose(JobManagerRunner::getJobMasterGateway);
			return leaderGatewayFuture.thenApplyAsync(
				(JobMasterGateway jobMasterGateway) -> {
					// check whether the retrieved JobMasterGateway belongs still to a running JobMaster
					if (jobManagerRunnerFutures.containsKey(jobId)) {
						return jobMasterGateway;
					} else {
						throw new CompletionException(new FlinkJobNotFoundException(jobId));
					}
				},
				getMainThreadExecutor());
		}
	}

	private CompletableFuture<ResourceManagerGateway> getResourceManagerGateway() {
		return resourceManagerGatewayRetriever.getFuture();
	}

	private <T> CompletableFuture<T> runResourceManagerCommand(Function<ResourceManagerGateway, CompletableFuture<T>> resourceManagerCommand) {
		return getResourceManagerGateway().thenApply(resourceManagerCommand).thenCompose(Function.identity());
	}

	private <T> List<T> flattenOptionalCollection(Collection<Optional<T>> optionalCollection) {
		return optionalCollection.stream().filter(Optional::isPresent).map(Optional::get).collect(Collectors.toList());
	}

	@Nonnull
	private <T> List<CompletableFuture<Optional<T>>> queryJobMastersForInformation(Function<JobMasterGateway, CompletableFuture<T>> queryFunction) {
		final int numberJobsRunning = jobManagerRunnerFutures.size();

		ArrayList<CompletableFuture<Optional<T>>> optionalJobInformation = new ArrayList<>(
			numberJobsRunning);

		for (JobID jobId : jobManagerRunnerFutures.keySet()) {
			final CompletableFuture<JobMasterGateway> jobMasterGatewayFuture = getJobMasterGatewayFuture(jobId);

			final CompletableFuture<Optional<T>> optionalRequest = jobMasterGatewayFuture
				.thenCompose(queryFunction::apply)
				.handle((T value, Throwable throwable) -> Optional.ofNullable(value));

			optionalJobInformation.add(optionalRequest);
		}
		return optionalJobInformation;
	}

	private CompletableFuture<Void> waitForTerminatingJobManager(JobID jobId, JobGraph jobGraph, FunctionWithException<JobGraph, CompletableFuture<Void>, ?> action) {
		final CompletableFuture<Void> jobManagerTerminationFuture = getJobTerminationFuture(jobId)
			.exceptionally((Throwable throwable) -> {
				throw new CompletionException(
					new DispatcherException(
						String.format("Termination of previous JobManager for job %s failed. Cannot submit job under the same job id.", jobId),
						throwable)); });

		return jobManagerTerminationFuture.thenComposeAsync(
			FunctionUtils.uncheckedFunction((ignored) -> {
				jobManagerTerminationFutures.remove(jobId);
				return action.apply(jobGraph);
			}),
			getMainThreadExecutor());
	}

	CompletableFuture<Void> getJobTerminationFuture(JobID jobId) {
		if (jobManagerRunnerFutures.containsKey(jobId)) {
			return FutureUtils.completedExceptionally(new DispatcherException(String.format("Job with job id %s is still running.", jobId)));
		} else {
			return jobManagerTerminationFutures.getOrDefault(jobId, CompletableFuture.completedFuture(null));
		}
	}

	private void registerDispatcherMetrics(MetricGroup jobManagerMetricGroup) {
		jobManagerMetricGroup.gauge(MetricNames.NUM_RUNNING_JOBS,
			() -> (long) jobManagerRunnerFutures.size());
	}

	public CompletableFuture<Void> onRemovedJobGraph(JobID jobId) {
		return CompletableFuture.runAsync(
			() -> removeJobAndRegisterTerminationFuture(jobId, false),
			getMainThreadExecutor());
	}
}
