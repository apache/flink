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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.checkpoint.Checkpoints;
import org.apache.flink.runtime.client.JobSubmissionException;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.RunningJobsRegistry;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.SubmittedJobGraph;
import org.apache.flink.runtime.jobmanager.SubmittedJobGraphStore;
import org.apache.flink.runtime.jobmaster.JobManagerRunner;
import org.apache.flink.runtime.jobmaster.JobManagerSharedServices;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.jobmaster.JobNotFinishedException;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.jobmaster.RescalingBehaviour;
import org.apache.flink.runtime.jobmaster.factories.DefaultJobManagerJobMetricGroupFactory;
import org.apache.flink.runtime.jobmaster.factories.JobManagerJobMetricGroupFactory;
import org.apache.flink.runtime.leaderelection.LeaderContender;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.messages.webmonitor.ClusterOverview;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.messages.webmonitor.JobsOverview;
import org.apache.flink.runtime.messages.webmonitor.MultipleJobsDetails;
import org.apache.flink.runtime.metrics.groups.JobManagerMetricGroup;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.ResourceOverview;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.OperatorBackPressureStatsResponse;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.FencedRpcEndpoint;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Base class for the Dispatcher component. The Dispatcher component is responsible
 * for receiving job submissions, persisting them, spawning JobManagers to execute
 * the jobs and to recover them in case of a master failure. Furthermore, it knows
 * about the state of the Flink session cluster.
 */
public abstract class Dispatcher extends FencedRpcEndpoint<DispatcherId> implements
	DispatcherGateway, LeaderContender, SubmittedJobGraphStore.SubmittedJobGraphListener {

	public static final String DISPATCHER_NAME = "dispatcher";

	private final Configuration configuration;

	private final SubmittedJobGraphStore submittedJobGraphStore;
	private final RunningJobsRegistry runningJobsRegistry;

	private final HighAvailabilityServices highAvailabilityServices;
	private final ResourceManagerGateway resourceManagerGateway;
	private final JobManagerSharedServices jobManagerSharedServices;
	private final HeartbeatServices heartbeatServices;
	private final BlobServer blobServer;

	private final FatalErrorHandler fatalErrorHandler;

	private final Map<JobID, JobManagerRunner> jobManagerRunners;

	private final LeaderElectionService leaderElectionService;

	private final ArchivedExecutionGraphStore archivedExecutionGraphStore;

	private final JobManagerRunnerFactory jobManagerRunnerFactory;

	private final JobManagerMetricGroup jobManagerMetricGroup;

	@Nullable
	private final String metricQueryServicePath;

	@Nullable
	protected final String restAddress;

	private CompletableFuture<Void> orphanedJobManagerRunnersTerminationFuture = CompletableFuture.completedFuture(null);

	public Dispatcher(
			RpcService rpcService,
			String endpointId,
			Configuration configuration,
			HighAvailabilityServices highAvailabilityServices,
			SubmittedJobGraphStore submittedJobGraphStore,
			ResourceManagerGateway resourceManagerGateway,
			BlobServer blobServer,
			HeartbeatServices heartbeatServices,
			JobManagerMetricGroup jobManagerMetricGroup,
			@Nullable String metricServiceQueryPath,
			ArchivedExecutionGraphStore archivedExecutionGraphStore,
			JobManagerRunnerFactory jobManagerRunnerFactory,
			FatalErrorHandler fatalErrorHandler,
			@Nullable String restAddress) throws Exception {
		super(rpcService, endpointId);

		this.configuration = Preconditions.checkNotNull(configuration);
		this.highAvailabilityServices = Preconditions.checkNotNull(highAvailabilityServices);
		this.resourceManagerGateway = Preconditions.checkNotNull(resourceManagerGateway);
		this.heartbeatServices = Preconditions.checkNotNull(heartbeatServices);
		this.blobServer = Preconditions.checkNotNull(blobServer);
		this.fatalErrorHandler = Preconditions.checkNotNull(fatalErrorHandler);
		this.submittedJobGraphStore = Preconditions.checkNotNull(submittedJobGraphStore);
		this.jobManagerMetricGroup = Preconditions.checkNotNull(jobManagerMetricGroup);
		this.metricQueryServicePath = metricServiceQueryPath;

		this.jobManagerSharedServices = JobManagerSharedServices.fromConfiguration(
			configuration,
			this.blobServer);

		this.runningJobsRegistry = highAvailabilityServices.getRunningJobsRegistry();

		jobManagerRunners = new HashMap<>(16);

		leaderElectionService = highAvailabilityServices.getDispatcherLeaderElectionService();

		this.restAddress = restAddress;

		this.archivedExecutionGraphStore = Preconditions.checkNotNull(archivedExecutionGraphStore);

		this.jobManagerRunnerFactory = Preconditions.checkNotNull(jobManagerRunnerFactory);
	}

	//------------------------------------------------------
	// Lifecycle methods
	//------------------------------------------------------

	@Override
	public CompletableFuture<Void> postStop() {
		log.info("Stopping dispatcher {}.", getAddress());

		final CompletableFuture<Void> jobManagerRunnersTerminationFuture = terminateJobManagerRunners();

		final CompletableFuture<Void> allJobManagerRunnersTerminationFuture = FutureUtils.completeAll(Arrays.asList(
			jobManagerRunnersTerminationFuture,
			orphanedJobManagerRunnersTerminationFuture));

		return FutureUtils.runAfterwards(
			allJobManagerRunnersTerminationFuture,
			() -> {
				Exception exception = null;
				try {
					jobManagerSharedServices.shutdown();
				} catch (Exception e) {
					exception = ExceptionUtils.firstOrSuppressed(e, exception);
				}

				try {
					submittedJobGraphStore.stop();
				} catch (Exception e) {
					exception = ExceptionUtils.firstOrSuppressed(e, exception);
				}

				try {
					leaderElectionService.stop();
				} catch (Exception e) {
					exception = ExceptionUtils.firstOrSuppressed(e, exception);
				}

				jobManagerMetricGroup.close();

				if (exception != null) {
					throw exception;
				} else {
					log.info("Stopped dispatcher {}.", getAddress());
				}
			});
	}

	@Override
	public void start() throws Exception {
		super.start();

		submittedJobGraphStore.start(this);
		leaderElectionService.start(this);
	}

	//------------------------------------------------------
	// RPCs
	//------------------------------------------------------

	@Override
	public CompletableFuture<Acknowledge> submitJob(JobGraph jobGraph, Time timeout) {
		final JobID jobId = jobGraph.getJobID();

		log.info("Submitting job {} ({}).", jobId, jobGraph.getName());

		try {
			final RunningJobsRegistry.JobSchedulingStatus jobSchedulingStatus = runningJobsRegistry.getJobSchedulingStatus(jobId);

			if (jobSchedulingStatus == RunningJobsRegistry.JobSchedulingStatus.DONE || jobManagerRunners.containsKey(jobId)) {
				return FutureUtils.completedExceptionally(
					new JobSubmissionException(jobId, String.format("Job has already been submitted and is in state %s.", jobSchedulingStatus)));
			} else {
				persistAndRunJob(jobGraph);

				return CompletableFuture.completedFuture(Acknowledge.get());
			}
		} catch (Exception e) {
			return FutureUtils.completedExceptionally(new FlinkException(String.format("Failed to submit job %s.", jobId), e));
		}
	}

	private void persistAndRunJob(JobGraph jobGraph) throws Exception {
		submittedJobGraphStore.putJobGraph(new SubmittedJobGraph(jobGraph, null));

		try {
			runJob(jobGraph);
		} catch (Exception e) {
			try {
				submittedJobGraphStore.removeJobGraph(jobGraph.getJobID());
			} catch (Exception ie) {
				e.addSuppressed(ie);
			}

			throw e;
		}
	}

	private void runJob(JobGraph jobGraph) throws Exception {
		Preconditions.checkState(!jobManagerRunners.containsKey(jobGraph.getJobID()));

		final JobManagerRunner jobManagerRunner = createJobManagerRunner(jobGraph);

		jobManagerRunner.start();

		jobManagerRunners.put(jobGraph.getJobID(), jobManagerRunner);
	}

	private JobManagerRunner createJobManagerRunner(JobGraph jobGraph) throws Exception {
		final JobID jobId = jobGraph.getJobID();

		final JobManagerRunner jobManagerRunner = jobManagerRunnerFactory.createJobManagerRunner(
			ResourceID.generate(),
			jobGraph,
			configuration,
			getRpcService(),
			highAvailabilityServices,
			heartbeatServices,
			blobServer,
			jobManagerSharedServices,
			new DefaultJobManagerJobMetricGroupFactory(jobManagerMetricGroup),
			fatalErrorHandler);

		jobManagerRunner.getResultFuture().whenCompleteAsync(
			(ArchivedExecutionGraph archivedExecutionGraph, Throwable throwable) -> {
				// check if we are still the active JobManagerRunner by checking the identity
				//noinspection ObjectEquality
				if (jobManagerRunner == jobManagerRunners.get(jobId)) {
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
			}, getMainThreadExecutor());

		return jobManagerRunner;
	}

	@Override
	public CompletableFuture<Collection<JobID>> listJobs(Time timeout) {
		if (jobManagerRunners.isEmpty()) {
			log.info("empty");
		}
		return CompletableFuture.completedFuture(
			Collections.unmodifiableSet(new HashSet<>(jobManagerRunners.keySet())));
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
	public CompletableFuture<Acknowledge> stopJob(JobID jobId, Time timeout) {
		final CompletableFuture<JobMasterGateway> jobMasterGatewayFuture = getJobMasterGatewayFuture(jobId);

		return jobMasterGatewayFuture.thenCompose((JobMasterGateway jobMasterGateway) -> jobMasterGateway.stop(timeout));
	}

	@Override
	public CompletableFuture<Acknowledge> rescaleJob(JobID jobId, int newParallelism, RescalingBehaviour rescalingBehaviour, Time timeout) {
		final CompletableFuture<JobMasterGateway> jobMasterGatewayFuture = getJobMasterGatewayFuture(jobId);

		return jobMasterGatewayFuture.thenCompose(
			(JobMasterGateway jobMasterGateway) ->
				jobMasterGateway.rescaleJob(newParallelism, rescalingBehaviour, timeout));
	}

	@Override
	public CompletableFuture<String> requestRestAddress(Time timeout) {
		if (restAddress != null) {
			return CompletableFuture.completedFuture(restAddress);
		} else {
			return FutureUtils.completedExceptionally(new DispatcherException("The Dispatcher has not been started with a REST endpoint."));
		}
	}

	@Override
	public CompletableFuture<ClusterOverview> requestClusterOverview(Time timeout) {
		CompletableFuture<ResourceOverview> taskManagerOverviewFuture = resourceManagerGateway.requestResourceOverview(timeout);

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
		final JobManagerRunner jobManagerRunner = jobManagerRunners.get(jobId);

		if (jobManagerRunner == null) {
			final ArchivedExecutionGraph archivedExecutionGraph = archivedExecutionGraphStore.get(jobId);

			if (archivedExecutionGraph == null) {
				return FutureUtils.completedExceptionally(new FlinkJobNotFoundException(jobId));
			} else {
				return CompletableFuture.completedFuture(JobResult.createFrom(archivedExecutionGraph));
			}
		} else {
			return jobManagerRunner.getResultFuture().thenApply(JobResult::createFrom);
		}
	}

	@Override
	public CompletableFuture<Collection<String>> requestMetricQueryServicePaths(Time timeout) {
		if (metricQueryServicePath != null) {
			return CompletableFuture.completedFuture(Collections.singleton(metricQueryServicePath));
		} else {
			return CompletableFuture.completedFuture(Collections.emptyList());
		}
	}

	@Override
	public CompletableFuture<Collection<Tuple2<ResourceID, String>>> requestTaskManagerMetricQueryServicePaths(Time timeout) {
		return resourceManagerGateway.requestTaskManagerMetricQueryServicePaths(timeout);
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
	public CompletableFuture<Acknowledge> shutDownCluster() {
		shutDown();
		return CompletableFuture.completedFuture(Acknowledge.get());
	}

	/**
	 * Cleans up the job related data from the dispatcher. If cleanupHA is true, then
	 * the data will also be removed from HA.
	 *
	 * @param jobId JobID identifying the job to clean up
	 * @param cleanupHA True iff HA data shall also be cleaned up
	 */
	private void removeJob(JobID jobId, boolean cleanupHA) {
		JobManagerRunner jobManagerRunner = jobManagerRunners.remove(jobId);

		if (jobManagerRunner != null) {
			final CompletableFuture<Void> jobManagerRunnerTerminationFuture = jobManagerRunner.closeAsync();
			registerOrphanedJobManagerTerminationFuture(jobManagerRunnerTerminationFuture);
		}

		jobManagerMetricGroup.removeJob(jobId);

		if (cleanupHA) {
			try {
				submittedJobGraphStore.removeJobGraph(jobId);
			} catch (Exception e) {
				log.warn("Could not properly remove job {} from submitted job graph store.", jobId);
			}
		}

		// TODO: remove job related files from blob server
	}

	/**
	 * Terminate all currently running {@link JobManagerRunner}.
	 *
	 * @return Future which is completed once all {@link JobManagerRunner} have terminated
	 */
	private CompletableFuture<Void> terminateJobManagerRunners() {
		log.info("Stopping all currently running jobs of dispatcher {}.", getAddress());
		final List<CompletableFuture<Void>> terminationFutures = jobManagerRunners.values().stream()
			.map(JobManagerRunner::closeAsync)
			.collect(Collectors.toList());

		return FutureUtils.completeAll(terminationFutures);
	}

	/**
	 * Recovers all jobs persisted via the submitted job graph store.
	 */
	@VisibleForTesting
	CompletableFuture<Collection<JobGraph>> recoverJobs() {
		log.info("Recovering all persisted jobs.");
		return FutureUtils.supplyAsync(
			() -> {
				final Collection<JobID> jobIds = submittedJobGraphStore.getJobIds();

				final List<JobGraph> jobGraphs = new ArrayList<>(jobIds.size());

				for (JobID jobId : jobIds) {
					jobGraphs.add(recoverJob(jobId));
				}

				return jobGraphs;
			},
			getRpcService().getExecutor());
	}

	private JobGraph recoverJob(JobID jobId) throws Exception {
		log.debug("Recover job {}.", jobId);
		SubmittedJobGraph submittedJobGraph = submittedJobGraphStore.recoverJobGraph(jobId);

		if (submittedJobGraph != null) {
			return submittedJobGraph.getJobGraph();
		} else {
			throw new FlinkJobNotFoundException(jobId);
		}
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

		try {
			archivedExecutionGraphStore.put(archivedExecutionGraph);
		} catch (IOException e) {
			log.info(
				"Could not store completed job {}({}).",
				archivedExecutionGraph.getJobName(),
				archivedExecutionGraph.getJobID(),
				e);
		}

		final JobID jobId = archivedExecutionGraph.getJobID();

		removeJob(jobId, true);
	}

	protected void jobNotFinished(JobID jobId) {
		log.info("Job {} was not finished by JobManager.", jobId);

		removeJob(jobId, false);
	}

	private void jobMasterFailed(JobID jobId, Throwable cause) {
		// we fail fatally in case of a JobMaster failure in order to restart the
		// dispatcher to recover the jobs again. This only works in HA mode, though
		onFatalError(new FlinkException(String.format("JobMaster for job %s failed.", jobId), cause));
	}

	private void registerOrphanedJobManagerTerminationFuture(CompletableFuture<Void> jobManagerRunnerTerminationFuture) {
		orphanedJobManagerRunnersTerminationFuture = FutureUtils.completeAll(Arrays.asList(
			orphanedJobManagerRunnersTerminationFuture,
			jobManagerRunnerTerminationFuture));
	}

	private CompletableFuture<JobMasterGateway> getJobMasterGatewayFuture(JobID jobId) {
		final JobManagerRunner jobManagerRunner = jobManagerRunners.get(jobId);

		if (jobManagerRunner == null) {
			return FutureUtils.completedExceptionally(new FlinkJobNotFoundException(jobId));
		} else {
			final CompletableFuture<JobMasterGateway> leaderGatewayFuture = jobManagerRunner.getLeaderGatewayFuture();
			return leaderGatewayFuture.thenApplyAsync(
				(JobMasterGateway jobMasterGateway) -> {
					// check whether the retrieved JobMasterGateway belongs still to a running JobMaster
					if (jobManagerRunners.containsKey(jobId)) {
						return jobMasterGateway;
					} else {
						throw new CompletionException(new FlinkJobNotFoundException(jobId));
					}
				},
				getMainThreadExecutor());
		}
	}

	private <T> List<T> flattenOptionalCollection(Collection<Optional<T>> optionalCollection) {
		return optionalCollection.stream().filter(Optional::isPresent).map(Optional::get).collect(Collectors.toList());
	}

	@Nonnull
	private <T> List<CompletableFuture<Optional<T>>> queryJobMastersForInformation(Function<JobMasterGateway, CompletableFuture<T>> queryFunction) {
		final int numberJobsRunning = jobManagerRunners.size();

		ArrayList<CompletableFuture<Optional<T>>> optionalJobInformation = new ArrayList<>(
			numberJobsRunning);

		for (JobID jobId : jobManagerRunners.keySet()) {
			final CompletableFuture<JobMasterGateway> jobMasterGatewayFuture = getJobMasterGatewayFuture(jobId);

			final CompletableFuture<Optional<T>> optionalRequest = jobMasterGatewayFuture
				.thenCompose(queryFunction::apply)
				.handle((T value, Throwable throwable) -> Optional.ofNullable(value));

			optionalJobInformation.add(optionalRequest);
		}
		return optionalJobInformation;
	}

	//------------------------------------------------------
	// Leader contender
	//------------------------------------------------------

	/**
	 * Callback method when current resourceManager is granted leadership.
	 *
	 * @param newLeaderSessionID unique leadershipID
	 */
	@Override
	public void grantLeadership(final UUID newLeaderSessionID) {
		final DispatcherId dispatcherId = DispatcherId.fromUuid(newLeaderSessionID);
		log.info("Dispatcher {} was granted leadership with fencing token {}", getAddress(), dispatcherId);

		final CompletableFuture<Collection<JobGraph>> recoveredJobsFuture = recoverJobs();

		final CompletableFuture<Void> fencingTokenFuture = recoveredJobsFuture.thenAcceptAsync(
			(Collection<JobGraph> recoveredJobs) -> {
				setNewFencingToken(dispatcherId);

				for (JobGraph recoveredJob : recoveredJobs) {
					try {
						runJob(recoveredJob);
					} catch (Exception e) {
						throw new CompletionException(
							new FlinkException(
								String.format("Failed to recover job %s.", recoveredJob.getJobID()),
								e));
					}
				}
			},
			getUnfencedMainThreadExecutor());

		final CompletableFuture<Void> confirmationFuture = fencingTokenFuture.thenRunAsync(
			() -> leaderElectionService.confirmLeaderSessionID(newLeaderSessionID),
			getRpcService().getExecutor());

		confirmationFuture.whenComplete(
			(Void ignored, Throwable throwable) -> {
				if (throwable != null) {
					onFatalError(ExceptionUtils.stripCompletionException(throwable));
				}
			});
	}

	private void setNewFencingToken(@Nullable DispatcherId dispatcherId) {
		// clear the state if we've been the leader before
		if (getFencingToken() != null) {
			clearDispatcherState();
		}

		setFencingToken(dispatcherId);
	}

	private void clearDispatcherState() {
		final CompletableFuture<Void> jobManagerRunnersTerminationFuture = terminateJobManagerRunners();
		registerOrphanedJobManagerTerminationFuture(jobManagerRunnersTerminationFuture);
		jobManagerRunners.clear();
	}

	/**
	 * Callback method when current resourceManager loses leadership.
	 */
	@Override
	public void revokeLeadership() {
		runAsyncWithoutFencing(
			() -> {
				log.info("Dispatcher {} was revoked leadership.", getAddress());

				setNewFencingToken(null);
			});
	}

	/**
	 * Handles error occurring in the leader election service.
	 *
	 * @param exception Exception being thrown in the leader election service
	 */
	@Override
	public void handleError(final Exception exception) {
		onFatalError(new DispatcherException("Received an error from the LeaderElectionService.", exception));
	}

	//------------------------------------------------------
	// SubmittedJobGraphListener
	//------------------------------------------------------

	@Override
	public void onAddedJobGraph(final JobID jobId) {
		getRpcService().execute(() -> {
			final SubmittedJobGraph submittedJobGraph;
			try {
				submittedJobGraph = submittedJobGraphStore.recoverJobGraph(jobId);
			} catch (final Exception e) {
				log.error("Could not recover job graph for job {}.", jobId, e);
				return;
			}
			runAsync(() -> {
				submitJob(submittedJobGraph.getJobGraph(), RpcUtils.INF_TIMEOUT);
			});
		});
	}

	@Override
	public void onRemovedJobGraph(final JobID jobId) {
		runAsync(() -> {
			try {
				removeJob(jobId, false);
			} catch (final Exception e) {
				log.error("Could not remove job {}.", jobId, e);
			}
		});
	}

	//------------------------------------------------------
	// Factories
	//------------------------------------------------------

	/**
	 * Factory for a {@link JobManagerRunner}.
	 */
	@FunctionalInterface
	public interface JobManagerRunnerFactory {
		JobManagerRunner createJobManagerRunner(
			ResourceID resourceId,
			JobGraph jobGraph,
			Configuration configuration,
			RpcService rpcService,
			HighAvailabilityServices highAvailabilityServices,
			HeartbeatServices heartbeatServices,
			BlobServer blobServer,
			JobManagerSharedServices jobManagerServices,
			JobManagerJobMetricGroupFactory jobManagerJobMetricGroupFactory,
			FatalErrorHandler fatalErrorHandler) throws Exception;
	}

	/**
	 * Singleton default factory for {@link JobManagerRunner}.
	 */
	public enum DefaultJobManagerRunnerFactory implements JobManagerRunnerFactory {
		INSTANCE;

		@Override
		public JobManagerRunner createJobManagerRunner(
				ResourceID resourceId,
				JobGraph jobGraph,
				Configuration configuration,
				RpcService rpcService,
				HighAvailabilityServices highAvailabilityServices,
				HeartbeatServices heartbeatServices,
				BlobServer blobServer,
				JobManagerSharedServices jobManagerServices,
				JobManagerJobMetricGroupFactory jobManagerJobMetricGroupFactory,
				FatalErrorHandler fatalErrorHandler) throws Exception {
			return new JobManagerRunner(
				resourceId,
				jobGraph,
				configuration,
				rpcService,
				highAvailabilityServices,
				heartbeatServices,
				blobServer,
				jobManagerServices,
				jobManagerJobMetricGroupFactory,
				fatalErrorHandler);
		}
	}
}
