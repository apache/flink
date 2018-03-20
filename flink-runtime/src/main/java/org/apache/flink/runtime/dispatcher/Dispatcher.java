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
import org.apache.flink.runtime.jobmaster.JobNotFinishedException;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.jobmaster.RescalingBehaviour;
import org.apache.flink.runtime.leaderelection.LeaderContender;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.messages.webmonitor.ClusterOverview;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.messages.webmonitor.JobsOverview;
import org.apache.flink.runtime.messages.webmonitor.MultipleJobsDetails;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
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
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
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

		log.info("Submitting job {} ({}).", jobGraph.getJobID(), jobGraph.getName());

		final RunningJobsRegistry.JobSchedulingStatus jobSchedulingStatus;

		try {
			jobSchedulingStatus = runningJobsRegistry.getJobSchedulingStatus(jobId);
		} catch (IOException e) {
			log.warn("Cannot retrieve job status for {}.", jobId, e);
			return FutureUtils.completedExceptionally(
				new JobSubmissionException(jobId, "Could not retrieve the job status.", e));
		}

		if (jobSchedulingStatus != RunningJobsRegistry.JobSchedulingStatus.DONE &&
			!jobManagerRunners.containsKey(jobId)) {
			try {
				submittedJobGraphStore.putJobGraph(new SubmittedJobGraph(jobGraph, null));
			} catch (Exception e) {
				log.warn("Cannot persist JobGraph.", e);
				return FutureUtils.completedExceptionally(
					new JobSubmissionException(jobId, "Could not persist JobGraph.", e));
			}

			final JobManagerRunner jobManagerRunner;

			try {
				jobManagerRunner = jobManagerRunnerFactory.createJobManagerRunner(
					ResourceID.generate(),
					jobGraph,
					configuration,
					getRpcService(),
					highAvailabilityServices,
					heartbeatServices,
					blobServer,
					jobManagerSharedServices,
					jobManagerMetricGroup.addJob(jobGraph),
					metricQueryServicePath,
					restAddress);

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
									onFatalError(new FlinkException("JobManagerRunner for job " + jobId + " failed.", strippedThrowable));
								}
							}
						} else {
							log.debug("There is a newer JobManagerRunner for the job {}.", jobId);
						}
					}, getMainThreadExecutor());

				jobManagerRunner.start();
			} catch (Exception e) {
				try {
					// We should only remove a job from the submitted job graph store
					// if the initial submission failed. Never in case of a recovery
					submittedJobGraphStore.removeJobGraph(jobId);
				} catch (Throwable t) {
					log.warn("Cannot remove job graph from submitted job graph store.", t);
					e.addSuppressed(t);
				}

				return FutureUtils.completedExceptionally(
					new JobSubmissionException(jobId, "Could not start JobManager.", e));
			}

			jobManagerRunners.put(jobId, jobManagerRunner);

			return CompletableFuture.completedFuture(Acknowledge.get());
		} else {
			return FutureUtils.completedExceptionally(
				new JobSubmissionException(jobId, "Job has already been submitted and " +
					"is currently in state " + jobSchedulingStatus + '.'));
		}
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
	public CompletableFuture<Acknowledge> cancelJob(JobID jobId, Time timeout) {
		JobManagerRunner jobManagerRunner = jobManagerRunners.get(jobId);

		if (jobManagerRunner == null) {
			return FutureUtils.completedExceptionally(new FlinkJobNotFoundException(jobId));
		} else {
			return jobManagerRunner.getJobManagerGateway().cancel(timeout);
		}
	}

	@Override
	public CompletableFuture<Acknowledge> stopJob(JobID jobId, Time timeout) {
		JobManagerRunner jobManagerRunner = jobManagerRunners.get(jobId);

		if (jobManagerRunner == null) {
			return FutureUtils.completedExceptionally(new FlinkJobNotFoundException(jobId));
		} else {
			return jobManagerRunner.getJobManagerGateway().stop(timeout);
		}
	}

	@Override
	public CompletableFuture<Acknowledge> rescaleJob(JobID jobId, int newParallelism, RescalingBehaviour rescalingBehaviour, Time timeout) {
		JobManagerRunner jobManagerRunner = jobManagerRunners.get(jobId);

		if (jobManagerRunner == null) {
			return FutureUtils.completedExceptionally(new FlinkJobNotFoundException(jobId));
		} else {
			return jobManagerRunner.getJobManagerGateway().rescaleJob(newParallelism, rescalingBehaviour, timeout);
		}
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

		ArrayList<CompletableFuture<JobStatus>> jobStatus = new ArrayList<>(jobManagerRunners.size());

		for (Map.Entry<JobID, JobManagerRunner> jobManagerRunnerEntry : jobManagerRunners.entrySet()) {
			CompletableFuture<JobStatus> jobStatusFuture = jobManagerRunnerEntry.getValue().getJobManagerGateway().requestJobStatus(timeout);

			jobStatus.add(jobStatusFuture);
		}

		CompletableFuture<Collection<JobStatus>> allJobsFuture = FutureUtils.combineAll(jobStatus);

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
		final int numberJobsRunning = jobManagerRunners.size();

		ArrayList<CompletableFuture<JobDetails>> individualJobDetails = new ArrayList<>(numberJobsRunning);

		for (JobManagerRunner jobManagerRunner : jobManagerRunners.values()) {
			individualJobDetails.add(jobManagerRunner.getJobManagerGateway().requestJobDetails(timeout));
		}

		CompletableFuture<Collection<JobDetails>> combinedJobDetails = FutureUtils.combineAll(individualJobDetails);

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
		final JobManagerRunner jobManagerRunner = jobManagerRunners.get(jobId);

		if (jobManagerRunner != null) {
			return jobManagerRunner.getJobManagerGateway().requestJobStatus(timeout);
		} else {
			final JobDetails jobDetails = archivedExecutionGraphStore.getAvailableJobDetails(jobId);

			if (jobDetails != null) {
				return CompletableFuture.completedFuture(jobDetails.getStatus());
			} else {
				return FutureUtils.completedExceptionally(new FlinkJobNotFoundException(jobId));
			}
		}
	}

	@Override
	public CompletableFuture<OperatorBackPressureStatsResponse> requestOperatorBackPressureStats(
			final JobID jobId, final JobVertexID jobVertexId) {
			final JobManagerRunner jobManagerRunner = jobManagerRunners.get(jobId);
		if (jobManagerRunner == null) {
			return FutureUtils.completedExceptionally(new FlinkJobNotFoundException(jobId));
		} else {
			return jobManagerRunner.getJobManagerGateway().requestOperatorBackPressureStats(jobVertexId);
		}
	}

	@Override
	public CompletableFuture<ArchivedExecutionGraph> requestJob(JobID jobId, Time timeout) {
		final JobManagerRunner jobManagerRunner = jobManagerRunners.get(jobId);

		if (jobManagerRunner == null) {
			final ArchivedExecutionGraph serializableExecutionGraph = archivedExecutionGraphStore.get(jobId);

			// check whether it is a completed job
			if (serializableExecutionGraph == null) {
				return FutureUtils.completedExceptionally(new FlinkJobNotFoundException(jobId));
			} else {
				return CompletableFuture.completedFuture(serializableExecutionGraph);
			}
		} else {
			return jobManagerRunner.getJobManagerGateway().requestJob(timeout);
		}
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
		if (jobManagerRunners.containsKey(jobId)) {
			return jobManagerRunners.get(jobId)
				.getJobManagerGateway()
				.triggerSavepoint(targetDirectory, cancelJob, timeout);
		} else {
			return FutureUtils.completedExceptionally(new FlinkJobNotFoundException(jobId));
		}
	}

	/**
	 * Cleans up the job related data from the dispatcher. If cleanupHA is true, then
	 * the data will also be removed from HA.
	 *
	 * @param jobId JobID identifying the job to clean up
	 * @param cleanupHA True iff HA data shall also be cleaned up
	 */
	private void removeJob(JobID jobId, boolean cleanupHA) throws Exception {
		JobManagerRunner jobManagerRunner = jobManagerRunners.remove(jobId);

		if (jobManagerRunner != null) {
			final CompletableFuture<Void> jobManagerRunnerTerminationFuture = jobManagerRunner.closeAsync();
			registerOrphanedJobManagerTerminationFuture(jobManagerRunnerTerminationFuture);
		}

		jobManagerMetricGroup.removeJob(jobId);

		if (cleanupHA) {
			submittedJobGraphStore.removeJobGraph(jobId);
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
	void recoverJobs() {
		log.info("Recovering all persisted jobs.");

		getRpcService().execute(
			() -> {
				final Collection<JobID> jobIds;

				try {
					jobIds = submittedJobGraphStore.getJobIds();
				} catch (Exception e) {
					log.error("Could not recover job ids from the submitted job graph store. Aborting recovery.", e);
					return;
				}

				for (JobID jobId : jobIds) {
					try {
						SubmittedJobGraph submittedJobGraph = submittedJobGraphStore.recoverJobGraph(jobId);

						runAsync(() -> submitJob(submittedJobGraph.getJobGraph(), RpcUtils.INF_TIMEOUT));
					} catch (Exception e) {
						log.error("Could not recover the job graph for " + jobId + '.', e);
					}
				}
			});
	}

	protected void onFatalError(Throwable throwable) {
		log.error("Fatal error occurred in dispatcher {}.", getAddress(), throwable);

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

		try {
			removeJob(jobId, true);
		} catch (Exception e) {
			log.warn("Could not properly remove job {} from the dispatcher.", jobId, e);
		}
	}

	protected void jobNotFinished(JobID jobId) {
		log.info("Job {} was not finished by JobManager.", jobId);

		try {
			removeJob(jobId, false);
		} catch (Exception e) {
			log.warn("Could not properly remove job {} from the dispatcher.", jobId, e);
		}
	}

	private void registerOrphanedJobManagerTerminationFuture(CompletableFuture<Void> jobManagerRunnerTerminationFuture) {
		orphanedJobManagerRunnersTerminationFuture = FutureUtils.completeAll(Arrays.asList(
			orphanedJobManagerRunnersTerminationFuture,
			jobManagerRunnerTerminationFuture));
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
		runAsyncWithoutFencing(
			() -> {
				final DispatcherId dispatcherId = new DispatcherId(newLeaderSessionID);

				log.info("Dispatcher {} was granted leadership with fencing token {}", getAddress(), dispatcherId);

				// clear the state if we've been the leader before
				if (getFencingToken() != null) {
					final CompletableFuture<Void> jobManagerRunnersTerminationFuture = terminateJobManagerRunners();
					registerOrphanedJobManagerTerminationFuture(jobManagerRunnersTerminationFuture);
					jobManagerRunners.clear();
				}

				setFencingToken(dispatcherId);

				// confirming the leader session ID might be blocking,
				getRpcService().execute(
					() -> leaderElectionService.confirmLeaderSessionID(newLeaderSessionID));

				recoverJobs();
			});
	}

	/**
	 * Callback method when current resourceManager loses leadership.
	 */
	@Override
	public void revokeLeadership() {
		runAsyncWithoutFencing(
			() -> {
				log.info("Dispatcher {} was revoked leadership.", getAddress());

				final CompletableFuture<Void> jobManagerRunnersTerminationFuture = terminateJobManagerRunners();
				registerOrphanedJobManagerTerminationFuture(jobManagerRunnersTerminationFuture);
				jobManagerRunners.clear();

				// clear the fencing token indicating that we don't have the leadership right now
				setFencingToken(null);
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
			JobManagerJobMetricGroup jobManagerJobMetricGroup,
			@Nullable String metricQueryServicePath,
			@Nullable String restAddress) throws Exception;
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
				JobManagerJobMetricGroup jobManagerJobMetricGroup,
				@Nullable String metricQueryServicePath,
				@Nullable String restAddress) throws Exception {
			return new JobManagerRunner(
				resourceId,
				jobGraph,
				configuration,
				rpcService,
				highAvailabilityServices,
				heartbeatServices,
				blobServer,
				jobManagerServices,
				jobManagerJobMetricGroup,
				metricQueryServicePath,
				restAddress);
		}
	}
}
