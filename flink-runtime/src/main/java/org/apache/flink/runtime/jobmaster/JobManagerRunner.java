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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.RunningJobsRegistry;
import org.apache.flink.runtime.highavailability.RunningJobsRegistry.JobSchedulingStatus;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.OnCompletionActions;
import org.apache.flink.runtime.leaderelection.LeaderContender;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.util.AutoCloseableAsync;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The runner for the job manager. It deals with job level leader election and make underlying job manager
 * properly reacted.
 */
public class JobManagerRunner implements LeaderContender, OnCompletionActions, FatalErrorHandler, AutoCloseableAsync {

	private static final Logger log = LoggerFactory.getLogger(JobManagerRunner.class);

	// ------------------------------------------------------------------------

	/** Lock to ensure that this runner can deal with leader election event and job completion notifies simultaneously. */
	private final Object lock = new Object();

	/** The job graph needs to run. */
	private final JobGraph jobGraph;

	/** Used to check whether a job needs to be run. */
	private final RunningJobsRegistry runningJobsRegistry;

	/** Leader election for this job. */
	private final LeaderElectionService leaderElectionService;

	private final JobManagerSharedServices jobManagerSharedServices;

	private final JobMaster jobManager;

	private final Time rpcTimeout;

	private final CompletableFuture<ArchivedExecutionGraph> resultFuture;

	private final CompletableFuture<Void> terminationFuture;

	/** flag marking the runner as shut down. */
	private volatile boolean shutdown;

	// ------------------------------------------------------------------------

	/**
	 * Exceptions that occur while creating the JobManager or JobManagerRunner are directly
	 * thrown and not reported to the given {@code FatalErrorHandler}.
	 *
	 * @throws Exception Thrown if the runner cannot be set up, because either one of the
	 *                   required services could not be started, ot the Job could not be initialized.
	 */
	public JobManagerRunner(
			final ResourceID resourceId,
			final JobGraph jobGraph,
			final Configuration configuration,
			final RpcService rpcService,
			final HighAvailabilityServices haServices,
			final HeartbeatServices heartbeatServices,
			final BlobServer blobServer,
			final JobManagerSharedServices jobManagerSharedServices,
			final JobManagerJobMetricGroup jobManagerJobMetricGroup,
			@Nullable final String metricQueryServicePath,
			@Nullable final String restAddress) throws Exception {

		this.resultFuture = new CompletableFuture<>();
		this.terminationFuture = new CompletableFuture<>();

		// make sure we cleanly shut down out JobManager services if initialization fails
		try {
			this.jobGraph = checkNotNull(jobGraph);
			this.jobManagerSharedServices = checkNotNull(jobManagerSharedServices);

			checkArgument(jobGraph.getNumberOfVertices() > 0, "The given job is empty");

			// libraries and class loader first
			final LibraryCacheManager libraryCacheManager = jobManagerSharedServices.getLibraryCacheManager();
			try {
				libraryCacheManager.registerJob(
						jobGraph.getJobID(), jobGraph.getUserJarBlobKeys(), jobGraph.getClasspaths());
			} catch (IOException e) {
				throw new Exception("Cannot set up the user code libraries: " + e.getMessage(), e);
			}

			final ClassLoader userCodeLoader = libraryCacheManager.getClassLoader(jobGraph.getJobID());
			if (userCodeLoader == null) {
				throw new Exception("The user code class loader could not be initialized.");
			}

			// high availability services next
			this.runningJobsRegistry = haServices.getRunningJobsRegistry();
			this.leaderElectionService = haServices.getJobManagerLeaderElectionService(jobGraph.getJobID());

			final JobMasterConfiguration jobMasterConfiguration = JobMasterConfiguration.fromConfiguration(configuration);

			this.rpcTimeout = jobMasterConfiguration.getRpcTimeout();

			// now start the JobManager
			this.jobManager = new JobMaster(
				rpcService,
				jobMasterConfiguration,
				resourceId,
				jobGraph,
				haServices,
				jobManagerSharedServices,
				heartbeatServices,
				blobServer,
				jobManagerJobMetricGroup,
				this,
				this,
				userCodeLoader,
				restAddress,
				metricQueryServicePath);
		}
		catch (Throwable t) {
			terminationFuture.completeExceptionally(t);
			resultFuture.completeExceptionally(t);

			throw new JobExecutionException(jobGraph.getJobID(), "Could not set up JobManager", t);
		}
	}

	//----------------------------------------------------------------------------------------------
	// Getter
	//----------------------------------------------------------------------------------------------

	public JobMasterGateway getJobManagerGateway() {
		return jobManager.getSelfGateway(JobMasterGateway.class);
	}

	public JobGraph getJobGraph() {
		return jobGraph;
	}

	public CompletableFuture<ArchivedExecutionGraph> getResultFuture() {
		return resultFuture;
	}

	//----------------------------------------------------------------------------------------------
	// Lifecycle management
	//----------------------------------------------------------------------------------------------

	public void start() throws Exception {
		try {
			leaderElectionService.start(this);
		}
		catch (Exception e) {
			log.error("Could not start the JobManager because the leader election service did not start.", e);
			throw new Exception("Could not start the leader election service.", e);
		}
	}

	@Override
	public CompletableFuture<Void> closeAsync() {
		synchronized (lock) {
			if (!shutdown) {
				shutdown = true;

				jobManager.shutDown();

				final CompletableFuture<Void> jobManagerTerminationFuture = jobManager.getTerminationFuture();

				jobManagerTerminationFuture.whenComplete(
					(Void ignored, Throwable throwable) -> {
						try {
							leaderElectionService.stop();
						} catch (Throwable t) {
							throwable = ExceptionUtils.firstOrSuppressed(t, ExceptionUtils.stripCompletionException(throwable));
						}

						if (throwable != null) {
							terminationFuture.completeExceptionally(
								new FlinkException("Could not properly shut down the JobManagerRunner", throwable));
						} else {
							terminationFuture.complete(null);
						}
					});

				terminationFuture.whenComplete(
					(Void ignored, Throwable throwable) -> {
						resultFuture.completeExceptionally(new JobNotFinishedException(jobGraph.getJobID()));
					});
			}

			return terminationFuture;
		}
	}

	//----------------------------------------------------------------------------------------------
	// Result and error handling methods
	//----------------------------------------------------------------------------------------------

	/**
	 * Job completion notification triggered by JobManager.
	 */
	@Override
	public void jobReachedGloballyTerminalState(ArchivedExecutionGraph executionGraph) {
		// complete the result future with the terminal execution graph
		unregisterJobFromHighAvailability();
		resultFuture.complete(executionGraph);
	}

	/**
	 * Job completion notification triggered by self.
	 */
	@Override
	public void jobFinishedByOther() {
		resultFuture.completeExceptionally(new JobNotFinishedException(jobGraph.getJobID()));
	}

	/**
	 * Job completion notification triggered by JobManager or self.
	 */
	@Override
	public void onFatalError(Throwable exception) {
		// we log first to make sure an explaining message goes into the log
		// we even guard the log statement here to increase chances that the error handler
		// gets the notification on hard critical situations like out-of-memory errors
		try {
			log.error("JobManager runner encountered a fatal error.", exception);
		} catch (Throwable ignored) {}

		resultFuture.completeExceptionally(exception);
	}

	/**
	 * Marks this runner's job as not running. Other JobManager will not recover the job
	 * after this call.
	 *
	 * <p>This method never throws an exception.
	 */
	private void unregisterJobFromHighAvailability() {
		try {
			runningJobsRegistry.setJobFinished(jobGraph.getJobID());
		}
		catch (Throwable t) {
			log.error("Could not un-register from high-availability services job {} ({})." +
					"Other JobManager's may attempt to recover it and re-execute it.",
					jobGraph.getName(), jobGraph.getJobID(), t);
		}
	}

	//----------------------------------------------------------------------------------------------
	// Leadership methods
	//----------------------------------------------------------------------------------------------

	@Override
	public void grantLeadership(final UUID leaderSessionID) {
		synchronized (lock) {
			if (shutdown) {
				log.info("JobManagerRunner already shutdown.");
				return;
			}

			log.info("JobManager runner for job {} ({}) was granted leadership with session id {} at {}.",
				jobGraph.getName(), jobGraph.getJobID(), leaderSessionID, getAddress());

			// The operation may be blocking, but since this runner is idle before it been granted the leadership,
			// it's okay that job manager wait for the operation complete
			leaderElectionService.confirmLeaderSessionID(leaderSessionID);

			final JobSchedulingStatus schedulingStatus;
			try {
				schedulingStatus = runningJobsRegistry.getJobSchedulingStatus(jobGraph.getJobID());
			}
			catch (Throwable t) {
				log.error("Could not access status (running/finished) of job {}. ", jobGraph.getJobID(), t);
				onFatalError(t);
				return;
			}

			if (schedulingStatus == JobSchedulingStatus.DONE) {
				log.info("Granted leader ship but job {} has been finished. ", jobGraph.getJobID());
				jobFinishedByOther();
				return;
			}

			// Double check the leadership after we confirm that, there is a small chance that multiple
			// job managers schedule the same job after if they try to recover at the same time.
			// This will eventually be noticed, but can not be ruled out from the beginning.
			if (leaderElectionService.hasLeadership()) {
				try {
					// Now set the running status is after getting leader ship and
					// set finished status after job in terminated status.
					// So if finding the job is running, it means someone has already run the job, need recover.
					if (schedulingStatus == JobSchedulingStatus.PENDING) {
						runningJobsRegistry.setJobRunning(jobGraph.getJobID());
					}

					CompletableFuture<Acknowledge> startingFuture = jobManager.start(new JobMasterId(leaderSessionID), rpcTimeout);

					startingFuture.whenCompleteAsync(
						(Acknowledge ack, Throwable throwable) -> {
							if (throwable != null) {
								onFatalError(new Exception("Could not start the job manager.", throwable));
							}
						},
						jobManagerSharedServices.getScheduledExecutorService());
				} catch (Exception e) {
					onFatalError(new Exception("Could not start the job manager.", e));
				}
			}
		}
	}

	@Override
	public void revokeLeadership() {
		synchronized (lock) {
			if (shutdown) {
				log.info("JobManagerRunner already shutdown.");
				return;
			}

			log.info("JobManager for job {} ({}) was revoked leadership at {}.",
				jobGraph.getName(), jobGraph.getJobID(), getAddress());

			CompletableFuture<Acknowledge>  suspendFuture = jobManager.suspend(new FlinkException("JobManager is no longer the leader."), rpcTimeout);

			suspendFuture.whenCompleteAsync(
				(Acknowledge ack, Throwable throwable) -> {
					if (throwable != null) {
						onFatalError(new Exception("Could not start the job manager.", throwable));
					}
				},
				jobManagerSharedServices.getScheduledExecutorService());
		}
	}

	@Override
	public String getAddress() {
		return jobManager.getAddress();
	}

	@Override
	public void handleError(Exception exception) {
		log.error("Leader Election Service encountered a fatal error.", exception);
		onFatalError(exception);
	}

	//----------------------------------------------------------------------------------------------
	// Testing
	//----------------------------------------------------------------------------------------------

	@VisibleForTesting
	boolean isShutdown() {
		return shutdown;
	}
}
