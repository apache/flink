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
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.execution.librarycache.BlobLibraryCacheManager;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.RunningJobsRegistry;
import org.apache.flink.runtime.highavailability.RunningJobsRegistry.JobSchedulingStatus;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.OnCompletionActions;
import org.apache.flink.runtime.leaderelection.LeaderContender;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.metrics.groups.JobManagerMetricGroup;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The runner for the job manager. It deals with job level leader election and make underlying job manager
 * properly reacted.
 */
public class JobManagerRunner implements LeaderContender, OnCompletionActions, FatalErrorHandler {

	private static final Logger log = LoggerFactory.getLogger(JobManagerRunner.class);

	// ------------------------------------------------------------------------

	/** Lock to ensure that this runner can deal with leader election event and job completion notifies simultaneously */
	private final Object lock = new Object();

	/** The job graph needs to run */
	private final JobGraph jobGraph;

	/** The listener to notify once the job completes - either successfully or unsuccessfully */
	private final OnCompletionActions toNotifyOnComplete;

	/** The handler to call in case of fatal (unrecoverable) errors */ 
	private final FatalErrorHandler errorHandler;

	/** Used to check whether a job needs to be run */
	private final RunningJobsRegistry runningJobsRegistry;

	/** Leader election for this job */
	private final LeaderElectionService leaderElectionService;

	private final JobManagerServices jobManagerServices;

	private final JobMaster jobManager;

	private final JobManagerMetricGroup jobManagerMetricGroup;

	/** flag marking the runner as shut down */
	private volatile boolean shutdown;

	// ------------------------------------------------------------------------

	public JobManagerRunner(
			final ResourceID resourceId,
			final JobGraph jobGraph,
			final Configuration configuration,
			final RpcService rpcService,
			final HighAvailabilityServices haServices,
			final HeartbeatServices heartbeatServices,
			final OnCompletionActions toNotifyOnComplete,
			final FatalErrorHandler errorHandler) throws Exception {
		this(
			resourceId,
			jobGraph,
			configuration,
			rpcService,
			haServices,
			heartbeatServices,
			new MetricRegistry(MetricRegistryConfiguration.fromConfiguration(configuration)),
			toNotifyOnComplete,
			errorHandler);
	}

	public JobManagerRunner(
			final ResourceID resourceId,
			final JobGraph jobGraph,
			final Configuration configuration,
			final RpcService rpcService,
			final HighAvailabilityServices haServices,
			final HeartbeatServices heartbeatServices,
			final MetricRegistry metricRegistry,
			final OnCompletionActions toNotifyOnComplete,
			final FatalErrorHandler errorHandler) throws Exception {
		this(
			resourceId,
			jobGraph,
			configuration,
			rpcService,
			haServices,
			heartbeatServices,
			JobManagerServices.fromConfiguration(configuration, haServices),
			metricRegistry,
			toNotifyOnComplete,
			errorHandler);
	}

	/**
	 * 
	 * <p>Exceptions that occur while creating the JobManager or JobManagerRunner are directly
	 * thrown and not reported to the given {@code FatalErrorHandler}.
	 * 
	 * <p>This JobManagerRunner assumes that it owns the given {@code JobManagerServices}.
	 * It will shut them down on error and on calls to {@link #shutdown()}.
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
			final JobManagerServices jobManagerServices,
			final MetricRegistry metricRegistry,
			final OnCompletionActions toNotifyOnComplete,
			final FatalErrorHandler errorHandler) throws Exception {

		JobManagerMetricGroup jobManagerMetrics = null;

		// make sure we cleanly shut down out JobManager services if initialization fails
		try {
			this.jobGraph = checkNotNull(jobGraph);
			this.toNotifyOnComplete = checkNotNull(toNotifyOnComplete);
			this.errorHandler = checkNotNull(errorHandler);
			this.jobManagerServices = checkNotNull(jobManagerServices);

			checkArgument(jobGraph.getNumberOfVertices() > 0, "The given job is empty");

			final String hostAddress = rpcService.getAddress().isEmpty() ? "localhost" : rpcService.getAddress();
			jobManagerMetrics = new JobManagerMetricGroup(metricRegistry, hostAddress);
			this.jobManagerMetricGroup = jobManagerMetrics;

			// libraries and class loader first
			final BlobLibraryCacheManager libraryCacheManager = jobManagerServices.libraryCacheManager;
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

			// now start the JobManager
			this.jobManager = new JobMaster(
				rpcService,
				resourceId,
				jobGraph,
				configuration,
				haServices,
				heartbeatServices,
				jobManagerServices.executorService,
				jobManagerServices.libraryCacheManager,
				jobManagerServices.restartStrategyFactory,
				jobManagerServices.rpcAskTimeout,
				jobManagerMetrics,
				this,
				this,
				userCodeLoader);
		}
		catch (Throwable t) {
			// clean up everything
			try {
				jobManagerServices.shutdown();
			} catch (Throwable tt) {
				log.error("Error while shutting down JobManager services", tt);
			}

			if (jobManagerMetrics != null) {
				jobManagerMetrics.close();
			}

			throw new JobExecutionException(jobGraph.getJobID(), "Could not set up JobManager", t);
		}
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

	public void shutdown() {
		shutdownInternally();
	}

	private void shutdownInternally() {
		synchronized (lock) {
			shutdown = true;

			if (leaderElectionService != null) {
				try {
					leaderElectionService.stop();
				} catch (Throwable t) {
					log.error("Could not properly shutdown the leader election service", t);
				}
			}

			try {
				jobManager.shutDown();
			} catch (Throwable t) {
				log.error("Error shutting down JobManager", t);
			}

			try {
				jobManagerServices.shutdown();
			} catch (Throwable t) {
				log.error("Error shutting down JobManager services", t);
			}

			// make all registered metrics go away
			try {
				jobManagerMetricGroup.close();
			} catch (Throwable t) {
				log.error("Error while unregistering metrics", t);
			}
		}
	}

	//----------------------------------------------------------------------------------------------
	// Result and error handling methods
	//----------------------------------------------------------------------------------------------

	/**
	 * Job completion notification triggered by JobManager
	 */
	@Override
	public void jobFinished(JobExecutionResult result) {
		try {
			unregisterJobFromHighAvailability();
			shutdownInternally();
		}
		finally {
			if (toNotifyOnComplete != null) {
				toNotifyOnComplete.jobFinished(result);
			}
		}
	}

	/**
	 * Job completion notification triggered by JobManager
	 */
	@Override
	public void jobFailed(Throwable cause) {
		try {
			unregisterJobFromHighAvailability();
			shutdownInternally();
		}
		finally {
			if (toNotifyOnComplete != null) {
				toNotifyOnComplete.jobFailed(cause);
			}
		}
	}

	/**
	 * Job completion notification triggered by self
	 */
	@Override
	public void jobFinishedByOther() {
		try {
			shutdownInternally();
		}
		finally {
			if (toNotifyOnComplete != null) {
				toNotifyOnComplete.jobFinishedByOther();
			}
		}
	}

	/**
	 * Job completion notification triggered by JobManager or self
	 */
	@Override
	public void onFatalError(Throwable exception) {
		// we log first to make sure an explaining message goes into the log
		// we even guard the log statement here to increase chances that the error handler
		// gets the notification on hard critical situations like out-of-memory errors
		try {
			log.error("JobManager runner encountered a fatal error.", exception);
		} catch (Throwable ignored) {}

		// in any case, notify our handler, so it can react fast
		try {
			if (errorHandler != null) {
				errorHandler.onFatalError(exception);
			}
		}
		finally {
			// the shutdown may not even needed any more, if the fatal error
			// handler kills the process. that is fine, a process kill cleans up better than anything.
			shutdownInternally();
		}
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

					jobManager.start(leaderSessionID);
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

			jobManager.getSelf().suspendExecution(new Exception("JobManager is no longer the leader."));
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
