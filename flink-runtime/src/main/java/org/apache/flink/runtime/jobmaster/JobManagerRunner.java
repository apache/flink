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
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.OnCompletionActions;
import org.apache.flink.runtime.jobmanager.SubmittedJobGraphStore;
import org.apache.flink.runtime.jobmanager.scheduler.Scheduler;
import org.apache.flink.runtime.leaderelection.LeaderContender;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.rpc.RpcService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

/**
 * The runner for the job manager. It deals with job level leader election and make underlying job manager
 * properly reacted.
 */
public class JobManagerRunner implements LeaderContender, OnCompletionActions {

	private final Logger log = LoggerFactory.getLogger(JobManagerRunner.class);

	/** Lock to ensure that this runner can deal with leader election event and job completion notifies simultaneously */
	private final Object lock = new Object();

	/** The job graph needs to run */
	private final JobGraph jobGraph;

	private final OnCompletionActions toNotify;

	/** Used to check whether a job needs to be run */
	private final SubmittedJobGraphStore submittedJobGraphStore;

	/** Leader election for this job */
	private final LeaderElectionService leaderElectionService;

	private final JobMaster jobManager;

	/** flag marking the runner as shut down */
	private volatile boolean shutdown;

	public JobManagerRunner(
		final JobGraph jobGraph,
		final Configuration configuration,
		final RpcService rpcService,
		final HighAvailabilityServices haServices,
		final OnCompletionActions toNotify) throws Exception
	{
		this(jobGraph, configuration, rpcService, haServices,
			JobManagerServices.fromConfiguration(configuration), toNotify);
	}

	public JobManagerRunner(
		final JobGraph jobGraph,
		final Configuration configuration,
		final RpcService rpcService,
		final HighAvailabilityServices haServices,
		final JobManagerServices jobManagerServices,
		final OnCompletionActions toNotify) throws Exception
	{
		this.jobGraph = jobGraph;
		this.toNotify = toNotify;
		this.submittedJobGraphStore = haServices.getSubmittedJobGraphStore();
		this.leaderElectionService = haServices.getJobManagerLeaderElectionService(jobGraph.getJobID());

		this.jobManager = new JobMaster(
			jobGraph, configuration, rpcService, haServices,
			jobManagerServices.libraryCacheManager,
			jobManagerServices.restartStrategyFactory,
			jobManagerServices.savepointStore,
			jobManagerServices.timeout,
			new Scheduler(jobManagerServices.executorService),
			jobManagerServices.jobManagerMetricGroup,
			this);
	}

	//----------------------------------------------------------------------------------------------
	// Lifecycle management
	//----------------------------------------------------------------------------------------------

	public void start() throws Exception {
		jobManager.init();
		jobManager.start();

		try {
			leaderElectionService.start(this);
		}
		catch (Exception e) {
			log.error("Could not start the JobManager because the leader election service did not start.", e);
			throw new Exception("Could not start the leader election service.", e);
		}
	}

	public void shutdown() {
		shutdown(new Exception("The JobManager runner is shutting down"));
	}

	public void shutdown(Throwable cause) {
		// TODO what is the cause used for ?
		shutdownInternally();
	}

	private void shutdownInternally() {
		synchronized (lock) {
			shutdown = true;

			if (leaderElectionService != null) {
				try {
					leaderElectionService.stop();
				} catch (Exception e) {
					log.error("Could not properly shutdown the leader election service.");
				}
			}

			jobManager.shutDown();
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
			shutdownInternally();
		}
		finally {
			if (toNotify != null) {
				toNotify.jobFinished(result);
			}
		}
	}

	/**
	 * Job completion notification triggered by JobManager
	 */
	@Override
	public void jobFailed(Throwable cause) {
		try {
			shutdownInternally();
		}
		finally {
			if (toNotify != null) {
				toNotify.jobFailed(cause);
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
			if (toNotify != null) {
				toNotify.jobFinishedByOther();
			}
		}
	}

	/**
	 * Job completion notification triggered by JobManager or self
	 */
	@Override
	public void onFatalError(Throwable exception) {
		// first and in any case, notify our handler, so it can react fast
		try {
			if (toNotify != null) {
				toNotify.onFatalError(exception);
			}
		}
		finally {
			log.error("JobManager runner encountered a fatal error.", exception);
			shutdownInternally();
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

			// Double check the leadership after we confirm that, there is a small chance that multiple
			// job managers schedule the same job after if they try to recover at the same time.
			// This will eventually be noticed, but can not be ruled out from the beginning.
			if (leaderElectionService.hasLeadership()) {
				if (isJobFinishedByOthers()) {
					log.info("Job {} ({}) already finished by others.", jobGraph.getName(), jobGraph.getJobID());
					jobFinishedByOther();
				} else {
					jobManager.getSelf().startJob(leaderSessionID);
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

			jobManager.getSelf().suspendJob(new Exception("JobManager is no longer the leader."));
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

	@VisibleForTesting
	boolean isJobFinishedByOthers() {
		// TODO: Fix
		return false;
	}

	@VisibleForTesting
	boolean isShutdown() {
		return shutdown;
	}
}
