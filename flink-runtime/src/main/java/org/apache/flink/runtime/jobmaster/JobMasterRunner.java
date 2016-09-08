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

import akka.dispatch.OnComplete;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.savepoint.SavepointStore;
import org.apache.flink.runtime.execution.librarycache.BlobLibraryCacheManager;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategyFactory;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.scheduler.Scheduler;
import org.apache.flink.runtime.leaderelection.LeaderContender;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.metrics.groups.JobManagerMetricGroup;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.StartStoppable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.concurrent.ExecutionContext;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.util.UUID;

/**
 * The runner for the job manager. It deals with job level leader election and make underlying job manager
 * properly reacted. Also this runner takes care of determining whether job manager should be recovered,
 * until it's been fully disposed.
 */
public class JobMasterRunner implements StartStoppable, LeaderContender {

	private final Logger log = LoggerFactory.getLogger(JobMasterRunner.class);

	/** The job graph needs to run */
	private final JobGraph jobGraph;

	/** Whether is job is an initial submission or recovered */
	private volatile boolean isRecovery;

	/** Provides services needed by high availability */
	private final HighAvailabilityServices highAvailabilityServices;

	/** The manager of the job */
	private final JobMaster jobManager;

	/** The execution context which is used to execute futures */
	private final ExecutionContext executionContext;

	/** Leader election for this job */
	private LeaderElectionService leaderElectionService;

	/** Leader session id when granted leadership */
	private UUID leaderSessionID;

	public JobMasterRunner(
		final JobGraph jobGraph,
		final boolean isRecovery,
		final Configuration configuration,
		final RpcService rpcService,
		final HighAvailabilityServices highAvailabilityServices,
		final BlobLibraryCacheManager libraryCacheManager,
		final RestartStrategyFactory restartStrategyFactory,
		final SavepointStore savepointStore,
		final FiniteDuration timeout,
		final Scheduler scheduler,
		final JobManagerMetricGroup jobManagerMetricGroup)
	{
		this.jobGraph = jobGraph;
		this.isRecovery = isRecovery;
		this.highAvailabilityServices = highAvailabilityServices;
		this.jobManager = new JobMaster(jobGraph, configuration, rpcService, highAvailabilityServices,
			libraryCacheManager, restartStrategyFactory, savepointStore, timeout, scheduler, jobManagerMetricGroup);
		this.executionContext = rpcService.getExecutionContext();

		this.leaderElectionService = null;
		this.leaderSessionID = null;
	}

	//----------------------------------------------------------------------------------------------
	// Lifecycle management
	//----------------------------------------------------------------------------------------------

	public void start() {
		jobManager.start();

		try {
			leaderElectionService = highAvailabilityServices.getJobMasterLeaderElectionService(jobGraph.getJobID());
			leaderElectionService.start(this);
		} catch (Exception e) {
			log.error("Could not start the JobManager because the leader election service did not start.", e);
			throw new RuntimeException("Could not start the leader election service.", e);
		}
	}

	public void stop() {
		if (leaderElectionService != null) {
			try {
				leaderElectionService.stop();
			} catch (Exception e) {
				log.error("Could not properly shutdown the leader election service.");
			}
		}

		jobManager.shutDown();
	}

	public void done() {
		// TODO: called when job is done
	}

	//----------------------------------------------------------------------------------------------
	// Leadership methods
	//----------------------------------------------------------------------------------------------

	@Override
	public void grantLeadership(UUID leaderSessionID) {
		log.info("JobManager for job {} ({}) was granted leadership with session id {} at {}.",
			jobGraph.getJobID(), jobGraph.getName(), leaderSessionID, getAddress());

		// The operation may be blocking, but since this runner is idle before it been granted the leadership,
		// it's okay that job manager wait for the operation complete
		leaderElectionService.confirmLeaderSessionID(leaderSessionID);
		this.leaderSessionID = leaderSessionID;

		Future<Boolean> submitResult = jobManager.getSelf().submitJob(isRecovery);
		submitResult.onComplete(new OnComplete<Boolean>() {
			@Override
			public void onComplete(Throwable failure, Boolean success) throws Throwable {
				if (failure != null) {
					log.error("Failed to submit job {} ({})", jobGraph.getJobID(), jobGraph.getName(), failure);
					// TODO: notify JobMasterRunner holder to shutdown this runner.
				} else {
					if (success) {
						if (leaderElectionService.hasLeadership()) {
							// There is a small chance that multiple job managers schedule the same job after if
							// they try to recover at the same time. This will eventually be noticed, but can not be
							// ruled out from the beginning.

							// NOTE: Scheduling the job for execution is a separate action from the job submission.
							// The success of submitting the job must be independent from the success of scheduling
							// the job.
							jobManager.getSelf().startJob();

							if (!isRecovery) {
								// we have started a newly submitted job, after this, every time we want to restart
								// this job again, we should treat it as recovery
								isRecovery = true;
							}
						} else {
							// Do nothing here, since revokeLeadership will either be called soon or
							// has already been called
							log.warn("Submitted job {} ({}), but not leader already, waiting to get leadership" +
								"and then retry.", jobGraph.getJobID(), jobGraph.getName());
						}
					} else {
						// TODO: notify JobMasterRunner holder to shutdown this runner.
					}
				}
			}
		}, executionContext);

	}

	@Override
	public void revokeLeadership() {
		log.info("JobManager for job {} ({}) was revoked leadership at {}.",
			jobGraph.getJobID(), jobGraph.getName(), getAddress());
		leaderSessionID = null;
		jobManager.getSelf().suspendJob(new Exception("JobManager is no longer the leader."));
	}

	@Override
	public String getAddress() {
		return jobManager.getAddress();
	}

	@Override
	public void handleError(Exception exception) {
		log.error("Received an error from the leader election service.", exception);
		// TODO: let the outside know and stop this runner
	}
}
