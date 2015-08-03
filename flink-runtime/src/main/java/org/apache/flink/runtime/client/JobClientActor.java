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

package org.apache.flink.runtime.client;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.PoisonPill;
import akka.actor.ReceiveTimeout;
import akka.actor.Status;
import akka.actor.Terminated;
import com.google.common.base.Preconditions;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.FlinkUntypedActor;
import org.apache.flink.runtime.akka.ListeningBehaviour;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.messages.ExecutionGraphMessages;
import org.apache.flink.runtime.messages.JobClientMessages;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.slf4j.Logger;

import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

import java.util.UUID;

/**
 * Actor which constitutes the bridge between the non-actor code and the JobManager. The JobClient
 * is used to submit jobs to the JobManager and to request the port of the BlobManager.
 */
public class JobClientActor extends FlinkUntypedActor {
	
	private final ActorRef jobManager;
	private final Logger logger;
	private final boolean sysoutUpdates;

	/** leader session ID of the JobManager when this actor was created */
	private final UUID leaderSessionID;

	/** Actor which submits a job to the JobManager via this actor */
	private ActorRef submitter;

	// timeout for a message from the job manager
	private static FiniteDuration JOB_CLIENT_JOB_MANAGER_TIMEOUT;

	// heartbeat interval for pinging the job manager for job status
	private static FiniteDuration JOB_CLIENT_HEARTBEAT_INTERVAL;

	// initial time delay before starting pinging job manager over regular intervals
	private static FiniteDuration JOB_CLIENT_INITIAL_PING_DELAY;

	// maximum waiting time for a job to go to running status (milliseconds)
	private static long JOB_CLIENT_JOB_STATUS_TIMEOUT;

	// time at which the current job was created
	private long currentJobCreatedAt;

	// current job id
	private JobID currentJobId;

	// scheduler to ping JobManager after a time interval
	private Cancellable scheduler;

	// maintain when we got our last ping from the Job Manager.
	private long jobManagerPinged = 0;

	// maintain the last time we got a terminal state message
	private long terminalStateAt = 0;


	public JobClientActor(ActorRef jobManager, Logger logger, boolean sysoutUpdates,
			UUID leaderSessionID, Configuration config) {
		this.jobManager = Preconditions.checkNotNull(jobManager, "The JobManager ActorRef must not be null.");
		this.logger = Preconditions.checkNotNull(logger, "The logger must not be null.");

		this.leaderSessionID = leaderSessionID;
		this.sysoutUpdates = sysoutUpdates;
		// set this to 0 to indicate the job hasn't been created yet.
		this.currentJobCreatedAt = 0;
		this.terminalStateAt = 0;
		parseTimes(config);
	}
	
	@Override
	protected void handleMessage(Object message) {
		
		// first see if the message was from the Job Manager
		if (getContext().sender() == jobManager) {
			this.jobManagerPinged = System.currentTimeMillis();
		}

		// ======= Job status messages on regular intervals ==============
		if (message instanceof JobManagerMessages.CurrentJobStatus) {
			JobStatus statusReport = ((JobManagerMessages.CurrentJobStatus) message).status();
			long timeDiff;
			switch (statusReport) {
				case RUNNING:
					// Vincent, we happy?
					resetTimeouts();
					break;
				case FINISHED:
					// Yeah! We happy!
					resetTimeouts();
					break;
				case CREATED:
					// we're still at Job CREATED. Let's see if we're over the limit.
					timeDiff = (System.currentTimeMillis() - this.currentJobCreatedAt);
					if (timeDiff > JOB_CLIENT_JOB_STATUS_TIMEOUT) {
						failWithTimeout(timeDiff);
					} // otherwise just wait a bit longer.
					break;
				case RESTARTING:
					if (this.currentJobCreatedAt == 0) {
						// we effectively have re-created the job
						this.currentJobCreatedAt = System.currentTimeMillis();
					}
					else {
						// it was already at either CREATED or RESTARTING. See if we're over the limit.
						timeDiff = (System.currentTimeMillis() - this.currentJobCreatedAt);
						if (timeDiff > JOB_CLIENT_JOB_STATUS_TIMEOUT) {
							failWithTimeout(timeDiff);
						} // otherwise let's wait a bit more.
					}
					break;
				default:
					if (this.terminalStateAt == 0) {
						this.terminalStateAt = System.currentTimeMillis();
					}
					else {
						timeDiff = (System.currentTimeMillis() - this.terminalStateAt);

						if (timeDiff > JOB_CLIENT_JOB_STATUS_TIMEOUT) {
							failWithTimeout(timeDiff);
						}
					}
			}

		}
		// =========== State Change Messages ===============

		if (message instanceof ExecutionGraphMessages.ExecutionStateChanged) {
			logAndPrintMessage(message);
			// if we get SCHEDULED, DEPLOYING, RUNNING OR FINISHED, we're okay :)
			switch (((ExecutionGraphMessages.ExecutionStateChanged) message).newExecutionState()) {
				case SCHEDULED:
					resetTimeouts();
					break;
				case DEPLOYING:
					resetTimeouts();
					break;
				case RUNNING:
					// we don't wanna miss a running state
					resetTimeouts();
					break;
				case FINISHED:
					resetTimeouts();
					break;
				default:
					// do nothing. This will be handled in the regular RequestJobStatus messages.
			}
		}
		else if (message instanceof ExecutionGraphMessages.JobStatusChanged) {
			// even though we're polling the Job Manager regularly, this is kept for logging purposes
			logAndPrintMessage(message);
		}

		// =========== Job Life Cycle Messages ===============
		
		// submit a job to the JobManager
		else if (message instanceof JobClientMessages.SubmitJobAndWait) {
			// sanity check that this no job was submitted through this actor before -
			// it is a one-shot actor after all
			if (this.submitter == null) {
				JobGraph jobGraph = ((JobClientMessages.SubmitJobAndWait) message).jobGraph();
				if (jobGraph == null) {
					logger.error("Received null JobGraph");
					sender().tell(
							decorateMessage(new Status.Failure(new Exception("JobGraph is null"))),
							getSelf());
				}
				else {
					logger.info("Sending message to JobManager {} to submit job {} ({}) and wait for progress",
							jobManager.path().toString(), jobGraph.getName(), jobGraph.getJobID());

					this.submitter = getSender();
					jobManager.tell(
						decorateMessage(
							new JobManagerMessages.SubmitJob(
								jobGraph,
								ListeningBehaviour.EXECUTION_RESULT_AND_STATE_CHANGES)),
							getSelf());
					
					this.currentJobId = jobGraph.getJobID();
					this.currentJobCreatedAt = System.currentTimeMillis();
					getContext().setReceiveTimeout(JOB_CLIENT_JOB_MANAGER_TIMEOUT);
					// make sure we notify the sender when the connection got lost
					getContext().watch(jobManager);
				}
			}
			else {
				// repeated submission - tell failure to sender and kill self
				String msg = "Received repeated 'SubmitJobAndWait'";
				logger.error(msg);
				getSender().tell(
					decorateMessage(new Status.Failure(new Exception(msg))), ActorRef.noSender());

				// cancel scheduler and inactivity triggered receive messages
				getContext().unwatch(jobManager);
				resetContextAndActor();
				getSelf().tell(decorateMessage(PoisonPill.getInstance()), ActorRef.noSender());
			}
		}
		// acknowledgement to submit job is only logged, our original
		// submitter is only interested in the final job result
		else if (message instanceof JobManagerMessages.JobResultSuccess ||
				message instanceof JobManagerMessages.JobResultFailure) {
			
			if (logger.isDebugEnabled()) {
				logger.debug("Received {} message from JobManager", message.getClass().getSimpleName());
			}

			// forward the success to the original job submitter
			if (this.submitter != null) {
				this.submitter.tell(decorateMessage(message), getSelf());
			}
			
			// we are done, stop ourselves
			getContext().unwatch(jobManager);
			resetContextAndActor();
			getSelf().tell(decorateMessage(PoisonPill.getInstance()), ActorRef.noSender());
		}
		else if (message instanceof JobManagerMessages.JobSubmitSuccess) {
			// job was successfully submitted :-)
			// schedule to receive updates from Job manager after heartbeat interval
			this.scheduler = getContext().system().scheduler().schedule(
					JOB_CLIENT_INITIAL_PING_DELAY,
					JOB_CLIENT_HEARTBEAT_INTERVAL,
					jobManager,
					decorateMessage(new JobManagerMessages.RequestJobStatus(currentJobId)),
					getContext().dispatcher(),
					self()
			);
			logger.info("Job was successfully submitted to the JobManager");
		}

		else if (message instanceof Status.Failure) {
			// job execution failed, inform the actor that submitted the job
			logger.debug("Received failure from JobManager", ((Status.Failure) message).cause());
			if (submitter != null) {
				submitter.tell(decorateMessage(message), sender());
				resetContextAndActor();
			}
		}

		// =========== Actor / Communication Failure ===============
		
		else if (message instanceof Terminated) {
			ActorRef target = ((Terminated) message).getActor();
			if (jobManager.equals(target)) {
				String msg = "Lost connection to JobManager " + jobManager.path();
				logger.info(msg);
				submitter.tell(decorateMessage(new Status.Failure(new JobExecutionException(currentJobId, msg))), getSelf());
				resetContextAndActor();
			}
			else {
				logger.error("Received 'Terminated' for unknown actor " + target);
			}
		}

		// ============= No messgaes received in the job manager timeout duration ========
		else if (message instanceof ReceiveTimeout) {
			double tolerance = 0.1 * JOB_CLIENT_JOB_MANAGER_TIMEOUT.toMillis();
			if (System.currentTimeMillis() - jobManagerPinged < tolerance) {
				// do nothing. This is a stray timeout message. It was enqueued but we still had a message just now.
				// otherwise, this time difference must be close to the actual desired timeout. If not, akka screwed up. :')
			}
			String msg = "Connection to JobManager " + jobManager.path() + " timed out";
			logger.info(msg);
			submitter.tell(decorateMessage(new Status.Failure(new JobExecutionException(currentJobId, msg))), getSelf());
			resetContextAndActor();
		}

		// =========== Unknown Messages ===============
		
		else {
			logger.error("JobClient received unknown message: " + message);
		}
	}

	private void failWithTimeout(long timeDiff) {
		String msg = "Job hasn't been running for more than " + JOB_CLIENT_JOB_STATUS_TIMEOUT / 1.0e+3 + " seconds";

		logger.debug(msg);
		if (submitter != null) {
			submitter.tell(decorateMessage(new Status.Failure(new JobExecutionException(currentJobId, msg))), sender());
			resetContextAndActor();
		}
	}

	@Override
	protected UUID getLeaderSessionID() {
		return leaderSessionID;
	}

	private void logAndPrintMessage(Object message) {
		logger.info(message.toString());
		if (sysoutUpdates) {
			System.out.println(message.toString());
		}
	}

	private void resetContextAndActor() {
		scheduler.cancel();
		this.currentJobCreatedAt = 0;
		this.currentJobId = null;
		getContext().setReceiveTimeout(Duration.Undefined());
	}

	private void parseTimes(Configuration config) {
		// parse times from configuration
		Duration job_client_heartbeat_interval = Duration.apply(config.getString(
				ConfigConstants.JOB_CLIENT_HEARTBEAT_INTERVAL, ConfigConstants.DEFAULT_JOB_CLIENT_HEARTBEAT_INTERVAL));
		Duration job_client_initial_delay = Duration.apply(config.getString(
				ConfigConstants.JOB_CLIENT_INITIAL_DELAY, ConfigConstants.DEFAULT_JOB_CLIENT_INITIAL_DELAY));
		Duration job_client_timeout_jobmanager = Duration.apply(config.getString(
				ConfigConstants.JOB_CLIENT_JOB_MANAGER_TIMEOUT, ConfigConstants.DEFAULT_JOB_CLIENT_JOB_MANAGER_TIMEOUT));
		Duration job_client_timeout_jobstatus = Duration.apply(config.getString(
				ConfigConstants.JOB_CLIENT_JOB_STATUS_TIMEOUT, ConfigConstants.DEFAULT_JOB_CLIENT_JOB_STATUS_TIMEOUT));

		JOB_CLIENT_HEARTBEAT_INTERVAL = new FiniteDuration(job_client_heartbeat_interval.length(), job_client_heartbeat_interval.unit());
		JOB_CLIENT_INITIAL_PING_DELAY = new FiniteDuration(job_client_initial_delay.length(), job_client_initial_delay.unit());
		JOB_CLIENT_JOB_MANAGER_TIMEOUT = new FiniteDuration(job_client_timeout_jobmanager.length(), job_client_timeout_jobmanager.unit());
		JOB_CLIENT_JOB_STATUS_TIMEOUT = job_client_timeout_jobstatus.toMillis();

	}

	private void resetTimeouts() {
		this.currentJobCreatedAt = 0;
		this.terminalStateAt = 0;
	}
}
