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
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.Status;
import akka.actor.Terminated;
import akka.dispatch.Futures;
import akka.dispatch.OnSuccess;
import com.google.common.base.Preconditions;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.akka.FlinkUntypedActor;
import org.apache.flink.runtime.akka.ListeningBehaviour;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.instance.AkkaActorGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.messages.ExecutionGraphMessages;
import org.apache.flink.runtime.messages.JobClientMessages;
import org.apache.flink.runtime.messages.JobClientMessages.JobManagerActorRef;
import org.apache.flink.runtime.messages.JobClientMessages.JobManagerLeaderAddress;
import org.apache.flink.runtime.messages.JobClientMessages.SubmitJobAndWait;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.util.SerializedThrowable;
import scala.concurrent.duration.FiniteDuration;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.Callable;

/**
 * Actor which constitutes the bridge between the non-actor code and the JobManager. The JobClient
 * is used to submit jobs to the JobManager and to request the port of the BlobManager.
 */
public class JobClientActor extends FlinkUntypedActor implements LeaderRetrievalListener {

	private final LeaderRetrievalService leaderRetrievalService;

	/** timeout for futures */
	private final FiniteDuration timeout;

	/** true if status messages shall be printed to sysout */
	private final boolean sysoutUpdates;

	/** true if a SubmitJobSuccess message has been received */
	private boolean jobSuccessfullySubmitted = false;

	/** true if a PoisonPill was taken */
	private boolean terminated = false;

	/** ActorRef to the current leader */
	private ActorRef jobManager;

	/** leader session ID of the JobManager when this actor was created */
	private UUID leaderSessionID;

	/** Actor which submits a job to the JobManager via this actor */
	private ActorRef submitter;

	/** JobGraph which shall be submitted to the JobManager */
	private JobGraph jobGraph;

	public JobClientActor(
			LeaderRetrievalService leaderRetrievalService,
			FiniteDuration timeout,
			boolean sysoutUpdates) {
		this.leaderRetrievalService = Preconditions.checkNotNull(leaderRetrievalService);
		this.timeout = Preconditions.checkNotNull(timeout);
		this.sysoutUpdates = sysoutUpdates;
	}

	@Override
	public void preStart() {
		try {
			leaderRetrievalService.start(this);
		} catch (Exception e) {
			LOG.error("Could not start the leader retrieval service.");
			throw new RuntimeException("Could not start the leader retrieval service.", e);
		}
	}

	@Override
	public void postStop() {
		try {
			leaderRetrievalService.stop();
		} catch (Exception e) {
			LOG.warn("Could not properly stop the leader retrieval service.");
		}
	}

	@Override
	protected void handleMessage(Object message) {
		
		// =========== State Change Messages ===============

		if (message instanceof ExecutionGraphMessages.ExecutionStateChanged) {
			logAndPrintMessage((ExecutionGraphMessages.ExecutionStateChanged) message);
		} else if (message instanceof ExecutionGraphMessages.JobStatusChanged) {
			logAndPrintMessage((ExecutionGraphMessages.JobStatusChanged) message);
		}

		// ============ JobManager ActorRef resolution ===============

		else if (message instanceof JobManagerLeaderAddress) {
			JobManagerLeaderAddress msg = (JobManagerLeaderAddress) message;

			disconnectFromJobManager();

			this.leaderSessionID = msg.leaderSessionID();

			if (msg.address() != null) {
				// Resolve the job manager leader address to obtain an ActorRef
				AkkaUtils.getActorRefFuture(msg.address(), getContext().system(), timeout)
					.onSuccess(new OnSuccess<ActorRef>() {
						@Override
						public void onSuccess(ActorRef result) throws Throwable {
							getSelf().tell(decorateMessage(new JobManagerActorRef(result)), ActorRef.noSender());
						}
					}, getContext().dispatcher());
			}
		} else if (message instanceof JobManagerActorRef) {
			// Resolved JobManager ActorRef
			JobManagerActorRef msg = (JobManagerActorRef) message;
			connectToJobManager(msg.jobManager());

			if (jobGraph != null && !jobSuccessfullySubmitted) {
				// if we haven't yet submitted the job successfully
				tryToSubmitJob(jobGraph);
			}
		}

		// =========== Job Life Cycle Messages ===============
		
		// submit a job to the JobManager
		else if (message instanceof SubmitJobAndWait) {
			// only accept SubmitJobWait messages if we're not about to terminate
			if (!terminated) {
				// sanity check that this no job was submitted through this actor before -
				// it is a one-shot actor after all
				if (this.submitter == null) {
					jobGraph = ((SubmitJobAndWait) message).jobGraph();
					if (jobGraph == null) {
						LOG.error("Received null JobGraph");
						sender().tell(
							decorateMessage(new Status.Failure(new Exception("JobGraph is null"))),
							getSelf());
					} else {
						LOG.info("Received job {} ({}).", jobGraph.getName(), jobGraph.getJobID());

						this.submitter = getSender();

						// is only successful if we already know the job manager leader
						tryToSubmitJob(jobGraph);
					}
				} else {
					// repeated submission - tell failure to sender and kill self
					String msg = "Received repeated 'SubmitJobAndWait'";
					LOG.error(msg);
					getSender().tell(
						decorateMessage(new Status.Failure(new Exception(msg))), ActorRef.noSender());

					terminate();
				}
			} else {
				// we're about to receive a PoisonPill because terminated == true
				String msg = getClass().getName() + " is about to be terminated. Therefore, the " +
					"job submission cannot be executed.";
				LOG.error(msg);
				getSender().tell(
					decorateMessage(new Status.Failure(new Exception(msg))), ActorRef.noSender());
			}
		}
		// acknowledgement to submit job is only logged, our original
		// submitter is only interested in the final job result
		else if (message instanceof JobManagerMessages.JobResultSuccess ||
				message instanceof JobManagerMessages.JobResultFailure) {
			
			if (LOG.isDebugEnabled()) {
				LOG.debug("Received {} message from JobManager", message.getClass().getSimpleName());
			}

			// forward the success to the original job submitter
			if (hasJobBeenSubmitted()) {
				this.submitter.tell(decorateMessage(message), getSelf());
			}

			terminate();
		}
		else if (message instanceof JobManagerMessages.JobSubmitSuccess) {
			// job was successfully submitted :-)
			LOG.info("Job was successfully submitted to the JobManager {}.", getSender().path());
			jobSuccessfullySubmitted = true;
		}

		// =========== Actor / Communication Failure / Timeouts ===============
		
		else if (message instanceof Terminated) {
			ActorRef target = ((Terminated) message).getActor();
			if (jobManager.equals(target)) {
				LOG.info("Lost connection to JobManager {}. Triggering connection timeout.",
					jobManager.path());
				disconnectFromJobManager();

				// we only issue a connection timeout if we have submitted a job before
				// otherwise, we might have some more time to find another job manager
				// Important: The ConnectionTimeout message is filtered out in case that we are
				// notified about a new leader by setting the new leader session ID, because
				// ConnectionTimeout extends RequiresLeaderSessionID
				if (hasJobBeenSubmitted()) {
					getContext().system().scheduler().scheduleOnce(
						timeout,
						getSelf(),
						decorateMessage(JobClientMessages.getConnectionTimeout()),
						getContext().dispatcher(),
						ActorRef.noSender());
				}
			} else {
				LOG.warn("Received 'Terminated' for unknown actor " + target);
			}
		} else if (JobClientMessages.getConnectionTimeout().equals(message)) {
			// check if we haven't found a job manager yet
			if (!isConnected()) {
				if (hasJobBeenSubmitted()) {
					submitter.tell(
						decorateMessage(new Status.Failure(
							new JobClientActorConnectionTimeoutException("Lost connection to the JobManager."))),
						getSelf());
				}
				// Connection timeout reached, let's terminate
				terminate();
			}
		} else if (JobClientMessages.getSubmissionTimeout().equals(message)) {
			// check if our job submission was successful in the meantime
			if (!jobSuccessfullySubmitted) {
				if (hasJobBeenSubmitted()) {
					submitter.tell(
						decorateMessage(new Status.Failure(
							new JobClientActorSubmissionTimeoutException("Job submission to the JobManager timed out."))),
						getSelf());
				}

				// We haven't heard back from the job manager after sending the job graph to him,
				// therefore terminate
				terminate();
			}
		}

		// =========== Unknown Messages ===============
		
		else {
			LOG.error("JobClient received unknown message: " + message);
		}
	}

	@Override
	protected UUID getLeaderSessionID() {
		return leaderSessionID;
	}

	private void logAndPrintMessage(ExecutionGraphMessages.ExecutionStateChanged message) {
		LOG.info(message.toString());
		if (sysoutUpdates) {
			System.out.println(message.toString());
		}
	}

	private void logAndPrintMessage(ExecutionGraphMessages.JobStatusChanged message) {
		// by default, this only prints the status, and not any exception.
		// in state FAILING, we report the exception in addition
		if (message.newJobStatus() != JobStatus.FAILING || message.error() == null) {
			LOG.info(message.toString());
			if (sysoutUpdates) {
				System.out.println(message.toString());
			}
		} else {
			LOG.info(message.toString(), message.error());
			if (sysoutUpdates) {
				System.out.println(message.toString());
				message.error().printStackTrace(System.out);
			}
		}
	}

	@Override
	public void notifyLeaderAddress(String leaderAddress, UUID leaderSessionID) {
		getSelf().tell(
			decorateMessage(new JobManagerLeaderAddress(leaderAddress, leaderSessionID)),
			getSelf());
	}

	@Override
	public void handleError(Exception exception) {
		LOG.error("Error occurred in the LeaderRetrievalService.", exception);
		getSelf().tell(decorateMessage(PoisonPill.getInstance()), getSelf());
	}

	private void disconnectFromJobManager() {
		LOG.info("Disconnect from JobManager {}.", jobManager);
		if (jobManager != ActorRef.noSender()) {
			getContext().unwatch(jobManager);
			jobManager = ActorRef.noSender();
		}
	}

	private void connectToJobManager(ActorRef jobManager) {
		LOG.info("Connect to JobManager {}.", jobManager);
		if (jobManager != ActorRef.noSender()) {
			getContext().unwatch(jobManager);
		}

		LOG.info("Connected to new JobManager {}.", jobManager.path());

		this.jobManager = jobManager;
		getContext().watch(jobManager);
	}

	private void tryToSubmitJob(final JobGraph jobGraph) {
		this.jobGraph = jobGraph;

		if (isConnected()) {
			LOG.info("Sending message to JobManager {} to submit job {} ({}) and wait for progress",
				jobManager.path().toString(), jobGraph.getName(), jobGraph.getJobID());

			Futures.future(new Callable<Object>() {
				@Override
				public Object call() throws Exception {
					ActorGateway jobManagerGateway = new AkkaActorGateway(jobManager, leaderSessionID);

					LOG.info("Upload jar files to job manager {}.", jobManager.path());

					try {
						JobClient.uploadJarFiles(jobGraph, jobManagerGateway, timeout);
					} catch (IOException exception) {
						getSelf().tell(
							decorateMessage(new JobManagerMessages.JobResultFailure(
								new SerializedThrowable(
									new JobSubmissionException(
										jobGraph.getJobID(),
										"Could not upload the jar files to the job manager.",
										exception)
								)
							)),
							ActorRef.noSender()
						);
					}

					LOG.info("Submit job to the job manager {}.", jobManager.path());

					jobManager.tell(
						decorateMessage(
							new JobManagerMessages.SubmitJob(
								jobGraph,
								ListeningBehaviour.EXECUTION_RESULT_AND_STATE_CHANGES)),
						getSelf());

					// issue a SubmissionTimeout message to check that we submit the job within
					// the given timeout
					getContext().system().scheduler().scheduleOnce(
						timeout,
						getSelf(),
						decorateMessage(JobClientMessages.getSubmissionTimeout()),
						getContext().dispatcher(),
						ActorRef.noSender());

					return null;
				}
			}, getContext().dispatcher());
		} else {
			LOG.info("Could not submit job {} ({}), because there is no connection to a " +
					"JobManager.",
				jobGraph.getName(), jobGraph.getJobID());

			// We want to submit a job, but we haven't found a job manager yet.
			// Let's give him another chance to find a job manager within the given timeout.
			getContext().system().scheduler().scheduleOnce(
				timeout,
				getSelf(),
				decorateMessage(JobClientMessages.getConnectionTimeout()),
				getContext().dispatcher(),
				ActorRef.noSender()
			);
		}
	}

	private void terminate() {
		LOG.info("Terminate JobClientActor.");
		terminated = true;
		disconnectFromJobManager();
		getSelf().tell(decorateMessage(PoisonPill.getInstance()), ActorRef.noSender());
	}

	private boolean isConnected() {
		return jobManager != ActorRef.noSender();
	}

	private boolean hasJobBeenSubmitted() {
		return submitter != ActorRef.noSender();
	}

	public static Props createJobClientActorProps(
			LeaderRetrievalService leaderRetrievalService,
			FiniteDuration timeout,
			boolean sysoutUpdates) {
		return Props.create(
			JobClientActor.class,
			leaderRetrievalService,
			timeout,
			sysoutUpdates);
	}
}
