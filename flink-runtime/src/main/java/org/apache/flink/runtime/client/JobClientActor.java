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
import akka.actor.Status;
import akka.actor.Terminated;
import akka.dispatch.OnSuccess;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.akka.FlinkUntypedActor;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.messages.ExecutionGraphMessages;
import org.apache.flink.runtime.messages.JobClientMessages;
import org.apache.flink.runtime.messages.JobClientMessages.JobManagerActorRef;
import org.apache.flink.runtime.messages.JobClientMessages.JobManagerLeaderAddress;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.util.SerializedThrowable;
import org.apache.flink.util.Preconditions;
import scala.concurrent.duration.FiniteDuration;

import java.util.Objects;
import java.util.UUID;


/**
 * Actor which constitutes the bridge between the non-actor code and the JobManager.
 * This base class handles the connection to the JobManager and notifies in case of timeouts. It also
 * receives and prints job updates until job completion.
 */
public abstract class JobClientActor extends FlinkUntypedActor implements LeaderRetrievalListener {

	private final LeaderRetrievalService leaderRetrievalService;

	/** timeout for futures */
	protected final FiniteDuration timeout;

	/** true if status messages shall be printed to sysout */
	private final boolean sysoutUpdates;

	/** true if a PoisonPill about to be taken */
	private boolean toBeTerminated = false;

	/** ActorRef to the current leader */
	protected ActorRef jobManager;

	/** leader session ID of the JobManager when this actor was created */
	protected UUID leaderSessionID;

	/** The client which the actor is responsible for */
	protected ActorRef client;

	private Cancellable connectionTimeout;

	private UUID connectionTimeoutId;

	public JobClientActor(
			LeaderRetrievalService leaderRetrievalService,
			FiniteDuration timeout,
			boolean sysoutUpdates) {
		this.leaderRetrievalService = Preconditions.checkNotNull(leaderRetrievalService);
		this.timeout = Preconditions.checkNotNull(timeout);
		this.sysoutUpdates = sysoutUpdates;
		this.jobManager = ActorRef.noSender();
		this.leaderSessionID = null;

		connectionTimeout = null;
		connectionTimeoutId = null;
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

	/**
	 * Hook to be called once a connection has been established with the JobManager.
	 */
	protected abstract void connectedToJobManager();

	/**
	 * Hook to handle custom client message which are not handled by the base class.
	 * @param message The message to be handled
	 */
	protected abstract void handleCustomMessage(Object message);

	/**
	 * Hook to let the client know about messages that should start a timer for a timeout
	 * @return The message class after which a timeout should be started
	 */
	protected abstract Class getClientMessageClass();


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

			if (jobManager != null) {
				// only print this message when we had been connected to a JobManager before
				logAndPrintMessage("New JobManager elected. Connecting to " + msg.address());
			}

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
			} else if (isClientConnected() && connectionTimeoutId == null) {
				// msg.address == null means that the leader has lost its leadership
				registerConnectionTimeout();
			}
		} else if (message instanceof JobManagerActorRef) {
			// Resolved JobManager ActorRef
			JobManagerActorRef msg = (JobManagerActorRef) message;
			connectToJobManager(msg.jobManager());

			logAndPrintMessage("Connected to JobManager at " + msg.jobManager() +
				" with leader session id " + leaderSessionID + '.');

			connectedToJobManager();
		}

		// =========== Job Life Cycle Messages ===============

		// acknowledgement to submit job is only logged, our original
		// client is only interested in the final job result
		else if (message instanceof JobManagerMessages.JobResultMessage) {

			if (LOG.isDebugEnabled()) {
				LOG.debug("Received {} message from JobManager", message.getClass().getSimpleName());
			}

			// forward the success to the original client
			if (isClientConnected()) {
				this.client.tell(decorateMessage(message), getSelf());
			}

			terminate();
		}

		// =========== Actor / Communication Failure / Timeouts ===============

		else if (message instanceof Terminated) {
			ActorRef target = ((Terminated) message).getActor();
			if (jobManager.equals(target)) {
				LOG.info("Lost connection to JobManager {}. Triggering connection timeout.",
					jobManager.path());
				disconnectFromJobManager();

				if (isClientConnected()) {
					if (connectionTimeoutId == null) {
						// only register a connection timeout if we haven't done this before
						registerConnectionTimeout();
					}
				}
			} else {
				LOG.warn("Received 'Terminated' for unknown actor " + target);
			}
		}
		else if (message instanceof JobClientMessages.ConnectionTimeout) {
			JobClientMessages.ConnectionTimeout timeoutMessage = (JobClientMessages.ConnectionTimeout) message;

			if (Objects.equals(connectionTimeoutId, timeoutMessage.id())) {
				// check if we haven't found a job manager yet
				if (!isJobManagerConnected()) {
					final JobClientActorConnectionTimeoutException errorMessage =
						new JobClientActorConnectionTimeoutException("Lost connection to the JobManager.");
					final Object replyMessage = decorateMessage(new Status.Failure(errorMessage));
					if (isClientConnected()) {
						client.tell(
							replyMessage,
							getSelf());
					}
					// Connection timeout reached, let's terminate
					terminate();
				}
			} else {
				LOG.debug("Received outdated connection timeout.");
			}
		}

		// =========== Message Delegation ===============

		else if (!isJobManagerConnected() && getClientMessageClass().equals(message.getClass())) {
			LOG.info(
				"Received {} but there is no connection to a JobManager yet.",
				message);
			// We want to submit/attach to a job, but we haven't found a job manager yet.
			// Let's give him another chance to find a job manager within the given timeout.
			if (connectionTimeoutId == null) {
				// only register the connection timeout once
				registerConnectionTimeout();
			}
			handleCustomMessage(message);
		}
		else {
			if (!toBeTerminated) {
				handleCustomMessage(message);
			} else {
				// we're about to receive a PoisonPill because toBeTerminated == true
				String msg = getClass().getName() + " is about to be terminated. Therefore, the " +
					"job submission cannot be executed.";
				LOG.error(msg);
				getSender().tell(
					decorateMessage(new Status.Failure(new Exception(msg))), ActorRef.noSender());
			}
		}
	}


	@Override
	protected UUID getLeaderSessionID() {
		return leaderSessionID;
	}

	protected void logAndPrintMessage(String message) {
		LOG.info(message);
		if (sysoutUpdates) {
			System.out.println(message);
		}
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
			Throwable error = SerializedThrowable.get(message.error(), getClass().getClassLoader());
			LOG.info(message.toString(), error);
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

		leaderSessionID = null;
	}

	private void connectToJobManager(ActorRef jobManager) {
		LOG.info("Connect to JobManager {}.", jobManager);
		if (jobManager != ActorRef.noSender()) {
			getContext().unwatch(jobManager);
		}

		this.jobManager = jobManager;
		getContext().watch(jobManager);

		unregisterConnectionTimeout();
	}

	protected void terminate() {
		LOG.info("Terminate JobClientActor.");
		toBeTerminated = true;
		disconnectFromJobManager();
		getSelf().tell(decorateMessage(PoisonPill.getInstance()), ActorRef.noSender());
	}

	private boolean isJobManagerConnected() {
		return jobManager != ActorRef.noSender();
	}

	protected boolean isClientConnected() {
		return client != ActorRef.noSender();
	}

	private void registerConnectionTimeout() {
		if (connectionTimeout != null) {
			connectionTimeout.cancel();
		}

		connectionTimeoutId = UUID.randomUUID();

		connectionTimeout = getContext().system().scheduler().scheduleOnce(
			timeout,
			getSelf(),
			decorateMessage(new JobClientMessages.ConnectionTimeout(connectionTimeoutId)),
			getContext().dispatcher(),
			ActorRef.noSender()
		);
	}

	private void unregisterConnectionTimeout() {
		if (connectionTimeout != null) {
			connectionTimeout.cancel();
			connectionTimeout = null;
			connectionTimeoutId = null;
		}
	}

}
