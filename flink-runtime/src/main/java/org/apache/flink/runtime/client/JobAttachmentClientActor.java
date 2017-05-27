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
import akka.actor.Props;
import akka.actor.Status;
import akka.dispatch.Futures;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.runtime.akka.ListeningBehaviour;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.messages.JobClientMessages;
import org.apache.flink.runtime.messages.JobClientMessages.AttachToJobAndWait;
import org.apache.flink.runtime.messages.JobManagerMessages;
import scala.concurrent.duration.FiniteDuration;

import java.util.concurrent.Callable;


/**
 * Actor which handles Job attachment process and provides Job updates until completion.
 */
public class JobAttachmentClientActor extends JobClientActor {

	/** JobID to attach to when the JobClientActor retrieves a job */
	private JobID jobID;
	/** true if a JobRegistrationSuccess message has been received */
	private boolean successfullyRegisteredForJob = false;

	public JobAttachmentClientActor(
			LeaderRetrievalService leaderRetrievalService,
			FiniteDuration timeout,
			boolean sysoutUpdates) {
		super(leaderRetrievalService, timeout, sysoutUpdates);
	}

	@Override
	public void connectedToJobManager() {
		if (jobID != null && !successfullyRegisteredForJob) {
			tryToAttachToJob();
		}
	}

	@Override
	protected Class getClientMessageClass() {
		return AttachToJobAndWait.class;
	}

	@Override
	public void handleCustomMessage(Object message) {
		if (message instanceof AttachToJobAndWait) {
			// sanity check that this no job registration was performed through this actor before -
			// it is a one-shot actor after all
			if (this.client == null) {
				jobID = ((AttachToJobAndWait) message).jobID();
				if (jobID == null) {
					LOG.error("Received null JobID");
					sender().tell(
						decorateMessage(new Status.Failure(new Exception("JobID is null"))),
						getSelf());
				} else {
					LOG.info("Received JobID {}.", jobID);

					this.client = getSender();

					// is only successful if we already know the job manager leader
					if (jobManager != null) {
						tryToAttachToJob();
					}
				}
			} else {
				// repeated submission - tell failure to sender and kill self
				String msg = "Received repeated 'AttachToJobAndWait'";
				LOG.error(msg);
				getSender().tell(
					decorateMessage(new Status.Failure(new Exception(msg))), ActorRef.noSender());

				terminate();
			}
		}
		else if (message instanceof JobManagerMessages.RegisterJobClientSuccess) {
			// job registration was successful :o)
			JobManagerMessages.RegisterJobClientSuccess msg = ((JobManagerMessages.RegisterJobClientSuccess) message);
			logAndPrintMessage("Successfully registered at the JobManager for Job " + msg.jobId());
			successfullyRegisteredForJob = true;
		}
		else if (message instanceof JobManagerMessages.JobNotFound) {
			LOG.info("Couldn't register JobClient for JobID {}",
				((JobManagerMessages.JobNotFound) message).jobID());
			client.tell(decorateMessage(message), getSelf());
			terminate();
		}
		else if (JobClientMessages.getRegistrationTimeout().equals(message)) {
			// check if our registration for a job was successful in the meantime
			if (!successfullyRegisteredForJob) {
				if (isClientConnected()) {
					client.tell(
						decorateMessage(new Status.Failure(
							new JobClientActorRegistrationTimeoutException("Registration for Job at the JobManager " +
								"timed out. " +	"You may increase '" + AkkaOptions.CLIENT_TIMEOUT.key() +
								"' in case the JobManager needs more time to confirm the job client registration."))),
						getSelf());
				}

				// We haven't heard back from the job manager after attempting registration for a job
				// therefore terminate
				terminate();
			}
		} else {
			LOG.error("{} received unknown message: ", getClass());
		}

	}

	private void tryToAttachToJob() {
		LOG.info("Sending message to JobManager {} to attach to job {} and wait for progress",
			jobManager, jobID);

		Futures.future(new Callable<Object>() {
			@Override
			public Object call() throws Exception {
				LOG.info("Attaching to job {} at the job manager {}.", jobID, jobManager.path());

				jobManager.tell(
					decorateMessage(
						new JobManagerMessages.RegisterJobClient(
							jobID,
							ListeningBehaviour.EXECUTION_RESULT_AND_STATE_CHANGES)),
					getSelf());

				// issue a RegistrationTimeout message to check that we submit the job within
				// the given timeout
				getContext().system().scheduler().scheduleOnce(
					timeout,
					getSelf(),
					decorateMessage(JobClientMessages.getRegistrationTimeout()),
					getContext().dispatcher(),
					ActorRef.noSender());

				return null;
			}
		}, getContext().dispatcher());
	}

	public static Props createActorProps(
			LeaderRetrievalService leaderRetrievalService,
			FiniteDuration timeout,
			boolean sysoutUpdates) {
		return Props.create(
			JobAttachmentClientActor.class,
			leaderRetrievalService,
			timeout,
			sysoutUpdates);
	}
}
