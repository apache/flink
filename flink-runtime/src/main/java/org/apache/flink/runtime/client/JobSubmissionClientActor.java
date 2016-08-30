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
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.runtime.akka.ListeningBehaviour;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.instance.AkkaActorGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.messages.JobClientMessages;
import org.apache.flink.runtime.messages.JobClientMessages.SubmitJobAndWait;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.util.SerializedThrowable;
import scala.concurrent.duration.FiniteDuration;

import java.io.IOException;
import java.util.concurrent.Callable;


/**
 * Actor which handles Job submission process and provides Job updates until completion.
 */
public class JobSubmissionClientActor extends JobClientActor {

	/** JobGraph which shall be submitted to the JobManager */
	private JobGraph jobGraph;
	/** true if a SubmitJobSuccess message has been received */
	private boolean jobSuccessfullySubmitted = false;

	public JobSubmissionClientActor(
			LeaderRetrievalService leaderRetrievalService,
			FiniteDuration timeout,
			boolean sysoutUpdates) {
		super(leaderRetrievalService, timeout, sysoutUpdates);
	}


	@Override
	public void connectedToJobManager() {
		if (jobGraph != null && !jobSuccessfullySubmitted) {
			// if we haven't yet submitted the job successfully
			tryToSubmitJob();
		}
	}

	@Override
	protected Class getClientMessageClass() {
		return SubmitJobAndWait.class;
	}

	@Override
	public void handleCustomMessage(Object message) {
		// submit a job to the JobManager
		if (message instanceof SubmitJobAndWait) {
			// sanity check that this no job was submitted through this actor before -
			// it is a one-shot actor after all
			if (this.client == null) {
				jobGraph = ((SubmitJobAndWait) message).jobGraph();
				if (jobGraph == null) {
					LOG.error("Received null JobGraph");
					sender().tell(
						decorateMessage(new Status.Failure(new Exception("JobGraph is null"))),
						getSelf());
				} else {
					LOG.info("Received job {} ({}).", jobGraph.getName(), jobGraph.getJobID());

					this.client = getSender();

					// is only successful if we already know the job manager leader
					if (jobManager != null) {
						tryToSubmitJob();
					}
				}
			} else {
				// repeated submission - tell failure to sender and kill self
				String msg = "Received repeated 'SubmitJobAndWait'";
				LOG.error(msg);
				getSender().tell(
					decorateMessage(new Status.Failure(new Exception(msg))), ActorRef.noSender());

				terminate();
			}
		} else if (message instanceof JobManagerMessages.JobSubmitSuccess) {
			// job was successfully submitted :-)
			LOG.info("Job {} was successfully submitted to the JobManager {}.",
				((JobManagerMessages.JobSubmitSuccess) message).jobId(),
				getSender().path());
			jobSuccessfullySubmitted = true;
		} else if (JobClientMessages.getSubmissionTimeout().equals(message)) {
			// check if our job submission was successful in the meantime
			if (!jobSuccessfullySubmitted) {
				if (isClientConnected()) {
					client.tell(
						decorateMessage(new Status.Failure(
							new JobClientActorSubmissionTimeoutException("Job submission to the JobManager timed out. " +
								"You may increase '" + ConfigConstants.AKKA_CLIENT_TIMEOUT + "' in case the JobManager " +
								"needs more time to configure and confirm the job submission."))),
						getSelf());
				}

				// We haven't heard back from the job manager after sending the job graph to him,
				// therefore terminate
				terminate();
			}
		} else {
			LOG.error("{} received unknown message: ", getClass());
		}
	}

	private void tryToSubmitJob() {
		LOG.info("Sending message to JobManager {} to submit job {} ({}) and wait for progress",
			jobManager.path().toString(), jobGraph.getName(), jobGraph.getJobID());

		Futures.future(new Callable<Object>() {
			@Override
			public Object call() throws Exception {
				ActorGateway jobManagerGateway = new AkkaActorGateway(jobManager, leaderSessionID);

				LOG.info("Upload jar files to job manager {}.", jobManager.path());

				try {
					jobGraph.uploadUserJars(jobManagerGateway, timeout);
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
	}


	public static Props createActorProps(
			LeaderRetrievalService leaderRetrievalService,
			FiniteDuration timeout,
			boolean sysoutUpdates) {
		return Props.create(
			JobSubmissionClientActor.class,
			leaderRetrievalService,
			timeout,
			sysoutUpdates);
	}
}
