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
import akka.actor.ActorSystem;
import akka.actor.Address;
import akka.actor.Identify;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.pattern.Patterns;
import akka.util.Timeout;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.akka.ListeningBehaviour;
import org.apache.flink.runtime.blob.BlobCache;
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.execution.librarycache.FlinkUserCodeClassLoader;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.messages.JobClientMessages;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.util.SerializedThrowable;
import org.apache.flink.util.NetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.Some;
import scala.Tuple2;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The JobClient bridges between the JobManager's asynchronous actor messages and
 * the synchronous method calls to trigger.
 */
public class JobClient {

	private static final Logger LOG = LoggerFactory.getLogger(JobClient.class);


	public static ActorSystem startJobClientActorSystem(Configuration config)
			throws IOException {
		LOG.info("Starting JobClient actor system");
		Option<Tuple2<String, Object>> remoting = new Some<>(new Tuple2<String, Object>("", 0));

		// start a remote actor system to listen on an arbitrary port
		ActorSystem system = AkkaUtils.createActorSystem(config, remoting);
		Address address = system.provider().getDefaultAddress();

		String hostAddress = address.host().isDefined() ?
				NetUtils.ipAddressToUrlString(InetAddress.getByName(address.host().get())) :
				"(unknown)";
		int port = address.port().isDefined() ? ((Integer) address.port().get()) : -1;
		LOG.info("Started JobClient actor system at " + hostAddress + ':' + port);

		return system;
	}

	/**
	 * Submits a job to a Flink cluster (non-blocking) and returns a JobListeningContext which can be
	 * passed to {@code awaitJobResult} to get the result of the submission.
	 * @return JobListeningContext which may be used to retrieve the JobExecutionResult via
	 * 			{@code awaitJobResult(JobListeningContext context)}.
	 */
	public static JobListeningContext submitJob(
			ActorSystem actorSystem,
			LeaderRetrievalService leaderRetrievalService,
			JobGraph jobGraph,
			FiniteDuration timeout,
			boolean sysoutLogUpdates,
			ClassLoader classLoader) {

		checkNotNull(actorSystem, "The actorSystem must not be null.");
		checkNotNull(leaderRetrievalService, "The jobManagerGateway must not be null.");
		checkNotNull(jobGraph, "The jobGraph must not be null.");
		checkNotNull(timeout, "The timeout must not be null.");

		// for this job, we create a proxy JobClientActor that deals with all communication with
		// the JobManager. It forwards the job submission, checks the success/failure responses, logs
		// update messages, watches for disconnect between client and JobManager, ...

		Props jobClientActorProps = JobSubmissionClientActor.createActorProps(
			leaderRetrievalService,
			timeout,
			sysoutLogUpdates);

		ActorRef jobClientActor = actorSystem.actorOf(jobClientActorProps);

		Future<Object> submissionFuture = Patterns.ask(
				jobClientActor,
				new JobClientMessages.SubmitJobAndWait(jobGraph),
				new Timeout(AkkaUtils.INF_TIMEOUT()));

		return new JobListeningContext(
				jobGraph.getJobID(),
				submissionFuture,
				jobClientActor,
				timeout,
				classLoader);
	}


	/**
	 * Attaches to a running Job using the JobID.
	 * Reconstructs the user class loader by downloading the jars from the JobManager.
	 */
	public static JobListeningContext attachToRunningJob(
			JobID jobID,
			ActorGateway jobManagerGateWay,
			Configuration configuration,
			ActorSystem actorSystem,
			LeaderRetrievalService leaderRetrievalService,
			FiniteDuration timeout,
			boolean sysoutLogUpdates) {

		checkNotNull(jobID, "The jobID must not be null.");
		checkNotNull(jobManagerGateWay, "The jobManagerGateWay must not be null.");
		checkNotNull(configuration, "The configuration must not be null.");
		checkNotNull(actorSystem, "The actorSystem must not be null.");
		checkNotNull(leaderRetrievalService, "The jobManagerGateway must not be null.");
		checkNotNull(timeout, "The timeout must not be null.");

		// we create a proxy JobClientActor that deals with all communication with
		// the JobManager. It forwards the job attachments, checks the success/failure responses, logs
		// update messages, watches for disconnect between client and JobManager, ...
		Props jobClientActorProps = JobAttachmentClientActor.createActorProps(
			leaderRetrievalService,
			timeout,
			sysoutLogUpdates);

		ActorRef jobClientActor = actorSystem.actorOf(jobClientActorProps);

		Future<Object> attachmentFuture = Patterns.ask(
				jobClientActor,
				new JobClientMessages.AttachToJobAndWait(jobID),
				new Timeout(AkkaUtils.INF_TIMEOUT()));

		return new JobListeningContext(
				jobID,
				attachmentFuture,
				jobClientActor,
				timeout,
				actorSystem,
				configuration);
	}

	/**
	 * Reconstructs the class loader by first requesting information about it at the JobManager
	 * and then downloading missing jar files.
	 * @param jobID id of job
	 * @param jobManager gateway to the JobManager
	 * @param config the flink configuration
	 * @return A classloader that should behave like the original classloader
	 * @throws JobRetrievalException if anything goes wrong
	 */
	public static ClassLoader retrieveClassLoader(
		JobID jobID,
		ActorGateway jobManager,
		Configuration config)
		throws JobRetrievalException {

		final Object jmAnswer;
		try {
			jmAnswer = Await.result(
				jobManager.ask(
					new JobManagerMessages.RequestClassloadingProps(jobID),
					AkkaUtils.getDefaultTimeout()),
				AkkaUtils.getDefaultTimeout());
		} catch (Exception e) {
			throw new JobRetrievalException(jobID, "Couldn't retrieve class loading properties from JobManager.", e);
		}

		if (jmAnswer instanceof JobManagerMessages.ClassloadingProps) {
			JobManagerMessages.ClassloadingProps props = ((JobManagerMessages.ClassloadingProps) jmAnswer);

			Option<String> jmHost = jobManager.actor().path().address().host();
			String jmHostname = jmHost.isDefined() ? jmHost.get() : "localhost";
			InetSocketAddress serverAddress = new InetSocketAddress(jmHostname, props.blobManagerPort());
			final BlobCache blobClient = new BlobCache(serverAddress, config);

			final List<BlobKey> requiredJarFiles = props.requiredJarFiles();
			final List<URL> requiredClasspaths = props.requiredClasspaths();

			final URL[] allURLs = new URL[requiredJarFiles.size() + requiredClasspaths.size()];

			int pos = 0;
			for (BlobKey blobKey : props.requiredJarFiles()) {
				try {
					allURLs[pos++] = blobClient.getURL(blobKey);
				} catch (Exception e) {
					blobClient.shutdown();
					throw new JobRetrievalException(jobID, "Failed to download BlobKey " + blobKey);
				}
			}

			for (URL url : requiredClasspaths) {
				allURLs[pos++] = url;
			}

			return new FlinkUserCodeClassLoader(allURLs, JobClient.class.getClassLoader());
		} else if (jmAnswer instanceof JobManagerMessages.JobNotFound) {
			throw new JobRetrievalException(jobID, "Couldn't retrieve class loader. Job " + jobID + " not found");
		} else {
			throw new JobRetrievalException(jobID, "Unknown response from JobManager: " + jmAnswer);
		}
	}

	/**
	 * Given a JobListeningContext, awaits the result of the job execution that this context is bound to
	 * @param listeningContext The listening context of the job execution
	 * @return The result of the execution
	 * @throws JobExecutionException if anything goes wrong while monitoring the job
	 */
	public static JobExecutionResult awaitJobResult(JobListeningContext listeningContext) throws JobExecutionException {

		final JobID jobID = listeningContext.getJobID();
		final ActorRef jobClientActor = listeningContext.getJobClientActor();
		final Future<Object> jobSubmissionFuture = listeningContext.getJobResultFuture();
		final FiniteDuration askTimeout = listeningContext.getTimeout();
		// retrieves class loader if necessary
		final ClassLoader classLoader = listeningContext.getClassLoader();

		// wait for the future which holds the result to be ready
		// ping the JobClientActor from time to time to check if it is still running
		while (!jobSubmissionFuture.isCompleted()) {
			try {
				Await.ready(jobSubmissionFuture, askTimeout);
			} catch (InterruptedException e) {
				throw new JobExecutionException(
					jobID,
					"Interrupted while waiting for job completion.");
			} catch (TimeoutException e) {
				try {
					Await.result(
						Patterns.ask(
							jobClientActor,
							// Ping the Actor to see if it is alive
							new Identify(true),
							Timeout.durationToTimeout(askTimeout)),
						askTimeout);
					// we got a reply, continue waiting for the job result
				} catch (Exception eInner) {
					// we could have a result but the JobClientActor might have been killed and
					// thus the health check failed
					if (!jobSubmissionFuture.isCompleted()) {
						throw new JobExecutionException(
							jobID,
							"JobClientActor seems to have died before the JobExecutionResult could be retrieved.",
							eInner);
					}
				}
			}
		}

		final Object answer;
		try {
			// we have already awaited the result, zero time to wait here
			answer = Await.result(jobSubmissionFuture, Duration.Zero());
		}
		catch (Throwable throwable) {
			throw new JobExecutionException(jobID,
				"Couldn't retrieve the JobExecutionResult from the JobManager.", throwable);
		}
		finally {
			// failsafe shutdown of the client actor
			jobClientActor.tell(PoisonPill.getInstance(), ActorRef.noSender());
		}

		// second block handles the actual response
		if (answer instanceof JobManagerMessages.JobResultSuccess) {
			LOG.info("Job execution complete");

			SerializedJobExecutionResult result = ((JobManagerMessages.JobResultSuccess) answer).result();
			if (result != null) {
				try {
					return result.toJobExecutionResult(classLoader);
				} catch (Throwable t) {
					throw new JobExecutionException(jobID,
						"Job was successfully executed but JobExecutionResult could not be deserialized.");
				}
			} else {
				throw new JobExecutionException(jobID,
					"Job was successfully executed but result contained a null JobExecutionResult.");
			}
		}
		else if (answer instanceof JobManagerMessages.JobResultFailure) {
			LOG.info("Job execution failed");

			SerializedThrowable serThrowable = ((JobManagerMessages.JobResultFailure) answer).cause();
			if (serThrowable != null) {
				Throwable cause = serThrowable.deserializeError(classLoader);
				if (cause instanceof JobExecutionException) {
					throw (JobExecutionException) cause;
				} else {
					throw new JobExecutionException(jobID, "Job execution failed", cause);
				}
			} else {
				throw new JobExecutionException(jobID,
					"Job execution failed with null as failure cause.");
			}
		}
		else if (answer instanceof JobManagerMessages.JobNotFound) {
			throw new JobRetrievalException(
				((JobManagerMessages.JobNotFound) answer).jobID(),
				"Couldn't retrieve Job " + jobID + " because it was not running.");
		}
		else {
			throw new JobExecutionException(jobID,
				"Unknown answer from JobManager after submitting the job: " + answer);
		}
	}

	/**
	 * Sends a [[JobGraph]] to the JobClient actor specified by jobClient which submits it then to
	 * the JobManager. The method blocks until the job has finished or the JobManager is no longer
	 * alive. In the former case, the [[SerializedJobExecutionResult]] is returned and in the latter
	 * case a [[JobExecutionException]] is thrown.
	 *
	 * @param actorSystem The actor system that performs the communication.
	 * @param leaderRetrievalService Leader retrieval service which used to find the current leading
	 *                               JobManager
	 * @param jobGraph    JobGraph describing the Flink job
	 * @param timeout     Timeout for futures
	 * @param sysoutLogUpdates prints log updates to system out if true
	 * @param classLoader The class loader for deserializing the results
	 * @return The job execution result
	 * @throws org.apache.flink.runtime.client.JobExecutionException Thrown if the job
	 *                                                               execution fails.
	 */
	public static JobExecutionResult submitJobAndWait(
			ActorSystem actorSystem,
			LeaderRetrievalService leaderRetrievalService,
			JobGraph jobGraph,
			FiniteDuration timeout,
			boolean sysoutLogUpdates,
			ClassLoader classLoader) throws JobExecutionException {

		JobListeningContext jobListeningContext = submitJob(
				actorSystem,
				leaderRetrievalService,
				jobGraph,
				timeout,
				sysoutLogUpdates,
				classLoader);

		return awaitJobResult(jobListeningContext);
	}

	/**
	 * Submits a job in detached mode. The method sends the JobGraph to the
	 * JobManager and waits for the answer whether the job could be started or not.
	 *
	 * @param jobManagerGateway Gateway to the JobManager which will execute the jobs
	 * @param jobGraph The job
	 * @param timeout  Timeout in which the JobManager must have responded.
	 */
	public static void submitJobDetached(
			ActorGateway jobManagerGateway,
			JobGraph jobGraph,
			FiniteDuration timeout,
			ClassLoader classLoader) throws JobExecutionException {

		checkNotNull(jobManagerGateway, "The jobManagerGateway must not be null.");
		checkNotNull(jobGraph, "The jobGraph must not be null.");
		checkNotNull(timeout, "The timeout must not be null.");

		LOG.info("Checking and uploading JAR files");
		try {
			jobGraph.uploadUserJars(jobManagerGateway, timeout);
		}
		catch (IOException e) {
			throw new JobSubmissionException(jobGraph.getJobID(),
				"Could not upload the program's JAR files to the JobManager.", e);
		}

		Object result;
		try {
			Future<Object> future = jobManagerGateway.ask(
				new JobManagerMessages.SubmitJob(
					jobGraph,
					ListeningBehaviour.DETACHED // only receive the Acknowledge for the job submission message
				),
				timeout);

			result = Await.result(future, timeout);
		}
		catch (TimeoutException e) {
			throw new JobTimeoutException(jobGraph.getJobID(),
					"JobManager did not respond within " + timeout.toString(), e);
		}
		catch (Throwable t) {
			throw new JobSubmissionException(jobGraph.getJobID(),
					"Failed to send job to JobManager: " + t.getMessage(), t.getCause());
		}

		if (result instanceof JobManagerMessages.JobSubmitSuccess) {
			JobID respondedID = ((JobManagerMessages.JobSubmitSuccess) result).jobId();

			// validate response
			if (!respondedID.equals(jobGraph.getJobID())) {
				throw new JobExecutionException(jobGraph.getJobID(),
						"JobManager responded for wrong Job. This Job: " +
						jobGraph.getJobID() + ", response: " + respondedID);
			}
		}
		else if (result instanceof JobManagerMessages.JobResultFailure) {
			try {
				SerializedThrowable t = ((JobManagerMessages.JobResultFailure) result).cause();
				throw t.deserializeError(classLoader);
			}
			catch (JobExecutionException e) {
				throw e;
			}
			catch (Throwable t) {
				throw new JobExecutionException(jobGraph.getJobID(),
						"JobSubmission failed: " + t.getMessage(), t);
			}
		}
		else {
			throw new JobExecutionException(jobGraph.getJobID(), "Unexpected response from JobManager: " + result);
		}
	}

}
