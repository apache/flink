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

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.akka.ListeningBehaviour;
import org.apache.flink.runtime.blob.BlobClient;
import org.apache.flink.runtime.blob.PermanentBlobCache;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.runtime.execution.librarycache.FlinkUserCodeClassLoaders;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.JobManagerGateway;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.JobClientMessages;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.SerializedThrowable;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Identify;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.pattern.Patterns;
import akka.util.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The JobClient bridges between the JobManager's asynchronous actor messages and
 * the synchronous method calls to trigger.
 */
public class JobClient {

	private static final Logger LOG = LoggerFactory.getLogger(JobClient.class);

	public static ActorSystem startJobClientActorSystem(Configuration config, String hostname)
			throws Exception {
		LOG.info("Starting JobClient actor system");

		// start a remote actor system to listen on an arbitrary port
		ActorSystem system = BootstrapTools.startActorSystem(config, hostname, 0, LOG);

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
			Configuration config,
			HighAvailabilityServices highAvailabilityServices,
			JobGraph jobGraph,
			FiniteDuration timeout,
			boolean sysoutLogUpdates,
			ClassLoader classLoader) {

		checkNotNull(actorSystem, "The actorSystem must not be null.");
		checkNotNull(highAvailabilityServices, "The high availability services must not be null.");
		checkNotNull(jobGraph, "The jobGraph must not be null.");
		checkNotNull(timeout, "The timeout must not be null.");

		// for this job, we create a proxy JobClientActor that deals with all communication with
		// the JobManager. It forwards the job submission, checks the success/failure responses, logs
		// update messages, watches for disconnect between client and JobManager, ...

		Props jobClientActorProps = JobSubmissionClientActor.createActorProps(
			highAvailabilityServices.getJobManagerLeaderRetriever(HighAvailabilityServices.DEFAULT_JOB_ID),
			timeout,
			sysoutLogUpdates,
			config);

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
			classLoader,
			highAvailabilityServices);
	}


	/**
	 * Attaches to a running Job using the JobID.
	 * Reconstructs the user class loader by downloading the jars from the JobManager.
	 */
	public static JobListeningContext attachToRunningJob(
			JobID jobID,
			Configuration configuration,
			ActorSystem actorSystem,
			HighAvailabilityServices highAvailabilityServices,
			FiniteDuration timeout,
			boolean sysoutLogUpdates) {

		checkNotNull(jobID, "The jobID must not be null.");
		checkNotNull(configuration, "The configuration must not be null.");
		checkNotNull(actorSystem, "The actorSystem must not be null.");
		checkNotNull(highAvailabilityServices, "The high availability services must not be null.");
		checkNotNull(timeout, "The timeout must not be null.");

		// we create a proxy JobClientActor that deals with all communication with
		// the JobManager. It forwards the job attachments, checks the success/failure responses, logs
		// update messages, watches for disconnect between client and JobManager, ...
		Props jobClientActorProps = JobAttachmentClientActor.createActorProps(
			highAvailabilityServices.getJobManagerLeaderRetriever(HighAvailabilityServices.DEFAULT_JOB_ID),
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
			configuration,
			highAvailabilityServices);
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
			JobManagerGateway jobManager,
			Configuration config,
			HighAvailabilityServices highAvailabilityServices,
			Time timeout)
		throws JobRetrievalException {

		final CompletableFuture<Optional<JobManagerMessages.ClassloadingProps>> clPropsFuture = jobManager
			.requestClassloadingProps(jobID, timeout);

		final Optional<JobManagerMessages.ClassloadingProps> optProps;

		try {
			optProps = clPropsFuture.get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);
		} catch (Exception e) {
			throw new JobRetrievalException(jobID, "Could not retrieve the class loading properties from JobManager.", e);
		}

		if (optProps.isPresent()) {
			JobManagerMessages.ClassloadingProps props = optProps.get();

			InetSocketAddress serverAddress = new InetSocketAddress(jobManager.getHostname(), props.blobManagerPort());
			final PermanentBlobCache permanentBlobCache;
			try {
				// TODO: Fix lifecycle of PermanentBlobCache to properly close it upon usage
				permanentBlobCache = new PermanentBlobCache(config, highAvailabilityServices.createBlobStore(), serverAddress);
			} catch (IOException e) {
				throw new JobRetrievalException(
					jobID,
					"Failed to setup BlobCache.",
					e);
			}

			final Collection<PermanentBlobKey> requiredJarFiles = props.requiredJarFiles();
			final Collection<URL> requiredClasspaths = props.requiredClasspaths();

			final URL[] allURLs = new URL[requiredJarFiles.size() + requiredClasspaths.size()];

			int pos = 0;
			for (PermanentBlobKey blobKey : props.requiredJarFiles()) {
				try {
					allURLs[pos++] = permanentBlobCache.getFile(jobID, blobKey).toURI().toURL();
				} catch (Exception e) {
					try {
						permanentBlobCache.close();
					} catch (IOException ioe) {
						LOG.warn("Could not properly close the BlobClient.", ioe);
					}

					throw new JobRetrievalException(jobID, "Failed to download BlobKey " + blobKey, e);
				}
			}

			for (URL url : requiredClasspaths) {
				allURLs[pos++] = url;
			}

			return FlinkUserCodeClassLoaders.parentFirst(allURLs, JobClient.class.getClassLoader());
		} else {
			throw new JobRetrievalException(jobID, "Couldn't retrieve class loader. Job " + jobID + " not found");
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
	 * @param config The cluster wide configuration.
	 * @param highAvailabilityServices Service factory for high availability services
	 * @param jobGraph    JobGraph describing the Flink job
	 * @param timeout     Timeout for futures
	 * @param sysoutLogUpdates prints log updates to system out if true
	 * @param classLoader The class loader for deserializing the results
	 * @return The job execution result
	 * @throws JobExecutionException Thrown if the job
	 *                                                               execution fails.
	 */
	public static JobExecutionResult submitJobAndWait(
			ActorSystem actorSystem,
			Configuration config,
			HighAvailabilityServices highAvailabilityServices,
			JobGraph jobGraph,
			FiniteDuration timeout,
			boolean sysoutLogUpdates,
			ClassLoader classLoader) throws JobExecutionException {

		JobListeningContext jobListeningContext = submitJob(
				actorSystem,
				config,
				highAvailabilityServices,
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
	 * @param config The cluster wide configuration.
	 * @param jobGraph The job
	 * @param timeout Timeout in which the JobManager must have responded.
	 */
	public static void submitJobDetached(
			JobManagerGateway jobManagerGateway,
			Configuration config,
			JobGraph jobGraph,
			Time timeout,
			ClassLoader classLoader) throws JobExecutionException {

		checkNotNull(jobManagerGateway, "The jobManagerGateway must not be null.");
		checkNotNull(jobGraph, "The jobGraph must not be null.");
		checkNotNull(timeout, "The timeout must not be null.");

		LOG.info("Checking and uploading JAR files");

		final CompletableFuture<InetSocketAddress> blobServerAddressFuture = retrieveBlobServerAddress(
			jobManagerGateway,
			timeout);

		final InetSocketAddress blobServerAddress;

		try {
			blobServerAddress = blobServerAddressFuture.get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);
		} catch (Exception e) {
			throw new JobSubmissionException(jobGraph.getJobID(), "Could not retrieve BlobServer address.", e);
		}

		try {
			ClientUtils.extractAndUploadJobGraphFiles(jobGraph, () -> new BlobClient(blobServerAddress, config));
		} catch (FlinkException e) {
			throw new JobSubmissionException(jobGraph.getJobID(),
					"Could not upload job files.", e);
		}

		CompletableFuture<Acknowledge> submissionFuture = jobManagerGateway.submitJob(jobGraph, ListeningBehaviour.DETACHED, timeout);

		try {
			submissionFuture.get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);
		} catch (TimeoutException e) {
			throw new JobTimeoutException(jobGraph.getJobID(),
				"JobManager did not respond within " + timeout, e);
		} catch (Throwable throwable) {
			Throwable stripped = ExceptionUtils.stripExecutionException(throwable);

			try {
				ExceptionUtils.tryDeserializeAndThrow(stripped, classLoader);
			} catch (JobExecutionException jee) {
				throw jee;
			} catch (Throwable t) {
				throw new JobExecutionException(
					jobGraph.getJobID(),
					"JobSubmission failed.",
					t);
			}
		}
	}

	/**
	 * Utility method to retrieve the BlobServer address from the given JobManager gateway.
	 *
	 * @param jobManagerGateway to obtain the BlobServer address from
	 * @param timeout for this operation
	 * @return CompletableFuture containing the BlobServer address
	 */
	public static CompletableFuture<InetSocketAddress> retrieveBlobServerAddress(
			JobManagerGateway jobManagerGateway,
			Time timeout) {

		CompletableFuture<Integer> futureBlobPort = jobManagerGateway.requestBlobServerPort(timeout);

		final String jmHostname = jobManagerGateway.getHostname();

		return futureBlobPort.thenApply(
			(Integer blobPort) -> new InetSocketAddress(jmHostname, blobPort));
	}
}
