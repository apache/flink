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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
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
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;

import akka.actor.ActorSystem;
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
