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

import akka.actor.ActorSystem;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.blob.BlobCache;
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.util.LeaderRetrievalUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.net.InetSocketAddress;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The JobListeningContext holds the state necessary to monitor a running job and receive its results.
 */
public class JobListeningContext {

	private final Logger LOG = LoggerFactory.getLogger(getClass());

	/** The Job id of the Job */
	private final JobID jobID;
	/** The Future which is completed upon job completion */
	private final Future<Object> jobResultFuture;
	/** The JobClientActor which handles communication and monitoring of the job */
	private final ActorGateway jobClientActor;
	/** Timeout used Asks */
	private final FiniteDuration timeout;
	/** Finalization code to run when cleaning up */
	private Runnable finalizer;

	/** ActorSystem for leader retrieval */
	private ActorSystem actorSystem;
	/** Flink configuration for initializing the BlobService */
	private Configuration configuration;

	/** The class loader (either provided at job submission or reconstructed when it is needed */
	private ClassLoader classLoader;

	/**
	 * Constructor to use when the class loader is available.
	 */
	public JobListeningContext(
			JobID jobID,
			Configuration configuration,
			Future<Object> jobResultFuture,
			ActorGateway jobClientActor,
			FiniteDuration timeout,
			ClassLoader classLoader) {
		this(
			jobID,
			configuration,
			jobResultFuture,
			jobClientActor,
			timeout,
			null,
			classLoader);
	}

	/**
	 * Constructor to use when the class loader is available.
	 */
	public JobListeningContext(
			JobID jobID,
			Configuration configuration,
			Future<Object> jobResultFuture,
			ActorGateway jobClientActor,
			FiniteDuration timeout,
			Runnable finalizer,
			ClassLoader classLoader) {
		this.jobID = checkNotNull(jobID);
		this.jobResultFuture = checkNotNull(jobResultFuture);
		this.jobClientActor = checkNotNull(jobClientActor);
		this.timeout = checkNotNull(timeout);
		this.configuration = checkNotNull(configuration);
		this.finalizer = finalizer;
		this.classLoader = checkNotNull(classLoader);
	}

	/**
	 * Constructor to use when the class loader is not available.
	 */
	public JobListeningContext(
			JobID jobID,
			Future<Object> jobResultFuture,
			ActorGateway jobClientActor,
			FiniteDuration timeout,
			ActorSystem actorSystem,
			Configuration configuration) {
		this(
			jobID,
			jobResultFuture,
			jobClientActor,
			timeout,
			actorSystem,
			configuration,
			null);
	}

	/**
	 * Constructor to use when the class loader is not available.
	 */
	public JobListeningContext(
			JobID jobID,
			Future<Object> jobResultFuture,
			ActorGateway jobClientActor,
			FiniteDuration timeout,
			ActorSystem actorSystem,
			Configuration configuration,
			Runnable finalizer) {
		this.jobID = checkNotNull(jobID);
		this.jobResultFuture = checkNotNull(jobResultFuture);
		this.jobClientActor = checkNotNull(jobClientActor);
		this.timeout = checkNotNull(timeout);
		this.actorSystem = checkNotNull(actorSystem);
		this.configuration = checkNotNull(configuration);
		this.finalizer = finalizer;
	}

	/**
	 * @return The Job ID that this context is bound to.
	 */
	public JobID getJobID() {
		return jobID;
	}

	/**
	 * @return The Future that eventually holds the result of the execution.
	 */
	public Future<Object> getJobResultFuture() {
		return jobResultFuture;
	}

	/**
	 * @return The Job Client actor which communicats with the JobManager.
	 */
	public ActorGateway getJobClientGateway() {
		return jobClientActor;
	}

	/**
	 * @return The default timeout of Akka asks
	 */
	public FiniteDuration getTimeout() {
		return timeout;
	}

	/**
	 * Runs the finalization Runnable.
	 */
	public void runFinalizer() {
		if (finalizer != null) {
			finalizer.run();
			finalizer = null;
		}
	}

	/**
	 * The class loader necessary to deserialize the result of a job execution,
	 * i.e. JobExecutionResult or Exceptions
	 * @return The class loader for the job id
	 * @throws JobRetrievalException if anything goes wrong
	 */
	public ClassLoader getClassLoader() throws JobRetrievalException {
		if (classLoader == null) {
			// lazily initializes the class loader when it is needed
			classLoader = retrieveClassLoader(jobID, getJobManager(), configuration);
			LOG.info("Reconstructed class loader for Job {}", jobID);
		}
		return classLoader;
	}

	public ActorGateway getJobManager() throws JobRetrievalException {
		try {
			return LeaderRetrievalUtils.retrieveLeaderGateway(
				LeaderRetrievalUtils.createLeaderRetrievalService(configuration),
				actorSystem,
				AkkaUtils.getLookupTimeout(configuration));
		} catch (Exception e) {
			throw new JobRetrievalException(jobID, "Couldn't retrieve leading JobManager.", e);
		}
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
	private static ClassLoader retrieveClassLoader(
		JobID jobID,
		ActorGateway jobManager,
		Configuration config)
		throws JobRetrievalException {

		final Object jmAnswer;
		try {
			jmAnswer = Await.result(
				jobManager.ask(
					new JobManagerMessages.RequestClassloadingProps(jobID),
					AkkaUtils.getDefaultTimeoutAsFiniteDuration()),
				AkkaUtils.getDefaultTimeoutAsFiniteDuration());
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

			return new URLClassLoader(allURLs, JobClientActorUtils.class.getClassLoader());
		} else if (jmAnswer instanceof JobManagerMessages.JobNotFound) {
			throw new JobRetrievalException(jobID, "Couldn't retrieve class loader. Job " + jobID + " not running.");
		} else {
			throw new JobRetrievalException(jobID, "Unknown response from JobManager: " + jmAnswer);
		}
	}

}
