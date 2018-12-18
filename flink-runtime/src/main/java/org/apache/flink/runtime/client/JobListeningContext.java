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
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaJobManagerGateway;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.util.LeaderRetrievalUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The JobListeningContext holds the state necessary to monitor a running job and receive its results.
 */
public final class JobListeningContext {

	private final Logger LOG = LoggerFactory.getLogger(getClass());

	/** The Job id of the Job */
	private final JobID jobID;
	/** The Future which is completed upon job completion */
	private final Future<Object> jobResultFuture;
	/** The JobClientActor which handles communication and monitoring of the job */
	private final ActorRef jobClientActor;
	/** Timeout used Asks */
	private final FiniteDuration timeout;

	/** Service factory for high availability services */
	private final HighAvailabilityServices highAvailabilityServices;

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
			Future<Object> jobResultFuture,
			ActorRef jobClientActor,
			FiniteDuration timeout,
			ClassLoader classLoader,
			HighAvailabilityServices highAvailabilityServices) {
		this.jobID = checkNotNull(jobID);
		this.jobResultFuture = checkNotNull(jobResultFuture);
		this.jobClientActor = checkNotNull(jobClientActor);
		this.timeout = checkNotNull(timeout);
		this.classLoader = checkNotNull(classLoader);
		this.highAvailabilityServices = checkNotNull(highAvailabilityServices, "highAvailabilityServices");
	}

	/**
	 * Constructor to use when the class loader is not available.
	 */
	public JobListeningContext(
		JobID jobID,
		Future<Object> jobResultFuture,
		ActorRef jobClientActor,
		FiniteDuration timeout,
		ActorSystem actorSystem,
		Configuration configuration,
		HighAvailabilityServices highAvailabilityServices) {
		this.jobID = checkNotNull(jobID);
		this.jobResultFuture = checkNotNull(jobResultFuture);
		this.jobClientActor = checkNotNull(jobClientActor);
		this.timeout = checkNotNull(timeout);
		this.actorSystem = checkNotNull(actorSystem);
		this.configuration = checkNotNull(configuration);
		this.highAvailabilityServices = checkNotNull(highAvailabilityServices, "highAvailabilityServices");
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
	 * @return The Job Client actor which communicates with the JobManager.
	 */
	public ActorRef getJobClientActor() {
		return jobClientActor;
	}

	/**
	 * @return The default timeout of Akka asks
	 */
	public FiniteDuration getTimeout() {
		return timeout;
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
			classLoader = JobClient.retrieveClassLoader(
				jobID,
				new AkkaJobManagerGateway(getJobManager()),
				configuration,
				highAvailabilityServices,
				Time.milliseconds(timeout.toMillis()));
			LOG.info("Reconstructed class loader for Job {}", jobID);
		}
		return classLoader;
	}

	private ActorGateway getJobManager() throws JobRetrievalException {
		try {
			return LeaderRetrievalUtils.retrieveLeaderGateway(
				highAvailabilityServices.getJobManagerLeaderRetriever(HighAvailabilityServices.DEFAULT_JOB_ID),
				actorSystem,
				AkkaUtils.getLookupTimeout(configuration));
		} catch (Exception e) {
			throw new JobRetrievalException(jobID, "Couldn't retrieve leading JobManager.", e);
		}
	}
}
