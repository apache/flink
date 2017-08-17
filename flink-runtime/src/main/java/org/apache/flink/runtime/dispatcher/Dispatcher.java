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

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.client.JobSubmissionException;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.RunningJobsRegistry;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.OnCompletionActions;
import org.apache.flink.runtime.jobmanager.SubmittedJobGraph;
import org.apache.flink.runtime.jobmanager.SubmittedJobGraphStore;
import org.apache.flink.runtime.jobmaster.JobManagerRunner;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Base class for the Dispatcher component. The Dispatcher component is responsible
 * for receiving job submissions, persisting them, spawning JobManagers to execute
 * the jobs and to recover them in case of a master failure. Furthermore, it knows
 * about the state of the Flink session cluster.
 */
public abstract class Dispatcher extends RpcEndpoint implements DispatcherGateway {

	public static final String DISPATCHER_NAME = "dispatcher";

	private final Configuration configuration;

	private final SubmittedJobGraphStore submittedJobGraphStore;
	private final RunningJobsRegistry runningJobsRegistry;

	private final HighAvailabilityServices highAvailabilityServices;
	private final BlobServer blobServer;
	private final HeartbeatServices heartbeatServices;
	private final MetricRegistry metricRegistry;

	private final FatalErrorHandler fatalErrorHandler;

	private final Map<JobID, JobManagerRunner> jobManagerRunners;

	protected Dispatcher(
			RpcService rpcService,
			String endpointId,
			Configuration configuration,
			HighAvailabilityServices highAvailabilityServices,
			BlobServer blobServer,
			HeartbeatServices heartbeatServices,
			MetricRegistry metricRegistry,
			FatalErrorHandler fatalErrorHandler) throws Exception {
		super(rpcService, endpointId);

		this.configuration = Preconditions.checkNotNull(configuration);
		this.highAvailabilityServices = Preconditions.checkNotNull(highAvailabilityServices);
		this.blobServer = Preconditions.checkNotNull(blobServer);
		this.heartbeatServices = Preconditions.checkNotNull(heartbeatServices);
		this.metricRegistry = Preconditions.checkNotNull(metricRegistry);
		this.fatalErrorHandler = Preconditions.checkNotNull(fatalErrorHandler);

		this.submittedJobGraphStore = highAvailabilityServices.getSubmittedJobGraphStore();
		this.runningJobsRegistry = highAvailabilityServices.getRunningJobsRegistry();

		jobManagerRunners = new HashMap<>(16);
	}

	//------------------------------------------------------
	// Lifecycle methods
	//------------------------------------------------------

	@Override
	public void postStop() throws Exception {
		Exception exception = null;
		// stop all currently running JobManagerRunners
		for (JobManagerRunner jobManagerRunner : jobManagerRunners.values()) {
			jobManagerRunner.shutdown();
		}

		jobManagerRunners.clear();

		try {
			submittedJobGraphStore.stop();
		} catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(e, exception);
		}

		try {
			super.postStop();
		} catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(e, exception);
		}

		if (exception != null) {
			throw new FlinkException("Could not properly terminate the Dispatcher.", exception);
		}
	}

	//------------------------------------------------------
	// RPCs
	//------------------------------------------------------

	@Override
	public CompletableFuture<Acknowledge> submitJob(JobGraph jobGraph, Time timeout) {
		final JobID jobId = jobGraph.getJobID();

		log.info("Submitting job {} ({}).", jobGraph.getJobID(), jobGraph.getName());

		final RunningJobsRegistry.JobSchedulingStatus jobSchedulingStatus;

		try {
			jobSchedulingStatus = runningJobsRegistry.getJobSchedulingStatus(jobId);
		} catch (IOException e) {
			log.warn("Cannot retrieve job status for {}.", jobId, e);
			return FutureUtils.completedExceptionally(
				new JobSubmissionException(jobId, "Could not retrieve the job status.", e));
		}

		if (jobSchedulingStatus == RunningJobsRegistry.JobSchedulingStatus.PENDING) {
			try {
				submittedJobGraphStore.putJobGraph(new SubmittedJobGraph(jobGraph, null));
			} catch (Exception e) {
				log.warn("Cannot persist JobGraph.", e);
				return FutureUtils.completedExceptionally(
					new JobSubmissionException(jobId, "Could not persist JobGraph.", e));
			}

			final JobManagerRunner jobManagerRunner;

			try {
				jobManagerRunner = createJobManagerRunner(
					ResourceID.generate(),
					jobGraph,
					configuration,
					getRpcService(),
					highAvailabilityServices,
					blobServer,
					heartbeatServices,
					metricRegistry,
					new DispatcherOnCompleteActions(jobGraph.getJobID()),
					fatalErrorHandler);

				jobManagerRunner.start();
			} catch (Exception e) {
				try {
					// We should only remove a job from the submitted job graph store
					// if the initial submission failed. Never in case of a recovery
					submittedJobGraphStore.removeJobGraph(jobId);
				} catch (Throwable t) {
					log.warn("Cannot remove job graph from submitted job graph store.", t);
					e.addSuppressed(t);
				}

				return FutureUtils.completedExceptionally(
					new JobSubmissionException(jobId, "Could not start JobManager.", e));
			}

			jobManagerRunners.put(jobId, jobManagerRunner);

			return CompletableFuture.completedFuture(Acknowledge.get());
		} else {
			return FutureUtils.completedExceptionally(
				new JobSubmissionException(jobId, "Job has already been submitted and " +
					"is currently in state " + jobSchedulingStatus + '.'));
		}
	}

	@Override
	public CompletableFuture<Collection<JobID>> listJobs(Time timeout) {
		// TODO: return proper list of running jobs
		return CompletableFuture.completedFuture(jobManagerRunners.keySet());
	}

	/**
	 * Cleans up the job related data from the dispatcher. If cleanupHA is true, then
	 * the data will also be removed from HA.
	 *
	 * @param jobId JobID identifying the job to clean up
	 * @param cleanupHA True iff HA data shall also be cleaned up
	 */
	private void removeJob(JobID jobId, boolean cleanupHA) throws Exception {
		JobManagerRunner jobManagerRunner = jobManagerRunners.remove(jobId);

		if (jobManagerRunner != null) {
			jobManagerRunner.shutdown();
		}

		if (cleanupHA) {
			submittedJobGraphStore.removeJobGraph(jobId);
		}

		// TODO: remove job related files from blob server
	}

	protected abstract JobManagerRunner createJobManagerRunner(
		ResourceID resourceId,
		JobGraph jobGraph,
		Configuration configuration,
		RpcService rpcService,
		HighAvailabilityServices highAvailabilityServices,
		BlobServer blobServer,
		HeartbeatServices heartbeatServices,
		MetricRegistry metricRegistry,
		OnCompletionActions onCompleteActions,
		FatalErrorHandler fatalErrorHandler) throws Exception;

	//------------------------------------------------------
	// Utility classes
	//------------------------------------------------------

	private class DispatcherOnCompleteActions implements OnCompletionActions {

		private final JobID jobId;

		private DispatcherOnCompleteActions(JobID jobId) {
			this.jobId = Preconditions.checkNotNull(jobId);
		}

		@Override
		public void jobFinished(JobExecutionResult result) {
			log.info("Job {} finished.", jobId);

			runAsync(new Runnable() {
				@Override
				public void run() {
					try {
						removeJob(jobId, true);
					} catch (Exception e) {
						log.warn("Could not properly remove job {} from the dispatcher.", jobId, e);
					}
				}
			});
		}

		@Override
		public void jobFailed(Throwable cause) {
			log.info("Job {} failed.", jobId);

			runAsync(new Runnable() {
				@Override
				public void run() {
					try {
						removeJob(jobId, true);
					} catch (Exception e) {
						log.warn("Could not properly remove job {} from the dispatcher.", jobId, e);
					}
				}
			});
		}

		@Override
		public void jobFinishedByOther() {
			log.info("Job {} was finished by other JobManager.", jobId);

			runAsync(new Runnable() {
				@Override
				public void run() {
					try {
						removeJob(jobId, false);
					} catch (Exception e) {
						log.warn("Could not properly remove job {} from the dispatcher.", jobId, e);
					}
				}
			});
		}
	}
}
