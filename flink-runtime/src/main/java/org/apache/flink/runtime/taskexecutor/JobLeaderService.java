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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.jobmaster.JMTMRegistrationSuccess;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.registration.RegisteredRpcConnection;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.registration.RetryingRegistration;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * This service has the responsibility to monitor the job leaders (the job manager which is leader
 * for a given job) for all registered jobs. Upon gaining leadership for a job and detection by the
 * job leader service, the service tries to establish a connection to the job leader. After
 * successfully establishing a connection, the job leader listener is notified about the new job
 * leader and its connection. In case that a job leader loses leadership, the job leader listener
 * is notified as well.
 */
public class JobLeaderService {

	private static final Logger LOG = LoggerFactory.getLogger(JobLeaderService.class);

	/** Self's location, used for the job manager connection */
	private final TaskManagerLocation ownLocation;

	/** The leader retrieval service and listener for each registered job */
	private final Map<JobID, Tuple2<LeaderRetrievalService, JobLeaderService.JobManagerLeaderListener>> jobLeaderServices;

	/** Internal state of the service */
	private volatile JobLeaderService.State state;

	/** Address of the owner of this service. This address is used for the job manager connection */
	private String ownerAddress;

	/** Rpc service to use for establishing connections */
	private RpcService rpcService;

	/** High availability services to create the leader retrieval services from */
	private HighAvailabilityServices highAvailabilityServices;

	/** Job leader listener listening for job leader changes */
	private JobLeaderListener jobLeaderListener;

	public JobLeaderService(TaskManagerLocation location) {
		this.ownLocation = Preconditions.checkNotNull(location);

		jobLeaderServices = new HashMap<>(4);

		state = JobLeaderService.State.CREATED;

		ownerAddress = null;
		rpcService = null;
		highAvailabilityServices = null;
		jobLeaderListener = null;
	}

	// -------------------------------------------------------------------------------
	// Methods
	// -------------------------------------------------------------------------------

	/**
	 * Start the job leader service with the given services.
	 *
 	 * @param initialOwnerAddress to be used for establishing connections (source address)
	 * @param initialRpcService to be used to create rpc connections
	 * @param initialHighAvailabilityServices to create leader retrieval services for the different jobs
	 * @param initialJobLeaderListener listening for job leader changes
	 */
	public void start(
			final String initialOwnerAddress,
			final RpcService initialRpcService,
			final HighAvailabilityServices initialHighAvailabilityServices,
			final JobLeaderListener initialJobLeaderListener) {

		if (JobLeaderService.State.CREATED != state) {
			throw new IllegalStateException("The service has already been started.");
		} else {
			LOG.info("Start job leader service.");

			this.ownerAddress = Preconditions.checkNotNull(initialOwnerAddress);
			this.rpcService = Preconditions.checkNotNull(initialRpcService);
			this.highAvailabilityServices = Preconditions.checkNotNull(initialHighAvailabilityServices);
			this.jobLeaderListener = Preconditions.checkNotNull(initialJobLeaderListener);
			state = JobLeaderService.State.STARTED;
		}
	}

	/**
	 * Stop the job leader services. This implies stopping all leader retrieval services for the
	 * different jobs and their leader retrieval listeners.
	 *
	 * @throws Exception if an error occurs while stopping the service
	 */
	public void stop() throws Exception {
		LOG.info("Stop job leader service.");

		if (JobLeaderService.State.STARTED == state) {

			for (Tuple2<LeaderRetrievalService, JobLeaderService.JobManagerLeaderListener> leaderRetrievalServiceEntry: jobLeaderServices.values()) {
				LeaderRetrievalService leaderRetrievalService = leaderRetrievalServiceEntry.f0;
				JobLeaderService.JobManagerLeaderListener jobManagerLeaderListener = leaderRetrievalServiceEntry.f1;

				jobManagerLeaderListener.stop();
				leaderRetrievalService.stop();
			}

			jobLeaderServices.clear();
		}

		state = JobLeaderService.State.STOPPED;
	}

	/**
	 * Check whether the service monitors the given job.
	 *
	 * @param jobId identifying the job
	 * @return True if the given job is monitored; otherwise false
	 */
	public boolean containsJob(JobID jobId) {
		Preconditions.checkState(JobLeaderService.State.STARTED == state, "The service is currently not running.");

		return jobLeaderServices.containsKey(jobId);
	}

	/**
	 * Remove the given job from being monitored by the job leader service.
	 *
	 * @param jobId identifying the job to remove from monitoring
	 * @throws Exception if an error occurred while stopping the leader retrieval service and listener
	 */
	public void removeJob(JobID jobId) throws Exception {
		Preconditions.checkState(JobLeaderService.State.STARTED == state, "The service is currently not running.");

		Tuple2<LeaderRetrievalService, JobLeaderService.JobManagerLeaderListener> entry = jobLeaderServices.remove(jobId);

		if (entry != null) {
			LOG.info("Remove job {} from job leader monitoring.", jobId);

			LeaderRetrievalService leaderRetrievalService = entry.f0;
			JobLeaderService.JobManagerLeaderListener jobManagerLeaderListener = entry.f1;

			leaderRetrievalService.stop();
			jobManagerLeaderListener.stop();
		}
	}

	/**
	 * Add the given job to be monitored. This means that the service tries to detect leaders for
	 * this job and then tries to establish a connection to it.
	 *
	 * @param jobId identifying the job to monitor
	 * @param defaultTargetAddress of the job leader
	 * @throws Exception if an error occurs while starting the leader retrieval service
	 */
	public void addJob(final JobID jobId, final String defaultTargetAddress) throws Exception {
		Preconditions.checkState(JobLeaderService.State.STARTED == state, "The service is currently not running.");

		LOG.info("Add job {} for job leader monitoring.", jobId);

		final LeaderRetrievalService leaderRetrievalService = highAvailabilityServices.getJobManagerLeaderRetriever(
			jobId,
			defaultTargetAddress);

		JobLeaderService.JobManagerLeaderListener jobManagerLeaderListener = new JobManagerLeaderListener(jobId);

		leaderRetrievalService.start(jobManagerLeaderListener);

		jobLeaderServices.put(jobId, Tuple2.of(leaderRetrievalService, jobManagerLeaderListener));
	}

	/**
	 * Leader listener which tries to establish a connection to a newly detected job leader.
	 */
	private final class JobManagerLeaderListener implements LeaderRetrievalListener {

		/** Job id identifying the job to look for a leader */
		private final JobID jobId;

		/** Rpc connection to the job leader */
		private RegisteredRpcConnection<JobMasterGateway, JMTMRegistrationSuccess> rpcConnection;

		/** State of the listener */
		private volatile boolean stopped;

		/** Leader id of the current job leader */
		private volatile UUID currentLeaderId;

		private JobManagerLeaderListener(JobID jobId) {
			this.jobId = Preconditions.checkNotNull(jobId);

			stopped = false;
			rpcConnection = null;
			currentLeaderId = null;
		}

		public void stop() {
			stopped = true;

			if (rpcConnection != null) {
				rpcConnection.close();
			}
		}

		@Override
		public void notifyLeaderAddress(final String leaderAddress, final UUID leaderId) {
			if (stopped) {
				LOG.debug("{}'s leader retrieval listener reported a new leader for job {}. " +
					"However, the service is no longer running.", JobLeaderService.class.getSimpleName(), jobId);
			} else {
				LOG.debug("New leader information for job {}. Address: {}, leader id: {}.",
					jobId, leaderAddress, leaderId);

				if (leaderAddress == null || leaderAddress.isEmpty()) {
					// the leader lost leadership but there is no other leader yet.
					if (rpcConnection != null) {
						rpcConnection.close();
					}

					jobLeaderListener.jobManagerLostLeadership(jobId, currentLeaderId);

					currentLeaderId = leaderId;
				} else {
					currentLeaderId = leaderId;

					if (rpcConnection != null) {
						// check if we are already trying to connect to this leader
						if (!leaderId.equals(rpcConnection.getTargetLeaderId())) {
							rpcConnection.close();

							rpcConnection = new JobManagerRegisteredRpcConnection(
								LOG,
								leaderAddress,
								leaderId,
								rpcService.getExecutor());
						}
					} else {
						rpcConnection = new JobManagerRegisteredRpcConnection(
							LOG,
							leaderAddress,
							leaderId,
							rpcService.getExecutor());
					}

					// double check for a concurrent stop operation
					if (stopped) {
						rpcConnection.close();
					} else {
						LOG.info("Try to register at job manager {} with leader id {}.", leaderAddress, leaderId);
						rpcConnection.start();
					}
				}
			}
		}

		@Override
		public void handleError(Exception exception) {
			if (stopped) {
				LOG.debug("{}'s leader retrieval listener reported an exception for job {}. " +
						"However, the service is no longer running.", JobLeaderService.class.getSimpleName(),
					jobId, exception);
			} else {
				jobLeaderListener.handleError(exception);
			}
		}

		/**
		 * Rpc connection for the job manager <--> task manager connection.
		 */
		private final class JobManagerRegisteredRpcConnection extends RegisteredRpcConnection<JobMasterGateway, JMTMRegistrationSuccess> {

			JobManagerRegisteredRpcConnection(
				Logger log,
				String targetAddress,
				UUID targetLeaderId,
				Executor executor) {
				super(log, targetAddress, targetLeaderId, executor);
			}

			@Override
			protected RetryingRegistration<JobMasterGateway, JMTMRegistrationSuccess> generateRegistration() {
				return new JobLeaderService.JobManagerRetryingRegistration(
						LOG,
						rpcService,
						"JobManager",
						JobMasterGateway.class,
						getTargetAddress(),
						getTargetLeaderId(),
						ownerAddress,
						ownLocation);
			}

			@Override
			protected void onRegistrationSuccess(JMTMRegistrationSuccess success) {
				// filter out old registration attempts
				if (getTargetLeaderId().equals(currentLeaderId)) {
					log.info("Successful registration at job manager {} for job {}.", getTargetAddress(), jobId);

					jobLeaderListener.jobManagerGainedLeadership(jobId, getTargetGateway(), getTargetLeaderId(), success);
				} else {
					log.debug("Encountered obsolete JobManager registration success from {} with leader session ID {}.", getTargetAddress(), getTargetLeaderId());
				}
			}

			@Override
			protected void onRegistrationFailure(Throwable failure) {
				// filter out old registration attempts
				if (getTargetLeaderId().equals(currentLeaderId)) {
					log.info("Failed to register at job manager {} for job {}.", getTargetAddress(), jobId);
					jobLeaderListener.handleError(failure);
				} else {
					log.debug("Obsolete JobManager registration failure from {} with leader session ID {}.", getTargetAddress(), getTargetLeaderId(), failure);
				}
			}
		}
	}

	/**
	 * Retrying registration for the job manager <--> task manager connection.
	 */
	private static final class JobManagerRetryingRegistration
			extends RetryingRegistration<JobMasterGateway, JMTMRegistrationSuccess>
	{

		private final String taskManagerRpcAddress;

		private final TaskManagerLocation taskManagerLocation;

		JobManagerRetryingRegistration(
				Logger log,
				RpcService rpcService,
				String targetName,
				Class<JobMasterGateway> targetType,
				String targetAddress,
				UUID leaderId,
				String taskManagerRpcAddress,
				TaskManagerLocation taskManagerLocation)
		{
			super(log, rpcService, targetName, targetType, targetAddress, leaderId);

			this.taskManagerRpcAddress = taskManagerRpcAddress;
			this.taskManagerLocation = Preconditions.checkNotNull(taskManagerLocation);
		}

		@Override
		protected CompletableFuture<RegistrationResponse> invokeRegistration(
				JobMasterGateway gateway, UUID leaderId, long timeoutMillis) throws Exception
		{
			return FutureUtils.toJava(gateway.registerTaskManager(taskManagerRpcAddress, taskManagerLocation,
					leaderId, Time.milliseconds(timeoutMillis)));
		}
	}

	/**
	 * Internal state of the service
	 */
	private enum State {
		CREATED, STARTED, STOPPED
	}
}
