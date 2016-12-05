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

package org.apache.flink.runtime.resourcemanager;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.messages.InfoMessage;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.concurrent.AcceptFunction;
import org.apache.flink.runtime.concurrent.ApplyFunction;
import org.apache.flink.runtime.concurrent.BiFunction;
import org.apache.flink.runtime.concurrent.Future;
import org.apache.flink.runtime.concurrent.impl.FlinkCompletableFuture;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.LeaderIdMismatchException;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.jobmaster.JobMasterRegistrationSuccess;
import org.apache.flink.runtime.leaderelection.LeaderContender;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException;
import org.apache.flink.runtime.resourcemanager.messages.jobmanager.RMSlotRequestRejected;
import org.apache.flink.runtime.resourcemanager.messages.jobmanager.RMSlotRequestReply;
import org.apache.flink.runtime.resourcemanager.registration.JobManagerRegistration;
import org.apache.flink.runtime.resourcemanager.registration.WorkerRegistration;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManagerFactory;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcMethod;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.jobmaster.JobMaster;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.registration.RegistrationResponse;

import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TaskExecutorRegistrationSuccess;
import org.apache.flink.util.ExceptionUtils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * ResourceManager implementation. The resource manager is responsible for resource de-/allocation
 * and bookkeeping.
 *
 * It offers the following methods as part of its rpc interface to interact with him remotely:
 * <ul>
 *     <li>{@link #registerJobManager(UUID, UUID, String, JobID)} registers a {@link JobMaster} at the resource manager</li>
 *     <li>{@link #requestSlot(UUID, UUID, SlotRequest)} requests a slot from the resource manager</li>
 * </ul>
 */
public abstract class ResourceManager<WorkerType extends Serializable>
		extends RpcEndpoint<ResourceManagerGateway>
		implements LeaderContender {

	/** Configuration of the resource manager */
	private final ResourceManagerConfiguration resourceManagerConfiguration;

	/** All currently registered JobMasterGateways scoped by JobID. */
	private final Map<JobID, JobManagerRegistration> jobManagerRegistrations;

	/** Service to retrieve the job leader ids */
	private final JobLeaderIdService jobLeaderIdService;

	/** All currently registered TaskExecutors with there framework specific worker information. */
	private final Map<ResourceID, WorkerRegistration<WorkerType>> taskExecutors;

	/** High availability services for leader retrieval and election. */
	private final HighAvailabilityServices highAvailabilityServices;

	/** The factory to construct the SlotManager. */
	private final SlotManagerFactory slotManagerFactory;

	/** Registry to use for metrics */
	private final MetricRegistry metricRegistry;

	/** Fatal error handler */
	private final FatalErrorHandler fatalErrorHandler;

	/** The SlotManager created by the slotManagerFactory when the ResourceManager is started. */
	private SlotManager slotManager;

	/** The service to elect a ResourceManager leader. */
	private LeaderElectionService leaderElectionService;

	/** ResourceManager's leader session id which is updated on leader election. */
	private volatile UUID leaderSessionId;

	/** All registered listeners for status updates of the ResourceManager. */
	private ConcurrentMap<String, InfoMessageListenerRpcGateway> infoMessageListeners;

	public ResourceManager(
			RpcService rpcService,
			ResourceManagerConfiguration resourceManagerConfiguration,
			HighAvailabilityServices highAvailabilityServices,
			SlotManagerFactory slotManagerFactory,
			MetricRegistry metricRegistry,
			JobLeaderIdService jobLeaderIdService,
			FatalErrorHandler fatalErrorHandler) {

		super(rpcService);

		this.resourceManagerConfiguration = checkNotNull(resourceManagerConfiguration);
		this.highAvailabilityServices = checkNotNull(highAvailabilityServices);
		this.slotManagerFactory = checkNotNull(slotManagerFactory);
		this.metricRegistry = checkNotNull(metricRegistry);
		this.jobLeaderIdService = checkNotNull(jobLeaderIdService);
		this.fatalErrorHandler = checkNotNull(fatalErrorHandler);

		this.jobManagerRegistrations = new HashMap<>(4);
		this.taskExecutors = new HashMap<>(8);
		this.leaderSessionId = null;
		infoMessageListeners = new ConcurrentHashMap<>(8);
	}

	// ------------------------------------------------------------------------
	//  RPC lifecycle methods
	// ------------------------------------------------------------------------

	@Override
	public void start() throws Exception {
		// start a leader
		super.start();

		try {
			// SlotManager should start first
			slotManager = slotManagerFactory.create(createResourceManagerServices());
		} catch (Exception e) {
			throw new ResourceManagerException("Could not create the slot manager.", e);
		}

		leaderElectionService = highAvailabilityServices.getResourceManagerLeaderElectionService();

		try {
			leaderElectionService.start(this);
		} catch (Exception e) {
			throw new ResourceManagerException("Could not start the leader election service.", e);
		}

		try {
			jobLeaderIdService.start(new JobLeaderIdActionsImpl());
		} catch (Exception e) {
			throw new ResourceManagerException("Could not start the job leader id service.", e);
		}

		initialize();
	}

	@Override
	public void shutDown() throws Exception {
		Exception exception = null;

		try {
			jobLeaderIdService.stop();
		} catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(e, exception);
		}

		try {
			leaderElectionService.stop();
		} catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(e, exception);
		}

		clearState();

		try {
			super.shutDown();
		} catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(e, exception);
		}

		if (exception != null) {
			ExceptionUtils.rethrowException(exception, "Error while shutting the ResourceManager down.");
		}
	}

	// ------------------------------------------------------------------------
	//  RPC methods
	// ------------------------------------------------------------------------

	@RpcMethod
	public Future<RegistrationResponse> registerJobManager(
			final UUID resourceManagerLeaderId,
			final UUID jobManagerLeaderId,
			final String jobManagerAddress,
			final JobID jobId) {

		checkNotNull(resourceManagerLeaderId);
		checkNotNull(jobManagerLeaderId);
		checkNotNull(jobManagerAddress);
		checkNotNull(jobId);

		if (isValid(resourceManagerLeaderId)) {
			if (!jobLeaderIdService.containsJob(jobId)) {
				try {
					jobLeaderIdService.addJob(jobId);
				} catch (Exception e) {
					ResourceManagerException exception = new ResourceManagerException("Could not add the job " +
						jobId + " to the job id leader service.", e);

					onFatalErrorAsync(exception);

					log.error("Could not add job {} to job leader id service.", jobId, e);
					return FlinkCompletableFuture.completedExceptionally(exception);
				}
			}

			log.info("Registering job manager {}@{} for job {}.", jobManagerLeaderId, jobManagerAddress, jobId);

			Future<UUID> jobLeaderIdFuture;

			try {
				jobLeaderIdFuture = jobLeaderIdService.getLeaderId(jobId);
			} catch (Exception e) {
				// we cannot check the job leader id so let's fail
				// TODO: Maybe it's also ok to skip this check in case that we cannot check the leader id
				ResourceManagerException exception = new ResourceManagerException("Cannot obtain the " +
					"job leader id future to verify the correct job leader.", e);

				onFatalErrorAsync(exception);

				log.debug("Could not obtain the job leader id future to verify the correct job leader.");
				return FlinkCompletableFuture.completedExceptionally(exception);
			}

			Future<JobMasterGateway> jobMasterGatewayFuture = getRpcService().connect(jobManagerAddress, JobMasterGateway.class);

			Future<RegistrationResponse> registrationResponseFuture = jobMasterGatewayFuture.thenCombineAsync(jobLeaderIdFuture, new BiFunction<JobMasterGateway, UUID, RegistrationResponse>() {
				@Override
				public RegistrationResponse apply(JobMasterGateway jobMasterGateway, UUID jobLeaderId) {
					if (isValid(resourceManagerLeaderId)) {
						if (jobLeaderId.equals(jobManagerLeaderId)) {
							if (jobManagerRegistrations.containsKey(jobId)) {
								JobManagerRegistration oldJobManagerRegistration = jobManagerRegistrations.get(jobId);

								if (oldJobManagerRegistration.getLeaderID().equals(jobLeaderId)) {
									// same registration
									log.debug("Job manager {}@{} was already registered.", jobManagerLeaderId, jobManagerAddress);
								} else {
									// tell old job manager that he is no longer the job leader
									disconnectJobManager(
										oldJobManagerRegistration.getJobID(),
										new Exception("New job leader for job " + jobId + " found."));

									JobManagerRegistration jobManagerRegistration = new JobManagerRegistration(jobId, jobLeaderId, jobMasterGateway);
									jobManagerRegistrations.put(jobId, jobManagerRegistration);
								}
							} else {
								// new registration for the job
								JobManagerRegistration jobManagerRegistration = new JobManagerRegistration(jobId, jobLeaderId, jobMasterGateway);

								jobManagerRegistrations.put(jobId, jobManagerRegistration);
							}

							log.info("Registered job manager {}@{} for job {}.", jobManagerLeaderId, jobManagerAddress, jobId);

							return new JobMasterRegistrationSuccess(
								resourceManagerConfiguration.getHeartbeatInterval().toMilliseconds(),
								getLeaderSessionId());

						} else {
							log.debug("The job manager leader id {} did not match the job " +
								"leader id {}.", jobManagerLeaderId, jobLeaderId);
							return new RegistrationResponse.Decline("Job manager leader id did not match.");
						}
					} else {
						log.debug("The resource manager leader id changed {}. Discarding job " +
							"manager registration from {}.", getLeaderSessionId(), jobManagerAddress);
						return new RegistrationResponse.Decline("Resource manager leader id changed.");
					}
				}
			}, getMainThreadExecutor());

			// handle exceptions which might have occurred in one of the futures inputs of combine
			return registrationResponseFuture.handleAsync(new BiFunction<RegistrationResponse, Throwable, RegistrationResponse>() {
				@Override
				public RegistrationResponse apply(RegistrationResponse registrationResponse, Throwable throwable) {
					if (throwable != null) {
						if (log.isDebugEnabled()) {
							log.debug("Registration of job manager {}@{} failed.", jobManagerLeaderId, jobManagerAddress, throwable);
						} else {
							log.info("Registration of job manager {}@{} failed.", jobManagerLeaderId, jobManagerAddress);
						}

						return new RegistrationResponse.Decline(throwable.getMessage());
					} else {
						return registrationResponse;
					}
				}
			}, getRpcService().getExecutor());
		} else {
			log.debug("Discard register job manager message from {}, because the leader id " +
				"{} did not match the expected leader id {}.", jobManagerAddress,
				resourceManagerLeaderId, leaderSessionId);

			return FlinkCompletableFuture.<RegistrationResponse>completed(
				new RegistrationResponse.Decline("Resource manager leader id did not match."));
		}
	}

	/**
	 * Register a {@link org.apache.flink.runtime.taskexecutor.TaskExecutor} at the resource manager
	 *
	 * @param resourceManagerLeaderId  The fencing token for the ResourceManager leader
	 * @param taskExecutorAddress      The address of the TaskExecutor that registers
	 * @param resourceID               The resource ID of the TaskExecutor that registers
	 *
	 * @return The response by the ResourceManager.
	 */
	@RpcMethod
	public Future<RegistrationResponse> registerTaskExecutor(
		final UUID resourceManagerLeaderId,
		final String taskExecutorAddress,
		final ResourceID resourceID,
		final SlotReport slotReport) {

		if (leaderSessionId.equals(resourceManagerLeaderId)) {
			Future<TaskExecutorGateway> taskExecutorGatewayFuture = getRpcService().connect(taskExecutorAddress, TaskExecutorGateway.class);

			return taskExecutorGatewayFuture.handleAsync(new BiFunction<TaskExecutorGateway, Throwable, RegistrationResponse>() {
				@Override
				public RegistrationResponse apply(TaskExecutorGateway taskExecutorGateway, Throwable throwable) {
					if (throwable != null) {
						return new RegistrationResponse.Decline(throwable.getMessage());
					} else {
						WorkerRegistration<WorkerType> oldRegistration = taskExecutors.remove(resourceID);
						if (oldRegistration != null) {
							// TODO :: suggest old taskExecutor to stop itself
							log.info("Replacing old instance of worker for ResourceID {}", resourceID);
						}

						WorkerType newWorker = workerStarted(resourceID);
						WorkerRegistration<WorkerType> registration =
							new WorkerRegistration<>(taskExecutorGateway, newWorker);

						taskExecutors.put(resourceID, registration);
						slotManager.registerTaskExecutor(resourceID, registration, slotReport);

						return new TaskExecutorRegistrationSuccess(
							registration.getInstanceID(),
							resourceManagerConfiguration.getHeartbeatInterval().toMilliseconds());
					}
				}
			}, getMainThreadExecutor());
		} else {
			log.warn("Discard registration from TaskExecutor {} at ({}) because the expected leader session ID {} did " +
					"not equal the received leader session ID  {}",
				resourceID, taskExecutorAddress, leaderSessionId, resourceManagerLeaderId);

			return FlinkCompletableFuture.<RegistrationResponse>completed(
				new RegistrationResponse.Decline("Discard registration because the leader id " +
					resourceManagerLeaderId + " does not match the expected leader id " +
					leaderSessionId + '.'));
		}
	}

	/**
	 * Requests a slot from the resource manager.
	 *
	 * @param slotRequest Slot request
	 * @return Slot assignment
	 */
	@RpcMethod
	public RMSlotRequestReply requestSlot(
			UUID jobMasterLeaderID,
			UUID resourceManagerLeaderID,
			SlotRequest slotRequest) {

		log.info("Request slot with profile {} for job {} with allocation id {}.",
			slotRequest.getResourceProfile(),
			slotRequest.getJobId(),
			slotRequest.getAllocationId());

		JobID jobId = slotRequest.getJobId();
		JobManagerRegistration jobManagerRegistration = jobManagerRegistrations.get(jobId);

		if (jobManagerRegistration != null
				&& jobMasterLeaderID.equals(jobManagerRegistration.getLeaderID())
				&& resourceManagerLeaderID.equals(leaderSessionId)) {
			return slotManager.requestSlot(slotRequest);
		} else {
			log.info("Ignoring slot request for unknown JobMaster with JobID {}", jobId);
			return new RMSlotRequestRejected(slotRequest.getAllocationId());
		}
	}

	/**
	 * Notification from a TaskExecutor that a slot has become available
	 * @param resourceManagerLeaderId TaskExecutor's resource manager leader id
	 * @param instanceID TaskExecutor's instance id
	 * @param slotId The slot id of the available slot
	 * @return SlotAvailableReply
	 */
	@RpcMethod
	public void notifySlotAvailable(
			final UUID resourceManagerLeaderId,
			final InstanceID instanceID,
			final SlotID slotId) {

		if (resourceManagerLeaderId.equals(leaderSessionId)) {
			final ResourceID resourceId = slotId.getResourceID();
			WorkerRegistration<WorkerType> registration = taskExecutors.get(resourceId);

			if (registration != null) {
				InstanceID registrationId = registration.getInstanceID();

				if (registrationId.equals(instanceID)) {
					slotManager.notifySlotAvailable(resourceId, slotId);
				} else {
					log.debug("Invalid registration id for slot available message. This indicates an" +
						" outdated request.");
				}
			} else {
				log.debug("Could not find registration for resource id {}. Discarding the slot available" +
					"message {}.", resourceId, slotId);
			}
		} else {
			log.debug("Discarding notify slot available message for slot {}, because the " +
				"leader id {} did not match the expected leader id {}.", slotId,
				resourceManagerLeaderId, leaderSessionId);
		}
	}

	/**
	 * Registers an info message listener
	 *
	 * @param address address of infoMessage listener to register to this resource manager
	 */
	@RpcMethod
	public void registerInfoMessageListener(final String address) {
		if(infoMessageListeners.containsKey(address)) {
			log.warn("Receive a duplicate registration from info message listener on ({})", address);
		} else {
			Future<InfoMessageListenerRpcGateway> infoMessageListenerRpcGatewayFuture = getRpcService().connect(address, InfoMessageListenerRpcGateway.class);

			infoMessageListenerRpcGatewayFuture.thenAcceptAsync(new AcceptFunction<InfoMessageListenerRpcGateway>() {
				@Override
				public void accept(InfoMessageListenerRpcGateway gateway) {
					log.info("Receive a registration from info message listener on ({})", address);
					infoMessageListeners.put(address, gateway);
				}
			}, getMainThreadExecutor());

			infoMessageListenerRpcGatewayFuture.exceptionallyAsync(new ApplyFunction<Throwable, Void>() {
				@Override
				public Void apply(Throwable failure) {
					log.warn("Receive a registration from unreachable info message listener on ({})", address);
					return null;
				}
			}, getRpcService().getExecutor());
		}
	}

	/**
	 * Unregisters an info message listener
	 *
	 * @param address of the  info message listener to unregister from this resource manager
	 *
	 */
	@RpcMethod
	public void unRegisterInfoMessageListener(final String address) {
		infoMessageListeners.remove(address);
	}

	/**
	 * Cleanup application and shut down cluster
	 *
	 * @param finalStatus
	 * @param optionalDiagnostics
	 */
	@RpcMethod
	public void shutDownCluster(final ApplicationStatus finalStatus, final String optionalDiagnostics) {
		log.info("shut down cluster because application is in {}, diagnostics {}", finalStatus, optionalDiagnostics);
		shutDownApplication(finalStatus, optionalDiagnostics);
	}

	@RpcMethod
	public Integer getNumberOfRegisteredTaskManagers(UUID leaderSessionId) throws LeaderIdMismatchException {
		if (this.leaderSessionId != null && this.leaderSessionId.equals(leaderSessionId)) {
			return taskExecutors.size();
		}
		else {
			throw new LeaderIdMismatchException(this.leaderSessionId, leaderSessionId);
		}
	}

	// ------------------------------------------------------------------------
	//  Testing methods
	// ------------------------------------------------------------------------

	/**
	 * Gets the leader session id of current resourceManager.
	 *
	 * @return return the leaderSessionId of current resourceManager, this returns null until the current resourceManager is granted leadership.
	 */
	@VisibleForTesting
	UUID getLeaderSessionId() {
		return leaderSessionId;
	}

	// ------------------------------------------------------------------------
	//  Internal methods
	// ------------------------------------------------------------------------

	private void clearState() {
		jobManagerRegistrations.clear();
		taskExecutors.clear();
		slotManager.clearState();

		try {
			jobLeaderIdService.clear();
		} catch (Exception e) {
			onFatalError(new ResourceManagerException("Could not properly clear the job leader id service.", e));
		}

		leaderSessionId = null;
	}

	/**
	 * Disconnects the job manager which is connected for the given job from the resource manager.
	 *
	 * @param jobId identifying the job whose leader shall be disconnected
	 */
	protected void disconnectJobManager(JobID jobId, Exception cause) {
		JobManagerRegistration jobManagerRegistration = jobManagerRegistrations.remove(jobId);

		if (jobManagerRegistration != null) {
			log.info("Disconnect job manager {}@{} for job {} from the resource manager.",
				jobManagerRegistration.getLeaderID(),
				jobManagerRegistration.getJobManagerGateway().getAddress(),
				jobId);

			JobMasterGateway jobMasterGateway = jobManagerRegistration.getJobManagerGateway();

			// tell the job manager about the disconnect
			jobMasterGateway.disconnectResourceManager(jobManagerRegistration.getLeaderID(), getLeaderSessionId(), cause);
		} else {
			log.debug("There was no registered job manager for job {}.", jobId);
		}
	}

	/**
	 * Checks whether the given resource manager leader id is matching the current leader id and
	 * not null.
	 *
	 * @param resourceManagerLeaderId to check
	 * @return True if the given leader id matches the actual leader id and is not null; otherwise false
	 */
	protected boolean isValid(UUID resourceManagerLeaderId) {
		if (resourceManagerLeaderId == null) {
			return false;
		} else {
			return resourceManagerLeaderId.equals(leaderSessionId);
		}
	}

	protected void removeJob(JobID jobId) {
		try {
			jobLeaderIdService.removeJob(jobId);
		} catch (Exception e) {
			log.warn("Could not properly remove the job {} from the job leader id service.", jobId, e);
		}

		if (jobManagerRegistrations.containsKey(jobId)) {
			disconnectJobManager(jobId, new Exception("Job " + jobId + "was removed"));
		}
	}

	protected void jobLeaderLostLeadership(JobID jobId, UUID oldJobLeaderId) {
		if (jobManagerRegistrations.containsKey(jobId)) {
			JobManagerRegistration jobManagerRegistration = jobManagerRegistrations.get(jobId);

			if (jobManagerRegistration.getLeaderID().equals(oldJobLeaderId)) {
				disconnectJobManager(jobId, new Exception("Job leader lost leadership."));
			} else {
				log.debug("Discarding job leader lost leadership, because a new job leader was found for job {}. ", jobId);
			}
		} else {
			log.debug("Discard job leader lost leadership for outdated leader {} for job {}.", oldJobLeaderId, jobId);
		}
	}

	// ------------------------------------------------------------------------
	//  Info messaging
	// ------------------------------------------------------------------------

	public void sendInfoMessage(final String message) {
		getRpcService().execute(new Runnable() {
			@Override
			public void run() {
				InfoMessage infoMessage = new InfoMessage(message);
				for (InfoMessageListenerRpcGateway listenerRpcGateway : infoMessageListeners.values()) {
					listenerRpcGateway
						.notifyInfoMessage(infoMessage);
				}
			}
		});
	}

	// ------------------------------------------------------------------------
	//  Error Handling
	// ------------------------------------------------------------------------

	/**
	 * Notifies the ResourceManager that a fatal error has occurred and it cannot proceed.
	 * This method should be used when asynchronous threads want to notify the
	 * ResourceManager of a fatal error.
	 *
	 * @param t The exception describing the fatal error
	 */
	protected void onFatalErrorAsync(final Throwable t) {
		runAsync(new Runnable() {
			@Override
			public void run() {
				onFatalError(t);
			}
		});
	}

	/**
	 * Notifies the ResourceManager that a fatal error has occurred and it cannot proceed.
	 * This method must only be called from within the ResourceManager's main thread.
	 *
	 * @param t The exception describing the fatal error
	 */
	void onFatalError(Throwable t) {
		log.error("Fatal error occurred.", t);
		fatalErrorHandler.onFatalError(t);
	}

	// ------------------------------------------------------------------------
	//  Leader Contender
	// ------------------------------------------------------------------------

	/**
	 * Callback method when current resourceManager is granted leadership
	 *
	 * @param newLeaderSessionID unique leadershipID
	 */
	@Override
	public void grantLeadership(final UUID newLeaderSessionID) {
		runAsync(new Runnable() {
			@Override
			public void run() {
				log.info("ResourceManager {} was granted leadership with leader session ID {}", getAddress(), newLeaderSessionID);

				// clear the state if we've been the leader before
				if (leaderSessionId != null) {
					clearState();
				}

				leaderSessionId = newLeaderSessionID;

				getRpcService().execute(new Runnable() {
					@Override
					public void run() {
						// confirming the leader session ID might be blocking,
						leaderElectionService.confirmLeaderSessionID(newLeaderSessionID);
					}
				});
			}
		});
	}

	/**
	 * Callback method when current resourceManager loses leadership.
	 */
	@Override
	public void revokeLeadership() {
		runAsync(new Runnable() {
			@Override
			public void run() {
				log.info("ResourceManager {} was revoked leadership.", getAddress());

				clearState();

				leaderSessionId = null;
			}
		});
	}

	/**
	 * Handles error occurring in the leader election service
	 *
	 * @param exception Exception being thrown in the leader election service
	 */
	@Override
	public void handleError(final Exception exception) {
		onFatalErrorAsync(new ResourceManagerException("Received an error from the LeaderElectionService.", exception));
	}

	/**
	 * This method should be called by the framework once it detects that a currently registered
	 * task executor has failed.
	 *
	 * @param resourceID Id of the worker that has failed.
	 * @param message An informational message that explains why the worker failed.
	 */
	public void notifyWorkerFailed(final ResourceID resourceID, final String message) {
		runAsync(new Runnable() {
			@Override
			public void run() {
				WorkerRegistration<WorkerType> workerRegistration = taskExecutors.remove(resourceID);

				if (workerRegistration != null) {
					log.info("Task manager {} failed because {}.", resourceID, message);
					// TODO :: suggest failed task executor to stop itself
					slotManager.notifyTaskManagerFailure(resourceID);
				} else {
					log.debug("Could not find a registered task manager with the process id {}.", resourceID);
				}
			}
		});
	}

	// ------------------------------------------------------------------------
	//  Framework specific behavior
	// ------------------------------------------------------------------------

	/**
	 * Initializes the framework specific components.
	 *
	 * @throws ResourceManagerException which occurs during initialization and causes the resource manager to fail.
	 */
	protected abstract void initialize() throws ResourceManagerException;

	/**
	 * The framework specific code for shutting down the application. This should report the
	 * application's final status and shut down the resource manager cleanly.
	 *
	 * This method also needs to make sure all pending containers that are not registered
	 * yet are returned.
	 *
	 * @param finalStatus The application status to report.
	 * @param optionalDiagnostics An optional diagnostics message.
	 */
	protected abstract void shutDownApplication(ApplicationStatus finalStatus, String optionalDiagnostics);

	/**
	 * Allocates a resource using the resource profile.
	 * @param resourceProfile The resource description
	 */
	@VisibleForTesting
	public abstract void startNewWorker(ResourceProfile resourceProfile);

	/**
	 * Callback when a worker was started.
	 * @param resourceID The worker resource id
	 */
	protected abstract WorkerType workerStarted(ResourceID resourceID);

	// ------------------------------------------------------------------------
	//  Static utility classes
	// ------------------------------------------------------------------------

	protected ResourceManagerServices createResourceManagerServices() {
		return new DefaultResourceManagerServices();
	}

	private class DefaultResourceManagerServices implements ResourceManagerServices {

		@Override
		public UUID getLeaderID() {
			return ResourceManager.this.leaderSessionId;
		}

		@Override
		public void allocateResource(ResourceProfile resourceProfile) {
			ResourceManager.this.startNewWorker(resourceProfile);
		}

		@Override
		public Executor getAsyncExecutor() {
			return ResourceManager.this.getRpcService().getExecutor();
		}

		@Override
		public Executor getMainThreadExecutor() {
			return ResourceManager.this.getMainThreadExecutor();
		}
	}

	private class JobLeaderIdActionsImpl implements JobLeaderIdActions {

		@Override
		public void jobLeaderLostLeadership(final JobID jobId, final UUID oldJobLeaderId) {
			ResourceManager.this.jobLeaderLostLeadership(jobId, oldJobLeaderId);
		}

		@Override
		public void removeJob(final JobID jobId) {
			runAsync(new Runnable() {
				@Override
				public void run() {
					ResourceManager.this.removeJob(jobId);
				}
			});
		}

		@Override
		public void handleError(Throwable error) {
			onFatalErrorAsync(error);
		}
	}
}

