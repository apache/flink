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
import org.apache.flink.api.common.time.Time;
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
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.jobmaster.JobMasterRegistrationSuccess;
import org.apache.flink.runtime.leaderelection.LeaderContender;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException;
import org.apache.flink.runtime.resourcemanager.messages.jobmanager.RMSlotRequestRejected;
import org.apache.flink.runtime.resourcemanager.messages.jobmanager.RMSlotRequestReply;
import org.apache.flink.runtime.resourcemanager.registration.JobMasterRegistration;
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
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
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
 *     <li>{@link #registerJobMaster(UUID, UUID, String, JobID)} registers a {@link JobMaster} at the resource manager</li>
 *     <li>{@link #requestSlot(UUID, UUID, SlotRequest)} requests a slot from the resource manager</li>
 * </ul>
 */
public abstract class ResourceManager<WorkerType extends Serializable>
		extends RpcEndpoint<ResourceManagerGateway>
		implements LeaderContender {

	/** Configuration of the resource manager */
	private final ResourceManagerConfiguration resourceManagerConfiguration;

	/** All currently registered JobMasterGateways scoped by JobID. */
	private final Map<JobID, JobMasterRegistration> jobMasters;

	/** LeaderListeners for all registered JobIDs. */
	private final Map<JobID, JobIdLeaderListener> leaderListeners;

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
	private volatile UUID leaderSessionID;

	/** All registered listeners for status updates of the ResourceManager. */
	private ConcurrentMap<String, InfoMessageListenerRpcGateway> infoMessageListeners;

	public ResourceManager(
			RpcService rpcService,
			ResourceManagerConfiguration resourceManagerConfiguration,
			HighAvailabilityServices highAvailabilityServices,
			SlotManagerFactory slotManagerFactory,
			MetricRegistry metricRegistry,
			FatalErrorHandler fatalErrorHandler) {

		super(rpcService);

		this.resourceManagerConfiguration = checkNotNull(resourceManagerConfiguration);
		this.highAvailabilityServices = checkNotNull(highAvailabilityServices);
		this.slotManagerFactory = checkNotNull(slotManagerFactory);
		this.metricRegistry = checkNotNull(metricRegistry);
		this.fatalErrorHandler = checkNotNull(fatalErrorHandler);

		this.jobMasters = new HashMap<>(4);
		this.leaderListeners = new HashMap<>(4);
		this.taskExecutors = new HashMap<>(8);
		this.leaderSessionID = null;
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

		try {
			leaderElectionService = highAvailabilityServices.getResourceManagerLeaderElectionService();
			leaderElectionService.start(this);
		} catch (Exception e) {
			throw new ResourceManagerException("Could not start the leader election service.", e);
		}

		try {
			// framework specific initialization
			initialize();
		} catch (Exception e) {
			throw new ResourceManagerException("Could not initialize the resource manager.", e);
		}
	}

	@Override
	public void shutDown() throws Exception {
		Exception exception = null;

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

	/**
	 * Register a {@link JobMaster} at the resource manager.
	 *
	 * @param resourceManagerLeaderId The fencing token for the ResourceManager leader
	 * @param jobMasterAddress        The address of the JobMaster that registers
	 * @param jobID                   The Job ID of the JobMaster that registers
	 * @return Future registration response
	 */
	@RpcMethod
	public Future<RegistrationResponse> registerJobMaster(
		final UUID resourceManagerLeaderId, final UUID jobMasterLeaderId,
		final String jobMasterAddress, final JobID jobID) {

		checkNotNull(jobMasterAddress);
		checkNotNull(jobID);

		// create a leader retriever in case it doesn't exist
		final JobIdLeaderListener jobIdLeaderListener;
		if (leaderListeners.containsKey(jobID)) {
			jobIdLeaderListener = leaderListeners.get(jobID);
		} else {
			try {
				LeaderRetrievalService jobMasterLeaderRetriever =
					highAvailabilityServices.getJobManagerLeaderRetriever(jobID);
				jobIdLeaderListener = new JobIdLeaderListener(jobID, jobMasterLeaderRetriever);
			} catch (Exception e) {
				log.warn("Failed to start JobMasterLeaderRetriever for job id {}", jobID, e);

				return FlinkCompletableFuture.<RegistrationResponse>completed(
					new RegistrationResponse.Decline("Failed to retrieve JobMasterLeaderRetriever"));
			}

			leaderListeners.put(jobID, jobIdLeaderListener);
		}

		return getRpcService()
			.execute(new Callable<JobMasterGateway>() {
				@Override
				public JobMasterGateway call() throws Exception {

					if (!leaderSessionID.equals(resourceManagerLeaderId)) {
						log.warn("Discard registration from JobMaster {} at ({}) because the expected leader session ID {}" +
								" did not equal the received leader session ID  {}",
							jobID, jobMasterAddress, leaderSessionID, resourceManagerLeaderId);
						throw new Exception("Invalid leader session id");
					}

					final Time timeout = resourceManagerConfiguration.getTimeout();

					if (!jobIdLeaderListener.getLeaderID().get(timeout.getSize(), timeout.getUnit())
							.equals(jobMasterLeaderId)) {
						throw new Exception("Leader Id did not match");
					}

					return getRpcService().connect(jobMasterAddress, JobMasterGateway.class)
						.get(timeout.getSize(), timeout.getUnit());
				}
			})
			.handleAsync(new BiFunction<JobMasterGateway, Throwable, RegistrationResponse>() {
				@Override
				public RegistrationResponse apply(JobMasterGateway jobMasterGateway, Throwable throwable) {

				if (throwable != null) {
					return new RegistrationResponse.Decline(throwable.getMessage());
				} else {
					if (!leaderSessionID.equals(resourceManagerLeaderId)) {
						log.warn("Discard registration from JobMaster {} at ({}) because the expected leader session ID {}" +
								" did not equal the received leader session ID  {}",
							jobID, jobMasterAddress, leaderSessionID, resourceManagerLeaderId);
						return new RegistrationResponse.Decline("Invalid leader session id");
					}

					try {
						// LeaderID should be available now, but if not we fail the registration
						UUID currentJobMasterLeaderId = jobIdLeaderListener.getLeaderID().getNow(null);
						if (currentJobMasterLeaderId == null || !currentJobMasterLeaderId.equals(jobMasterLeaderId)) {
							throw new Exception("Leader Id did not match");
						}
					} catch (Exception e) {
						return new RegistrationResponse.Decline(e.getMessage());
					}

					final JobMasterRegistration registration =
						new JobMasterRegistration(jobID, jobMasterLeaderId, jobMasterGateway);

					final JobMasterRegistration existingRegistration = jobMasters.put(jobID, registration);
					if (existingRegistration != null) {
						log.info("Replacing JobMaster registration for newly registered JobMaster with JobID {}.", jobID);
					}
					return new JobMasterRegistrationSuccess(
						resourceManagerConfiguration.getHeartbeatInterval().toMilliseconds(),
						resourceManagerLeaderId);
				}
			}
		}, getMainThreadExecutor());
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

		if (leaderSessionID.equals(resourceManagerLeaderId)) {
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
				resourceID, taskExecutorAddress, leaderSessionID, resourceManagerLeaderId);

			return FlinkCompletableFuture.<RegistrationResponse>completed(
				new RegistrationResponse.Decline("Discard registration because the leader id " +
					resourceManagerLeaderId + " does not match the expected leader id " +
					leaderSessionID + '.'));
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

		JobID jobId = slotRequest.getJobId();
		JobMasterRegistration jobMasterRegistration = jobMasters.get(jobId);

		if (jobMasterRegistration != null
				&& jobMasterLeaderID.equals(jobMasterRegistration.getLeaderID())
				&& resourceManagerLeaderID.equals(leaderSessionID)) {
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

		if (resourceManagerLeaderId.equals(leaderSessionID)) {
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
				resourceManagerLeaderId, leaderSessionID);
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
				if (leaderSessionID != null) {
					clearState();
				}

				leaderSessionID = newLeaderSessionID;

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

				leaderSessionID = null;
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
	//  Error Handling
	// ------------------------------------------------------------------------

	/**
	 * Notifies the ResourceManager that a fatal error has occurred and it cannot proceed.
	 * This method should be used when asynchronous threads want to notify the
	 * ResourceManager of a fatal error.
	 *
	 * @param t The exception describing the fatal error
	 */
	void onFatalErrorAsync(final Throwable t) {
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
	//  Testing methods
	// ------------------------------------------------------------------------

	/**
	 * Gets the leader session id of current resourceManager.
	 *
	 * @return return the leaderSessionId of current resourceManager, this returns null until the current resourceManager is granted leadership.
	 */
	@VisibleForTesting
	UUID getLeaderSessionID() {
		return leaderSessionID;
	}

	// ------------------------------------------------------------------------
	//  Internal methods
	// ------------------------------------------------------------------------

	private void clearState() {
		jobMasters.clear();
		taskExecutors.clear();
		slotManager.clearState();
		Iterator<JobIdLeaderListener> leaderListenerIterator =
			leaderListeners.values().iterator();
		while (leaderListenerIterator.hasNext()) {
			JobIdLeaderListener listener = leaderListenerIterator.next();
			try {
				listener.stopService();
			} catch (Exception e) {
				onFatalError(e);
			}
			leaderListenerIterator.remove();
		}
		leaderSessionID = new UUID(0, 0);
	}

	// ------------------------------------------------------------------------
	//  Framework specific behavior
	// ------------------------------------------------------------------------

	/**
	 * Initializes the framework specific components.
	 *
	 * @throws Exception Exceptions during initialization cause the resource manager to fail.
	 */
	protected abstract void initialize() throws Exception;

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
	//  Resource Manager Services
	// ------------------------------------------------------------------------

	protected ResourceManagerServices createResourceManagerServices() {
		return new DefaultResourceManagerServices();
	}

	private class DefaultResourceManagerServices implements ResourceManagerServices {

		@Override
		public UUID getLeaderID() {
			return ResourceManager.this.leaderSessionID;
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

	/**
	 * Leader instantiated for each connected JobMaster
	 */
	private class JobIdLeaderListener implements LeaderRetrievalListener {

		private final JobID jobID;
		private final LeaderRetrievalService retrievalService;

		private final FlinkCompletableFuture<UUID> initialLeaderIdFuture;

		private volatile UUID leaderID;

		private JobIdLeaderListener(
				JobID jobID,
				LeaderRetrievalService retrievalService) throws Exception {
			this.jobID = jobID;
			this.retrievalService = retrievalService;
			this.initialLeaderIdFuture = new FlinkCompletableFuture<>();
			this.retrievalService.start(this);
		}

		public Future<UUID> getLeaderID() {
			if (!initialLeaderIdFuture.isDone()) {
				return initialLeaderIdFuture;
			} else {
				return FlinkCompletableFuture.completed(leaderID);
			}
		}

		public JobID getJobID() {
			return jobID;
		}


		public void stopService() throws Exception {
			retrievalService.stop();
		}

		@Override
		public void notifyLeaderAddress(final String leaderAddress, final UUID leaderSessionID) {
			this.leaderID = leaderSessionID;

			if (!initialLeaderIdFuture.isDone()) {
				initialLeaderIdFuture.complete(leaderSessionID);
			}

			ResourceManager.this.runAsync(new Runnable() {
				@Override
				public void run() {
					JobMasterRegistration jobMasterRegistration = ResourceManager.this.jobMasters.get(jobID);
					if (jobMasterRegistration == null || !jobMasterRegistration.getLeaderID().equals(leaderSessionID)) {
						// registration is not valid anymore, remove registration
						ResourceManager.this.jobMasters.remove(jobID);
						// leader listener is not necessary anymore
						JobIdLeaderListener listener = ResourceManager.this.leaderListeners.remove(jobID);
						if (listener != null) {
							try {
								listener.stopService();
							} catch (Exception e) {
								ResourceManager.this.handleError(e);
							}
						}
					}
				}
			});
		}

		@Override
		public void handleError(final Exception exception) {
			ResourceManager.this.handleError(exception);
		}
	}
}

