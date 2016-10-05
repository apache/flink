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
import org.apache.flink.runtime.resourcemanager.messages.jobmanager.RMSlotRequestRejected;
import org.apache.flink.runtime.resourcemanager.messages.jobmanager.RMSlotRequestReply;
import org.apache.flink.runtime.resourcemanager.registration.JobMasterRegistration;
import org.apache.flink.runtime.resourcemanager.registration.WorkerRegistration;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManagerFactory;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcMethod;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.jobmaster.JobMaster;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.registration.RegistrationResponse;

import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TaskExecutorRegistrationSuccess;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * ResourceManager implementation. The resource manager is responsible for resource de-/allocation
 * and bookkeeping.
 *
 * It offers the following methods as part of its rpc interface to interact with the him remotely:
 * <ul>
 *     <li>{@link #registerJobMaster(UUID, UUID, String, JobID)} registers a {@link JobMaster} at the resource manager</li>
 *     <li>{@link #requestSlot(UUID, UUID, SlotRequest)} requests a slot from the resource manager</li>
 * </ul>
 */
public abstract class ResourceManager<WorkerType extends Serializable>
		extends RpcEndpoint<ResourceManagerGateway>
		implements LeaderContender {

	/** The exit code with which the process is stopped in case of a fatal error. */
	protected static final int EXIT_CODE_FATAL_ERROR = -13;

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

	/** The SlotManager created by the slotManagerFactory when the ResourceManager is started. */
	private SlotManager slotManager;

	/** The service to elect a ResourceManager leader. */
	private LeaderElectionService leaderElectionService;

	/** ResourceManager's leader session id which is updated on leader election. */
	private volatile UUID leaderSessionID;

	/** All registered listeners for status updates of the ResourceManager. */
	private Map<String, InfoMessageListenerRpcGateway> infoMessageListeners;

	/** Default timeout for messages */
	private final Time timeout = Time.seconds(5);

	public ResourceManager(
			RpcService rpcService,
			HighAvailabilityServices highAvailabilityServices,
			SlotManagerFactory slotManagerFactory) {
		super(rpcService);
		this.highAvailabilityServices = checkNotNull(highAvailabilityServices);
		this.slotManagerFactory = checkNotNull(slotManagerFactory);
		this.jobMasters = new HashMap<>();
		this.leaderListeners = new HashMap<>();
		this.taskExecutors = new HashMap<>();
		this.leaderSessionID = new UUID(0, 0);
		infoMessageListeners = new HashMap<>();
	}

	@Override
	public void start() {
		// start a leader
		try {
			super.start();
			// SlotManager should start first
			slotManager = slotManagerFactory.create(createResourceManagerServices());
			leaderElectionService = highAvailabilityServices.getResourceManagerLeaderElectionService();
			leaderElectionService.start(this);
			// framework specific initialization
			initialize();
		} catch (Throwable e) {
			log.error("A fatal error happened when starting the ResourceManager", e);
			throw new RuntimeException("A fatal error happened when starting the ResourceManager", e);
		}
	}

	@Override
	public void shutDown() {
		try {
			leaderElectionService.stop();
			clearState();
			super.shutDown();
		} catch (Throwable e) {
			log.error("A fatal error happened when shutdown the ResourceManager", e);
			throw new RuntimeException("A fatal error happened when shutdown the ResourceManager", e);
		}
	}

	/**
	 * Gets the leader session id of current resourceManager.
	 *
	 * @return return the leaderSessionId of current resourceManager, this returns null until the current resourceManager is granted leadership.
	 */
	@VisibleForTesting
	UUID getLeaderSessionID() {
		return this.leaderSessionID;
	}

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
					highAvailabilityServices.getJobManagerLeaderRetriever(jobID, jobMasterAddress);
				jobIdLeaderListener = new JobIdLeaderListener(jobID, jobMasterLeaderRetriever);
			} catch (Exception e) {
				log.warn("Failed to start JobMasterLeaderRetriever for JobID {}", jobID);
				FlinkCompletableFuture<RegistrationResponse> responseFuture = new FlinkCompletableFuture<>();
				responseFuture.complete(new RegistrationResponse.Decline("Failed to retrieve JobMasterLeaderRetriever"));
				return responseFuture;
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
					}

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
					return new JobMasterRegistrationSuccess(5000, resourceManagerLeaderId);

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

		return getRpcService().execute(new Callable<TaskExecutorGateway>() {
			@Override
			public TaskExecutorGateway call() throws Exception {
				if (!leaderSessionID.equals(resourceManagerLeaderId)) {
					log.warn("Discard registration from TaskExecutor {} at ({}) because the expected leader session ID {} did " +
							"not equal the received leader session ID  {}",
						resourceID, taskExecutorAddress, leaderSessionID, resourceManagerLeaderId);
					throw new Exception("Invalid leader session id");
				}
				return getRpcService().connect(taskExecutorAddress, TaskExecutorGateway.class)
					.get(timeout.toMilliseconds(), timeout.getUnit());
			}
		}).handleAsync(new BiFunction<TaskExecutorGateway, Throwable, RegistrationResponse>() {
			@Override
			public RegistrationResponse apply(TaskExecutorGateway taskExecutorGateway, Throwable throwable) {
				if (throwable != null) {
					return new RegistrationResponse.Decline(throwable.getMessage());
				} else {
					WorkerRegistration oldRegistration = taskExecutors.remove(resourceID);
					if (oldRegistration != null) {
						// TODO :: suggest old taskExecutor to stop itself
						log.info("Replacing old instance of worker for ResourceID {}", resourceID);
					}
					WorkerType newWorker = workerStarted(resourceID);
					WorkerRegistration<WorkerType> registration =
						new WorkerRegistration<>(taskExecutorGateway, newWorker);
					taskExecutors.put(resourceID, registration);
					slotManager.registerTaskExecutor(resourceID, registration, slotReport);
					return new TaskExecutorRegistrationSuccess(registration.getInstanceID(), 5000);
				}
			}
		}, getMainThreadExecutor());
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
	 * @param slotID The slot id of the available slot
	 * @return SlotAvailableReply
	 */
	@RpcMethod
	public void notifySlotAvailable(
			final UUID resourceManagerLeaderId,
			final InstanceID instanceID,
			final SlotID slotID) {

		if (resourceManagerLeaderId.equals(leaderSessionID)) {
			final ResourceID resourceId = slotID.getResourceID();
			WorkerRegistration<WorkerType> registration = taskExecutors.get(resourceId);

			if (registration != null) {
				InstanceID registrationInstanceID = registration.getInstanceID();
				if (registrationInstanceID.equals(instanceID)) {
					runAsync(new Runnable() {
						@Override
						public void run() {
							slotManager.notifySlotAvailable(resourceId, slotID);
						}
					});
				}
			}
		}
	}


	// ------------------------------------------------------------------------
	//  Leader Contender
	// ------------------------------------------------------------------------

	/**
	 * Callback method when current resourceManager is granted leadership
	 *
	 * @param leaderSessionID unique leadershipID
	 */
	@Override
	public void grantLeadership(final UUID leaderSessionID) {
		runAsync(new Runnable() {
			@Override
			public void run() {
				log.info("ResourceManager {} was granted leadership with leader session ID {}", getAddress(), leaderSessionID);
				// confirming the leader session ID might be blocking,
				leaderElectionService.confirmLeaderSessionID(leaderSessionID);
				ResourceManager.this.leaderSessionID = leaderSessionID;
			}
		});
	}

	/**
	 * Callback method when current resourceManager lose leadership.
	 */
	@Override
	public void revokeLeadership() {
		runAsync(new Runnable() {
			@Override
			public void run() {
				log.info("ResourceManager {} was revoked leadership.", getAddress());
				clearState();
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
		log.error("ResourceManager received an error from the LeaderElectionService.", exception);
		// terminate ResourceManager in case of an error
		shutDown();
	}

	/**
	 * Registers an infoMessage listener
	 *
	 * @param infoMessageListenerAddress address of infoMessage listener to register to this resource manager
	 */
	@RpcMethod
	public void registerInfoMessageListener(final String infoMessageListenerAddress) {
		if(infoMessageListeners.containsKey(infoMessageListenerAddress)) {
			log.warn("Receive a duplicate registration from info message listener on ({})", infoMessageListenerAddress);
		} else {
			Future<InfoMessageListenerRpcGateway> infoMessageListenerRpcGatewayFuture = getRpcService().connect(infoMessageListenerAddress, InfoMessageListenerRpcGateway.class);

			infoMessageListenerRpcGatewayFuture.thenAcceptAsync(new AcceptFunction<InfoMessageListenerRpcGateway>() {
				@Override
				public void accept(InfoMessageListenerRpcGateway gateway) {
					log.info("Receive a registration from info message listener on ({})", infoMessageListenerAddress);
					infoMessageListeners.put(infoMessageListenerAddress, gateway);
				}
			}, getMainThreadExecutor());

			infoMessageListenerRpcGatewayFuture.exceptionallyAsync(new ApplyFunction<Throwable, Void>() {
				@Override
				public Void apply(Throwable failure) {
					log.warn("Receive a registration from unreachable info message listener on ({})", infoMessageListenerAddress);
					return null;
				}
			}, getMainThreadExecutor());
		}
	}

	/**
	 * Unregisters an infoMessage listener
	 *
	 * @param infoMessageListenerAddress address of infoMessage listener to unregister from this resource manager
	 *
	 */
	@RpcMethod
	public void unRegisterInfoMessageListener(final String infoMessageListenerAddress) {
		infoMessageListeners.remove(infoMessageListenerAddress);
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

	/**
	 * This method should be called by the framework once it detects that a currently registered task executor has failed.
	 *
	 * @param resourceID Id of the worker that has failed.
	 * @param message An informational message that explains why the worker failed.
	 */
	public void notifyWorkerFailed(final ResourceID resourceID, String message) {
		runAsync(new Runnable() {
			@Override
			public void run() {
				WorkerType worker = taskExecutors.remove(resourceID).getWorker();
				if (worker != null) {
					// TODO :: suggest failed task executor to stop itself
					slotManager.notifyTaskManagerFailure(resourceID);
				}
			}
		});
	}

	/**
	 * Gets the number of currently started TaskManagers.
	 *
	 * @return The number of currently started TaskManagers.
	 */
	public int getNumberOfStartedTaskManagers() {
		return taskExecutors.size();
	}

	/**
	 * Notifies the resource manager of a fatal error.
	 *
	 * <p><b>IMPORTANT:</b> This should not cleanly shut down this master, but exit it in
	 * such a way that a high-availability setting would restart this or fail over
	 * to another master.
	 */
	public void onFatalError(final String message, final Throwable error) {
		runAsync(new Runnable() {
			@Override
			public void run() {
				fatalError(message, error);
			}
		});
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
	 * Notifies the resource master of a fatal error.
	 *
	 * <p><b>IMPORTANT:</b> This should not cleanly shut down this master, but exit it in
	 * such a way that a high-availability setting would restart this or fail over
	 * to another master.
	 */
	protected abstract void fatalError(String message, Throwable error);

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
		runAsync(new Runnable() {
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
				handleError(e);
			}
			leaderListenerIterator.remove();
		}
		leaderSessionID = new UUID(0, 0);
	}
}

