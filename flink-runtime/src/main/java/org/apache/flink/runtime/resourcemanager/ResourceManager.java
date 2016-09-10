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
import org.apache.flink.runtime.concurrent.AcceptFunction;
import org.apache.flink.runtime.concurrent.ApplyFunction;
import org.apache.flink.runtime.concurrent.BiFunction;
import org.apache.flink.runtime.concurrent.Future;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.jobmaster.JobMasterRegistrationSuccess;
import org.apache.flink.runtime.leaderelection.LeaderContender;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcMethod;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.jobmaster.JobMaster;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.registration.RegistrationResponse;

import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TaskExecutorRegistrationSuccess;
import org.apache.flink.runtime.util.LeaderConnectionInfo;
import org.apache.flink.runtime.util.LeaderRetrievalUtils;
import scala.concurrent.duration.FiniteDuration;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * ResourceManager implementation. The resource manager is responsible for resource de-/allocation
 * and bookkeeping.
 *
 * It offers the following methods as part of its rpc interface to interact with the him remotely:
 * <ul>
 *     <li>{@link #registerJobMaster(UUID, UUID, String, JobID)} registers a {@link JobMaster} at the resource manager</li>
 *     <li>{@link #requestSlot(SlotRequest)} requests a slot from the resource manager</li>
 * </ul>
 */
public abstract class ResourceManager<ResourceManagerGateway, WorkerType extends TaskExecutorRegistration> extends RpcEndpoint implements LeaderContender {

	/** The exit code with which the process is stopped in case of a fatal error */
	protected static final int EXIT_CODE_FATAL_ERROR = -13;

	private final Map<JobID, JobMasterGateway> jobMasterGateways;

	private final Set<LeaderRetrievalListener> jobMasterLeaderRetrievalListeners;

	private final Map<ResourceID, WorkerType> taskExecutorGateways;

	private final HighAvailabilityServices highAvailabilityServices;

	private LeaderElectionService leaderElectionService;

	private final SlotManager slotManager;

	private UUID leaderSessionID;

	private Map<String, InfoMessageListenerRpcGateway> infoMessageListeners;

	public ResourceManager(RpcService rpcService, HighAvailabilityServices highAvailabilityServices, SlotManager slotManager) {
		super(rpcService);
		this.highAvailabilityServices = checkNotNull(highAvailabilityServices);
		this.jobMasterGateways = new HashMap<>();
		this.slotManager = checkNotNull(slotManager);
		this.jobMasterLeaderRetrievalListeners = new HashSet<>();
		this.taskExecutorGateways = new HashMap<>();
		infoMessageListeners = new HashMap<>();
	}

	@Override
	public void start() {
		// start a leader
		try {
			super.start();
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
			for(JobID jobID : jobMasterGateways.keySet()) {
				highAvailabilityServices.getJobMasterLeaderRetriever(jobID).stop();
			}
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

					final LeaderConnectionInfo jobMasterLeaderInfo;
					try {
						jobMasterLeaderInfo = LeaderRetrievalUtils.retrieveLeaderConnectionInfo(
							highAvailabilityServices.getJobMasterLeaderRetriever(jobID), new FiniteDuration(5, TimeUnit.SECONDS));
					} catch (Exception e) {
						log.warn("Failed to start JobMasterLeaderRetriever for JobID {}", jobID);
						throw new Exception("Failed to retrieve JobMasterLeaderRetriever");
					}

					if (!jobMasterLeaderId.equals(jobMasterLeaderInfo.getLeaderSessionID())) {
						log.info("Declining registration request from non-leading JobManager {}", jobMasterAddress);
						throw new Exception("JobManager is not leading");
					}

					return getRpcService().connect(jobMasterAddress, JobMasterGateway.class).get(5, TimeUnit.SECONDS);
				}
			})
			.handleAsync(new BiFunction<JobMasterGateway, Throwable, RegistrationResponse>() {
				@Override
				public RegistrationResponse apply(JobMasterGateway jobMasterGateway, Throwable throwable) {
					
					if (throwable != null) {
						return new RegistrationResponse.Decline(throwable.getMessage());
					} else {
						JobMasterLeaderListener jobMasterLeaderListener = new JobMasterLeaderListener(jobID);
						try {
							LeaderRetrievalService jobMasterLeaderRetriever = highAvailabilityServices.getJobMasterLeaderRetriever(jobID);
							jobMasterLeaderRetriever.start(jobMasterLeaderListener);
						} catch (Exception e) {
							log.warn("Failed to start JobMasterLeaderRetriever for JobID {}", jobID);
							return new RegistrationResponse.Decline("Failed to retrieve JobMasterLeaderRetriever");
						}
						jobMasterLeaderRetrievalListeners.add(jobMasterLeaderListener);
						final JobMasterGateway existingGateway = jobMasterGateways.put(jobID, jobMasterGateway);
						if (existingGateway != null) {
							log.info("Replacing gateway for registered JobID {}.", jobID);
						}
						return new JobMasterRegistrationSuccess(5000);
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
		final ResourceID resourceID) {

		return getRpcService().execute(new Callable<TaskExecutorGateway>() {
			@Override
			public TaskExecutorGateway call() throws Exception {
				if (!leaderSessionID.equals(resourceManagerLeaderId)) {
					log.warn("Discard registration from TaskExecutor {} at ({}) because the expected leader session ID {} did " +
							"not equal the received leader session ID  {}",
						resourceID, taskExecutorAddress, leaderSessionID, resourceManagerLeaderId);
					throw new Exception("Invalid leader session id");
				}

				return getRpcService().connect(taskExecutorAddress, TaskExecutorGateway.class).get(5, TimeUnit.SECONDS);
			}
		}).handleAsync(new BiFunction<TaskExecutorGateway, Throwable, RegistrationResponse>() {
			@Override
			public RegistrationResponse apply(TaskExecutorGateway taskExecutorGateway, Throwable throwable) {
				if (throwable != null) {
					return new RegistrationResponse.Decline(throwable.getMessage());
				} else {
					WorkerType startedWorker = taskExecutorGateways.get(resourceID);
					if(startedWorker != null) {
						String oldWorkerAddress = startedWorker.getTaskExecutorGateway().getAddress();
						if (taskExecutorAddress.equals(oldWorkerAddress)) {
							log.warn("Receive a duplicate registration from TaskExecutor {} at ({})", resourceID, taskExecutorAddress);
						} else {
							log.warn("Receive a duplicate registration from TaskExecutor {} at different address, previous ({}), new ({})",
								resourceID, oldWorkerAddress, taskExecutorAddress);
							// TODO :: suggest old taskExecutor to stop itself
							slotManager.notifyTaskManagerFailure(resourceID);
							startedWorker = workerStarted(resourceID, taskExecutorGateway);
							taskExecutorGateways.put(resourceID, startedWorker);
						}
					} else {
						startedWorker = workerStarted(resourceID, taskExecutorGateway);
						taskExecutorGateways.put(resourceID, startedWorker);
					}
					return new TaskExecutorRegistrationSuccess(startedWorker.getInstanceID(), 5000);
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
	public SlotRequestReply requestSlot(SlotRequest slotRequest) {
		final JobID jobId = slotRequest.getJobId();
		final JobMasterGateway jobMasterGateway = jobMasterGateways.get(jobId);

		if (jobMasterGateway != null) {
			return slotManager.requestSlot(slotRequest);
		} else {
			log.info("Ignoring slot request for unknown JobMaster with JobID {}", jobId);
			return new SlotRequestRejected(slotRequest.getAllocationId());
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
				// notify SlotManager
				slotManager.setLeaderUUID(leaderSessionID);
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
				jobMasterGateways.clear();
				taskExecutorGateways.clear();
				slotManager.clearState();
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
	 * Shutdowns cluster
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
				WorkerType worker = taskExecutorGateways.remove(resourceID);
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
		return taskExecutorGateways.size();
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
	 * Callback when a task executor register.
	 *
	 * @param resourceID The worker resource id
	 * @param taskExecutorGateway the task executor gateway
	 */
	protected abstract WorkerType workerStarted(ResourceID resourceID, TaskExecutorGateway taskExecutorGateway);

	/**
	 * Callback when a resource manager faced a fatal error
	 * @param message
	 * @param error
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

	private static class JobMasterLeaderListener implements LeaderRetrievalListener {

		private final JobID jobID;
		private UUID leaderID;

		private JobMasterLeaderListener(JobID jobID) {
			this.jobID = jobID;
		}

		@Override
		public void notifyLeaderAddress(final String leaderAddress, final UUID leaderSessionID) {
			this.leaderID = leaderSessionID;
		}

		@Override
		public void handleError(final Exception exception) {
			// TODO
		}
	}

}

