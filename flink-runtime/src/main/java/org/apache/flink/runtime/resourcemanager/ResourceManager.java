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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.blob.TransientBlobKey;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.messages.InfoMessage;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceIDRetrievable;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatListener;
import org.apache.flink.runtime.heartbeat.HeartbeatManager;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.heartbeat.HeartbeatTarget;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.instance.HardwareDescription;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.jobmaster.JobMaster;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.JobMasterRegistrationSuccess;
import org.apache.flink.runtime.leaderelection.LeaderContender;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.dump.MetricQueryService;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException;
import org.apache.flink.runtime.resourcemanager.exceptions.UnknownTaskExecutorException;
import org.apache.flink.runtime.resourcemanager.registration.JobManagerRegistration;
import org.apache.flink.runtime.resourcemanager.registration.WorkerRegistration;
import org.apache.flink.runtime.resourcemanager.slotmanager.ResourceActions;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManagerException;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerInfo;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.FencedRpcEndpoint;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.taskexecutor.FileType;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TaskExecutorRegistrationSuccess;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * ResourceManager implementation. The resource manager is responsible for resource de-/allocation
 * and bookkeeping.
 *
 * <p>It offers the following methods as part of its rpc interface to interact with him remotely:
 * <ul>
 *     <li>{@link #registerJobManager(JobMasterId, ResourceID, String, JobID, Time)} registers a {@link JobMaster} at the resource manager</li>
 *     <li>{@link #requestSlot(JobMasterId, SlotRequest, Time)} requests a slot from the resource manager</li>
 * </ul>
 */
public abstract class ResourceManager<WorkerType extends ResourceIDRetrievable>
		extends FencedRpcEndpoint<ResourceManagerId>
		implements ResourceManagerGateway, LeaderContender {

	public static final String RESOURCE_MANAGER_NAME = "resourcemanager";

	/** Unique id of the resource manager. */
	private final ResourceID resourceId;

	/** Configuration of the resource manager. */
	private final ResourceManagerConfiguration resourceManagerConfiguration;

	/** All currently registered JobMasterGateways scoped by JobID. */
	private final Map<JobID, JobManagerRegistration> jobManagerRegistrations;

	/** All currently registered JobMasterGateways scoped by ResourceID. */
	private final Map<ResourceID, JobManagerRegistration> jmResourceIdRegistrations;

	/** Service to retrieve the job leader ids. */
	private final JobLeaderIdService jobLeaderIdService;

	/** All currently registered TaskExecutors with there framework specific worker information. */
	private final Map<ResourceID, WorkerRegistration<WorkerType>> taskExecutors;

	/** High availability services for leader retrieval and election. */
	private final HighAvailabilityServices highAvailabilityServices;

	/** The heartbeat manager with task managers. */
	private final HeartbeatManager<SlotReport, Void> taskManagerHeartbeatManager;

	/** The heartbeat manager with job managers. */
	private final HeartbeatManager<Void, Void> jobManagerHeartbeatManager;

	/** Registry to use for metrics. */
	private final MetricRegistry metricRegistry;

	/** Fatal error handler. */
	private final FatalErrorHandler fatalErrorHandler;

	/** The slot manager maintains the available slots. */
	private final SlotManager slotManager;

	private final ClusterInformation clusterInformation;

	/** The service to elect a ResourceManager leader. */
	private LeaderElectionService leaderElectionService;

	/** All registered listeners for status updates of the ResourceManager. */
	private ConcurrentMap<String, InfoMessageListenerRpcGateway> infoMessageListeners;

	public ResourceManager(
			RpcService rpcService,
			String resourceManagerEndpointId,
			ResourceID resourceId,
			ResourceManagerConfiguration resourceManagerConfiguration,
			HighAvailabilityServices highAvailabilityServices,
			HeartbeatServices heartbeatServices,
			SlotManager slotManager,
			MetricRegistry metricRegistry,
			JobLeaderIdService jobLeaderIdService,
			ClusterInformation clusterInformation,
			FatalErrorHandler fatalErrorHandler) {

		super(rpcService, resourceManagerEndpointId);

		this.resourceId = checkNotNull(resourceId);
		this.resourceManagerConfiguration = checkNotNull(resourceManagerConfiguration);
		this.highAvailabilityServices = checkNotNull(highAvailabilityServices);
		this.slotManager = checkNotNull(slotManager);
		this.metricRegistry = checkNotNull(metricRegistry);
		this.jobLeaderIdService = checkNotNull(jobLeaderIdService);
		this.clusterInformation = checkNotNull(clusterInformation);
		this.fatalErrorHandler = checkNotNull(fatalErrorHandler);

		this.taskManagerHeartbeatManager = heartbeatServices.createHeartbeatManagerSender(
			resourceId,
			new TaskManagerHeartbeatListener(),
			rpcService.getScheduledExecutor(),
			log);

		this.jobManagerHeartbeatManager = heartbeatServices.createHeartbeatManagerSender(
			resourceId,
			new JobManagerHeartbeatListener(),
			rpcService.getScheduledExecutor(),
			log);

		this.jobManagerRegistrations = new HashMap<>(4);
		this.jmResourceIdRegistrations = new HashMap<>(4);
		this.taskExecutors = new HashMap<>(8);
		infoMessageListeners = new ConcurrentHashMap<>(8);
	}



	// ------------------------------------------------------------------------
	//  RPC lifecycle methods
	// ------------------------------------------------------------------------

	@Override
	public void start() throws Exception {
		// start a leader
		super.start();

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
	public CompletableFuture<Void> postStop() {
		Exception exception = null;

		taskManagerHeartbeatManager.stop();

		jobManagerHeartbeatManager.stop();

		try {
			slotManager.close();
		} catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(e, exception);
		}

		try {
			leaderElectionService.stop();
		} catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(e, exception);
		}

		try {
			jobLeaderIdService.stop();
		} catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(e, exception);
		}

		clearState();

		if (exception != null) {
			return FutureUtils.completedExceptionally(
				new FlinkException("Could not properly shut down the ResourceManager.", exception));
		} else {
			return CompletableFuture.completedFuture(null);
		}
	}

	// ------------------------------------------------------------------------
	//  RPC methods
	// ------------------------------------------------------------------------

	@Override
	public CompletableFuture<RegistrationResponse> registerJobManager(
			final JobMasterId jobMasterId,
			final ResourceID jobManagerResourceId,
			final String jobManagerAddress,
			final JobID jobId,
			final Time timeout) {

		checkNotNull(jobMasterId);
		checkNotNull(jobManagerResourceId);
		checkNotNull(jobManagerAddress);
		checkNotNull(jobId);

		if (!jobLeaderIdService.containsJob(jobId)) {
			try {
				jobLeaderIdService.addJob(jobId);
			} catch (Exception e) {
				ResourceManagerException exception = new ResourceManagerException("Could not add the job " +
					jobId + " to the job id leader service.", e);

					onFatalError(exception);

				log.error("Could not add job {} to job leader id service.", jobId, e);
				return FutureUtils.completedExceptionally(exception);
			}
		}

		log.info("Registering job manager {}@{} for job {}.", jobMasterId, jobManagerAddress, jobId);

		CompletableFuture<JobMasterId> jobMasterIdFuture;

		try {
			jobMasterIdFuture = jobLeaderIdService.getLeaderId(jobId);
		} catch (Exception e) {
			// we cannot check the job leader id so let's fail
			// TODO: Maybe it's also ok to skip this check in case that we cannot check the leader id
			ResourceManagerException exception = new ResourceManagerException("Cannot obtain the " +
				"job leader id future to verify the correct job leader.", e);

				onFatalError(exception);

			log.debug("Could not obtain the job leader id future to verify the correct job leader.");
			return FutureUtils.completedExceptionally(exception);
		}

		CompletableFuture<JobMasterGateway> jobMasterGatewayFuture = getRpcService().connect(jobManagerAddress, jobMasterId, JobMasterGateway.class);

		CompletableFuture<RegistrationResponse> registrationResponseFuture = jobMasterGatewayFuture.thenCombineAsync(
			jobMasterIdFuture,
			(JobMasterGateway jobMasterGateway, JobMasterId currentJobMasterId) -> {
				if (Objects.equals(currentJobMasterId, jobMasterId)) {
					return registerJobMasterInternal(
						jobMasterGateway,
						jobId,
						jobManagerAddress,
						jobManagerResourceId);
				} else {
					log.debug("The current JobMaster leader id {} did not match the received " +
						"JobMaster id {}.", jobMasterId, currentJobMasterId);
					return new RegistrationResponse.Decline("Job manager leader id did not match.");
				}
			},
			getMainThreadExecutor());

		// handle exceptions which might have occurred in one of the futures inputs of combine
		return registrationResponseFuture.handleAsync(
			(RegistrationResponse registrationResponse, Throwable throwable) -> {
				if (throwable != null) {
					if (log.isDebugEnabled()) {
						log.debug("Registration of job manager {}@{} failed.", jobMasterId, jobManagerAddress, throwable);
					} else {
						log.info("Registration of job manager {}@{} failed.", jobMasterId, jobManagerAddress);
					}

					return new RegistrationResponse.Decline(throwable.getMessage());
				} else {
					return registrationResponse;
				}
			},
			getRpcService().getExecutor());
	}

	@Override
	public CompletableFuture<RegistrationResponse> registerTaskExecutor(
			final String taskExecutorAddress,
			final ResourceID taskExecutorResourceId,
			final SlotReport slotReport,
			final int dataPort,
			final HardwareDescription hardwareDescription,
			final Time timeout) {

		CompletableFuture<TaskExecutorGateway> taskExecutorGatewayFuture = getRpcService().connect(taskExecutorAddress, TaskExecutorGateway.class);

		return taskExecutorGatewayFuture.handleAsync(
			(TaskExecutorGateway taskExecutorGateway, Throwable throwable) -> {
				if (throwable != null) {
					return new RegistrationResponse.Decline(throwable.getMessage());
				} else {
					return registerTaskExecutorInternal(
						taskExecutorGateway,
						taskExecutorAddress,
						taskExecutorResourceId,
						slotReport,
						dataPort,
						hardwareDescription);
				}
			},
			getMainThreadExecutor());
	}

	@Override
	public void heartbeatFromTaskManager(final ResourceID resourceID, final SlotReport slotReport) {
		taskManagerHeartbeatManager.receiveHeartbeat(resourceID, slotReport);
	}

	@Override
	public void heartbeatFromJobManager(final ResourceID resourceID) {
		jobManagerHeartbeatManager.receiveHeartbeat(resourceID, null);
	}

	@Override
	public void disconnectTaskManager(final ResourceID resourceId, final Exception cause) {
		closeTaskManagerConnection(resourceId, cause);
	}

	@Override
	public void disconnectJobManager(final JobID jobId, final Exception cause) {
		closeJobManagerConnection(jobId, cause);
	}

	@Override
	public CompletableFuture<Acknowledge> requestSlot(
			JobMasterId jobMasterId,
			SlotRequest slotRequest,
			final Time timeout) {

		JobID jobId = slotRequest.getJobId();
		JobManagerRegistration jobManagerRegistration = jobManagerRegistrations.get(jobId);

		if (null != jobManagerRegistration) {
			if (Objects.equals(jobMasterId, jobManagerRegistration.getJobMasterId())) {
				log.info("Request slot with profile {} for job {} with allocation id {}.",
					slotRequest.getResourceProfile(),
					slotRequest.getJobId(),
					slotRequest.getAllocationId());

				try {
					slotManager.registerSlotRequest(slotRequest);
				} catch (SlotManagerException e) {
					return FutureUtils.completedExceptionally(e);
				}

				return CompletableFuture.completedFuture(Acknowledge.get());
			} else {
				return FutureUtils.completedExceptionally(new ResourceManagerException("The job leader's id " +
					jobManagerRegistration.getJobMasterId() + " does not match the received id " + jobMasterId + '.'));
			}

		} else {
			return FutureUtils.completedExceptionally(new ResourceManagerException("Could not find registered job manager for job " + jobId + '.'));
		}
	}

	@Override
	public void cancelSlotRequest(AllocationID allocationID) {
		// As the slot allocations are async, it can not avoid all redundant slots, but should best effort.
		slotManager.unregisterSlotRequest(allocationID);
	}

	@Override
	public void notifySlotAvailable(
			final InstanceID instanceID,
			final SlotID slotId,
			final AllocationID allocationId) {

		final ResourceID resourceId = slotId.getResourceID();
		WorkerRegistration<WorkerType> registration = taskExecutors.get(resourceId);

		if (registration != null) {
			InstanceID registrationId = registration.getInstanceID();

			if (Objects.equals(registrationId, instanceID)) {
				slotManager.freeSlot(slotId, allocationId);
			} else {
				log.debug("Invalid registration id for slot available message. This indicates an" +
					" outdated request.");
			}
		} else {
			log.debug("Could not find registration for resource id {}. Discarding the slot available" +
				"message {}.", resourceId, slotId);
		}
	}

	/**
	 * Registers an info message listener.
	 *
	 * @param address address of infoMessage listener to register to this resource manager
	 */
	@Override
	public void registerInfoMessageListener(final String address) {
		if (infoMessageListeners.containsKey(address)) {
			log.warn("Receive a duplicate registration from info message listener on ({})", address);
		} else {
			CompletableFuture<InfoMessageListenerRpcGateway> infoMessageListenerRpcGatewayFuture = getRpcService()
				.connect(address, InfoMessageListenerRpcGateway.class);

			infoMessageListenerRpcGatewayFuture.whenCompleteAsync(
				(InfoMessageListenerRpcGateway gateway, Throwable failure) -> {
					if (failure != null) {
						log.warn("Receive a registration from unreachable info message listener on ({})", address);
					} else {
						log.info("Receive a registration from info message listener on ({})", address);
						infoMessageListeners.put(address, gateway);
					}
				},
				getMainThreadExecutor());
		}
	}

	/**
	 * Unregisters an info message listener.
	 *
	 * @param address of the  info message listener to unregister from this resource manager
	 *
	 */
	@Override
	public void unRegisterInfoMessageListener(final String address) {
		infoMessageListeners.remove(address);
	}

	/**
	 * Cleanup application and shut down cluster.
	 *
	 * @param finalStatus of the Flink application
	 * @param diagnostics diagnostics message for the Flink application or {@code null}
	 */
	@Override
	public CompletableFuture<Acknowledge> deregisterApplication(
			final ApplicationStatus finalStatus,
			@Nullable final String diagnostics) {
		log.info("Shut down cluster because application is in {}, diagnostics {}.", finalStatus, diagnostics);

		try {
			internalDeregisterApplication(finalStatus, diagnostics);
		} catch (ResourceManagerException e) {
			log.warn("Could not properly shutdown the application.", e);
		}

		return CompletableFuture.completedFuture(Acknowledge.get());
	}

	@Override
	public CompletableFuture<Integer> getNumberOfRegisteredTaskManagers() {
		return CompletableFuture.completedFuture(taskExecutors.size());
	}

	@Override
	public CompletableFuture<Collection<TaskManagerInfo>> requestTaskManagerInfo(Time timeout) {

		final ArrayList<TaskManagerInfo> taskManagerInfos = new ArrayList<>(taskExecutors.size());

		for (Map.Entry<ResourceID, WorkerRegistration<WorkerType>> taskExecutorEntry : taskExecutors.entrySet()) {
			final ResourceID resourceId = taskExecutorEntry.getKey();
			final WorkerRegistration<WorkerType> taskExecutor = taskExecutorEntry.getValue();

			taskManagerInfos.add(
				new TaskManagerInfo(
					resourceId,
					taskExecutor.getTaskExecutorGateway().getAddress(),
					taskExecutor.getDataPort(),
					taskManagerHeartbeatManager.getLastHeartbeatFrom(resourceId),
					slotManager.getNumberRegisteredSlotsOf(taskExecutor.getInstanceID()),
					slotManager.getNumberFreeSlotsOf(taskExecutor.getInstanceID()),
					taskExecutor.getHardwareDescription()));
		}

		return CompletableFuture.completedFuture(taskManagerInfos);
	}

	@Override
	public CompletableFuture<TaskManagerInfo> requestTaskManagerInfo(ResourceID resourceId, Time timeout) {

		final WorkerRegistration<WorkerType> taskExecutor = taskExecutors.get(resourceId);

		if (taskExecutor == null) {
			return FutureUtils.completedExceptionally(new UnknownTaskExecutorException(resourceId));
		} else {
			final InstanceID instanceId = taskExecutor.getInstanceID();
			final TaskManagerInfo taskManagerInfo = new TaskManagerInfo(
				resourceId,
				taskExecutor.getTaskExecutorGateway().getAddress(),
				taskExecutor.getDataPort(),
				taskManagerHeartbeatManager.getLastHeartbeatFrom(resourceId),
				slotManager.getNumberRegisteredSlotsOf(instanceId),
				slotManager.getNumberFreeSlotsOf(instanceId),
				taskExecutor.getHardwareDescription());

			return CompletableFuture.completedFuture(taskManagerInfo);
		}
	}

	@Override
	public CompletableFuture<ResourceOverview> requestResourceOverview(Time timeout) {
		final int numberSlots = slotManager.getNumberRegisteredSlots();
		final int numberFreeSlots = slotManager.getNumberFreeSlots();

		return CompletableFuture.completedFuture(
			new ResourceOverview(
				taskExecutors.size(),
				numberSlots,
				numberFreeSlots));
	}

	@Override
	public CompletableFuture<Collection<Tuple2<ResourceID, String>>> requestTaskManagerMetricQueryServicePaths(Time timeout) {
		final ArrayList<Tuple2<ResourceID, String>> metricQueryServicePaths = new ArrayList<>(taskExecutors.size());

		for (Map.Entry<ResourceID, WorkerRegistration<WorkerType>> workerRegistrationEntry : taskExecutors.entrySet()) {
			final ResourceID tmResourceId = workerRegistrationEntry.getKey();
			final WorkerRegistration<WorkerType> workerRegistration = workerRegistrationEntry.getValue();
			final String taskManagerAddress = workerRegistration.getTaskExecutorGateway().getAddress();
			final String tmMetricQueryServicePath = taskManagerAddress.substring(0, taskManagerAddress.lastIndexOf('/') + 1) +
				MetricQueryService.METRIC_QUERY_SERVICE_NAME + '_' + tmResourceId.getResourceIdString();

			metricQueryServicePaths.add(Tuple2.of(tmResourceId, tmMetricQueryServicePath));
		}

		return CompletableFuture.completedFuture(metricQueryServicePaths);
	}

	@Override
	public CompletableFuture<TransientBlobKey> requestTaskManagerFileUpload(ResourceID taskManagerId, FileType fileType, Time timeout) {
		log.debug("Request file {} upload from TaskExecutor {}.", fileType, taskManagerId);

		final WorkerRegistration<WorkerType> taskExecutor = taskExecutors.get(taskManagerId);

		if (taskExecutor == null) {
			log.debug("Requested file {} upload from unregistered TaskExecutor {}.", fileType, taskManagerId);
			return FutureUtils.completedExceptionally(new UnknownTaskExecutorException(taskManagerId));
		} else {
			return taskExecutor.getTaskExecutorGateway().requestFileUpload(fileType, timeout);
		}
	}

	// ------------------------------------------------------------------------
	//  Internal methods
	// ------------------------------------------------------------------------

	/**
	 * Registers a new JobMaster.
	 *
	 * @param jobMasterGateway to communicate with the registering JobMaster
	 * @param jobId of the job for which the JobMaster is responsible
	 * @param jobManagerAddress address of the JobMaster
	 * @param jobManagerResourceId ResourceID of the JobMaster
	 * @return RegistrationResponse
	 */
	private RegistrationResponse registerJobMasterInternal(
		final JobMasterGateway jobMasterGateway,
		JobID jobId,
		String jobManagerAddress,
		ResourceID jobManagerResourceId) {
		if (jobManagerRegistrations.containsKey(jobId)) {
			JobManagerRegistration oldJobManagerRegistration = jobManagerRegistrations.get(jobId);

			if (Objects.equals(oldJobManagerRegistration.getJobMasterId(), jobMasterGateway.getFencingToken())) {
				// same registration
				log.debug("Job manager {}@{} was already registered.", jobMasterGateway.getFencingToken(), jobManagerAddress);
			} else {
				// tell old job manager that he is no longer the job leader
				disconnectJobManager(
					oldJobManagerRegistration.getJobID(),
					new Exception("New job leader for job " + jobId + " found."));

				JobManagerRegistration jobManagerRegistration = new JobManagerRegistration(
					jobId,
					jobManagerResourceId,
					jobMasterGateway);
				jobManagerRegistrations.put(jobId, jobManagerRegistration);
				jmResourceIdRegistrations.put(jobManagerResourceId, jobManagerRegistration);
			}
		} else {
			// new registration for the job
			JobManagerRegistration jobManagerRegistration = new JobManagerRegistration(
				jobId,
				jobManagerResourceId,
				jobMasterGateway);
			jobManagerRegistrations.put(jobId, jobManagerRegistration);
			jmResourceIdRegistrations.put(jobManagerResourceId, jobManagerRegistration);
		}

		log.info("Registered job manager {}@{} for job {}.", jobMasterGateway.getFencingToken(), jobManagerAddress, jobId);

		jobManagerHeartbeatManager.monitorTarget(jobManagerResourceId, new HeartbeatTarget<Void>() {
			@Override
			public void receiveHeartbeat(ResourceID resourceID, Void payload) {
				// the ResourceManager will always send heartbeat requests to the JobManager
			}

			@Override
			public void requestHeartbeat(ResourceID resourceID, Void payload) {
				jobMasterGateway.heartbeatFromResourceManager(resourceID);
			}
		});

		return new JobMasterRegistrationSuccess(
			resourceManagerConfiguration.getHeartbeatInterval().toMilliseconds(),
			getFencingToken(),
			resourceId);
	}

	/**
	 * Registers a new TaskExecutor.
	 *
	 * @param taskExecutorGateway to communicate with the registering TaskExecutor
	 * @param taskExecutorAddress address of the TaskExecutor
	 * @param taskExecutorResourceId ResourceID of the TaskExecutor
	 * @param slotReport initial slot report from the TaskExecutor
	 * @param dataPort port used for data transfer
	 * @param hardwareDescription of the registering TaskExecutor
	 * @return RegistrationResponse
	 */
	private RegistrationResponse registerTaskExecutorInternal(
			TaskExecutorGateway taskExecutorGateway,
			String taskExecutorAddress,
			ResourceID taskExecutorResourceId,
			SlotReport slotReport,
			int dataPort,
			HardwareDescription hardwareDescription) {
		WorkerRegistration<WorkerType> oldRegistration = taskExecutors.remove(taskExecutorResourceId);
		if (oldRegistration != null) {
			// TODO :: suggest old taskExecutor to stop itself
			log.info("Replacing old registration of TaskExecutor {}.", taskExecutorResourceId);

			// remove old task manager registration from slot manager
			slotManager.unregisterTaskManager(oldRegistration.getInstanceID());
		}

		final WorkerType newWorker = workerStarted(taskExecutorResourceId);

		if (newWorker == null) {
			log.warn("Discard registration from TaskExecutor {} at ({}) because the framework did " +
				"not recognize it", taskExecutorResourceId, taskExecutorAddress);
			return new RegistrationResponse.Decline("unrecognized TaskExecutor");
		} else {
			WorkerRegistration<WorkerType> registration =
				new WorkerRegistration<>(taskExecutorGateway, newWorker, dataPort, hardwareDescription);

			taskExecutors.put(taskExecutorResourceId, registration);

			slotManager.registerTaskManager(registration, slotReport);

			taskManagerHeartbeatManager.monitorTarget(taskExecutorResourceId, new HeartbeatTarget<Void>() {
				@Override
				public void receiveHeartbeat(ResourceID resourceID, Void payload) {
					// the ResourceManager will always send heartbeat requests to the
					// TaskManager
				}

				@Override
				public void requestHeartbeat(ResourceID resourceID, Void payload) {
					taskExecutorGateway.heartbeatFromResourceManager(resourceID);
				}
			});

			return new TaskExecutorRegistrationSuccess(
				registration.getInstanceID(),
				resourceId,
				resourceManagerConfiguration.getHeartbeatInterval().toMilliseconds(),
				clusterInformation);
		}
	}

	private void clearState() {
		jobManagerRegistrations.clear();
		jmResourceIdRegistrations.clear();
		taskExecutors.clear();

		try {
			jobLeaderIdService.clear();
		} catch (Exception e) {
			onFatalError(new ResourceManagerException("Could not properly clear the job leader id service.", e));
		}
	}

	/**
	 * This method should be called by the framework once it detects that a currently registered
	 * job manager has failed.
	 *
	 * @param jobId identifying the job whose leader shall be disconnected.
	 * @param cause The exception which cause the JobManager failed.
	 */
	protected void closeJobManagerConnection(JobID jobId, Exception cause) {
		JobManagerRegistration jobManagerRegistration = jobManagerRegistrations.remove(jobId);

		if (jobManagerRegistration != null) {
			final ResourceID jobManagerResourceId = jobManagerRegistration.getJobManagerResourceID();
			final JobMasterGateway jobMasterGateway = jobManagerRegistration.getJobManagerGateway();
			final JobMasterId jobMasterId = jobManagerRegistration.getJobMasterId();

			log.info("Disconnect job manager {}@{} for job {} from the resource manager.",
				jobMasterId,
				jobMasterGateway.getAddress(),
				jobId);

			jobManagerHeartbeatManager.unmonitorTarget(jobManagerResourceId);

			jmResourceIdRegistrations.remove(jobManagerResourceId);

			// tell the job manager about the disconnect
			jobMasterGateway.disconnectResourceManager(getFencingToken(), cause);
		} else {
			log.debug("There was no registered job manager for job {}.", jobId);
		}
	}

	/**
	 * This method should be called by the framework once it detects that a currently registered
	 * task executor has failed.
	 *
	 * @param resourceID Id of the TaskManager that has failed.
	 * @param cause The exception which cause the TaskManager failed.
	 */
	protected void closeTaskManagerConnection(final ResourceID resourceID, final Exception cause) {
		taskManagerHeartbeatManager.unmonitorTarget(resourceID);

		WorkerRegistration<WorkerType> workerRegistration = taskExecutors.remove(resourceID);

		if (workerRegistration != null) {
			log.info("Closing TaskExecutor connection {} because: {}", resourceID, cause.getMessage());

			// TODO :: suggest failed task executor to stop itself
			slotManager.unregisterTaskManager(workerRegistration.getInstanceID());

			workerRegistration.getTaskExecutorGateway().disconnectResourceManager(cause);
		} else {
			log.debug("No open TaskExecutor connection {}. Ignoring close TaskExecutor connection.", resourceID);
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

	protected void jobLeaderLostLeadership(JobID jobId, JobMasterId oldJobMasterId) {
		if (jobManagerRegistrations.containsKey(jobId)) {
			JobManagerRegistration jobManagerRegistration = jobManagerRegistrations.get(jobId);

			if (Objects.equals(jobManagerRegistration.getJobMasterId(), oldJobMasterId)) {
				disconnectJobManager(jobId, new Exception("Job leader lost leadership."));
			} else {
				log.debug("Discarding job leader lost leadership, because a new job leader was found for job {}. ", jobId);
			}
		} else {
			log.debug("Discard job leader lost leadership for outdated leader {} for job {}.", oldJobMasterId, jobId);
		}
	}

	protected void releaseResource(InstanceID instanceId, Exception cause) {
		WorkerType worker = null;

		// TODO: Improve performance by having an index on the instanceId
		for (Map.Entry<ResourceID, WorkerRegistration<WorkerType>> entry : taskExecutors.entrySet()) {
			if (entry.getValue().getInstanceID().equals(instanceId)) {
				worker = entry.getValue().getWorker();
				break;
			}
		}

		if (worker != null) {
			if (stopWorker(worker)) {
				closeTaskManagerConnection(worker.getResourceID(), cause);
			} else {
				log.debug("Worker {} could not be stopped.", worker.getResourceID());
			}
		} else {
			// unregister in order to clean up potential left over state
			slotManager.unregisterTaskManager(instanceId);
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
	 *
	 * @param t The exception describing the fatal error
	 */
	protected void onFatalError(Throwable t) {
		try {
			log.error("Fatal error occurred in ResourceManager.", t);
		} catch (Throwable ignored) {}

		// The fatal error handler implementation should make sure that this call is non-blocking
		fatalErrorHandler.onFatalError(t);
	}

	// ------------------------------------------------------------------------
	//  Leader Contender
	// ------------------------------------------------------------------------

	/**
	 * Callback method when current resourceManager is granted leadership.
	 *
	 * @param newLeaderSessionID unique leadershipID
	 */
	@Override
	public void grantLeadership(final UUID newLeaderSessionID) {
		runAsyncWithoutFencing(
			() -> {
				final ResourceManagerId newResourceManagerId = new ResourceManagerId(newLeaderSessionID);

				log.info("ResourceManager {} was granted leadership with fencing token {}", getAddress(), newResourceManagerId);

				// clear the state if we've been the leader before
				if (getFencingToken() != null) {
					clearState();
				}

				setFencingToken(newResourceManagerId);

				slotManager.start(getFencingToken(), getMainThreadExecutor(), new ResourceActionsImpl());

				getRpcService().execute(
					() ->
						// confirming the leader session ID might be blocking,
						leaderElectionService.confirmLeaderSessionID(newLeaderSessionID));
			});
	}

	/**
	 * Callback method when current resourceManager loses leadership.
	 */
	@Override
	public void revokeLeadership() {
		runAsyncWithoutFencing(
			() -> {
				log.info("ResourceManager {} was revoked leadership. Clearing fencing token.", getAddress());

				clearState();

				setFencingToken(null);

				slotManager.suspend();
			});
	}

	/**
	 * Handles error occurring in the leader election service.
	 *
	 * @param exception Exception being thrown in the leader election service
	 */
	@Override
	public void handleError(final Exception exception) {
		onFatalError(new ResourceManagerException("Received an error from the LeaderElectionService.", exception));
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
	 * The framework specific code to deregister the application. This should report the
	 * application's final status and shut down the resource manager cleanly.
	 *
	 * <p>This method also needs to make sure all pending containers that are not registered
	 * yet are returned.
	 *
	 * @param finalStatus The application status to report.
	 * @param optionalDiagnostics A diagnostics message or {@code null}.
	 * @throws ResourceManagerException if the application could not be shut down.
	 */
	protected abstract void internalDeregisterApplication(
		ApplicationStatus finalStatus,
		@Nullable String optionalDiagnostics) throws ResourceManagerException;

	/**
	 * Allocates a resource using the resource profile.
	 *
	 * @param resourceProfile The resource description
	 */
	@VisibleForTesting
	public abstract void startNewWorker(ResourceProfile resourceProfile);

	/**
	 * Callback when a worker was started.
	 * @param resourceID The worker resource id
	 */
	protected abstract WorkerType workerStarted(ResourceID resourceID);

	/**
	 * Stops the given worker.
	 *
	 * @param worker The worker.
	 * @return True if the worker was stopped, otherwise false
	 */
	public abstract boolean stopWorker(WorkerType worker);

	// ------------------------------------------------------------------------
	//  Static utility classes
	// ------------------------------------------------------------------------

	private class ResourceActionsImpl implements ResourceActions {

		@Override
		public void releaseResource(InstanceID instanceId, Exception cause) {
			validateRunsInMainThread();

			ResourceManager.this.releaseResource(instanceId, cause);
		}

		@Override
		public void allocateResource(ResourceProfile resourceProfile) throws ResourceManagerException {
			validateRunsInMainThread();
			startNewWorker(resourceProfile);
		}

		@Override
		public void notifyAllocationFailure(JobID jobId, AllocationID allocationId, Exception cause) {
			validateRunsInMainThread();
			log.info("Slot request with allocation id {} for job {} failed.", allocationId, jobId, cause);
		}
	}

	private class JobLeaderIdActionsImpl implements JobLeaderIdActions {

		@Override
		public void jobLeaderLostLeadership(final JobID jobId, final JobMasterId oldJobMasterId) {
			runAsync(new Runnable() {
				@Override
				public void run() {
					ResourceManager.this.jobLeaderLostLeadership(jobId, oldJobMasterId);
				}
			});
		}

		@Override
		public void notifyJobTimeout(final JobID jobId, final UUID timeoutId) {
			runAsync(new Runnable() {
				@Override
				public void run() {
					if (jobLeaderIdService.isValidTimeout(jobId, timeoutId)) {
						removeJob(jobId);
					}
				}
			});
		}

		@Override
		public void handleError(Throwable error) {
			onFatalError(error);
		}
	}

	private class TaskManagerHeartbeatListener implements HeartbeatListener<SlotReport, Void> {

		@Override
		public void notifyHeartbeatTimeout(final ResourceID resourceID) {
			runAsync(new Runnable() {
				@Override
				public void run() {
					log.info("The heartbeat of TaskManager with id {} timed out.", resourceID);

					closeTaskManagerConnection(
							resourceID,
							new TimeoutException("The heartbeat of TaskManager with id " + resourceID + "  timed out."));
				}
			});
		}

		@Override
		public void reportPayload(final ResourceID resourceID, final SlotReport slotReport) {
			runAsync(new Runnable() {
				@Override
				public void run() {
					log.debug("Received new slot report from TaskManager {}.", resourceID);

					final WorkerRegistration<WorkerType> workerRegistration = taskExecutors.get(resourceID);

					if (workerRegistration == null) {
						log.debug("Received slot report from TaskManager {} which is no longer registered.", resourceID);
					} else {
						InstanceID instanceId = workerRegistration.getInstanceID();

						slotManager.reportSlotStatus(instanceId, slotReport);
					}
				}
			});
		}

		@Override
		public CompletableFuture<Void> retrievePayload(ResourceID resourceID) {
			return CompletableFuture.completedFuture(null);
		}
	}

	private class JobManagerHeartbeatListener implements HeartbeatListener<Void, Void> {

		@Override
		public void notifyHeartbeatTimeout(final ResourceID resourceID) {
			runAsync(new Runnable() {
				@Override
				public void run() {
					log.info("The heartbeat of JobManager with id {} timed out.", resourceID);

					if (jmResourceIdRegistrations.containsKey(resourceID)) {
						JobManagerRegistration jobManagerRegistration = jmResourceIdRegistrations.get(resourceID);

						if (jobManagerRegistration != null) {
							closeJobManagerConnection(
								jobManagerRegistration.getJobID(),
								new TimeoutException("The heartbeat of JobManager with id " + resourceID + " timed out."));
						}
					}
				}
			});
		}

		@Override
		public void reportPayload(ResourceID resourceID, Void payload) {
			// nothing to do since there is no payload
		}

		@Override
		public CompletableFuture<Void> retrievePayload(ResourceID resourceID) {
			return CompletableFuture.completedFuture(null);
		}
	}
}

