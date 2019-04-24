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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.queryablestate.KvStateID;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.accumulators.AccumulatorSnapshot;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraphBuilder;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.IntermediateResult;
import org.apache.flink.runtime.executiongraph.JobStatusListener;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategyResolving;
import org.apache.flink.runtime.heartbeat.HeartbeatListener;
import org.apache.flink.runtime.heartbeat.HeartbeatManager;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.heartbeat.HeartbeatTarget;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.jobmanager.OnCompletionActions;
import org.apache.flink.runtime.jobmanager.PartitionProducerDisposedException;
import org.apache.flink.runtime.jobmaster.factories.JobManagerJobMetricGroupFactory;
import org.apache.flink.runtime.jobmaster.slotpool.Scheduler;
import org.apache.flink.runtime.jobmaster.slotpool.SchedulerFactory;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPool;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPoolFactory;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.query.KvStateLocation;
import org.apache.flink.runtime.query.KvStateLocationRegistry;
import org.apache.flink.runtime.query.UnknownKvStateLocation;
import org.apache.flink.runtime.registration.RegisteredRpcConnection;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.registration.RetryingRegistration;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.BackPressureStatsTracker;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.OperatorBackPressureStats;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.OperatorBackPressureStatsResponse;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.FencedRpcEndpoint;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.akka.AkkaRpcServiceUtils;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.taskexecutor.AccumulatorReport;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.webmonitor.WebMonitorUtils;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * JobMaster implementation. The job master is responsible for the execution of a single
 * {@link JobGraph}.
 *
 * <p>It offers the following methods as part of its rpc interface to interact with the JobMaster
 * remotely:
 * <ul>
 * <li>{@link #updateTaskExecutionState} updates the task execution state for
 * given task</li>
 * </ul>
 */
public class JobMaster extends FencedRpcEndpoint<JobMasterId> implements JobMasterGateway, JobMasterService {

	/** Default names for Flink's distributed components. */
	public static final String JOB_MANAGER_NAME = "jobmanager";
	public static final String ARCHIVE_NAME = "archive";

	// ------------------------------------------------------------------------

	private final JobMasterConfiguration jobMasterConfiguration;

	private final ResourceID resourceId;

	private final JobGraph jobGraph;

	private final Time rpcTimeout;

	private final HighAvailabilityServices highAvailabilityServices;

	private final BlobWriter blobWriter;

	private final JobManagerJobMetricGroupFactory jobMetricGroupFactory;

	private final HeartbeatManager<AccumulatorReport, Void> taskManagerHeartbeatManager;

	private final HeartbeatManager<Void, Void> resourceManagerHeartbeatManager;

	private final ScheduledExecutorService scheduledExecutorService;

	private final OnCompletionActions jobCompletionActions;

	private final FatalErrorHandler fatalErrorHandler;

	private final ClassLoader userCodeLoader;

	private final SlotPool slotPool;

	private final Scheduler scheduler;

	private final RestartStrategy restartStrategy;

	// --------- BackPressure --------

	private final BackPressureStatsTracker backPressureStatsTracker;

	// --------- ResourceManager --------

	private final LeaderRetrievalService resourceManagerLeaderRetriever;

	// --------- TaskManagers --------

	private final Map<ResourceID, Tuple2<TaskManagerLocation, TaskExecutorGateway>> registeredTaskManagers;

	// -------- Mutable fields ---------

	private ExecutionGraph executionGraph;

	@Nullable
	private JobManagerJobStatusListener jobStatusListener;

	@Nullable
	private JobManagerJobMetricGroup jobManagerJobMetricGroup;

	@Nullable
	private ResourceManagerAddress resourceManagerAddress;

	@Nullable
	private ResourceManagerConnection resourceManagerConnection;

	@Nullable
	private EstablishedResourceManagerConnection establishedResourceManagerConnection;

	private Map<String, Object> accumulators;

	// ------------------------------------------------------------------------

	public JobMaster(
			RpcService rpcService,
			JobMasterConfiguration jobMasterConfiguration,
			ResourceID resourceId,
			JobGraph jobGraph,
			HighAvailabilityServices highAvailabilityService,
			SlotPoolFactory slotPoolFactory,
			SchedulerFactory schedulerFactory,
			JobManagerSharedServices jobManagerSharedServices,
			HeartbeatServices heartbeatServices,
			JobManagerJobMetricGroupFactory jobMetricGroupFactory,
			OnCompletionActions jobCompletionActions,
			FatalErrorHandler fatalErrorHandler,
			ClassLoader userCodeLoader) throws Exception {

		super(rpcService, AkkaRpcServiceUtils.createRandomName(JOB_MANAGER_NAME));

		final JobMasterGateway selfGateway = getSelfGateway(JobMasterGateway.class);

		this.jobMasterConfiguration = checkNotNull(jobMasterConfiguration);
		this.resourceId = checkNotNull(resourceId);
		this.jobGraph = checkNotNull(jobGraph);
		this.rpcTimeout = jobMasterConfiguration.getRpcTimeout();
		this.highAvailabilityServices = checkNotNull(highAvailabilityService);
		this.blobWriter = jobManagerSharedServices.getBlobWriter();
		this.scheduledExecutorService = jobManagerSharedServices.getScheduledExecutorService();
		this.jobCompletionActions = checkNotNull(jobCompletionActions);
		this.fatalErrorHandler = checkNotNull(fatalErrorHandler);
		this.userCodeLoader = checkNotNull(userCodeLoader);
		this.jobMetricGroupFactory = checkNotNull(jobMetricGroupFactory);

		this.taskManagerHeartbeatManager = heartbeatServices.createHeartbeatManagerSender(
			resourceId,
			new TaskManagerHeartbeatListener(selfGateway),
			rpcService.getScheduledExecutor(),
			log);

		this.resourceManagerHeartbeatManager = heartbeatServices.createHeartbeatManager(
				resourceId,
				new ResourceManagerHeartbeatListener(),
				rpcService.getScheduledExecutor(),
				log);

		final String jobName = jobGraph.getName();
		final JobID jid = jobGraph.getJobID();

		log.info("Initializing job {} ({}).", jobName, jid);

		final RestartStrategies.RestartStrategyConfiguration restartStrategyConfiguration =
				jobGraph.getSerializedExecutionConfig()
						.deserializeValue(userCodeLoader)
						.getRestartStrategy();

		this.restartStrategy = RestartStrategyResolving.resolve(restartStrategyConfiguration,
			jobManagerSharedServices.getRestartStrategyFactory(),
			jobGraph.isCheckpointingEnabled());

		log.info("Using restart strategy {} for {} ({}).", this.restartStrategy, jobName, jid);

		resourceManagerLeaderRetriever = highAvailabilityServices.getResourceManagerLeaderRetriever();

		this.slotPool = checkNotNull(slotPoolFactory).createSlotPool(jobGraph.getJobID());

		this.scheduler = checkNotNull(schedulerFactory).createScheduler(slotPool);

		this.registeredTaskManagers = new HashMap<>(4);

		this.backPressureStatsTracker = checkNotNull(jobManagerSharedServices.getBackPressureStatsTracker());

		this.jobManagerJobMetricGroup = jobMetricGroupFactory.create(jobGraph);
		this.executionGraph = createAndRestoreExecutionGraph(jobManagerJobMetricGroup);
		this.jobStatusListener = null;

		this.resourceManagerConnection = null;
		this.establishedResourceManagerConnection = null;

		this.accumulators = new HashMap<>();
	}

	//----------------------------------------------------------------------------------------------
	// Lifecycle management
	//----------------------------------------------------------------------------------------------

	/**
	 * Start the rpc service and begin to run the job.
	 *
	 * @param newJobMasterId The necessary fencing token to run the job
	 * @return Future acknowledge if the job could be started. Otherwise the future contains an exception
	 */
	public CompletableFuture<Acknowledge> start(final JobMasterId newJobMasterId) throws Exception {
		// make sure we receive RPC and async calls
		start();

		return callAsyncWithoutFencing(() -> startJobExecution(newJobMasterId), RpcUtils.INF_TIMEOUT);
	}

	/**
	 * Suspending job, all the running tasks will be cancelled, and communication with other components
	 * will be disposed.
	 *
	 * <p>Mostly job is suspended because of the leadership has been revoked, one can be restart this job by
	 * calling the {@link #start(JobMasterId)} method once we take the leadership back again.
	 *
	 * <p>This method is executed asynchronously
	 *
	 * @param cause The reason of why this job been suspended.
	 * @return Future acknowledge indicating that the job has been suspended. Otherwise the future contains an exception
	 */
	public CompletableFuture<Acknowledge> suspend(final Exception cause) {
		CompletableFuture<Acknowledge> suspendFuture = callAsyncWithoutFencing(
				() -> suspendExecution(cause),
				RpcUtils.INF_TIMEOUT);

		return suspendFuture.whenComplete((acknowledge, throwable) -> stop());
	}

	/**
	 * Suspend the job and shutdown all other services including rpc.
	 */
	@Override
	public CompletableFuture<Void> onStop() {
		log.info("Stopping the JobMaster for job {}({}).", jobGraph.getName(), jobGraph.getJobID());

		// disconnect from all registered TaskExecutors
		final Set<ResourceID> taskManagerResourceIds = new HashSet<>(registeredTaskManagers.keySet());
		final FlinkException cause = new FlinkException("Stopping JobMaster for job " + jobGraph.getName() +
			'(' + jobGraph.getJobID() + ").");

		for (ResourceID taskManagerResourceId : taskManagerResourceIds) {
			disconnectTaskManager(taskManagerResourceId, cause);
		}

		taskManagerHeartbeatManager.stop();
		resourceManagerHeartbeatManager.stop();

		// make sure there is a graceful exit
		suspendExecution(new FlinkException("JobManager is shutting down."));

		// shut down will internally release all registered slots
		slotPool.close();

		return CompletableFuture.completedFuture(null);
	}

	//----------------------------------------------------------------------------------------------
	// RPC methods
	//----------------------------------------------------------------------------------------------

	@Override
	public CompletableFuture<Acknowledge> cancel(Time timeout) {
		executionGraph.cancel();

		return CompletableFuture.completedFuture(Acknowledge.get());
	}

	/**
	 * Updates the task execution state for a given task.
	 *
	 * @param taskExecutionState New task execution state for a given task
	 * @return Acknowledge the task execution state update
	 */
	@Override
	public CompletableFuture<Acknowledge> updateTaskExecutionState(
			final TaskExecutionState taskExecutionState) {
		checkNotNull(taskExecutionState, "taskExecutionState");

		if (executionGraph.updateState(taskExecutionState)) {
			return CompletableFuture.completedFuture(Acknowledge.get());
		} else {
			return FutureUtils.completedExceptionally(
				new ExecutionGraphException("The execution attempt " +
					taskExecutionState.getID() + " was not found."));
		}
	}

	@Override
	public CompletableFuture<SerializedInputSplit> requestNextInputSplit(
			final JobVertexID vertexID,
			final ExecutionAttemptID executionAttempt) {

		final Execution execution = executionGraph.getRegisteredExecutions().get(executionAttempt);
		if (execution == null) {
			// can happen when JobManager had already unregistered this execution upon on task failure,
			// but TaskManager get some delay to aware of that situation
			if (log.isDebugEnabled()) {
				log.debug("Can not find Execution for attempt {}.", executionAttempt);
			}
			// but we should TaskManager be aware of this
			return FutureUtils.completedExceptionally(new Exception("Can not find Execution for attempt " + executionAttempt));
		}

		final ExecutionJobVertex vertex = executionGraph.getJobVertex(vertexID);
		if (vertex == null) {
			log.error("Cannot find execution vertex for vertex ID {}.", vertexID);
			return FutureUtils.completedExceptionally(new Exception("Cannot find execution vertex for vertex ID " + vertexID));
		}

		if (vertex.getSplitAssigner() == null) {
			log.error("No InputSplitAssigner for vertex ID {}.", vertexID);
			return FutureUtils.completedExceptionally(new Exception("No InputSplitAssigner for vertex ID " + vertexID));
		}

		final InputSplit nextInputSplit = execution.getNextInputSplit();

		if (log.isDebugEnabled()) {
			log.debug("Send next input split {}.", nextInputSplit);
		}

		try {
			final byte[] serializedInputSplit = InstantiationUtil.serializeObject(nextInputSplit);
			return CompletableFuture.completedFuture(new SerializedInputSplit(serializedInputSplit));
		} catch (Exception ex) {
			log.error("Could not serialize the next input split of class {}.", nextInputSplit.getClass(), ex);
			IOException reason = new IOException("Could not serialize the next input split of class " +
					nextInputSplit.getClass() + ".", ex);
			vertex.fail(reason);
			return FutureUtils.completedExceptionally(reason);
		}
	}

	@Override
	public CompletableFuture<ExecutionState> requestPartitionState(
			final IntermediateDataSetID intermediateResultId,
			final ResultPartitionID resultPartitionId) {

		final Execution execution = executionGraph.getRegisteredExecutions().get(resultPartitionId.getProducerId());
		if (execution != null) {
			return CompletableFuture.completedFuture(execution.getState());
		}
		else {
			final IntermediateResult intermediateResult =
					executionGraph.getAllIntermediateResults().get(intermediateResultId);

			if (intermediateResult != null) {
				// Try to find the producing execution
				Execution producerExecution = intermediateResult
						.getPartitionById(resultPartitionId.getPartitionId())
						.getProducer()
						.getCurrentExecutionAttempt();

				if (producerExecution.getAttemptId().equals(resultPartitionId.getProducerId())) {
					return CompletableFuture.completedFuture(producerExecution.getState());
				} else {
					return FutureUtils.completedExceptionally(new PartitionProducerDisposedException(resultPartitionId));
				}
			} else {
				return FutureUtils.completedExceptionally(new IllegalArgumentException("Intermediate data set with ID "
						+ intermediateResultId + " not found."));
			}
		}
	}

	@Override
	public CompletableFuture<Acknowledge> scheduleOrUpdateConsumers(
			final ResultPartitionID partitionID,
			final Time timeout) {
		try {
			executionGraph.scheduleOrUpdateConsumers(partitionID);
			return CompletableFuture.completedFuture(Acknowledge.get());
		} catch (Exception e) {
			return FutureUtils.completedExceptionally(e);
		}
	}

	@Override
	public CompletableFuture<Acknowledge> disconnectTaskManager(final ResourceID resourceID, final Exception cause) {
		log.debug("Disconnect TaskExecutor {} because: {}", resourceID, cause.getMessage());

		taskManagerHeartbeatManager.unmonitorTarget(resourceID);
		slotPool.releaseTaskManager(resourceID, cause);

		Tuple2<TaskManagerLocation, TaskExecutorGateway> taskManagerConnection = registeredTaskManagers.remove(resourceID);

		if (taskManagerConnection != null) {
			taskManagerConnection.f1.disconnectJobManager(jobGraph.getJobID(), cause);
		}

		return CompletableFuture.completedFuture(Acknowledge.get());
	}

	// TODO: This method needs a leader session ID
	@Override
	public void acknowledgeCheckpoint(
			final JobID jobID,
			final ExecutionAttemptID executionAttemptID,
			final long checkpointId,
			final CheckpointMetrics checkpointMetrics,
			final TaskStateSnapshot checkpointState) {

		final CheckpointCoordinator checkpointCoordinator = executionGraph.getCheckpointCoordinator();
		final AcknowledgeCheckpoint ackMessage = new AcknowledgeCheckpoint(
			jobID,
			executionAttemptID,
			checkpointId,
			checkpointMetrics,
			checkpointState);

		if (checkpointCoordinator != null) {
			getRpcService().execute(() -> {
				try {
					checkpointCoordinator.receiveAcknowledgeMessage(ackMessage);
				} catch (Throwable t) {
					log.warn("Error while processing checkpoint acknowledgement message", t);
				}
			});
		} else {
			String errorMessage = "Received AcknowledgeCheckpoint message for job {} with no CheckpointCoordinator";
			if (executionGraph.getState() == JobStatus.RUNNING) {
				log.error(errorMessage, jobGraph.getJobID());
			} else {
				log.debug(errorMessage, jobGraph.getJobID());
			}
		}
	}

	// TODO: This method needs a leader session ID
	@Override
	public void declineCheckpoint(DeclineCheckpoint decline) {
		final CheckpointCoordinator checkpointCoordinator = executionGraph.getCheckpointCoordinator();

		if (checkpointCoordinator != null) {
			getRpcService().execute(() -> {
				try {
					checkpointCoordinator.receiveDeclineMessage(decline);
				} catch (Exception e) {
					log.error("Error in CheckpointCoordinator while processing {}", decline, e);
				}
			});
		} else {
			String errorMessage = "Received DeclineCheckpoint message for job {} with no CheckpointCoordinator";
			if (executionGraph.getState() == JobStatus.RUNNING) {
				log.error(errorMessage, jobGraph.getJobID());
			} else {
				log.debug(errorMessage, jobGraph.getJobID());
			}
		}
	}

	@Override
	public CompletableFuture<KvStateLocation> requestKvStateLocation(final JobID jobId, final String registrationName) {
		// sanity check for the correct JobID
		if (jobGraph.getJobID().equals(jobId)) {
			if (log.isDebugEnabled()) {
				log.debug("Lookup key-value state for job {} with registration " +
					"name {}.", jobGraph.getJobID(), registrationName);
			}

			final KvStateLocationRegistry registry = executionGraph.getKvStateLocationRegistry();
			final KvStateLocation location = registry.getKvStateLocation(registrationName);
			if (location != null) {
				return CompletableFuture.completedFuture(location);
			} else {
				return FutureUtils.completedExceptionally(new UnknownKvStateLocation(registrationName));
			}
		} else {
			if (log.isDebugEnabled()) {
				log.debug("Request of key-value state location for unknown job {} received.", jobId);
			}
			return FutureUtils.completedExceptionally(new FlinkJobNotFoundException(jobId));
		}
	}

	@Override
	public CompletableFuture<Acknowledge> notifyKvStateRegistered(
			final JobID jobId,
			final JobVertexID jobVertexId,
			final KeyGroupRange keyGroupRange,
			final String registrationName,
			final KvStateID kvStateId,
			final InetSocketAddress kvStateServerAddress) {
		if (jobGraph.getJobID().equals(jobId)) {
			if (log.isDebugEnabled()) {
				log.debug("Key value state registered for job {} under name {}.",
					jobGraph.getJobID(), registrationName);
			}

			try {
				executionGraph.getKvStateLocationRegistry().notifyKvStateRegistered(
					jobVertexId, keyGroupRange, registrationName, kvStateId, kvStateServerAddress);

				return CompletableFuture.completedFuture(Acknowledge.get());
			} catch (Exception e) {
				log.error("Failed to notify KvStateRegistry about registration {}.", registrationName, e);
				return FutureUtils.completedExceptionally(e);
			}
		} else {
			if (log.isDebugEnabled()) {
				log.debug("Notification about key-value state registration for unknown job {} received.", jobId);
			}
			return FutureUtils.completedExceptionally(new FlinkJobNotFoundException(jobId));
		}
	}

	@Override
	public CompletableFuture<Acknowledge> notifyKvStateUnregistered(
			JobID jobId,
			JobVertexID jobVertexId,
			KeyGroupRange keyGroupRange,
			String registrationName) {
		if (jobGraph.getJobID().equals(jobId)) {
			if (log.isDebugEnabled()) {
				log.debug("Key value state unregistered for job {} under name {}.",
					jobGraph.getJobID(), registrationName);
			}

			try {
				executionGraph.getKvStateLocationRegistry().notifyKvStateUnregistered(
					jobVertexId, keyGroupRange, registrationName);

				return CompletableFuture.completedFuture(Acknowledge.get());
			} catch (Exception e) {
				log.error("Failed to notify KvStateRegistry about unregistration {}.", registrationName, e);
				return FutureUtils.completedExceptionally(e);
			}
		} else {
			if (log.isDebugEnabled()) {
				log.debug("Notification about key-value state deregistration for unknown job {} received.", jobId);
			}
			return FutureUtils.completedExceptionally(new FlinkJobNotFoundException(jobId));
		}
	}

	@Override
	public CompletableFuture<Collection<SlotOffer>> offerSlots(
			final ResourceID taskManagerId,
			final Collection<SlotOffer> slots,
			final Time timeout) {

		Tuple2<TaskManagerLocation, TaskExecutorGateway> taskManager = registeredTaskManagers.get(taskManagerId);

		if (taskManager == null) {
			return FutureUtils.completedExceptionally(new Exception("Unknown TaskManager " + taskManagerId));
		}

		final TaskManagerLocation taskManagerLocation = taskManager.f0;
		final TaskExecutorGateway taskExecutorGateway = taskManager.f1;

		final RpcTaskManagerGateway rpcTaskManagerGateway = new RpcTaskManagerGateway(taskExecutorGateway, getFencingToken());

		return CompletableFuture.completedFuture(
			slotPool.offerSlots(
				taskManagerLocation,
				rpcTaskManagerGateway,
				slots));
	}

	@Override
	public void failSlot(
			final ResourceID taskManagerId,
			final AllocationID allocationId,
			final Exception cause) {

		if (registeredTaskManagers.containsKey(taskManagerId)) {
			internalFailAllocation(allocationId, cause);
		} else {
			log.warn("Cannot fail slot " + allocationId + " because the TaskManager " +
			taskManagerId + " is unknown.");
		}
	}

	private void internalFailAllocation(AllocationID allocationId, Exception cause) {
		final Optional<ResourceID> resourceIdOptional = slotPool.failAllocation(allocationId, cause);
		resourceIdOptional.ifPresent(this::releaseEmptyTaskManager);
	}

	private void releaseEmptyTaskManager(ResourceID resourceId) {
		disconnectTaskManager(resourceId, new FlinkException(String.format("No more slots registered at JobMaster %s.", resourceId)));
	}

	@Override
	public CompletableFuture<RegistrationResponse> registerTaskManager(
			final String taskManagerRpcAddress,
			final TaskManagerLocation taskManagerLocation,
			final Time timeout) {

		final ResourceID taskManagerId = taskManagerLocation.getResourceID();

		if (registeredTaskManagers.containsKey(taskManagerId)) {
			final RegistrationResponse response = new JMTMRegistrationSuccess(resourceId);
			return CompletableFuture.completedFuture(response);
		} else {
			return getRpcService()
				.connect(taskManagerRpcAddress, TaskExecutorGateway.class)
				.handleAsync(
					(TaskExecutorGateway taskExecutorGateway, Throwable throwable) -> {
						if (throwable != null) {
							return new RegistrationResponse.Decline(throwable.getMessage());
						}

						slotPool.registerTaskManager(taskManagerId);
						registeredTaskManagers.put(taskManagerId, Tuple2.of(taskManagerLocation, taskExecutorGateway));

						// monitor the task manager as heartbeat target
						taskManagerHeartbeatManager.monitorTarget(taskManagerId, new HeartbeatTarget<Void>() {
							@Override
							public void receiveHeartbeat(ResourceID resourceID, Void payload) {
								// the task manager will not request heartbeat, so this method will never be called currently
							}

							@Override
							public void requestHeartbeat(ResourceID resourceID, Void payload) {
								taskExecutorGateway.heartbeatFromJobManager(resourceID);
							}
						});

						return new JMTMRegistrationSuccess(resourceId);
					},
					getMainThreadExecutor());
		}
	}

	@Override
	public void disconnectResourceManager(
			final ResourceManagerId resourceManagerId,
			final Exception cause) {

		if (isConnectingToResourceManager(resourceManagerId)) {
			reconnectToResourceManager(cause);
		}
	}

	private boolean isConnectingToResourceManager(ResourceManagerId resourceManagerId) {
		return resourceManagerAddress != null
				&& resourceManagerAddress.getResourceManagerId().equals(resourceManagerId);
	}

	@Override
	public void heartbeatFromTaskManager(final ResourceID resourceID, AccumulatorReport accumulatorReport) {
		taskManagerHeartbeatManager.receiveHeartbeat(resourceID, accumulatorReport);
	}

	@Override
	public void heartbeatFromResourceManager(final ResourceID resourceID) {
		resourceManagerHeartbeatManager.requestHeartbeat(resourceID, null);
	}

	@Override
	public CompletableFuture<JobDetails> requestJobDetails(Time timeout) {
		final ExecutionGraph currentExecutionGraph = executionGraph;
		return CompletableFuture.supplyAsync(() -> WebMonitorUtils.createDetailsForJob(currentExecutionGraph), scheduledExecutorService);
	}

	@Override
	public CompletableFuture<JobStatus> requestJobStatus(Time timeout) {
		return CompletableFuture.completedFuture(executionGraph.getState());
	}

	@Override
	public CompletableFuture<ArchivedExecutionGraph> requestJob(Time timeout) {
		return CompletableFuture.completedFuture(ArchivedExecutionGraph.createFrom(executionGraph));
	}

	@Override
	public CompletableFuture<String> triggerSavepoint(
			@Nullable final String targetDirectory,
			final boolean cancelJob,
			final Time timeout) {

		final CheckpointCoordinator checkpointCoordinator = executionGraph.getCheckpointCoordinator();
		if (checkpointCoordinator == null) {
			return FutureUtils.completedExceptionally(new IllegalStateException(
				String.format("Job %s is not a streaming job.", jobGraph.getJobID())));
		} else if (targetDirectory == null && !checkpointCoordinator.getCheckpointStorage().hasDefaultSavepointLocation()) {
			log.info("Trying to cancel job {} with savepoint, but no savepoint directory configured.", jobGraph.getJobID());

			return FutureUtils.completedExceptionally(new IllegalStateException(
				"No savepoint directory configured. You can either specify a directory " +
					"while cancelling via -s :targetDirectory or configure a cluster-wide " +
					"default via key '" + CheckpointingOptions.SAVEPOINT_DIRECTORY.key() + "'."));
		}

		if (cancelJob) {
			checkpointCoordinator.stopCheckpointScheduler();
		}
		return checkpointCoordinator
			.triggerSavepoint(System.currentTimeMillis(), targetDirectory)
			.thenApply(CompletedCheckpoint::getExternalPointer)
			.handleAsync((path, throwable) -> {
				if (throwable != null) {
					if (cancelJob) {
						startCheckpointScheduler(checkpointCoordinator);
					}
					throw new CompletionException(throwable);
				} else if (cancelJob) {
					log.info("Savepoint stored in {}. Now cancelling {}.", path, jobGraph.getJobID());
					cancel(timeout);
				}
				return path;
			}, getMainThreadExecutor());
	}

	@Override
	public CompletableFuture<String> stopWithSavepoint(
			@Nullable final String targetDirectory,
			final boolean advanceToEndOfEventTime,
			final Time timeout) {

		final CheckpointCoordinator checkpointCoordinator = executionGraph.getCheckpointCoordinator();

		if (checkpointCoordinator == null) {
			return FutureUtils.completedExceptionally(new IllegalStateException(
					String.format("Job %s is not a streaming job.", jobGraph.getJobID())));
		}

		if (targetDirectory == null && !checkpointCoordinator.getCheckpointStorage().hasDefaultSavepointLocation()) {
			log.info("Trying to cancel job {} with savepoint, but no savepoint directory configured.", jobGraph.getJobID());

			return FutureUtils.completedExceptionally(new IllegalStateException(
					"No savepoint directory configured. You can either specify a directory " +
							"while cancelling via -s :targetDirectory or configure a cluster-wide " +
							"default via key '" + CheckpointingOptions.SAVEPOINT_DIRECTORY.key() + "'."));
		}

		final long now = System.currentTimeMillis();

		// we stop the checkpoint coordinator so that we are guaranteed
		// to have only the data of the synchronous savepoint committed.
		// in case of failure, and if the job restarts, the coordinator
		// will be restarted by the CheckpointCoordinatorDeActivator.
		checkpointCoordinator.stopCheckpointScheduler();

		final CompletableFuture<String> savepointFuture = checkpointCoordinator
				.triggerSynchronousSavepoint(now, advanceToEndOfEventTime, targetDirectory)
				.handleAsync((completedCheckpoint, throwable) -> {
					if (throwable != null) {
						log.info("Failed during stopping job {} with a savepoint. Reason: {}", jobGraph.getJobID(), throwable.getMessage());
						throw new CompletionException(throwable);
					}
					return completedCheckpoint.getExternalPointer();
				}, getMainThreadExecutor());

		final CompletableFuture<JobStatus> terminationFuture = executionGraph
				.getTerminationFuture()
				.handleAsync((jobstatus, throwable) -> {

					if (throwable != null) {
						log.info("Failed during stopping job {} with a savepoint. Reason: {}", jobGraph.getJobID(), throwable.getMessage());
						throw new CompletionException(throwable);
					} else if(jobstatus != JobStatus.FINISHED) {
						log.info("Failed during stopping job {} with a savepoint. Reason: Reached state {} instead of FINISHED.", jobGraph.getJobID(), jobstatus);
						throw new CompletionException(new FlinkException("Reached state " + jobstatus + " instead of FINISHED."));
					}
					return jobstatus;
				}, getMainThreadExecutor());

		return savepointFuture.thenCompose((path) ->
			terminationFuture.thenApply((jobStatus -> path)));
	}

	private void startCheckpointScheduler(final CheckpointCoordinator checkpointCoordinator) {
		if (checkpointCoordinator.isPeriodicCheckpointingConfigured()) {
			try {
				checkpointCoordinator.startCheckpointScheduler();
			} catch (IllegalStateException ignored) {
				// Concurrent shut down of the coordinator
			}
		}
	}

	@Override
	public CompletableFuture<OperatorBackPressureStatsResponse> requestOperatorBackPressureStats(final JobVertexID jobVertexId) {
		final ExecutionJobVertex jobVertex = executionGraph.getJobVertex(jobVertexId);
		if (jobVertex == null) {
			return FutureUtils.completedExceptionally(new FlinkException("JobVertexID not found " +
				jobVertexId));
		}

		final Optional<OperatorBackPressureStats> operatorBackPressureStats =
			backPressureStatsTracker.getOperatorBackPressureStats(jobVertex);
		return CompletableFuture.completedFuture(OperatorBackPressureStatsResponse.of(
			operatorBackPressureStats.orElse(null)));
	}

	@Override
	public void notifyAllocationFailure(AllocationID allocationID, Exception cause) {
		internalFailAllocation(allocationID, cause);
	}

	@Override
	public CompletableFuture<Object> updateGlobalAggregate(String aggregateName, Object aggregand, byte[] serializedAggregateFunction) {

		AggregateFunction aggregateFunction = null;
		try {
			aggregateFunction = InstantiationUtil.deserializeObject(serializedAggregateFunction, userCodeLoader);
		} catch (Exception e) {
			log.error("Error while attempting to deserialize user AggregateFunction.");
			return FutureUtils.completedExceptionally(e);
		}

		Object accumulator = accumulators.get(aggregateName);
		if(null == accumulator) {
			accumulator = aggregateFunction.createAccumulator();
		}
		accumulator = aggregateFunction.add(aggregand, accumulator);
		accumulators.put(aggregateName, accumulator);
		return CompletableFuture.completedFuture(aggregateFunction.getResult(accumulator));
	}

	//----------------------------------------------------------------------------------------------
	// Internal methods
	//----------------------------------------------------------------------------------------------

	//-- job starting and stopping  -----------------------------------------------------------------

	private Acknowledge startJobExecution(JobMasterId newJobMasterId) throws Exception {

		validateRunsInMainThread();

		checkNotNull(newJobMasterId, "The new JobMasterId must not be null.");

		if (Objects.equals(getFencingToken(), newJobMasterId)) {
			log.info("Already started the job execution with JobMasterId {}.", newJobMasterId);

			return Acknowledge.get();
		}

		setNewFencingToken(newJobMasterId);

		startJobMasterServices();

		log.info("Starting execution of job {} ({}) under job master id {}.", jobGraph.getName(), jobGraph.getJobID(), newJobMasterId);

		resetAndScheduleExecutionGraph();

		return Acknowledge.get();
	}

	private void startJobMasterServices() throws Exception {
		// start the slot pool make sure the slot pool now accepts messages for this leader
		slotPool.start(getFencingToken(), getAddress(), getMainThreadExecutor());
		scheduler.start(getMainThreadExecutor());

		//TODO: Remove once the ZooKeeperLeaderRetrieval returns the stored address upon start
		// try to reconnect to previously known leader
		reconnectToResourceManager(new FlinkException("Starting JobMaster component."));

		// job is ready to go, try to establish connection with resource manager
		//   - activate leader retrieval for the resource manager
		//   - on notification of the leader, the connection will be established and
		//     the slot pool will start requesting slots
		resourceManagerLeaderRetriever.start(new ResourceManagerLeaderListener());
	}

	private void setNewFencingToken(JobMasterId newJobMasterId) {
		if (getFencingToken() != null) {
			log.info("Restarting old job with JobMasterId {}. The new JobMasterId is {}.", getFencingToken(), newJobMasterId);

			// first we have to suspend the current execution
			suspendExecution(new FlinkException("Old job with JobMasterId " + getFencingToken() +
				" is restarted with a new JobMasterId " + newJobMasterId + '.'));
		}

		// set new leader id
		setFencingToken(newJobMasterId);
	}

	/**
	 * Suspending job, all the running tasks will be cancelled, and communication with other components
	 * will be disposed.
	 *
	 * <p>Mostly job is suspended because of the leadership has been revoked, one can be restart this job by
	 * calling the {@link #start(JobMasterId)} method once we take the leadership back again.
	 *
	 * @param cause The reason of why this job been suspended.
	 */
	private Acknowledge suspendExecution(final Exception cause) {
		validateRunsInMainThread();

		if (getFencingToken() == null) {
			log.debug("Job has already been suspended or shutdown.");
			return Acknowledge.get();
		}

		// not leader anymore --> set the JobMasterId to null
		setFencingToken(null);

		try {
			resourceManagerLeaderRetriever.stop();
			resourceManagerAddress = null;
		} catch (Throwable t) {
			log.warn("Failed to stop resource manager leader retriever when suspending.", t);
		}

		suspendAndClearExecutionGraphFields(cause);

		// the slot pool stops receiving messages and clears its pooled slots
		slotPool.suspend();

		// disconnect from resource manager:
		closeResourceManagerConnection(cause);

		return Acknowledge.get();
	}

	private void assignExecutionGraph(
			ExecutionGraph newExecutionGraph,
			JobManagerJobMetricGroup newJobManagerJobMetricGroup) {
		validateRunsInMainThread();
		checkState(executionGraph.getState().isTerminalState());
		checkState(jobManagerJobMetricGroup == null);

		executionGraph = newExecutionGraph;
		jobManagerJobMetricGroup = newJobManagerJobMetricGroup;
	}

	private void resetAndScheduleExecutionGraph() throws Exception {
		validateRunsInMainThread();

		final CompletableFuture<Void> executionGraphAssignedFuture;

		if (executionGraph.getState() == JobStatus.CREATED) {
			executionGraphAssignedFuture = CompletableFuture.completedFuture(null);
			executionGraph.start(getMainThreadExecutor());
		} else {
			suspendAndClearExecutionGraphFields(new FlinkException("ExecutionGraph is being reset in order to be rescheduled."));
			final JobManagerJobMetricGroup newJobManagerJobMetricGroup = jobMetricGroupFactory.create(jobGraph);
			final ExecutionGraph newExecutionGraph = createAndRestoreExecutionGraph(newJobManagerJobMetricGroup);

			executionGraphAssignedFuture = executionGraph.getTerminationFuture().handle(
				(JobStatus ignored, Throwable throwable) -> {
					newExecutionGraph.start(getMainThreadExecutor());
					assignExecutionGraph(newExecutionGraph, newJobManagerJobMetricGroup);
					return null;
				});
		}

		executionGraphAssignedFuture.thenRun(this::scheduleExecutionGraph);
	}

	private void scheduleExecutionGraph() {
		checkState(jobStatusListener == null);
		// register self as job status change listener
		jobStatusListener = new JobManagerJobStatusListener();
		executionGraph.registerJobStatusListener(jobStatusListener);

		try {
			executionGraph.scheduleForExecution();
		}
		catch (Throwable t) {
			executionGraph.failGlobal(t);
		}
	}

	private ExecutionGraph createAndRestoreExecutionGraph(JobManagerJobMetricGroup currentJobManagerJobMetricGroup) throws Exception {

		ExecutionGraph newExecutionGraph = createExecutionGraph(currentJobManagerJobMetricGroup);

		final CheckpointCoordinator checkpointCoordinator = newExecutionGraph.getCheckpointCoordinator();

		if (checkpointCoordinator != null) {
			// check whether we find a valid checkpoint
			if (!checkpointCoordinator.restoreLatestCheckpointedState(
				newExecutionGraph.getAllVertices(),
				false,
				false)) {

				// check whether we can restore from a savepoint
				tryRestoreExecutionGraphFromSavepoint(newExecutionGraph, jobGraph.getSavepointRestoreSettings());
			}
		}

		return newExecutionGraph;
	}

	private ExecutionGraph createExecutionGraph(JobManagerJobMetricGroup currentJobManagerJobMetricGroup) throws JobExecutionException, JobException {
		return ExecutionGraphBuilder.buildGraph(
			null,
			jobGraph,
			jobMasterConfiguration.getConfiguration(),
			scheduledExecutorService,
			scheduledExecutorService,
			scheduler,
			userCodeLoader,
			highAvailabilityServices.getCheckpointRecoveryFactory(),
			rpcTimeout,
			restartStrategy,
			currentJobManagerJobMetricGroup,
			blobWriter,
			jobMasterConfiguration.getSlotRequestTimeout(),
			log);
	}

	private void suspendAndClearExecutionGraphFields(Exception cause) {
		suspendExecutionGraph(cause);
		clearExecutionGraphFields();
	}

	private void suspendExecutionGraph(Exception cause) {
		executionGraph.suspend(cause);

		if (jobManagerJobMetricGroup != null) {
			jobManagerJobMetricGroup.close();
		}

		if (jobStatusListener != null) {
			jobStatusListener.stop();
		}
	}

	private void clearExecutionGraphFields() {
		jobManagerJobMetricGroup = null;
		jobStatusListener = null;
	}

	/**
	 * Tries to restore the given {@link ExecutionGraph} from the provided {@link SavepointRestoreSettings}.
	 *
	 * @param executionGraphToRestore {@link ExecutionGraph} which is supposed to be restored
	 * @param savepointRestoreSettings {@link SavepointRestoreSettings} containing information about the savepoint to restore from
	 * @throws Exception if the {@link ExecutionGraph} could not be restored
	 */
	private void tryRestoreExecutionGraphFromSavepoint(ExecutionGraph executionGraphToRestore, SavepointRestoreSettings savepointRestoreSettings) throws Exception {
		if (savepointRestoreSettings.restoreSavepoint()) {
			final CheckpointCoordinator checkpointCoordinator = executionGraphToRestore.getCheckpointCoordinator();
			if (checkpointCoordinator != null) {
				checkpointCoordinator.restoreSavepoint(
					savepointRestoreSettings.getRestorePath(),
					savepointRestoreSettings.allowNonRestoredState(),
					executionGraphToRestore.getAllVertices(),
					userCodeLoader);
			}
		}
	}

	//----------------------------------------------------------------------------------------------

	private void handleJobMasterError(final Throwable cause) {
		if (ExceptionUtils.isJvmFatalError(cause)) {
			log.error("Fatal error occurred on JobManager.", cause);
			// The fatal error handler implementation should make sure that this call is non-blocking
			fatalErrorHandler.onFatalError(cause);
		} else {
			jobCompletionActions.jobMasterFailed(cause);
		}
	}

	private void jobStatusChanged(
			final JobStatus newJobStatus,
			long timestamp,
			@Nullable final Throwable error) {
		validateRunsInMainThread();

		if (newJobStatus.isGloballyTerminalState()) {
			final ArchivedExecutionGraph archivedExecutionGraph = ArchivedExecutionGraph.createFrom(executionGraph);
			scheduledExecutorService.execute(() -> jobCompletionActions.jobReachedGloballyTerminalState(archivedExecutionGraph));
		}
	}

	private void notifyOfNewResourceManagerLeader(final String newResourceManagerAddress, final ResourceManagerId resourceManagerId) {
		resourceManagerAddress = createResourceManagerAddress(newResourceManagerAddress, resourceManagerId);

		reconnectToResourceManager(new FlinkException(String.format("ResourceManager leader changed to new address %s", resourceManagerAddress)));
	}

	@Nullable
	private ResourceManagerAddress createResourceManagerAddress(@Nullable String newResourceManagerAddress, @Nullable ResourceManagerId resourceManagerId) {
		if (newResourceManagerAddress != null) {
			// the contract is: address == null <=> id == null
			checkNotNull(resourceManagerId);
			return new ResourceManagerAddress(newResourceManagerAddress, resourceManagerId);
		} else {
			return null;
		}
	}

	private void reconnectToResourceManager(Exception cause) {
		closeResourceManagerConnection(cause);
		tryConnectToResourceManager();
	}

	private void tryConnectToResourceManager() {
		if (resourceManagerAddress != null) {
			connectToResourceManager();
		}
	}

	private void connectToResourceManager() {
		assert(resourceManagerAddress != null);
		assert(resourceManagerConnection == null);
		assert(establishedResourceManagerConnection == null);

		log.info("Connecting to ResourceManager {}", resourceManagerAddress);

		resourceManagerConnection = new ResourceManagerConnection(
			log,
			jobGraph.getJobID(),
			resourceId,
			getAddress(),
			getFencingToken(),
			resourceManagerAddress.getAddress(),
			resourceManagerAddress.getResourceManagerId(),
			scheduledExecutorService);

		resourceManagerConnection.start();
	}

	private void establishResourceManagerConnection(final JobMasterRegistrationSuccess success) {
		final ResourceManagerId resourceManagerId = success.getResourceManagerId();

		// verify the response with current connection
		if (resourceManagerConnection != null
				&& Objects.equals(resourceManagerConnection.getTargetLeaderId(), resourceManagerId)) {

			log.info("JobManager successfully registered at ResourceManager, leader id: {}.", resourceManagerId);

			final ResourceManagerGateway resourceManagerGateway = resourceManagerConnection.getTargetGateway();

			final ResourceID resourceManagerResourceId = success.getResourceManagerResourceId();

			establishedResourceManagerConnection = new EstablishedResourceManagerConnection(
				resourceManagerGateway,
				resourceManagerResourceId);

			slotPool.connectToResourceManager(resourceManagerGateway);

			resourceManagerHeartbeatManager.monitorTarget(resourceManagerResourceId, new HeartbeatTarget<Void>() {
				@Override
				public void receiveHeartbeat(ResourceID resourceID, Void payload) {
					resourceManagerGateway.heartbeatFromJobManager(resourceID);
				}

				@Override
				public void requestHeartbeat(ResourceID resourceID, Void payload) {
					// request heartbeat will never be called on the job manager side
				}
			});
		} else {
			log.debug("Ignoring resource manager connection to {} because it's duplicated or outdated.", resourceManagerId);

		}
	}

	private void closeResourceManagerConnection(Exception cause) {
		if (establishedResourceManagerConnection != null) {
			dissolveResourceManagerConnection(establishedResourceManagerConnection, cause);
			establishedResourceManagerConnection = null;
		}

		if (resourceManagerConnection != null) {
			// stop a potentially ongoing registration process
			resourceManagerConnection.close();
			resourceManagerConnection = null;
		}
	}

	private void dissolveResourceManagerConnection(EstablishedResourceManagerConnection establishedResourceManagerConnection, Exception cause) {
		final ResourceID resourceManagerResourceID = establishedResourceManagerConnection.getResourceManagerResourceID();

		if (log.isDebugEnabled()) {
			log.debug("Close ResourceManager connection {}.", resourceManagerResourceID, cause);
		} else {
			log.info("Close ResourceManager connection {}: {}.", resourceManagerResourceID, cause.getMessage());
		}

		resourceManagerHeartbeatManager.unmonitorTarget(resourceManagerResourceID);

		ResourceManagerGateway resourceManagerGateway = establishedResourceManagerConnection.getResourceManagerGateway();
		resourceManagerGateway.disconnectJobManager(jobGraph.getJobID(), cause);
		slotPool.disconnectResourceManager();
	}

	//----------------------------------------------------------------------------------------------
	// Service methods
	//----------------------------------------------------------------------------------------------

	@Override
	public JobMasterGateway getGateway() {
		return getSelfGateway(JobMasterGateway.class);
	}

	//----------------------------------------------------------------------------------------------
	// Utility classes
	//----------------------------------------------------------------------------------------------

	private class ResourceManagerLeaderListener implements LeaderRetrievalListener {

		@Override
		public void notifyLeaderAddress(final String leaderAddress, final UUID leaderSessionID) {
			runAsync(
				() -> notifyOfNewResourceManagerLeader(
					leaderAddress,
					ResourceManagerId.fromUuidOrNull(leaderSessionID)));
		}

		@Override
		public void handleError(final Exception exception) {
			handleJobMasterError(new Exception("Fatal error in the ResourceManager leader service", exception));
		}
	}

	//----------------------------------------------------------------------------------------------

	private class ResourceManagerConnection
			extends RegisteredRpcConnection<ResourceManagerId, ResourceManagerGateway, JobMasterRegistrationSuccess> {
		private final JobID jobID;

		private final ResourceID jobManagerResourceID;

		private final String jobManagerRpcAddress;

		private final JobMasterId jobMasterId;

		ResourceManagerConnection(
				final Logger log,
				final JobID jobID,
				final ResourceID jobManagerResourceID,
				final String jobManagerRpcAddress,
				final JobMasterId jobMasterId,
				final String resourceManagerAddress,
				final ResourceManagerId resourceManagerId,
				final Executor executor) {
			super(log, resourceManagerAddress, resourceManagerId, executor);
			this.jobID = checkNotNull(jobID);
			this.jobManagerResourceID = checkNotNull(jobManagerResourceID);
			this.jobManagerRpcAddress = checkNotNull(jobManagerRpcAddress);
			this.jobMasterId = checkNotNull(jobMasterId);
		}

		@Override
		protected RetryingRegistration<ResourceManagerId, ResourceManagerGateway, JobMasterRegistrationSuccess> generateRegistration() {
			return new RetryingRegistration<ResourceManagerId, ResourceManagerGateway, JobMasterRegistrationSuccess>(
				log,
				getRpcService(),
				"ResourceManager",
				ResourceManagerGateway.class,
				getTargetAddress(),
				getTargetLeaderId(),
				jobMasterConfiguration.getRetryingRegistrationConfiguration()) {

				@Override
				protected CompletableFuture<RegistrationResponse> invokeRegistration(
						ResourceManagerGateway gateway, ResourceManagerId fencingToken, long timeoutMillis) {
					Time timeout = Time.milliseconds(timeoutMillis);

					return gateway.registerJobManager(
						jobMasterId,
						jobManagerResourceID,
						jobManagerRpcAddress,
						jobID,
						timeout);
				}
			};
		}

		@Override
		protected void onRegistrationSuccess(final JobMasterRegistrationSuccess success) {
			runAsync(() -> {
				// filter out outdated connections
				//noinspection ObjectEquality
				if (this == resourceManagerConnection) {
					establishResourceManagerConnection(success);
				}
			});
		}

		@Override
		protected void onRegistrationFailure(final Throwable failure) {
			handleJobMasterError(failure);
		}
	}

	//----------------------------------------------------------------------------------------------

	private class JobManagerJobStatusListener implements JobStatusListener {

		private volatile boolean running = true;

		@Override
		public void jobStatusChanges(
				final JobID jobId,
				final JobStatus newJobStatus,
				final long timestamp,
				final Throwable error) {

			if (running) {
				// run in rpc thread to avoid concurrency
				runAsync(() -> jobStatusChanged(newJobStatus, timestamp, error));
			}
		}

		private void stop() {
			running = false;
		}
	}

	private class TaskManagerHeartbeatListener implements HeartbeatListener<AccumulatorReport, Void> {

		private final JobMasterGateway jobMasterGateway;

		private TaskManagerHeartbeatListener(JobMasterGateway jobMasterGateway) {
			this.jobMasterGateway = Preconditions.checkNotNull(jobMasterGateway);
		}

		@Override
		public void notifyHeartbeatTimeout(ResourceID resourceID) {
			jobMasterGateway.disconnectTaskManager(
				resourceID,
				new TimeoutException("Heartbeat of TaskManager with id " + resourceID + " timed out."));
		}

		@Override
		public void reportPayload(ResourceID resourceID, AccumulatorReport payload) {
			for (AccumulatorSnapshot snapshot : payload.getAccumulatorSnapshots()) {
				executionGraph.updateAccumulators(snapshot);
			}
		}

		@Override
		public CompletableFuture<Void> retrievePayload(ResourceID resourceID) {
			return CompletableFuture.completedFuture(null);
		}
	}

	private class ResourceManagerHeartbeatListener implements HeartbeatListener<Void, Void> {

		@Override
		public void notifyHeartbeatTimeout(final ResourceID resourceId) {
			runAsync(() -> {
				log.info("The heartbeat of ResourceManager with id {} timed out.", resourceId);

				if (establishedResourceManagerConnection != null && establishedResourceManagerConnection.getResourceManagerResourceID().equals(resourceId)) {
					reconnectToResourceManager(
						new JobMasterException(
							String.format("The heartbeat of ResourceManager with id %s timed out.", resourceId)));
				}
			});
		}

		@Override
		public void reportPayload(ResourceID resourceID, Void payload) {
			// nothing to do since the payload is of type Void
		}

		@Override
		public CompletableFuture<Void> retrievePayload(ResourceID resourceID) {
			return CompletableFuture.completedFuture(null);
		}
	}

	@VisibleForTesting
	RestartStrategy getRestartStrategy() {
		return restartStrategy;
	}
}
