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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.queryablestate.KvStateID;
import org.apache.flink.runtime.StoppingException;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
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
import org.apache.flink.runtime.executiongraph.restart.RestartStrategyFactory;
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
import org.apache.flink.runtime.jobmanager.OnCompletionActions;
import org.apache.flink.runtime.jobmanager.PartitionProducerDisposedException;
import org.apache.flink.runtime.jobmaster.message.ClassloadingProps;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPool;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPoolGateway;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.metrics.groups.JobManagerMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
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
import org.apache.flink.runtime.rpc.RpcTimeout;
import org.apache.flink.runtime.rpc.akka.AkkaRpcServiceUtils;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.util.clock.SystemClock;
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
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.util.Preconditions.checkNotNull;

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
public class JobMaster extends FencedRpcEndpoint<JobMasterId> implements JobMasterGateway {

	/** Default names for Flink's distributed components. */
	public static final String JOB_MANAGER_NAME = "jobmanager";
	public static final String ARCHIVE_NAME = "archive";

	// ------------------------------------------------------------------------

	private final JobMasterConfiguration jobMasterConfiguration;

	private final ResourceID resourceId;

	/** Logical representation of the job. */
	private final JobGraph jobGraph;

	private final Time rpcTimeout;

	/** Service to contend for and retrieve the leadership of JM and RM. */
	private final HighAvailabilityServices highAvailabilityServices;

	/** Blob server used across jobs. */
	private final BlobServer blobServer;

	/** The metrics for the job. */
	private final MetricGroup jobMetricGroup;

	/** The heartbeat manager with task managers. */
	private final HeartbeatManager<Void, Void> taskManagerHeartbeatManager;

	/** The heartbeat manager with resource manager. */
	private final HeartbeatManager<Void, Void> resourceManagerHeartbeatManager;

	/** The execution context which is used to execute futures. */
	private final ScheduledExecutorService scheduledExecutorService;

	private final OnCompletionActions jobCompletionActions;

	private final FatalErrorHandler errorHandler;

	private final ClassLoader userCodeLoader;

	/** The execution graph of this job. */
	private final ExecutionGraph executionGraph;

	private final SlotPool slotPool;

	private final SlotPoolGateway slotPoolGateway;

	private final CompletableFuture<String> restAddressFuture;

	private final String metricQueryServicePath;

	// --------- BackPressure --------

	private final BackPressureStatsTracker backPressureStatsTracker;

	// --------- ResourceManager --------

	/** Leader retriever service used to locate ResourceManager's address. */
	private LeaderRetrievalService resourceManagerLeaderRetriever;

	/** Connection with ResourceManager, null if not located address yet or we close it initiative. */
	private ResourceManagerConnection resourceManagerConnection;

	// --------- TaskManagers --------

	private final Map<ResourceID, Tuple2<TaskManagerLocation, TaskExecutorGateway>> registeredTaskManagers;

	// ------------------------------------------------------------------------

	public JobMaster(
			RpcService rpcService,
			JobMasterConfiguration jobMasterConfiguration,
			ResourceID resourceId,
			JobGraph jobGraph,
			HighAvailabilityServices highAvailabilityService,
			JobManagerSharedServices jobManagerSharedServices,
			HeartbeatServices heartbeatServices,
			BlobServer blobServer,
			@Nullable JobManagerMetricGroup jobManagerMetricGroup,
			OnCompletionActions jobCompletionActions,
			FatalErrorHandler errorHandler,
			ClassLoader userCodeLoader,
			@Nullable String restAddress,
			@Nullable String metricQueryServicePath) throws Exception {

		super(rpcService, AkkaRpcServiceUtils.createRandomName(JOB_MANAGER_NAME));

		final JobMasterGateway selfGateway = getSelfGateway(JobMasterGateway.class);

		this.jobMasterConfiguration = checkNotNull(jobMasterConfiguration);
		this.resourceId = checkNotNull(resourceId);
		this.jobGraph = checkNotNull(jobGraph);
		this.rpcTimeout = jobMasterConfiguration.getRpcTimeout();
		this.highAvailabilityServices = checkNotNull(highAvailabilityService);
		this.blobServer = checkNotNull(blobServer);
		this.scheduledExecutorService = jobManagerSharedServices.getScheduledExecutorService();
		this.jobCompletionActions = checkNotNull(jobCompletionActions);
		this.errorHandler = checkNotNull(errorHandler);
		this.userCodeLoader = checkNotNull(userCodeLoader);

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

		if (jobManagerMetricGroup != null) {
			this.jobMetricGroup = jobManagerMetricGroup.addJob(jobGraph);
		} else {
			this.jobMetricGroup = UnregisteredMetricGroups.createUnregisteredJobManagerJobMetricGroup();
		}

		log.info("Initializing job {} ({}).", jobName, jid);

		final RestartStrategies.RestartStrategyConfiguration restartStrategyConfiguration =
				jobGraph.getSerializedExecutionConfig()
						.deserializeValue(userCodeLoader)
						.getRestartStrategy();

		final RestartStrategy restartStrategy = (restartStrategyConfiguration != null) ?
				RestartStrategyFactory.createRestartStrategy(restartStrategyConfiguration) :
				jobManagerSharedServices.getRestartStrategyFactory().createRestartStrategy();

		log.info("Using restart strategy {} for {} ({}).", restartStrategy, jobName, jid);

		CheckpointRecoveryFactory checkpointRecoveryFactory = highAvailabilityServices.getCheckpointRecoveryFactory();

		resourceManagerLeaderRetriever = highAvailabilityServices.getResourceManagerLeaderRetriever();

		this.slotPool = new SlotPool(
			rpcService,
			jobGraph.getJobID(),
			SystemClock.getInstance(),
			rpcTimeout,
			jobMasterConfiguration.getSlotRequestTimeout(),
			jobMasterConfiguration.getSlotIdleTimeout());

		this.slotPoolGateway = slotPool.getSelfGateway(SlotPoolGateway.class);

		this.executionGraph = ExecutionGraphBuilder.buildGraph(
			null,
			jobGraph,
			jobMasterConfiguration.getConfiguration(),
			scheduledExecutorService,
			scheduledExecutorService,
			slotPool.getSlotProvider(),
			userCodeLoader,
			checkpointRecoveryFactory,
			rpcTimeout,
			restartStrategy,
			jobMetricGroup,
			-1,
			blobServer,
			jobMasterConfiguration.getSlotRequestTimeout(),
			log);

		// register self as job status change listener
		executionGraph.registerJobStatusListener(new JobManagerJobStatusListener());

		this.registeredTaskManagers = new HashMap<>(4);

		this.restAddressFuture = Optional.ofNullable(restAddress)
			.map(CompletableFuture::completedFuture)
			.orElse(FutureUtils.completedExceptionally(new JobMasterException("The JobMaster has not been started with a REST endpoint.")));

		this.metricQueryServicePath = metricQueryServicePath;
		this.backPressureStatsTracker = checkNotNull(jobManagerSharedServices.getBackPressureStatsTracker());
	}

	//----------------------------------------------------------------------------------------------
	// Lifecycle management
	//----------------------------------------------------------------------------------------------

	@Override
	public void start() {
		throw new UnsupportedOperationException("Should never call start() without leader ID");
	}

	/**
	 * Start the rpc service and begin to run the job.
	 *
	 * @param newJobMasterId The necessary fencing token to run the job
	 * @param timeout for the operation
	 * @return Future acknowledge if the job could be started. Otherwise the future contains an exception
	 */
	public CompletableFuture<Acknowledge> start(final JobMasterId newJobMasterId, final Time timeout) throws Exception {
		// make sure we receive RPC and async calls
		super.start();

		return callAsyncWithoutFencing(() -> startJobExecution(newJobMasterId), timeout);
	}

	/**
	 * Suspending job, all the running tasks will be cancelled, and communication with other components
	 * will be disposed.
	 *
	 * <p>Mostly job is suspended because of the leadership has been revoked, one can be restart this job by
	 * calling the {@link #start(JobMasterId, Time)} method once we take the leadership back again.
	 *
	 * <p>This method is executed asynchronously
	 *
	 * @param cause The reason of why this job been suspended.
	 * @param timeout for this operation
	 * @return Future acknowledge indicating that the job has been suspended. Otherwise the future contains an exception
	 */
	public CompletableFuture<Acknowledge> suspend(final Exception cause, final Time timeout) {
		CompletableFuture<Acknowledge> suspendFuture = callAsyncWithoutFencing(() -> suspendExecution(cause), timeout);

		stop();

		return suspendFuture;
	}

	/**
	 * Suspend the job and shutdown all other services including rpc.
	 */
	@Override
	public void postStop() throws Exception {
		log.info("Stopping the JobMaster for job " + jobGraph.getName() + '(' + jobGraph.getJobID() + ").");

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
		slotPool.shutDown();
		CompletableFuture<Boolean> terminationFuture = slotPool.getTerminationFuture();

		Exception exception = null;

		// wait for the slot pool shut down
		try {
			terminationFuture.get(rpcTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);
		} catch (Exception e) {
			exception = e;
		}

		try {
			super.postStop();
		} catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(e, exception);
		}

		if (exception != null) {
			throw exception;
		}

		log.info("Stopped the JobMaster for job " + jobGraph.getName() + '(' + jobGraph.getJobID() + ").");
	}

	//----------------------------------------------------------------------------------------------
	// RPC methods
	//----------------------------------------------------------------------------------------------

	@Override
	public CompletableFuture<Acknowledge> cancel(Time timeout) {
		executionGraph.cancel();

		return CompletableFuture.completedFuture(Acknowledge.get());
	}

	@Override
	public CompletableFuture<Acknowledge> stop(Time timeout) {
		try {
			executionGraph.stop();
		} catch (StoppingException e) {
			return FutureUtils.completedExceptionally(e);
		}

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

		final InputSplitAssigner splitAssigner = vertex.getSplitAssigner();
		if (splitAssigner == null) {
			log.error("No InputSplitAssigner for vertex ID {}.", vertexID);
			return FutureUtils.completedExceptionally(new Exception("No InputSplitAssigner for vertex ID " + vertexID));
		}

		final LogicalSlot slot = execution.getAssignedResource();
		final int taskId = execution.getVertex().getParallelSubtaskIndex();
		final String host = slot != null ? slot.getTaskManagerLocation().getHostname() : null;
		final InputSplit nextInputSplit = splitAssigner.getNextInputSplit(host, taskId);

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

				if (producerExecution.getAttemptId() == resultPartitionId.getProducerId()) {
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
		taskManagerHeartbeatManager.unmonitorTarget(resourceID);
		CompletableFuture<Acknowledge> releaseFuture = slotPoolGateway.releaseTaskManager(resourceID);

		Tuple2<TaskManagerLocation, TaskExecutorGateway> taskManagerConnection = registeredTaskManagers.remove(resourceID);

		if (taskManagerConnection != null) {
			taskManagerConnection.f1.disconnectJobManager(jobGraph.getJobID(), cause);
		}

		return releaseFuture;
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
					log.warn("Error while processing checkpoint acknowledgement message");
				}
			});
		} else {
			log.error("Received AcknowledgeCheckpoint message for job {} with no CheckpointCoordinator",
					jobGraph.getJobID());
		}
	}

	// TODO: This method needs a leader session ID
	@Override
	public void declineCheckpoint(
			final JobID jobID,
			final ExecutionAttemptID executionAttemptID,
			final long checkpointID,
			final Throwable reason) {
		final DeclineCheckpoint decline = new DeclineCheckpoint(
				jobID, executionAttemptID, checkpointID, reason);
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
			log.error("Received DeclineCheckpoint message for job {} with no CheckpointCoordinator",
					jobGraph.getJobID());
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
				log.error("Failed to notify KvStateRegistry about registration {}.", registrationName);
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
				log.error("Failed to notify KvStateRegistry about registration {}.", registrationName, e);
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
	public CompletableFuture<ClassloadingProps> requestClassloadingProps() {
		return CompletableFuture.completedFuture(
			new ClassloadingProps(blobServer.getPort(),
				executionGraph.getRequiredJarFiles(),
				executionGraph.getRequiredClasspaths()));
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

		return slotPoolGateway.offerSlots(
			taskManagerLocation,
			rpcTaskManagerGateway,
			slots);
	}

	@Override
	public void failSlot(
			final ResourceID taskManagerId,
			final AllocationID allocationId,
			final Exception cause) {

		if (registeredTaskManagers.containsKey(taskManagerId)) {
			slotPoolGateway.failAllocation(allocationId, cause);
		} else {
			log.warn("Cannot fail slot " + allocationId + " because the TaskManager " +
			taskManagerId + " is unknown.");
		}
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

						slotPoolGateway.registerTaskManager(taskManagerId);
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

		if (resourceManagerConnection != null
				&& resourceManagerConnection.getTargetLeaderId().equals(resourceManagerId)) {
			closeResourceManagerConnection(cause);
		}
	}

	@Override
	public void heartbeatFromTaskManager(final ResourceID resourceID) {
		taskManagerHeartbeatManager.receiveHeartbeat(resourceID, null);
	}

	@Override
	public void heartbeatFromResourceManager(final ResourceID resourceID) {
		resourceManagerHeartbeatManager.requestHeartbeat(resourceID, null);
	}

	@Override
	public CompletableFuture<JobDetails> requestJobDetails(@RpcTimeout Time timeout) {
		return CompletableFuture.supplyAsync(() -> WebMonitorUtils.createDetailsForJob(executionGraph), scheduledExecutorService);
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
		final String targetDirectory,
		final Time timeout) {
		try {
			return executionGraph.getCheckpointCoordinator()
				.triggerSavepoint(System.currentTimeMillis(), targetDirectory)
				.thenApply(CompletedCheckpoint::getExternalPointer);
		} catch (Exception e) {
			return FutureUtils.completedExceptionally(e);
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

	//----------------------------------------------------------------------------------------------
	// Internal methods
	//----------------------------------------------------------------------------------------------

	//-- job starting and stopping  -----------------------------------------------------------------

	private Acknowledge startJobExecution(JobMasterId newJobMasterId) throws Exception {
		validateRunsInMainThread();

		Preconditions.checkNotNull(newJobMasterId, "The new JobMasterId must not be null.");

		if (Objects.equals(getFencingToken(), newJobMasterId)) {
			log.info("Already started the job execution with JobMasterId {}.", newJobMasterId);

			return Acknowledge.get();
		}

		if (getFencingToken() != null) {
			log.info("Restarting old job with JobMasterId {}. The new JobMasterId is {}.", getFencingToken(), newJobMasterId);

			// first we have to suspend the current execution
			suspendExecution(new FlinkException("Old job with JobMasterId " + getFencingToken() +
				" is restarted with a new JobMasterId " + newJobMasterId + '.'));
		}

		// set new leader id
		setFencingToken(newJobMasterId);

		log.info("Starting execution of job {} ({})", jobGraph.getName(), jobGraph.getJobID());

		try {
			// start the slot pool make sure the slot pool now accepts messages for this leader
			log.debug("Staring SlotPool component");
			slotPool.start(getFencingToken(), getAddress());

			// job is ready to go, try to establish connection with resource manager
			//   - activate leader retrieval for the resource manager
			//   - on notification of the leader, the connection will be established and
			//     the slot pool will start requesting slots
			resourceManagerLeaderRetriever.start(new ResourceManagerLeaderListener());
		}
		catch (Throwable t) {
			log.error("Failed to start job {} ({})", jobGraph.getName(), jobGraph.getJobID(), t);

			throw new Exception("Could not start job execution: Failed to start JobMaster services.", t);
		}

		// start scheduling job in another thread
		scheduledExecutorService.execute(
			() -> {
				try {
					executionGraph.scheduleForExecution();
				}
				catch (Throwable t) {
					executionGraph.failGlobal(t);
				}
			});

		return Acknowledge.get();
	}

	/**
	 * Suspending job, all the running tasks will be cancelled, and communication with other components
	 * will be disposed.
	 *
	 * <p>Mostly job is suspended because of the leadership has been revoked, one can be restart this job by
	 * calling the {@link #start(JobMasterId, Time)} method once we take the leadership back again.
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
		} catch (Throwable t) {
			log.warn("Failed to stop resource manager leader retriever when suspending.", t);
		}

		// tell the execution graph (JobManager is still processing messages here)
		executionGraph.suspend(cause);

		// the slot pool stops receiving messages and clears its pooled slots
		slotPoolGateway.suspend();

		// disconnect from resource manager:
		closeResourceManagerConnection(cause);

		return Acknowledge.get();
	}

	//----------------------------------------------------------------------------------------------

	private void handleFatalError(final Throwable cause) {

		try {
			log.error("Fatal error occurred on JobManager.", cause);
		} catch (Throwable ignore) {}

		// The fatal error handler implementation should make sure that this call is non-blocking
		errorHandler.onFatalError(cause);
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

	private void notifyOfNewResourceManagerLeader(final String resourceManagerAddress, final ResourceManagerId resourceManagerId) {
		if (resourceManagerConnection != null) {
			if (resourceManagerAddress != null) {
				if (Objects.equals(resourceManagerAddress, resourceManagerConnection.getTargetAddress())
					&& Objects.equals(resourceManagerId, resourceManagerConnection.getTargetLeaderId())) {
					// both address and leader id are not changed, we can keep the old connection
					return;
				}

				closeResourceManagerConnection(new Exception(
					"ResourceManager leader changed to new address " + resourceManagerAddress));

				log.info("ResourceManager leader changed from {} to {}. Registering at new leader.",
					resourceManagerConnection.getTargetAddress(), resourceManagerAddress);
			} else {
				log.info("Current ResourceManager {} lost leader status. Waiting for new ResourceManager leader.",
					resourceManagerConnection.getTargetAddress());
			}
		}

		if (resourceManagerAddress != null) {
			log.info("Attempting to register at ResourceManager {}", resourceManagerAddress);

			resourceManagerConnection = new ResourceManagerConnection(
				log,
				jobGraph.getJobID(),
				resourceId,
				getAddress(),
				getFencingToken(),
				resourceManagerAddress,
				resourceManagerId,
				scheduledExecutorService);

			resourceManagerConnection.start();
		}
	}

	private void establishResourceManagerConnection(final JobMasterRegistrationSuccess success) {
		final ResourceManagerId resourceManagerId = success.getResourceManagerId();

		// verify the response with current connection
		if (resourceManagerConnection != null
				&& Objects.equals(resourceManagerConnection.getTargetLeaderId(), resourceManagerId)) {

			log.info("JobManager successfully registered at ResourceManager, leader id: {}.", resourceManagerId);

			final ResourceManagerGateway resourceManagerGateway = resourceManagerConnection.getTargetGateway();

			slotPoolGateway.connectToResourceManager(resourceManagerGateway);

			resourceManagerHeartbeatManager.monitorTarget(success.getResourceManagerResourceId(), new HeartbeatTarget<Void>() {
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
			log.debug("Ignoring resource manager connection to {} because its a duplicate or outdated.", resourceManagerId);

		}
	}

	private void closeResourceManagerConnection(Exception cause) {
		if (resourceManagerConnection != null) {
			if (log.isDebugEnabled()) {
				log.debug("Close ResourceManager connection {}.", resourceManagerConnection.getResourceManagerResourceID(), cause);
			} else {
				log.info("Close ResourceManager connection {}: {}.", resourceManagerConnection.getResourceManagerResourceID(), cause.getMessage());
			}

			resourceManagerHeartbeatManager.unmonitorTarget(resourceManagerConnection.getResourceManagerResourceID());

			ResourceManagerGateway resourceManagerGateway = resourceManagerConnection.getTargetGateway();
			resourceManagerGateway.disconnectJobManager(resourceManagerConnection.getJobID(), cause);

			resourceManagerConnection.close();
			resourceManagerConnection = null;
		}

		slotPoolGateway.disconnectResourceManager();
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
					leaderSessionID != null ? new ResourceManagerId(leaderSessionID) : null));
		}

		@Override
		public void handleError(final Exception exception) {
			handleFatalError(new Exception("Fatal error in the ResourceManager leader service", exception));
		}
	}

	//----------------------------------------------------------------------------------------------

	private class ResourceManagerConnection
			extends RegisteredRpcConnection<ResourceManagerId, ResourceManagerGateway, JobMasterRegistrationSuccess> {
		private final JobID jobID;

		private final ResourceID jobManagerResourceID;

		private final String jobManagerRpcAddress;

		private final JobMasterId jobMasterId;

		private ResourceID resourceManagerResourceID;

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
					log, getRpcService(), "ResourceManager", ResourceManagerGateway.class,
					getTargetAddress(), getTargetLeaderId()) {
				@Override
				protected CompletableFuture<RegistrationResponse> invokeRegistration(
						ResourceManagerGateway gateway, ResourceManagerId fencingToken, long timeoutMillis) throws Exception {
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
				resourceManagerResourceID = success.getResourceManagerResourceId();
				establishResourceManagerConnection(success);
			});
		}

		@Override
		protected void onRegistrationFailure(final Throwable failure) {
			handleFatalError(failure);
		}

		public ResourceID getResourceManagerResourceID() {
			return resourceManagerResourceID;
		}

		public JobID getJobID() {
			return jobID;
		}
	}

	//----------------------------------------------------------------------------------------------

	private class JobManagerJobStatusListener implements JobStatusListener {

		@Override
		public void jobStatusChanges(
				final JobID jobId,
				final JobStatus newJobStatus,
				final long timestamp,
				final Throwable error) {

			// run in rpc thread to avoid concurrency
			runAsync(() -> jobStatusChanged(newJobStatus, timestamp, error));
		}
	}

	private class TaskManagerHeartbeatListener implements HeartbeatListener<Void, Void> {

		private final JobMasterGateway jobMasterGateway;

		private TaskManagerHeartbeatListener(JobMasterGateway jobMasterGateway) {
			this.jobMasterGateway = Preconditions.checkNotNull(jobMasterGateway);
		}

		@Override
		public void notifyHeartbeatTimeout(ResourceID resourceID) {
			log.info("Heartbeat of TaskManager with id {} timed out.", resourceID);

			jobMasterGateway.disconnectTaskManager(
				resourceID,
				new TimeoutException("Heartbeat of TaskManager with id " + resourceID + " timed out."));
		}

		@Override
		public void reportPayload(ResourceID resourceID, Void payload) {
			// nothing to do since there is no payload
		}

		@Override
		public CompletableFuture<Void> retrievePayload() {
			return CompletableFuture.completedFuture(null);
		}
	}

	private class ResourceManagerHeartbeatListener implements HeartbeatListener<Void, Void> {

		@Override
		public void notifyHeartbeatTimeout(final ResourceID resourceId) {
			runAsync(() -> {
				log.info("The heartbeat of ResourceManager with id {} timed out.", resourceId);

				closeResourceManagerConnection(
					new TimeoutException(
						"The heartbeat of ResourceManager with id " + resourceId + " timed out."));
			});
		}

		@Override
		public void reportPayload(ResourceID resourceID, Void payload) {
			// nothing to do since the payload is of type Void
		}

		@Override
		public CompletableFuture<Void> retrievePayload() {
			return CompletableFuture.completedFuture(null);
		}
	}
}
