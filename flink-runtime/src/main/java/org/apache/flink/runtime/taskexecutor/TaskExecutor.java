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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.accumulators.AccumulatorSnapshot;
import org.apache.flink.runtime.blob.BlobCacheService;
import org.apache.flink.runtime.blob.TransientBlobCache;
import org.apache.flink.runtime.blob.TransientBlobKey;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.JobManagerTaskRestore;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.execution.librarycache.BlobLibraryCacheManager;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.JobInformation;
import org.apache.flink.runtime.executiongraph.PartitionInfo;
import org.apache.flink.runtime.executiongraph.TaskInformation;
import org.apache.flink.runtime.filecache.FileCache;
import org.apache.flink.runtime.heartbeat.HeartbeatListener;
import org.apache.flink.runtime.heartbeat.HeartbeatManager;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.heartbeat.HeartbeatTarget;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.instance.HardwareDescription;
import org.apache.flink.runtime.io.network.NetworkEnvironment;
import org.apache.flink.runtime.io.network.netty.PartitionProducerStateChecker;
import org.apache.flink.runtime.io.network.partition.ResultPartitionConsumableNotifier;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.jobmaster.JMTMRegistrationSuccess;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.StackTraceSampleResponse;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.runtime.query.KvStateClientProxy;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.query.KvStateServer;
import org.apache.flink.runtime.registration.RegistrationConnectionListener;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.akka.AkkaRpcServiceUtils;
import org.apache.flink.runtime.state.TaskExecutorLocalStateStoresManager;
import org.apache.flink.runtime.state.TaskLocalStateStore;
import org.apache.flink.runtime.state.TaskStateManager;
import org.apache.flink.runtime.state.TaskStateManagerImpl;
import org.apache.flink.runtime.taskexecutor.exceptions.CheckpointException;
import org.apache.flink.runtime.taskexecutor.exceptions.PartitionException;
import org.apache.flink.runtime.taskexecutor.exceptions.SlotAllocationException;
import org.apache.flink.runtime.taskexecutor.exceptions.SlotOccupiedException;
import org.apache.flink.runtime.taskexecutor.exceptions.TaskException;
import org.apache.flink.runtime.taskexecutor.exceptions.TaskSubmissionException;
import org.apache.flink.runtime.taskexecutor.rpc.RpcCheckpointResponder;
import org.apache.flink.runtime.taskexecutor.rpc.RpcInputSplitProvider;
import org.apache.flink.runtime.taskexecutor.rpc.RpcKvStateRegistryListener;
import org.apache.flink.runtime.taskexecutor.rpc.RpcPartitionStateChecker;
import org.apache.flink.runtime.taskexecutor.rpc.RpcResultPartitionConsumableNotifier;
import org.apache.flink.runtime.taskexecutor.slot.SlotActions;
import org.apache.flink.runtime.taskexecutor.slot.SlotNotActiveException;
import org.apache.flink.runtime.taskexecutor.slot.SlotNotFoundException;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskexecutor.slot.TaskSlot;
import org.apache.flink.runtime.taskexecutor.slot.TaskSlotTable;
import org.apache.flink.runtime.taskmanager.CheckpointResponder;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.taskmanager.TaskManagerActions;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * TaskExecutor implementation. The task executor is responsible for the execution of multiple
 * {@link Task}.
 */
public class TaskExecutor extends RpcEndpoint implements TaskExecutorGateway {

	public static final String TASK_MANAGER_NAME = "taskmanager";

	/** The access to the leader election and retrieval services. */
	private final HighAvailabilityServices haServices;

	private final TaskManagerServices taskExecutorServices;

	/** The task manager configuration. */
	private final TaskManagerConfiguration taskManagerConfiguration;

	/** The heartbeat manager for job manager in the task manager. */
	private final HeartbeatManager<Void, AccumulatorReport> jobManagerHeartbeatManager;

	/** The heartbeat manager for resource manager in the task manager. */
	private final HeartbeatManager<Void, SlotReport> resourceManagerHeartbeatManager;

	/** The fatal error handler to use in case of a fatal error. */
	private final FatalErrorHandler fatalErrorHandler;

	private final BlobCacheService blobCacheService;

	// --------- TaskManager services --------

	/** The connection information of this task manager. */
	private final TaskManagerLocation taskManagerLocation;

	private final TaskManagerMetricGroup taskManagerMetricGroup;

	/** The state manager for this task, providing state managers per slot. */
	private final TaskExecutorLocalStateStoresManager localStateStoresManager;

	/** The network component in the task manager. */
	private final NetworkEnvironment networkEnvironment;

	// --------- resource manager --------

	private TaskExecutorToResourceManagerConnection resourceManagerConnection;

	// --------- job manager connections -----------

	private final Map<ResourceID, JobManagerConnection> jobManagerConnections;

	// --------- task slot allocation table -----------

	private final TaskSlotTable taskSlotTable;

	private final JobManagerTable jobManagerTable;

	private final JobLeaderService jobLeaderService;

	private final LeaderRetrievalService resourceManagerLeaderRetriever;

	// ------------------------------------------------------------------------

	private final HardwareDescription hardwareDescription;

	private FileCache fileCache;

	public TaskExecutor(
			RpcService rpcService,
			TaskManagerConfiguration taskManagerConfiguration,
			HighAvailabilityServices haServices,
			TaskManagerServices taskExecutorServices,
			HeartbeatServices heartbeatServices,
			TaskManagerMetricGroup taskManagerMetricGroup,
			BlobCacheService blobCacheService,
			FatalErrorHandler fatalErrorHandler) {

		super(rpcService, AkkaRpcServiceUtils.createRandomName(TASK_MANAGER_NAME));

		checkArgument(taskManagerConfiguration.getNumberSlots() > 0, "The number of slots has to be larger than 0.");

		this.taskManagerConfiguration = checkNotNull(taskManagerConfiguration);
		this.taskExecutorServices = checkNotNull(taskExecutorServices);
		this.haServices = checkNotNull(haServices);
		this.fatalErrorHandler = checkNotNull(fatalErrorHandler);
		this.taskManagerMetricGroup = checkNotNull(taskManagerMetricGroup);
		this.blobCacheService = checkNotNull(blobCacheService);

		this.taskSlotTable = taskExecutorServices.getTaskSlotTable();
		this.jobManagerTable = taskExecutorServices.getJobManagerTable();
		this.jobLeaderService = taskExecutorServices.getJobLeaderService();
		this.taskManagerLocation = taskExecutorServices.getTaskManagerLocation();
		this.localStateStoresManager = taskExecutorServices.getTaskManagerStateStore();
		this.networkEnvironment = taskExecutorServices.getNetworkEnvironment();
		this.resourceManagerLeaderRetriever = haServices.getResourceManagerLeaderRetriever();

		this.jobManagerConnections = new HashMap<>(4);

		final ResourceID resourceId = taskExecutorServices.getTaskManagerLocation().getResourceID();

		this.jobManagerHeartbeatManager = heartbeatServices.createHeartbeatManager(
			resourceId,
			new JobManagerHeartbeatListener(),
			rpcService.getScheduledExecutor(),
			log);

		this.resourceManagerHeartbeatManager = heartbeatServices.createHeartbeatManager(
			resourceId,
			new ResourceManagerHeartbeatListener(),
			rpcService.getScheduledExecutor(),
			log);

		hardwareDescription = HardwareDescription.extractFromSystem(
			taskExecutorServices.getMemoryManager().getMemorySize());
	}

	// ------------------------------------------------------------------------
	//  Life cycle
	// ------------------------------------------------------------------------

	@Override
	public void start() throws Exception {
		super.start();

		// start by connecting to the ResourceManager
		try {
			resourceManagerLeaderRetriever.start(new ResourceManagerLeaderListener());
		} catch (Exception e) {
			onFatalError(e);
		}

		// tell the task slot table who's responsible for the task slot actions
		taskSlotTable.start(new SlotActionsImpl());

		// start the job leader service
		jobLeaderService.start(getAddress(), getRpcService(), haServices, new JobLeaderListenerImpl());

		fileCache = new FileCache(taskManagerConfiguration.getTmpDirectories(), blobCacheService.getPermanentBlobService());
	}

	/**
	 * Called to shut down the TaskManager. The method closes all TaskManager services.
	 */
	@Override
	public CompletableFuture<Void> postStop() {
		log.info("Stopping TaskExecutor {}.", getAddress());

		Throwable throwable = null;

		if (isConnectedToResourceManager()) {
			resourceManagerConnection.close();
		}

		for (JobManagerConnection jobManagerConnection : jobManagerConnections.values()) {
			try {
				disassociateFromJobManager(jobManagerConnection, new FlinkException("The TaskExecutor is shutting down."));
			} catch (Throwable t) {
				throwable = ExceptionUtils.firstOrSuppressed(t, throwable);
			}
		}

		jobManagerHeartbeatManager.stop();

		resourceManagerHeartbeatManager.stop();

		try {
			resourceManagerLeaderRetriever.stop();
		} catch (Exception e) {
			throwable = ExceptionUtils.firstOrSuppressed(e, throwable);
		}

		try {
			taskExecutorServices.shutDown();
			fileCache.shutdown();
		} catch (Throwable t) {
			throwable = ExceptionUtils.firstOrSuppressed(t, throwable);
		}

		// it will call close() recursively from the parent to children
		taskManagerMetricGroup.close();

		if (throwable != null) {
			return FutureUtils.completedExceptionally(new FlinkException("Error while shutting the TaskExecutor down.", throwable));
		} else {
			log.info("Stopped TaskExecutor {}.", getAddress());
			return CompletableFuture.completedFuture(null);
		}
	}

	// ======================================================================
	//  RPC methods
	// ======================================================================

	@Override
	public CompletableFuture<StackTraceSampleResponse> requestStackTraceSample(
			final ExecutionAttemptID executionAttemptId,
			final int sampleId,
			final int numSamples,
			final Time delayBetweenSamples,
			final int maxStackTraceDepth,
			final Time timeout) {
		return requestStackTraceSample(
			executionAttemptId,
			sampleId,
			numSamples,
			delayBetweenSamples,
			maxStackTraceDepth,
			new ArrayList<>(numSamples),
			new CompletableFuture<>());
	}

	private CompletableFuture<StackTraceSampleResponse> requestStackTraceSample(
			final ExecutionAttemptID executionAttemptId,
			final int sampleId,
			final int numSamples,
			final Time delayBetweenSamples,
			final int maxStackTraceDepth,
			final List<StackTraceElement[]> currentTraces,
			final CompletableFuture<StackTraceSampleResponse> resultFuture) {

		final Optional<StackTraceElement[]> stackTrace = getStackTrace(executionAttemptId, maxStackTraceDepth);
		if (stackTrace.isPresent()) {
			currentTraces.add(stackTrace.get());
		} else if (!currentTraces.isEmpty()) {
			resultFuture.complete(new StackTraceSampleResponse(
				sampleId,
				executionAttemptId,
				currentTraces));
		} else {
			throw new IllegalStateException(String.format("Cannot sample task %s. " +
					"Either the task is not known to the task manager or it is not running.",
				executionAttemptId));
		}

		if (numSamples > 1) {
			scheduleRunAsync(() -> requestStackTraceSample(
				executionAttemptId,
				sampleId,
				numSamples - 1,
				delayBetweenSamples,
				maxStackTraceDepth,
				currentTraces,
				resultFuture), delayBetweenSamples.getSize(), delayBetweenSamples.getUnit());
			return resultFuture;
		} else {
			resultFuture.complete(new StackTraceSampleResponse(
				sampleId,
				executionAttemptId,
				currentTraces));
			return resultFuture;
		}
	}

	private Optional<StackTraceElement[]> getStackTrace(
			final ExecutionAttemptID executionAttemptId, final int maxStackTraceDepth) {
		final Task task = taskSlotTable.getTask(executionAttemptId);

		if (task != null && task.getExecutionState() == ExecutionState.RUNNING) {
			final StackTraceElement[] stackTrace = task.getExecutingThread().getStackTrace();

			if (maxStackTraceDepth > 0) {
				return Optional.of(Arrays.copyOfRange(stackTrace, 0, Math.min(maxStackTraceDepth, stackTrace.length)));
			} else {
				return Optional.of(stackTrace);
			}
		} else {
			return Optional.empty();
		}
	}

	// ----------------------------------------------------------------------
	// Task lifecycle RPCs
	// ----------------------------------------------------------------------

	@Override
	public CompletableFuture<Acknowledge> submitTask(
			TaskDeploymentDescriptor tdd,
			JobMasterId jobMasterId,
			Time timeout) {

		try {
			final JobID jobId = tdd.getJobId();
			final JobManagerConnection jobManagerConnection = jobManagerTable.get(jobId);

			if (jobManagerConnection == null) {
				final String message = "Could not submit task because there is no JobManager " +
					"associated for the job " + jobId + '.';

				log.debug(message);
				throw new TaskSubmissionException(message);
			}

			if (!Objects.equals(jobManagerConnection.getJobMasterId(), jobMasterId)) {
				final String message = "Rejecting the task submission because the job manager leader id " +
					jobMasterId + " does not match the expected job manager leader id " +
					jobManagerConnection.getJobMasterId() + '.';

				log.debug(message);
				throw new TaskSubmissionException(message);
			}

			if (!taskSlotTable.existsActiveSlot(jobId, tdd.getAllocationId())) {
				final String message = "No task slot allocated for job ID " + jobId +
					" and allocation ID " + tdd.getAllocationId() + '.';
				log.debug(message);
				throw new TaskSubmissionException(message);
			}

			// re-integrate offloaded data:
			try {
				tdd.loadBigData(blobCacheService.getPermanentBlobService());
			} catch (IOException | ClassNotFoundException e) {
				throw new TaskSubmissionException("Could not re-integrate offloaded TaskDeploymentDescriptor data.", e);
			}

			// deserialize the pre-serialized information
			final JobInformation jobInformation;
			final TaskInformation taskInformation;
			try {
				jobInformation = tdd.getSerializedJobInformation().deserializeValue(getClass().getClassLoader());
				taskInformation = tdd.getSerializedTaskInformation().deserializeValue(getClass().getClassLoader());
			} catch (IOException | ClassNotFoundException e) {
				throw new TaskSubmissionException("Could not deserialize the job or task information.", e);
			}

			if (!jobId.equals(jobInformation.getJobId())) {
				throw new TaskSubmissionException(
					"Inconsistent job ID information inside TaskDeploymentDescriptor (" +
						tdd.getJobId() + " vs. " + jobInformation.getJobId() + ")");
			}

			TaskMetricGroup taskMetricGroup = taskManagerMetricGroup.addTaskForJob(
				jobInformation.getJobId(),
				jobInformation.getJobName(),
				taskInformation.getJobVertexId(),
				tdd.getExecutionAttemptId(),
				taskInformation.getTaskName(),
				tdd.getSubtaskIndex(),
				tdd.getAttemptNumber());

			InputSplitProvider inputSplitProvider = new RpcInputSplitProvider(
				jobManagerConnection.getJobManagerGateway(),
				taskInformation.getJobVertexId(),
				tdd.getExecutionAttemptId(),
				taskManagerConfiguration.getTimeout());

			TaskManagerActions taskManagerActions = jobManagerConnection.getTaskManagerActions();
			CheckpointResponder checkpointResponder = jobManagerConnection.getCheckpointResponder();

			LibraryCacheManager libraryCache = jobManagerConnection.getLibraryCacheManager();
			ResultPartitionConsumableNotifier resultPartitionConsumableNotifier = jobManagerConnection.getResultPartitionConsumableNotifier();
			PartitionProducerStateChecker partitionStateChecker = jobManagerConnection.getPartitionStateChecker();

			final TaskLocalStateStore localStateStore = localStateStoresManager.localStateStoreForSubtask(
				jobId,
				tdd.getAllocationId(),
				taskInformation.getJobVertexId(),
				tdd.getSubtaskIndex());

			final JobManagerTaskRestore taskRestore = tdd.getTaskRestore();

			final TaskStateManager taskStateManager = new TaskStateManagerImpl(
				jobId,
				tdd.getExecutionAttemptId(),
				localStateStore,
				taskRestore,
				checkpointResponder);

			Task task = new Task(
				jobInformation,
				taskInformation,
				tdd.getExecutionAttemptId(),
				tdd.getAllocationId(),
				tdd.getSubtaskIndex(),
				tdd.getAttemptNumber(),
				tdd.getProducedPartitions(),
				tdd.getInputGates(),
				tdd.getTargetSlotNumber(),
				taskExecutorServices.getMemoryManager(),
				taskExecutorServices.getIOManager(),
				taskExecutorServices.getNetworkEnvironment(),
				taskExecutorServices.getBroadcastVariableManager(),
				taskStateManager,
				taskManagerActions,
				inputSplitProvider,
				checkpointResponder,
				blobCacheService,
				libraryCache,
				fileCache,
				taskManagerConfiguration,
				taskMetricGroup,
				resultPartitionConsumableNotifier,
				partitionStateChecker,
				getRpcService().getExecutor());

			log.info("Received task {}.", task.getTaskInfo().getTaskNameWithSubtasks());

			boolean taskAdded;

			try {
				taskAdded = taskSlotTable.addTask(task);
			} catch (SlotNotFoundException | SlotNotActiveException e) {
				throw new TaskSubmissionException("Could not submit task.", e);
			}

			if (taskAdded) {
				task.startTaskThread();

				return CompletableFuture.completedFuture(Acknowledge.get());
			} else {
				final String message = "TaskManager already contains a task for id " +
					task.getExecutionId() + '.';

				log.debug(message);
				throw new TaskSubmissionException(message);
			}
		} catch (TaskSubmissionException e) {
			return FutureUtils.completedExceptionally(e);
		}
	}

	@Override
	public CompletableFuture<Acknowledge> cancelTask(ExecutionAttemptID executionAttemptID, Time timeout) {
		final Task task = taskSlotTable.getTask(executionAttemptID);

		if (task != null) {
			try {
				task.cancelExecution();
				return CompletableFuture.completedFuture(Acknowledge.get());
			} catch (Throwable t) {
				return FutureUtils.completedExceptionally(
					new TaskException("Cannot cancel task for execution " + executionAttemptID + '.', t));
			}
		} else {
			final String message = "Cannot find task to stop for execution " + executionAttemptID + '.';

			log.debug(message);
			return FutureUtils.completedExceptionally(new TaskException(message));
		}
	}

	@Override
	public CompletableFuture<Acknowledge> stopTask(ExecutionAttemptID executionAttemptID, Time timeout) {
		final Task task = taskSlotTable.getTask(executionAttemptID);

		if (task != null) {
			try {
				task.stopExecution();
				return CompletableFuture.completedFuture(Acknowledge.get());
			} catch (Throwable t) {
				return FutureUtils.completedExceptionally(new TaskException("Cannot stop task for execution " + executionAttemptID + '.', t));
			}
		} else {
			final String message = "Cannot find task to stop for execution " + executionAttemptID + '.';

			log.debug(message);
			return FutureUtils.completedExceptionally(new TaskException(message));
		}
	}

	// ----------------------------------------------------------------------
	// Partition lifecycle RPCs
	// ----------------------------------------------------------------------

	@Override
	public CompletableFuture<Acknowledge> updatePartitions(
			final ExecutionAttemptID executionAttemptID,
			Iterable<PartitionInfo> partitionInfos,
			Time timeout) {
		final Task task = taskSlotTable.getTask(executionAttemptID);

		if (task != null) {
			for (final PartitionInfo partitionInfo: partitionInfos) {
				IntermediateDataSetID intermediateResultPartitionID = partitionInfo.getIntermediateDataSetID();

				final SingleInputGate singleInputGate = task.getInputGateById(intermediateResultPartitionID);

				if (singleInputGate != null) {
					// Run asynchronously because it might be blocking
					getRpcService().execute(
						() -> {
							try {
								singleInputGate.updateInputChannel(partitionInfo.getInputChannelDeploymentDescriptor());
							} catch (IOException | InterruptedException e) {
								log.error("Could not update input data location for task {}. Trying to fail task.", task.getTaskInfo().getTaskName(), e);

								try {
									task.failExternally(e);
								} catch (RuntimeException re) {
									// TODO: Check whether we need this or make exception in failExtenally checked
									log.error("Failed canceling task with execution ID {} after task update failure.", executionAttemptID, re);
								}
							}
						});
				} else {
					return FutureUtils.completedExceptionally(
						new PartitionException("No reader with ID " + intermediateResultPartitionID +
							" for task " + executionAttemptID + " was found."));
				}
			}

			return CompletableFuture.completedFuture(Acknowledge.get());
		} else {
			log.debug("Discard update for input partitions of task {}. Task is no longer running.", executionAttemptID);
			return CompletableFuture.completedFuture(Acknowledge.get());
		}
	}

	@Override
	public void failPartition(ExecutionAttemptID executionAttemptID) {
		log.info("Discarding the results produced by task execution {}.", executionAttemptID);

		try {
			networkEnvironment.getResultPartitionManager().releasePartitionsProducedBy(executionAttemptID);
		} catch (Throwable t) {
			// TODO: Do we still need this catch branch?
			onFatalError(t);
		}

		// TODO: Maybe it's better to return an Acknowledge here to notify the JM about the success/failure with an Exception
	}

	// ----------------------------------------------------------------------
	// Heartbeat RPC
	// ----------------------------------------------------------------------

	@Override
	public void heartbeatFromJobManager(ResourceID resourceID) {
		jobManagerHeartbeatManager.requestHeartbeat(resourceID, null);
	}

	@Override
	public void heartbeatFromResourceManager(ResourceID resourceID) {
		resourceManagerHeartbeatManager.requestHeartbeat(resourceID, null);
	}

	// ----------------------------------------------------------------------
	// Checkpointing RPCs
	// ----------------------------------------------------------------------

	@Override
	public CompletableFuture<Acknowledge> triggerCheckpoint(
			ExecutionAttemptID executionAttemptID,
			long checkpointId,
			long checkpointTimestamp,
			CheckpointOptions checkpointOptions) {
		log.debug("Trigger checkpoint {}@{} for {}.", checkpointId, checkpointTimestamp, executionAttemptID);

		final Task task = taskSlotTable.getTask(executionAttemptID);

		if (task != null) {
			task.triggerCheckpointBarrier(checkpointId, checkpointTimestamp, checkpointOptions);

			return CompletableFuture.completedFuture(Acknowledge.get());
		} else {
			final String message = "TaskManager received a checkpoint request for unknown task " + executionAttemptID + '.';

			log.debug(message);
			return FutureUtils.completedExceptionally(new CheckpointException(message));
		}
	}

	@Override
	public CompletableFuture<Acknowledge> confirmCheckpoint(
			ExecutionAttemptID executionAttemptID,
			long checkpointId,
			long checkpointTimestamp) {
		log.debug("Confirm checkpoint {}@{} for {}.", checkpointId, checkpointTimestamp, executionAttemptID);

		final Task task = taskSlotTable.getTask(executionAttemptID);

		if (task != null) {
			task.notifyCheckpointComplete(checkpointId);

			return CompletableFuture.completedFuture(Acknowledge.get());
		} else {
			final String message = "TaskManager received a checkpoint confirmation for unknown task " + executionAttemptID + '.';

			log.debug(message);
			return FutureUtils.completedExceptionally(new CheckpointException(message));
		}
	}

	// ----------------------------------------------------------------------
	// Slot allocation RPCs
	// ----------------------------------------------------------------------

	@Override
	public CompletableFuture<Acknowledge> requestSlot(
		final SlotID slotId,
		final JobID jobId,
		final AllocationID allocationId,
		final String targetAddress,
		final ResourceManagerId resourceManagerId,
		final Time timeout) {
		// TODO: Filter invalid requests from the resource manager by using the instance/registration Id

		log.info("Receive slot request {} for job {} from resource manager with leader id {}.",
			allocationId, jobId, resourceManagerId);

		try {
			if (resourceManagerConnection == null) {
				final String message = "TaskManager is not connected to a resource manager.";
				log.debug(message);
				throw new SlotAllocationException(message);
			}

			if (!Objects.equals(resourceManagerConnection.getTargetLeaderId(), resourceManagerId)) {
				final String message = "The leader id " + resourceManagerId +
					" does not match with the leader id of the connected resource manager " +
					resourceManagerConnection.getTargetLeaderId() + '.';

				log.debug(message);
				throw new SlotAllocationException(message);
			}

			if (taskSlotTable.isSlotFree(slotId.getSlotNumber())) {
				if (taskSlotTable.allocateSlot(slotId.getSlotNumber(), jobId, allocationId, taskManagerConfiguration.getTimeout())) {
					log.info("Allocated slot for {}.", allocationId);
				} else {
					log.info("Could not allocate slot for {}.", allocationId);
					throw new SlotAllocationException("Could not allocate slot.");
				}
			} else if (!taskSlotTable.isAllocated(slotId.getSlotNumber(), jobId, allocationId)) {
				final String message = "The slot " + slotId + " has already been allocated for a different job.";

				log.info(message);

				throw new SlotOccupiedException(message, taskSlotTable.getCurrentAllocation(slotId.getSlotNumber()));
			}

			if (jobManagerTable.contains(jobId)) {
				offerSlotsToJobManager(jobId);
			} else {
				try {
					jobLeaderService.addJob(jobId, targetAddress);
				} catch (Exception e) {
					// free the allocated slot
					try {
						taskSlotTable.freeSlot(allocationId);
					} catch (SlotNotFoundException slotNotFoundException) {
						// slot no longer existent, this should actually never happen, because we've
						// just allocated the slot. So let's fail hard in this case!
						onFatalError(slotNotFoundException);
					}

					// release local state under the allocation id.
					localStateStoresManager.releaseLocalStateForAllocationId(allocationId);

					// sanity check
					if (!taskSlotTable.isSlotFree(slotId.getSlotNumber())) {
						onFatalError(new Exception("Could not free slot " + slotId));
					}

					throw new SlotAllocationException("Could not add job to job leader service.", e);
				}
			}
		} catch (SlotAllocationException slotAllocationException) {
			return FutureUtils.completedExceptionally(slotAllocationException);
		}

		return CompletableFuture.completedFuture(Acknowledge.get());
	}

	@Override
	public CompletableFuture<Acknowledge> freeSlot(AllocationID allocationId, Throwable cause, Time timeout) {
		freeSlotInternal(allocationId, cause);

		return CompletableFuture.completedFuture(Acknowledge.get());
	}

	@Override
	public CompletableFuture<TransientBlobKey> requestFileUpload(FileType fileType, Time timeout) {
		log.debug("Request file {} upload.", fileType);

		final String filePath;

		switch (fileType) {
			case LOG:
				filePath = taskManagerConfiguration.getTaskManagerLogPath();
				break;
			case STDOUT:
				filePath = taskManagerConfiguration.getTaskManagerStdoutPath();
				break;
			default:
				filePath = null;
		}

		if (filePath != null && !filePath.isEmpty()) {
			final File file = new File(filePath);

			if (file.exists()) {
				final TransientBlobCache transientBlobService = blobCacheService.getTransientBlobService();
				final TransientBlobKey transientBlobKey;
				try (FileInputStream fileInputStream = new FileInputStream(file)) {
					transientBlobKey = transientBlobService.putTransient(fileInputStream);
				} catch (IOException e) {
					log.debug("Could not upload file {}.", fileType, e);
					return FutureUtils.completedExceptionally(new FlinkException("Could not upload file " + fileType + '.', e));
				}

				return CompletableFuture.completedFuture(transientBlobKey);
			} else {
				log.debug("The file {} does not exist on the TaskExecutor {}.", fileType, getResourceID());
				return FutureUtils.completedExceptionally(new FlinkException("The file " + fileType + " does not exist on the TaskExecutor."));
			}
		} else {
			log.debug("The file {} is unavailable on the TaskExecutor {}.", fileType, getResourceID());
			return FutureUtils.completedExceptionally(new FlinkException("The file " + fileType + " is not available on the TaskExecutor."));
		}
	}

	// ----------------------------------------------------------------------
	// Disconnection RPCs
	// ----------------------------------------------------------------------

	@Override
	public void disconnectJobManager(JobID jobId, Exception cause) {
		closeJobManagerConnection(jobId, cause);
		jobLeaderService.reconnect(jobId);
	}

	@Override
	public void disconnectResourceManager(Exception cause) {
		closeResourceManagerConnection(cause);
	}

	// ======================================================================
	//  Internal methods
	// ======================================================================

	// ------------------------------------------------------------------------
	//  Internal resource manager connection methods
	// ------------------------------------------------------------------------

	private void notifyOfNewResourceManagerLeader(String newLeaderAddress, ResourceManagerId newResourceManagerId) {
		if (resourceManagerConnection != null) {
			if (newLeaderAddress != null) {
				// the resource manager switched to a new leader
				log.info("ResourceManager leader changed from {} to {}. Registering at new leader.",
					resourceManagerConnection.getTargetAddress(), newLeaderAddress);
			}
			else {
				// address null means that the current leader is lost without a new leader being there, yet
				log.info("Current ResourceManager {} lost leader status. Waiting for new ResourceManager leader.",
					resourceManagerConnection.getTargetAddress());
			}

			// drop the current connection or connection attempt
			closeResourceManagerConnection(
				new FlinkException("New ResourceManager leader found under: " + newLeaderAddress +
					'(' + newResourceManagerId + ')'));
		}

		// establish a connection to the new leader
		if (newLeaderAddress != null) {
			log.info("Attempting to register at ResourceManager {} ({})", newLeaderAddress, newResourceManagerId);
			resourceManagerConnection =
				new TaskExecutorToResourceManagerConnection(
					log,
					getRpcService(),
					getAddress(),
					getResourceID(),
					taskSlotTable.createSlotReport(getResourceID()),
					taskManagerLocation.dataPort(),
					hardwareDescription,
					newLeaderAddress,
					newResourceManagerId,
					getMainThreadExecutor(),
					new ResourceManagerRegistrationListener());
			resourceManagerConnection.start();
		}
	}

	private void establishResourceManagerConnection(
			ResourceID resourceManagerResourceId,
			ClusterInformation clusterInformation) {
		// monitor the resource manager as heartbeat target
		resourceManagerHeartbeatManager.monitorTarget(resourceManagerResourceId, new HeartbeatTarget<SlotReport>() {
			@Override
			public void receiveHeartbeat(ResourceID resourceID, SlotReport slotReport) {
				ResourceManagerGateway resourceManagerGateway = resourceManagerConnection.getTargetGateway();
				resourceManagerGateway.heartbeatFromTaskManager(resourceID, slotReport);
			}

			@Override
			public void requestHeartbeat(ResourceID resourceID, SlotReport slotReport) {
				// the TaskManager won't send heartbeat requests to the ResourceManager
			}
		});

		// set the propagated blob server address
		final InetSocketAddress blobServerAddress = new InetSocketAddress(
			clusterInformation.getBlobServerHostname(),
			clusterInformation.getBlobServerPort());

		blobCacheService.setBlobServerAddress(blobServerAddress);
	}

	private void closeResourceManagerConnection(Exception cause) {
		if (resourceManagerConnection != null) {

			if (resourceManagerConnection.isConnected()) {
				if (log.isDebugEnabled()) {
					log.debug("Close ResourceManager connection {}.",
						resourceManagerConnection.getResourceManagerId(), cause);
				} else {
					log.info("Close ResourceManager connection {}.",
						resourceManagerConnection.getResourceManagerId());
				}
				resourceManagerHeartbeatManager.unmonitorTarget(resourceManagerConnection.getResourceManagerId());

				ResourceManagerGateway resourceManagerGateway = resourceManagerConnection.getTargetGateway();
				resourceManagerGateway.disconnectTaskManager(getResourceID(), cause);
			} else {
				if (log.isDebugEnabled()) {
					log.debug("Terminating registration attempts towards ResourceManager {}.",
						resourceManagerConnection.getTargetAddress(), cause);
				} else {
					log.info("Terminating registration attempts towards ResourceManager {}.",
						resourceManagerConnection.getTargetAddress());
				}
			}

			resourceManagerConnection.close();
			resourceManagerConnection = null;
		}
	}

	// ------------------------------------------------------------------------
	//  Internal job manager connection methods
	// ------------------------------------------------------------------------

	private void offerSlotsToJobManager(final JobID jobId) {
		final JobManagerConnection jobManagerConnection = jobManagerTable.get(jobId);

		if (jobManagerConnection == null) {
			log.debug("There is no job manager connection to the leader of job {}.", jobId);
		} else {
			if (taskSlotTable.hasAllocatedSlots(jobId)) {
				log.info("Offer reserved slots to the leader of job {}.", jobId);

				final JobMasterGateway jobMasterGateway = jobManagerConnection.getJobManagerGateway();

				final Iterator<TaskSlot> reservedSlotsIterator = taskSlotTable.getAllocatedSlots(jobId);
				final JobMasterId jobMasterId = jobManagerConnection.getJobMasterId();

				final Collection<SlotOffer> reservedSlots = new HashSet<>(2);

				while (reservedSlotsIterator.hasNext()) {
					SlotOffer offer = reservedSlotsIterator.next().generateSlotOffer();
					try {
						if (!taskSlotTable.markSlotActive(offer.getAllocationId())) {
							// the slot is either free or releasing at the moment
							final String message = "Could not mark slot " + jobId + " active.";
							log.debug(message);
							jobMasterGateway.failSlot(getResourceID(), offer.getAllocationId(), new Exception(message));
						}
					} catch (SlotNotFoundException e) {
						final String message = "Could not mark slot " + jobId + " active.";
						jobMasterGateway.failSlot(getResourceID(), offer.getAllocationId(), new Exception(message));
						continue;
					}
					reservedSlots.add(offer);
				}

				CompletableFuture<Collection<SlotOffer>> acceptedSlotsFuture = jobMasterGateway.offerSlots(
					getResourceID(),
					reservedSlots,
					taskManagerConfiguration.getTimeout());

				acceptedSlotsFuture.whenCompleteAsync(
					(Iterable<SlotOffer> acceptedSlots, Throwable throwable) -> {
						if (throwable != null) {
							if (throwable instanceof TimeoutException) {
								log.info("Slot offering to JobManager did not finish in time. Retrying the slot offering.");
								// We ran into a timeout. Try again.
								offerSlotsToJobManager(jobId);
							} else {
								log.warn("Slot offering to JobManager failed. Freeing the slots " +
									"and returning them to the ResourceManager.", throwable);

								// We encountered an exception. Free the slots and return them to the RM.
								for (SlotOffer reservedSlot: reservedSlots) {
									freeSlotInternal(reservedSlot.getAllocationId(), throwable);
								}
							}
						} else {
							// check if the response is still valid
							if (isJobManagerConnectionValid(jobId, jobMasterId)) {
								// mark accepted slots active
								for (SlotOffer acceptedSlot : acceptedSlots) {
									reservedSlots.remove(acceptedSlot);
								}

								final Exception e = new Exception("The slot was rejected by the JobManager.");

								for (SlotOffer rejectedSlot : reservedSlots) {
									freeSlotInternal(rejectedSlot.getAllocationId(), e);
								}
							} else {
								// discard the response since there is a new leader for the job
								log.debug("Discard offer slot response since there is a new leader " +
									"for the job {}.", jobId);
							}
						}
					},
					getMainThreadExecutor());

			} else {
				log.debug("There are no unassigned slots for the job {}.", jobId);
			}
		}
	}

	private void establishJobManagerConnection(JobID jobId, final JobMasterGateway jobMasterGateway, JMTMRegistrationSuccess registrationSuccess) {

		if (jobManagerTable.contains(jobId)) {
			JobManagerConnection oldJobManagerConnection = jobManagerTable.get(jobId);

			if (Objects.equals(oldJobManagerConnection.getJobMasterId(), jobMasterGateway.getFencingToken())) {
				// we already are connected to the given job manager
				log.debug("Ignore JobManager gained leadership message for {} because we are already connected to it.", jobMasterGateway.getFencingToken());
				return;
			} else {
				closeJobManagerConnection(jobId, new Exception("Found new job leader for job id " + jobId + '.'));
			}
		}

		log.info("Establish JobManager connection for job {}.", jobId);

		ResourceID jobManagerResourceID = registrationSuccess.getResourceID();
		JobManagerConnection newJobManagerConnection = associateWithJobManager(
				jobId,
				jobManagerResourceID,
				jobMasterGateway);
		jobManagerConnections.put(jobManagerResourceID, newJobManagerConnection);
		jobManagerTable.put(jobId, newJobManagerConnection);

		// monitor the job manager as heartbeat target
		jobManagerHeartbeatManager.monitorTarget(jobManagerResourceID, new HeartbeatTarget<AccumulatorReport>() {
			@Override
			public void receiveHeartbeat(ResourceID resourceID, AccumulatorReport payload) {
				jobMasterGateway.heartbeatFromTaskManager(resourceID, payload);
			}

			@Override
			public void requestHeartbeat(ResourceID resourceID, AccumulatorReport payload) {
				// request heartbeat will never be called on the task manager side
			}
		});

		offerSlotsToJobManager(jobId);
	}

	private void closeJobManagerConnection(JobID jobId, Exception cause) {
		if (log.isDebugEnabled()) {
			log.debug("Close JobManager connection for job {}.", jobId, cause);
		} else {
			log.info("Close JobManager connection for job {}.", jobId);
		}

		// 1. fail tasks running under this JobID
		Iterator<Task> tasks = taskSlotTable.getTasks(jobId);

		final FlinkException failureCause = new FlinkException("JobManager responsible for " + jobId +
			" lost the leadership.", cause);

		while (tasks.hasNext()) {
			tasks.next().failExternally(failureCause);
		}

		// 2. Move the active slots to state allocated (possible to time out again)
		Iterator<AllocationID> activeSlots = taskSlotTable.getActiveSlots(jobId);

		final FlinkException freeingCause = new FlinkException("Slot could not be marked inactive.");

		while (activeSlots.hasNext()) {
			AllocationID activeSlot = activeSlots.next();

			try {
				if (!taskSlotTable.markSlotInactive(activeSlot, taskManagerConfiguration.getTimeout())) {
					freeSlotInternal(activeSlot, freeingCause);
				}
			} catch (SlotNotFoundException e) {
				log.debug("Could not mark the slot {} inactive.", jobId, e);
			}
		}

		// 3. Disassociate from the JobManager
		JobManagerConnection jobManagerConnection = jobManagerTable.remove(jobId);

		if (jobManagerConnection != null) {
			try {
				jobManagerHeartbeatManager.unmonitorTarget(jobManagerConnection.getResourceID());

				jobManagerConnections.remove(jobManagerConnection.getResourceID());
				disassociateFromJobManager(jobManagerConnection, cause);
			} catch (IOException e) {
				log.warn("Could not properly disassociate from JobManager {}.",
					jobManagerConnection.getJobManagerGateway().getAddress(), e);
			}
		}
	}

	private JobManagerConnection associateWithJobManager(
			JobID jobID,
			ResourceID resourceID,
			JobMasterGateway jobMasterGateway) {
		checkNotNull(jobID);
		checkNotNull(resourceID);
		checkNotNull(jobMasterGateway);

		TaskManagerActions taskManagerActions = new TaskManagerActionsImpl(jobMasterGateway);

		CheckpointResponder checkpointResponder = new RpcCheckpointResponder(jobMasterGateway);

		final LibraryCacheManager libraryCacheManager = new BlobLibraryCacheManager(
			blobCacheService.getPermanentBlobService(),
			taskManagerConfiguration.getClassLoaderResolveOrder(),
			taskManagerConfiguration.getAlwaysParentFirstLoaderPatterns());

		ResultPartitionConsumableNotifier resultPartitionConsumableNotifier = new RpcResultPartitionConsumableNotifier(
			jobMasterGateway,
			getRpcService().getExecutor(),
			taskManagerConfiguration.getTimeout());

		PartitionProducerStateChecker partitionStateChecker = new RpcPartitionStateChecker(jobMasterGateway);

		registerQueryableState(jobID, jobMasterGateway);

		return new JobManagerConnection(
			jobID,
			resourceID,
			jobMasterGateway,
			taskManagerActions,
			checkpointResponder,
			libraryCacheManager,
			resultPartitionConsumableNotifier,
			partitionStateChecker);
	}

	private void disassociateFromJobManager(JobManagerConnection jobManagerConnection, Exception cause) throws IOException {
		checkNotNull(jobManagerConnection);

		final KvStateRegistry kvStateRegistry = networkEnvironment.getKvStateRegistry();

		if (kvStateRegistry != null) {
			kvStateRegistry.unregisterListener(jobManagerConnection.getJobID());
		}

		final KvStateClientProxy kvStateClientProxy = networkEnvironment.getKvStateProxy();

		if (kvStateClientProxy != null) {
			kvStateClientProxy.updateKvStateLocationOracle(jobManagerConnection.getJobID(), null);
		}

		JobMasterGateway jobManagerGateway = jobManagerConnection.getJobManagerGateway();
		jobManagerGateway.disconnectTaskManager(getResourceID(), cause);
		jobManagerConnection.getLibraryCacheManager().shutdown();
	}

	private void registerQueryableState(JobID jobId, JobMasterGateway jobMasterGateway) {
		final KvStateServer kvStateServer = networkEnvironment.getKvStateServer();
		final KvStateRegistry kvStateRegistry = networkEnvironment.getKvStateRegistry();

		if (kvStateServer != null && kvStateRegistry != null) {
			kvStateRegistry.registerListener(
				jobId,
				new RpcKvStateRegistryListener(
					jobMasterGateway,
					kvStateServer.getServerAddress()));
		}

		final KvStateClientProxy kvStateProxy = networkEnvironment.getKvStateProxy();

		if (kvStateProxy != null) {
			kvStateProxy.updateKvStateLocationOracle(jobId, jobMasterGateway);
		}
	}

	// ------------------------------------------------------------------------
	//  Internal task methods
	// ------------------------------------------------------------------------

	private void failTask(final ExecutionAttemptID executionAttemptID, final Throwable cause) {
		final Task task = taskSlotTable.getTask(executionAttemptID);

		if (task != null) {
			try {
				task.failExternally(cause);
			} catch (Throwable t) {
				log.error("Could not fail task {}.", executionAttemptID, t);
			}
		} else {
			log.debug("Cannot find task to fail for execution {}.", executionAttemptID);
		}
	}

	private void updateTaskExecutionState(
			final JobMasterGateway jobMasterGateway,
			final TaskExecutionState taskExecutionState) {
		final ExecutionAttemptID executionAttemptID = taskExecutionState.getID();

		CompletableFuture<Acknowledge> futureAcknowledge = jobMasterGateway.updateTaskExecutionState(taskExecutionState);

		futureAcknowledge.whenCompleteAsync(
			(ack, throwable) -> {
				if (throwable != null) {
					failTask(executionAttemptID, throwable);
				}
			},
			getMainThreadExecutor());
	}

	private void unregisterTaskAndNotifyFinalState(
			final JobMasterGateway jobMasterGateway,
			final ExecutionAttemptID executionAttemptID) {

		Task task = taskSlotTable.removeTask(executionAttemptID);
		if (task != null) {
			if (!task.getExecutionState().isTerminal()) {
				try {
					task.failExternally(new IllegalStateException("Task is being remove from TaskManager."));
				} catch (Exception e) {
					log.error("Could not properly fail task.", e);
				}
			}

			log.info("Un-registering task and sending final execution state {} to JobManager for task {} {}.",
				task.getExecutionState(), task.getTaskInfo().getTaskName(), task.getExecutionId());

			AccumulatorSnapshot accumulatorSnapshot = task.getAccumulatorRegistry().getSnapshot();

			updateTaskExecutionState(
					jobMasterGateway,
					new TaskExecutionState(
						task.getJobID(),
						task.getExecutionId(),
						task.getExecutionState(),
						task.getFailureCause(),
						accumulatorSnapshot,
						task.getMetricGroup().getIOMetricGroup().createSnapshot()));
		} else {
			log.error("Cannot find task with ID {} to unregister.", executionAttemptID);
		}
	}

	private void freeSlotInternal(AllocationID allocationId, Throwable cause) {
		checkNotNull(allocationId);

		log.debug("Free slot with allocation id {} because: {}", allocationId, cause.getMessage());

		try {
			final JobID jobId = taskSlotTable.getOwningJob(allocationId);

			final int slotIndex = taskSlotTable.freeSlot(allocationId, cause);

			if (slotIndex != -1) {

				if (isConnectedToResourceManager()) {
					// the slot was freed. Tell the RM about it
					ResourceManagerGateway resourceManagerGateway = resourceManagerConnection.getTargetGateway();

					resourceManagerGateway.notifySlotAvailable(
						resourceManagerConnection.getRegistrationId(),
						new SlotID(getResourceID(), slotIndex),
						allocationId);
				}

				if (jobId != null) {
					// check whether we still have allocated slots for the same job
					if (taskSlotTable.getAllocationIdsPerJob(jobId).isEmpty()) {
						// we can remove the job from the job leader service
						try {
							jobLeaderService.removeJob(jobId);
						} catch (Exception e) {
							log.info("Could not remove job {} from JobLeaderService.", jobId, e);
						}

						closeJobManagerConnection(
							jobId,
							new FlinkException("TaskExecutor " + getAddress() +
								" has no more allocated slots for job " + jobId + '.'));
					}
				}
			}
		} catch (SlotNotFoundException e) {
			log.debug("Could not free slot for allocation id {}.", allocationId, e);
		}

		localStateStoresManager.releaseLocalStateForAllocationId(allocationId);
	}

	private void timeoutSlot(AllocationID allocationId, UUID ticket) {
		checkNotNull(allocationId);
		checkNotNull(ticket);

		if (taskSlotTable.isValidTimeout(allocationId, ticket)) {
			freeSlotInternal(allocationId, new Exception("The slot " + allocationId + " has timed out."));
		} else {
			log.debug("Received an invalid timeout for allocation id {} with ticket {}.", allocationId, ticket);
		}
	}

	// ------------------------------------------------------------------------
	//  Internal utility methods
	// ------------------------------------------------------------------------

	private boolean isConnectedToResourceManager() {
		return (resourceManagerConnection != null && resourceManagerConnection.isConnected());
	}

	private boolean isJobManagerConnectionValid(JobID jobId, JobMasterId jobMasterId) {
		JobManagerConnection jmConnection = jobManagerTable.get(jobId);

		return jmConnection != null && Objects.equals(jmConnection.getJobMasterId(), jobMasterId);
	}

	// ------------------------------------------------------------------------
	//  Properties
	// ------------------------------------------------------------------------

	public ResourceID getResourceID() {
		return taskManagerLocation.getResourceID();
	}

	// ------------------------------------------------------------------------
	//  Error Handling
	// ------------------------------------------------------------------------

	/**
	 * Notifies the TaskExecutor that a fatal error has occurred and it cannot proceed.
	 *
	 * @param t The exception describing the fatal error
	 */
	void onFatalError(final Throwable t) {
		try {
			log.error("Fatal error occurred in TaskExecutor {}.", getAddress(), t);
		} catch (Throwable ignored) {}

		// The fatal error handler implementation should make sure that this call is non-blocking
		fatalErrorHandler.onFatalError(t);
	}

	// ------------------------------------------------------------------------
	//  Access to fields for testing
	// ------------------------------------------------------------------------

	@VisibleForTesting
	TaskExecutorToResourceManagerConnection getResourceManagerConnection() {
		return resourceManagerConnection;
	}

	@VisibleForTesting
	HeartbeatManager<Void, SlotReport> getResourceManagerHeartbeatManager() {
		return resourceManagerHeartbeatManager;
	}

	// ------------------------------------------------------------------------
	//  Utility classes
	// ------------------------------------------------------------------------

	/**
	 * The listener for leader changes of the resource manager.
	 */
	private final class ResourceManagerLeaderListener implements LeaderRetrievalListener {

		@Override
		public void notifyLeaderAddress(final String leaderAddress, final UUID leaderSessionID) {
			runAsync(
				() -> notifyOfNewResourceManagerLeader(
					leaderAddress,
					ResourceManagerId.fromUuidOrNull(leaderSessionID)));
		}

		@Override
		public void handleError(Exception exception) {
			onFatalError(exception);
		}
	}

	private final class JobLeaderListenerImpl implements JobLeaderListener {

		@Override
		public void jobManagerGainedLeadership(
			final JobID jobId,
			final JobMasterGateway jobManagerGateway,
			final JMTMRegistrationSuccess registrationMessage) {
			runAsync(
				() ->
					establishJobManagerConnection(
						jobId,
						jobManagerGateway,
						registrationMessage));
		}

		@Override
		public void jobManagerLostLeadership(final JobID jobId, final JobMasterId jobMasterId) {
			log.info("JobManager for job {} with leader id {} lost leadership.", jobId, jobMasterId);

			runAsync(() ->
				closeJobManagerConnection(
					jobId,
					new Exception("Job leader for job id " + jobId + " lost leadership.")));
		}

		@Override
		public void handleError(Throwable throwable) {
			onFatalError(throwable);
		}
	}

	private final class ResourceManagerRegistrationListener implements RegistrationConnectionListener<TaskExecutorRegistrationSuccess> {

		@Override
		public void onRegistrationSuccess(TaskExecutorRegistrationSuccess success) {
			final ResourceID resourceManagerId = success.getResourceManagerId();
			final ClusterInformation clusterInformation = success.getClusterInformation();

			runAsync(
				() -> establishResourceManagerConnection(resourceManagerId, clusterInformation)
			);
		}

		@Override
		public void onRegistrationFailure(Throwable failure) {
			onFatalError(failure);
		}
	}

	private final class TaskManagerActionsImpl implements TaskManagerActions {
		private final JobMasterGateway jobMasterGateway;

		private TaskManagerActionsImpl(JobMasterGateway jobMasterGateway) {
			this.jobMasterGateway = checkNotNull(jobMasterGateway);
		}

		@Override
		public void notifyFinalState(final ExecutionAttemptID executionAttemptID) {
			runAsync(() -> unregisterTaskAndNotifyFinalState(jobMasterGateway, executionAttemptID));
		}

		@Override
		public void notifyFatalError(String message, Throwable cause) {
			try {
				log.error(message, cause);
			} catch (Throwable ignored) {}

			// The fatal error handler implementation should make sure that this call is non-blocking
			fatalErrorHandler.onFatalError(cause);
		}

		@Override
		public void failTask(final ExecutionAttemptID executionAttemptID, final Throwable cause) {
			runAsync(() -> TaskExecutor.this.failTask(executionAttemptID, cause));
		}

		@Override
		public void updateTaskExecutionState(final TaskExecutionState taskExecutionState) {
			TaskExecutor.this.updateTaskExecutionState(jobMasterGateway, taskExecutionState);
		}
	}

	private class SlotActionsImpl implements SlotActions {

		@Override
		public void freeSlot(final AllocationID allocationId) {
			runAsync(() ->
				freeSlotInternal(
					allocationId,
					new FlinkException("TaskSlotTable requested freeing the TaskSlot " + allocationId + '.')));
		}

		@Override
		public void timeoutSlot(final AllocationID allocationId, final UUID ticket) {
			runAsync(() -> TaskExecutor.this.timeoutSlot(allocationId, ticket));
		}
	}

	private class JobManagerHeartbeatListener implements HeartbeatListener<Void, AccumulatorReport> {

		@Override
		public void notifyHeartbeatTimeout(final ResourceID resourceID) {
			runAsync(() -> {
				log.info("The heartbeat of JobManager with id {} timed out.", resourceID);

				if (jobManagerConnections.containsKey(resourceID)) {
					JobManagerConnection jobManagerConnection = jobManagerConnections.get(resourceID);

					if (jobManagerConnection != null) {
						closeJobManagerConnection(
							jobManagerConnection.getJobID(),
							new TimeoutException("The heartbeat of JobManager with id " + resourceID + " timed out."));

						jobLeaderService.reconnect(jobManagerConnection.getJobID());
					}
				}
			});
		}

		@Override
		public void reportPayload(ResourceID resourceID, Void payload) {
			// nothing to do since the payload is of type Void
		}

		@Override
		public CompletableFuture<AccumulatorReport> retrievePayload(ResourceID resourceID) {
			validateRunsInMainThread();
			JobManagerConnection jobManagerConnection = jobManagerConnections.get(resourceID);
			if (jobManagerConnection != null) {
				JobID jobId = jobManagerConnection.getJobID();

				List<AccumulatorSnapshot> accumulatorSnapshots = new ArrayList<>(16);
				Iterator<Task> allTasks = taskSlotTable.getTasks(jobId);

				while (allTasks.hasNext()) {
					Task task = allTasks.next();
					accumulatorSnapshots.add(task.getAccumulatorRegistry().getSnapshot());
				}
				return CompletableFuture.completedFuture(new AccumulatorReport(accumulatorSnapshots));
			} else {
				return CompletableFuture.completedFuture(new AccumulatorReport(Collections.emptyList()));
			}
		}
	}

	private class ResourceManagerHeartbeatListener implements HeartbeatListener<Void, SlotReport> {

		@Override
		public void notifyHeartbeatTimeout(final ResourceID resourceId) {
			runAsync(() -> {
				// first check whether the timeout is still valid
				if (resourceManagerConnection != null && resourceManagerConnection.getResourceManagerId().equals(resourceId)) {
					log.info("The heartbeat of ResourceManager with id {} timed out.", resourceId);

					closeResourceManagerConnection(
						new TimeoutException(
							"The heartbeat of ResourceManager with id " + resourceId + " timed out."));
				} else {
					log.debug("Received heartbeat timeout for outdated ResourceManager id {}. Ignoring the timeout.", resourceId);
				}
			});
		}

		@Override
		public void reportPayload(ResourceID resourceID, Void payload) {
			// nothing to do since the payload is of type Void
		}

		@Override
		public CompletableFuture<SlotReport> retrievePayload(ResourceID resourceID) {
			return callAsync(
					() -> taskSlotTable.createSlotReport(getResourceID()),
					taskManagerConfiguration.getTimeout());
		}
	}
}
