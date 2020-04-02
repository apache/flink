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
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.JobManagerTaskRestore;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
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
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionConsumableNotifier;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.TaskExecutorPartitionInfo;
import org.apache.flink.runtime.io.network.partition.TaskExecutorPartitionTracker;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;
import org.apache.flink.runtime.jobmaster.AllocatedSlotInfo;
import org.apache.flink.runtime.jobmaster.AllocatedSlotReport;
import org.apache.flink.runtime.jobmaster.JMTMRegistrationSuccess;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.ResourceManagerAddress;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.TaskBackPressureResponse;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.TaskNotRunningException;
import org.apache.flink.runtime.query.KvStateClientProxy;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.query.KvStateServer;
import org.apache.flink.runtime.registration.RegistrationConnectionListener;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.TaskExecutorRegistration;
import org.apache.flink.runtime.rest.messages.taskmanager.LogInfo;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcTimeout;
import org.apache.flink.runtime.rpc.akka.AkkaRpcServiceUtils;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleEnvironment;
import org.apache.flink.runtime.state.TaskExecutorLocalStateStoresManager;
import org.apache.flink.runtime.state.TaskLocalStateStore;
import org.apache.flink.runtime.state.TaskStateManager;
import org.apache.flink.runtime.state.TaskStateManagerImpl;
import org.apache.flink.runtime.taskexecutor.exceptions.RegistrationTimeoutException;
import org.apache.flink.runtime.taskexecutor.exceptions.SlotAllocationException;
import org.apache.flink.runtime.taskexecutor.exceptions.SlotOccupiedException;
import org.apache.flink.runtime.taskexecutor.exceptions.TaskException;
import org.apache.flink.runtime.taskexecutor.exceptions.TaskManagerException;
import org.apache.flink.runtime.taskexecutor.exceptions.TaskSubmissionException;
import org.apache.flink.runtime.taskexecutor.rpc.RpcCheckpointResponder;
import org.apache.flink.runtime.taskexecutor.rpc.RpcGlobalAggregateManager;
import org.apache.flink.runtime.taskexecutor.rpc.RpcInputSplitProvider;
import org.apache.flink.runtime.taskexecutor.rpc.RpcKvStateRegistryListener;
import org.apache.flink.runtime.taskexecutor.rpc.RpcPartitionStateChecker;
import org.apache.flink.runtime.taskexecutor.rpc.RpcResultPartitionConsumableNotifier;
import org.apache.flink.runtime.taskexecutor.rpc.RpcTaskOperatorEventGateway;
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
import org.apache.flink.runtime.taskmanager.UnresolvedTaskManagerLocation;
import org.apache.flink.types.SerializableOptional;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.StringUtils;

import org.apache.flink.shaded.guava18.com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

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
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

	/** The fatal error handler to use in case of a fatal error. */
	private final FatalErrorHandler fatalErrorHandler;

	private final BlobCacheService blobCacheService;

	/** The address to metric query service on this Task Manager. */
	private final String metricQueryServiceAddress;

	// --------- TaskManager services --------

	/** The connection information of this task manager. */
	private final UnresolvedTaskManagerLocation unresolvedTaskManagerLocation;

	private final TaskManagerMetricGroup taskManagerMetricGroup;

	/** The state manager for this task, providing state managers per slot. */
	private final TaskExecutorLocalStateStoresManager localStateStoresManager;

	/** The network component in the task manager. */
	private final ShuffleEnvironment<?, ?> shuffleEnvironment;

	/** The kvState registration service in the task manager. */
	private final KvStateService kvStateService;

	// --------- job manager connections -----------

	private final Map<ResourceID, JobManagerConnection> jobManagerConnections;

	// --------- task slot allocation table -----------

	private final TaskSlotTable<Task> taskSlotTable;

	private final JobManagerTable jobManagerTable;

	private final JobLeaderService jobLeaderService;

	private final LeaderRetrievalService resourceManagerLeaderRetriever;

	// ------------------------------------------------------------------------

	private final HardwareDescription hardwareDescription;

	private FileCache fileCache;

	/** The heartbeat manager for job manager in the task manager. */
	private final HeartbeatManager<AllocatedSlotReport, AccumulatorReport> jobManagerHeartbeatManager;

	/** The heartbeat manager for resource manager in the task manager. */
	private final HeartbeatManager<Void, TaskExecutorHeartbeatPayload> resourceManagerHeartbeatManager;

	private final TaskExecutorPartitionTracker partitionTracker;

	private final BackPressureSampleService backPressureSampleService;

	// --------- resource manager --------

	@Nullable
	private ResourceManagerAddress resourceManagerAddress;

	@Nullable
	private EstablishedResourceManagerConnection establishedResourceManagerConnection;

	@Nullable
	private TaskExecutorToResourceManagerConnection resourceManagerConnection;

	@Nullable
	private UUID currentRegistrationTimeoutId;

	private Map<JobID, Collection<CompletableFuture<ExecutionState>>> taskResultPartitionCleanupFuturesPerJob = new HashMap<>(8);

	public TaskExecutor(
			RpcService rpcService,
			TaskManagerConfiguration taskManagerConfiguration,
			HighAvailabilityServices haServices,
			TaskManagerServices taskExecutorServices,
			HeartbeatServices heartbeatServices,
			TaskManagerMetricGroup taskManagerMetricGroup,
			String metricQueryServiceAddress,
			BlobCacheService blobCacheService,
			FatalErrorHandler fatalErrorHandler,
			TaskExecutorPartitionTracker partitionTracker,
			BackPressureSampleService backPressureSampleService) {

		super(rpcService, AkkaRpcServiceUtils.createRandomName(TASK_MANAGER_NAME));

		checkArgument(taskManagerConfiguration.getNumberSlots() > 0, "The number of slots has to be larger than 0.");

		this.taskManagerConfiguration = checkNotNull(taskManagerConfiguration);
		this.taskExecutorServices = checkNotNull(taskExecutorServices);
		this.haServices = checkNotNull(haServices);
		this.fatalErrorHandler = checkNotNull(fatalErrorHandler);
		this.partitionTracker = partitionTracker;
		this.taskManagerMetricGroup = checkNotNull(taskManagerMetricGroup);
		this.blobCacheService = checkNotNull(blobCacheService);
		this.metricQueryServiceAddress = checkNotNull(metricQueryServiceAddress);
		this.backPressureSampleService = checkNotNull(backPressureSampleService);

		this.taskSlotTable = taskExecutorServices.getTaskSlotTable();
		this.jobManagerTable = taskExecutorServices.getJobManagerTable();
		this.jobLeaderService = taskExecutorServices.getJobLeaderService();
		this.unresolvedTaskManagerLocation = taskExecutorServices.getUnresolvedTaskManagerLocation();
		this.localStateStoresManager = taskExecutorServices.getTaskManagerStateStore();
		this.shuffleEnvironment = taskExecutorServices.getShuffleEnvironment();
		this.kvStateService = taskExecutorServices.getKvStateService();
		this.resourceManagerLeaderRetriever = haServices.getResourceManagerLeaderRetriever();

		this.jobManagerConnections = new HashMap<>(4);

		this.hardwareDescription = HardwareDescription.extractFromSystem(taskExecutorServices.getManagedMemorySize());

		this.resourceManagerAddress = null;
		this.resourceManagerConnection = null;
		this.currentRegistrationTimeoutId = null;

		final ResourceID resourceId = taskExecutorServices.getUnresolvedTaskManagerLocation().getResourceID();
		this.jobManagerHeartbeatManager = createJobManagerHeartbeatManager(heartbeatServices, resourceId);
		this.resourceManagerHeartbeatManager = createResourceManagerHeartbeatManager(heartbeatServices, resourceId);
	}

	private HeartbeatManager<Void, TaskExecutorHeartbeatPayload> createResourceManagerHeartbeatManager(HeartbeatServices heartbeatServices, ResourceID resourceId) {
		return heartbeatServices.createHeartbeatManager(
			resourceId,
			new ResourceManagerHeartbeatListener(),
			getMainThreadExecutor(),
			log);
	}

	private HeartbeatManager<AllocatedSlotReport, AccumulatorReport> createJobManagerHeartbeatManager(HeartbeatServices heartbeatServices, ResourceID resourceId) {
		return heartbeatServices.createHeartbeatManager(
			resourceId,
			new JobManagerHeartbeatListener(),
			getMainThreadExecutor(),
			log);
	}

	@Override
	public CompletableFuture<Boolean> canBeReleased() {
		return CompletableFuture.completedFuture(shuffleEnvironment.getPartitionsOccupyingLocalResources().isEmpty());
	}

	@Override
	public CompletableFuture<Collection<LogInfo>> requestLogList(Time timeout) {
		final String logDir = taskManagerConfiguration.getTaskManagerLogDir();
		if (logDir != null) {
			final File[] logFiles = new File(logDir).listFiles();

			if (logFiles == null) {
				return FutureUtils.completedExceptionally(new FlinkException(String.format("There isn't a log file in TaskExecutor’s log dir %s.", logDir)));
			}

			final List<LogInfo> logsWithLength = Arrays.stream(logFiles)
				.filter(File::isFile)
				.map(logFile -> new LogInfo(logFile.getName(), logFile.length()))
				.collect(Collectors.toList());
			return CompletableFuture.completedFuture(logsWithLength);
		}
		return CompletableFuture.completedFuture(Collections.emptyList());
	}

	// ------------------------------------------------------------------------
	//  Life cycle
	// ------------------------------------------------------------------------

	@Override
	public void onStart() throws Exception {
		try {
			startTaskExecutorServices();
		} catch (Exception e) {
			final TaskManagerException exception = new TaskManagerException(String.format("Could not start the TaskExecutor %s", getAddress()), e);
			onFatalError(exception);
			throw exception;
		}

		startRegistrationTimeout();
	}

	private void startTaskExecutorServices() throws Exception {
		try {
			// start by connecting to the ResourceManager
			resourceManagerLeaderRetriever.start(new ResourceManagerLeaderListener());

			// tell the task slot table who's responsible for the task slot actions
			taskSlotTable.start(new SlotActionsImpl(), getMainThreadExecutor());

			// start the job leader service
			jobLeaderService.start(getAddress(), getRpcService(), haServices, new JobLeaderListenerImpl());

			fileCache = new FileCache(taskManagerConfiguration.getTmpDirectories(), blobCacheService.getPermanentBlobService());
		} catch (Exception e) {
			handleStartTaskExecutorServicesException(e);
		}
	}

	private void handleStartTaskExecutorServicesException(Exception e) throws Exception {
		try {
			stopTaskExecutorServices();
		} catch (Exception inner) {
			e.addSuppressed(inner);
		}

		throw e;
	}

	/**
	 * Called to shut down the TaskManager. The method closes all TaskManager services.
	 */
	@Override
	public CompletableFuture<Void> onStop() {
		log.info("Stopping TaskExecutor {}.", getAddress());

		Throwable jobManagerDisconnectThrowable = null;

		FlinkException cause = new FlinkException("The TaskExecutor is shutting down.");

		closeResourceManagerConnection(cause);

		for (JobManagerConnection jobManagerConnection : jobManagerConnections.values()) {
			try {
				disassociateFromJobManager(jobManagerConnection, cause);
			} catch (Throwable t) {
				jobManagerDisconnectThrowable = ExceptionUtils.firstOrSuppressed(t, jobManagerDisconnectThrowable);
			}
		}

		final Throwable throwableBeforeTasksCompletion = jobManagerDisconnectThrowable;

		return FutureUtils
			.runAfterwards(
				taskSlotTable.closeAsync(),
				this::stopTaskExecutorServices)
  		    .handle((ignored, throwable) -> {
  		    	handleOnStopException(throwableBeforeTasksCompletion, throwable);
  		    	return null;
			});
	}

	private void handleOnStopException(Throwable throwableBeforeTasksCompletion, Throwable throwableAfterTasksCompletion) {
		final Throwable throwable;

		if (throwableBeforeTasksCompletion != null) {
			throwable = ExceptionUtils.firstOrSuppressed(throwableBeforeTasksCompletion, throwableAfterTasksCompletion);
		} else {
			throwable = throwableAfterTasksCompletion;
		}

		if (throwable != null) {
			throw new CompletionException(new FlinkException("Error while shutting the TaskExecutor down.", throwable));
		} else {
			log.info("Stopped TaskExecutor {}.", getAddress());
		}
	}

	private void stopTaskExecutorServices() throws Exception {
		Exception exception = null;

		try {
			jobLeaderService.stop();
		} catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(e, exception);
		}

		try {
			resourceManagerLeaderRetriever.stop();
		} catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(e, exception);
		}

		try {
			taskExecutorServices.shutDown();
		} catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(e, exception);
		}

		try {
			fileCache.shutdown();
		} catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(e, exception);
		}

		// it will call close() recursively from the parent to children
		taskManagerMetricGroup.close();

		ExceptionUtils.tryRethrowException(exception);
	}

	// ======================================================================
	//  RPC methods
	// ======================================================================

	@Override
	public CompletableFuture<TaskBackPressureResponse> requestTaskBackPressure(
			ExecutionAttemptID executionAttemptId,
			int requestId,
			@RpcTimeout Time timeout) {

		final Task task = taskSlotTable.getTask(executionAttemptId);
		if (task == null) {
			return FutureUtils.completedExceptionally(
				new IllegalStateException(String.format("Cannot request back pressure of task %s. " +
					"Task is not known to the task manager.", executionAttemptId)));
		}
		final CompletableFuture<Double> backPressureRatioFuture =
			backPressureSampleService.sampleTaskBackPressure(task);

		return backPressureRatioFuture.thenApply(backPressureRatio ->
			new TaskBackPressureResponse(requestId, executionAttemptId, backPressureRatio));
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
			final ExecutionAttemptID executionAttemptID = tdd.getExecutionAttemptId();
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

			if (!taskSlotTable.tryMarkSlotActive(jobId, tdd.getAllocationId())) {
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

			final TaskOperatorEventGateway taskOperatorEventGateway = new RpcTaskOperatorEventGateway(
				jobManagerConnection.getJobManagerGateway(),
				executionAttemptID,
				(t) -> runAsync(() -> failTask(executionAttemptID, t)));

			TaskManagerActions taskManagerActions = jobManagerConnection.getTaskManagerActions();
			CheckpointResponder checkpointResponder = jobManagerConnection.getCheckpointResponder();
			GlobalAggregateManager aggregateManager = jobManagerConnection.getGlobalAggregateManager();

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

			MemoryManager memoryManager;
			try {
				memoryManager = taskSlotTable.getTaskMemoryManager(tdd.getAllocationId());
			} catch (SlotNotFoundException e) {
				throw new TaskSubmissionException("Could not submit task.", e);
			}

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
				memoryManager,
				taskExecutorServices.getIOManager(),
				taskExecutorServices.getShuffleEnvironment(),
				taskExecutorServices.getKvStateService(),
				taskExecutorServices.getBroadcastVariableManager(),
				taskExecutorServices.getTaskEventDispatcher(),
				taskStateManager,
				taskManagerActions,
				inputSplitProvider,
				checkpointResponder,
				taskOperatorEventGateway,
				aggregateManager,
				blobCacheService,
				libraryCache,
				fileCache,
				taskManagerConfiguration,
				taskMetricGroup,
				resultPartitionConsumableNotifier,
				partitionStateChecker,
				getRpcService().getExecutor());

			taskMetricGroup.gauge(MetricNames.IS_BACKPRESSURED, task::isBackPressured);

			log.info("Received task {}.", task.getTaskInfo().getTaskNameWithSubtasks());

			boolean taskAdded;

			try {
				taskAdded = taskSlotTable.addTask(task);
			} catch (SlotNotFoundException | SlotNotActiveException e) {
				throw new TaskSubmissionException("Could not submit task.", e);
			}

			if (taskAdded) {
				task.startTaskThread();

				setupResultPartitionBookkeeping(
					tdd.getJobId(),
					tdd.getProducedPartitions(),
					task.getTerminationFuture());
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

	private void setupResultPartitionBookkeeping(
			JobID jobId,
			Collection<ResultPartitionDeploymentDescriptor> producedResultPartitions,
			CompletableFuture<ExecutionState> terminationFuture) {
		final Set<ResultPartitionID> partitionsRequiringRelease = filterPartitionsRequiringRelease(producedResultPartitions)
			.peek(rpdd -> partitionTracker.startTrackingPartition(jobId, TaskExecutorPartitionInfo.from(rpdd)))
			.map(ResultPartitionDeploymentDescriptor::getShuffleDescriptor)
			.map(ShuffleDescriptor::getResultPartitionID)
			.collect(Collectors.toSet());

		final CompletableFuture<ExecutionState> taskTerminationWithResourceCleanupFuture =
			terminationFuture.thenApplyAsync(
				executionState -> {
					if (executionState != ExecutionState.FINISHED) {
						partitionTracker.stopTrackingPartitions(partitionsRequiringRelease);
					}
					return executionState;
				},
				getMainThreadExecutor());

		taskResultPartitionCleanupFuturesPerJob.compute(
			jobId,
			(ignored, completableFutures) -> {
				if (completableFutures == null) {
					completableFutures = new ArrayList<>(4);
				}

				completableFutures.add(taskTerminationWithResourceCleanupFuture);
				return completableFutures;
			});
	}

	private Stream<ResultPartitionDeploymentDescriptor> filterPartitionsRequiringRelease(Collection<ResultPartitionDeploymentDescriptor> producedResultPartitions) {
		return producedResultPartitions.stream()
			// only blocking partitions require explicit release call
			.filter(d -> d.getPartitionType().isBlocking())
			// partitions without local resources don't store anything on the TaskExecutor
			.filter(d -> d.getShuffleDescriptor().storesLocalResourcesOn().isPresent());
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
				// Run asynchronously because it might be blocking
				FutureUtils.assertNoException(
					CompletableFuture.runAsync(
						() -> {
							try {
								if (!shuffleEnvironment.updatePartitionInfo(executionAttemptID, partitionInfo)) {
									log.debug(
										"Discard update for input gate partition {} of result {} in task {}. " +
											"The partition is no longer available.",
										partitionInfo.getShuffleDescriptor().getResultPartitionID(),
										partitionInfo.getIntermediateDataSetID(),
										executionAttemptID);
								}
							} catch (IOException | InterruptedException e) {
								log.error(
									"Could not update input data location for task {}. Trying to fail task.",
									task.getTaskInfo().getTaskName(),
									e);
								task.failExternally(e);
							}
						},
						getRpcService().getExecutor()));
			}
			return CompletableFuture.completedFuture(Acknowledge.get());
		} else {
			log.debug("Discard update for input partitions of task {}. Task is no longer running.", executionAttemptID);
			return CompletableFuture.completedFuture(Acknowledge.get());
		}
	}

	@Override
	public void releaseOrPromotePartitions(JobID jobId, Set<ResultPartitionID> partitionToRelease, Set<ResultPartitionID> partitionsToPromote) {
		try {
			partitionTracker.stopTrackingAndReleaseJobPartitions(partitionToRelease);
			partitionTracker.promoteJobPartitions(partitionsToPromote);

			closeJobManagerConnectionIfNoAllocatedResources(jobId);
		} catch (Throwable t) {
			// TODO: Do we still need this catch branch?
			onFatalError(t);
		}

		// TODO: Maybe it's better to return an Acknowledge here to notify the JM about the success/failure with an Exception
	}

	@Override
	public CompletableFuture<Acknowledge> releaseClusterPartitions(Collection<IntermediateDataSetID> dataSetsToRelease, Time timeout) {
		partitionTracker.stopTrackingAndReleaseClusterPartitions(dataSetsToRelease);
		return CompletableFuture.completedFuture(Acknowledge.get());
	}

	// ----------------------------------------------------------------------
	// Heartbeat RPC
	// ----------------------------------------------------------------------

	@Override
	public void heartbeatFromJobManager(ResourceID resourceID, AllocatedSlotReport allocatedSlotReport) {
		jobManagerHeartbeatManager.requestHeartbeat(resourceID, allocatedSlotReport);
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
			CheckpointOptions checkpointOptions,
			boolean advanceToEndOfEventTime) {
		log.debug("Trigger checkpoint {}@{} for {}.", checkpointId, checkpointTimestamp, executionAttemptID);

		final CheckpointType checkpointType = checkpointOptions.getCheckpointType();
		if (advanceToEndOfEventTime && !(checkpointType.isSynchronous() && checkpointType.isSavepoint())) {
			throw new IllegalArgumentException("Only synchronous savepoints are allowed to advance the watermark to MAX.");
		}

		final Task task = taskSlotTable.getTask(executionAttemptID);

		if (task != null) {
			task.triggerCheckpointBarrier(checkpointId, checkpointTimestamp, checkpointOptions, advanceToEndOfEventTime);

			return CompletableFuture.completedFuture(Acknowledge.get());
		} else {
			final String message = "TaskManager received a checkpoint request for unknown task " + executionAttemptID + '.';

			log.debug(message);
			return FutureUtils.completedExceptionally(new CheckpointException(message, CheckpointFailureReason.TASK_CHECKPOINT_FAILURE));
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
			return FutureUtils.completedExceptionally(new CheckpointException(message, CheckpointFailureReason.TASK_CHECKPOINT_FAILURE));
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
		final ResourceProfile resourceProfile,
		final String targetAddress,
		final ResourceManagerId resourceManagerId,
		final Time timeout) {
		// TODO: Filter invalid requests from the resource manager by using the instance/registration Id

		log.info("Receive slot request {} for job {} from resource manager with leader id {}.",
			allocationId, jobId, resourceManagerId);

		try {
			if (!isConnectedToResourceManager(resourceManagerId)) {
				final String message = String.format("TaskManager is not connected to the resource manager %s.", resourceManagerId);
				log.debug(message);
				throw new TaskManagerException(message);
			}

			if (taskSlotTable.isSlotFree(slotId.getSlotNumber())) {
				if (taskSlotTable.allocateSlot(slotId.getSlotNumber(), jobId, allocationId, resourceProfile, taskManagerConfiguration.getTimeout())) {
					log.info("Allocated slot for {}.", allocationId);
				} else {
					log.info("Could not allocate slot for {}.", allocationId);
					throw new SlotAllocationException("Could not allocate slot.");
				}
			} else if (!taskSlotTable.isAllocated(slotId.getSlotNumber(), jobId, allocationId)) {
				final String message = "The slot " + slotId + " has already been allocated for a different job.";

				log.info(message);

				final AllocationID allocationID = taskSlotTable.getCurrentAllocation(slotId.getSlotNumber());
				throw new SlotOccupiedException(message, allocationID, taskSlotTable.getOwningJob(allocationID));
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
		} catch (TaskManagerException taskManagerException) {
			return FutureUtils.completedExceptionally(taskManagerException);
		}

		return CompletableFuture.completedFuture(Acknowledge.get());
	}

	@Override
	public CompletableFuture<Acknowledge> freeSlot(AllocationID allocationId, Throwable cause, Time timeout) {
		freeSlotInternal(allocationId, cause);

		return CompletableFuture.completedFuture(Acknowledge.get());
	}

	@Override
	public CompletableFuture<TransientBlobKey> requestFileUploadByType(FileType fileType, Time timeout) {
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
		return requestFileUploadByFilePath(filePath, fileType.toString());
	}

	@Override
	public CompletableFuture<TransientBlobKey> requestFileUploadByName(String fileName, Time timeout) {
		final String filePath;
		final String logDir = taskManagerConfiguration.getTaskManagerLogDir();
		if (StringUtils.isNullOrWhitespaceOnly(logDir) || StringUtils.isNullOrWhitespaceOnly(fileName)) {
			filePath = null;
		} else {
			filePath = new File(logDir, new File(fileName).getName()).getPath();
		}
		return requestFileUploadByFilePath(filePath, fileName);
	}

	@Override
	public CompletableFuture<SerializableOptional<String>> requestMetricQueryServiceAddress(Time timeout) {
		return CompletableFuture.completedFuture(SerializableOptional.ofNullable(metricQueryServiceAddress));
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
		if (isRunning()) {
			reconnectToResourceManager(cause);
		}
	}

	// ----------------------------------------------------------------------
	// Other RPCs
	// ----------------------------------------------------------------------

	@Override
	public CompletableFuture<Acknowledge> sendOperatorEventToTask(
			ExecutionAttemptID executionAttemptID,
			OperatorID operatorId,
			SerializedValue<OperatorEvent> evt) {

		log.debug("Operator event for {} - {}", executionAttemptID, operatorId);

		final Task task = taskSlotTable.getTask(executionAttemptID);
		if (task == null) {
			return FutureUtils.completedExceptionally(new TaskNotRunningException(
				"Task " + executionAttemptID.toHexString() + " not running on TaskManager"));
		}

		try {
			task.deliverOperatorEvent(operatorId, evt);
			return CompletableFuture.completedFuture(Acknowledge.get());
		}
		catch (Throwable t) {
			ExceptionUtils.rethrowIfFatalError(t);
			return FutureUtils.completedExceptionally(t);
		}
	}

	// ======================================================================
	//  Internal methods
	// ======================================================================

	// ------------------------------------------------------------------------
	//  Internal resource manager connection methods
	// ------------------------------------------------------------------------

	private void notifyOfNewResourceManagerLeader(String newLeaderAddress, ResourceManagerId newResourceManagerId) {
		resourceManagerAddress = createResourceManagerAddress(newLeaderAddress, newResourceManagerId);
		reconnectToResourceManager(new FlinkException(String.format("ResourceManager leader changed to new address %s", resourceManagerAddress)));
	}

	@Nullable
	private ResourceManagerAddress createResourceManagerAddress(@Nullable String newLeaderAddress, @Nullable ResourceManagerId newResourceManagerId) {
		if (newLeaderAddress == null) {
			return null;
		} else {
			assert(newResourceManagerId != null);
			return new ResourceManagerAddress(newLeaderAddress, newResourceManagerId);
		}
	}

	private void reconnectToResourceManager(Exception cause) {
		closeResourceManagerConnection(cause);
		startRegistrationTimeout();
		tryConnectToResourceManager();
	}

	private void tryConnectToResourceManager() {
		if (resourceManagerAddress != null) {
			connectToResourceManager();
		}
	}

	private void connectToResourceManager() {
		assert(resourceManagerAddress != null);
		assert(establishedResourceManagerConnection == null);
		assert(resourceManagerConnection == null);

		log.info("Connecting to ResourceManager {}.", resourceManagerAddress);

		final TaskExecutorRegistration taskExecutorRegistration = new TaskExecutorRegistration(
			getAddress(),
			getResourceID(),
			unresolvedTaskManagerLocation.getDataPort(),
			hardwareDescription,
			taskManagerConfiguration.getDefaultSlotResourceProfile(),
			taskManagerConfiguration.getTotalResourceProfile()
		);

		resourceManagerConnection =
			new TaskExecutorToResourceManagerConnection(
				log,
				getRpcService(),
				taskManagerConfiguration.getRetryingRegistrationConfiguration(),
				resourceManagerAddress.getAddress(),
				resourceManagerAddress.getResourceManagerId(),
				getMainThreadExecutor(),
				new ResourceManagerRegistrationListener(),
				taskExecutorRegistration);
		resourceManagerConnection.start();
	}

	private void establishResourceManagerConnection(
			ResourceManagerGateway resourceManagerGateway,
			ResourceID resourceManagerResourceId,
			InstanceID taskExecutorRegistrationId,
			ClusterInformation clusterInformation) {

		final CompletableFuture<Acknowledge> slotReportResponseFuture = resourceManagerGateway.sendSlotReport(
			getResourceID(),
			taskExecutorRegistrationId,
			taskSlotTable.createSlotReport(getResourceID()),
			taskManagerConfiguration.getTimeout());

		slotReportResponseFuture.whenCompleteAsync(
			(acknowledge, throwable) -> {
				if (throwable != null) {
					reconnectToResourceManager(new TaskManagerException("Failed to send initial slot report to ResourceManager.", throwable));
				}
			}, getMainThreadExecutor());

		// monitor the resource manager as heartbeat target
		resourceManagerHeartbeatManager.monitorTarget(resourceManagerResourceId, new HeartbeatTarget<TaskExecutorHeartbeatPayload>() {
			@Override
			public void receiveHeartbeat(ResourceID resourceID, TaskExecutorHeartbeatPayload heartbeatPayload) {
				resourceManagerGateway.heartbeatFromTaskManager(resourceID, heartbeatPayload);
			}

			@Override
			public void requestHeartbeat(ResourceID resourceID, TaskExecutorHeartbeatPayload heartbeatPayload) {
				// the TaskManager won't send heartbeat requests to the ResourceManager
			}
		});

		// set the propagated blob server address
		final InetSocketAddress blobServerAddress = new InetSocketAddress(
			clusterInformation.getBlobServerHostname(),
			clusterInformation.getBlobServerPort());

		blobCacheService.setBlobServerAddress(blobServerAddress);

		establishedResourceManagerConnection = new EstablishedResourceManagerConnection(
			resourceManagerGateway,
			resourceManagerResourceId,
			taskExecutorRegistrationId);

		stopRegistrationTimeout();
	}

	private void closeResourceManagerConnection(Exception cause) {
		if (establishedResourceManagerConnection != null) {
			final ResourceID resourceManagerResourceId = establishedResourceManagerConnection.getResourceManagerResourceId();

			if (log.isDebugEnabled()) {
				log.debug("Close ResourceManager connection {}.",
					resourceManagerResourceId, cause);
			} else {
				log.info("Close ResourceManager connection {}.",
					resourceManagerResourceId);
			}
			resourceManagerHeartbeatManager.unmonitorTarget(resourceManagerResourceId);

			ResourceManagerGateway resourceManagerGateway = establishedResourceManagerConnection.getResourceManagerGateway();
			resourceManagerGateway.disconnectTaskManager(getResourceID(), cause);

			establishedResourceManagerConnection = null;

			partitionTracker.stopTrackingAndReleaseAllClusterPartitions();
		}

		if (resourceManagerConnection != null) {
			if (!resourceManagerConnection.isConnected()) {
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

	private void startRegistrationTimeout() {
		final Time maxRegistrationDuration = taskManagerConfiguration.getMaxRegistrationDuration();

		if (maxRegistrationDuration != null) {
			final UUID newRegistrationTimeoutId = UUID.randomUUID();
			currentRegistrationTimeoutId = newRegistrationTimeoutId;
			scheduleRunAsync(() -> registrationTimeout(newRegistrationTimeoutId), maxRegistrationDuration);
		}
	}

	private void stopRegistrationTimeout() {
		currentRegistrationTimeoutId = null;
	}

	private void registrationTimeout(@Nonnull UUID registrationTimeoutId) {
		if (registrationTimeoutId.equals(currentRegistrationTimeoutId)) {
			final Time maxRegistrationDuration = taskManagerConfiguration.getMaxRegistrationDuration();

			onFatalError(
				new RegistrationTimeoutException(
					String.format("Could not register at the ResourceManager within the specified maximum " +
						"registration duration %s. This indicates a problem with this instance. Terminating now.",
						maxRegistrationDuration)));
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

				final Iterator<TaskSlot<Task>> reservedSlotsIterator = taskSlotTable.getAllocatedSlots(jobId);
				final JobMasterId jobMasterId = jobManagerConnection.getJobMasterId();

				final Collection<SlotOffer> reservedSlots = new HashSet<>(2);

				while (reservedSlotsIterator.hasNext()) {
					SlotOffer offer = reservedSlotsIterator.next().generateSlotOffer();
					reservedSlots.add(offer);
				}

				CompletableFuture<Collection<SlotOffer>> acceptedSlotsFuture = jobMasterGateway.offerSlots(
					getResourceID(),
					reservedSlots,
					taskManagerConfiguration.getTimeout());

				acceptedSlotsFuture.whenCompleteAsync(
					handleAcceptedSlotOffers(jobId, jobMasterGateway, jobMasterId, reservedSlots),
					getMainThreadExecutor());
			} else {
				log.debug("There are no unassigned slots for the job {}.", jobId);
			}
		}
	}

	@Nonnull
	private BiConsumer<Iterable<SlotOffer>, Throwable> handleAcceptedSlotOffers(JobID jobId, JobMasterGateway jobMasterGateway, JobMasterId jobMasterId, Collection<SlotOffer> offeredSlots) {
		return (Iterable<SlotOffer> acceptedSlots, Throwable throwable) -> {
			if (throwable != null) {
				if (throwable instanceof TimeoutException) {
					log.info("Slot offering to JobManager did not finish in time. Retrying the slot offering.");
					// We ran into a timeout. Try again.
					offerSlotsToJobManager(jobId);
				} else {
					log.warn("Slot offering to JobManager failed. Freeing the slots " +
						"and returning them to the ResourceManager.", throwable);

					// We encountered an exception. Free the slots and return them to the RM.
					for (SlotOffer reservedSlot: offeredSlots) {
						freeSlotInternal(reservedSlot.getAllocationId(), throwable);
					}
				}
			} else {
				// check if the response is still valid
				if (isJobManagerConnectionValid(jobId, jobMasterId)) {
					// mark accepted slots active
					for (SlotOffer acceptedSlot : acceptedSlots) {
						try {
							if (!taskSlotTable.markSlotActive(acceptedSlot.getAllocationId())) {
								// the slot is either free or releasing at the moment
								final String message = "Could not mark slot " + jobId + " active.";
								log.debug(message);
								jobMasterGateway.failSlot(
									getResourceID(),
									acceptedSlot.getAllocationId(),
									new FlinkException(message));
							}
						} catch (SlotNotFoundException e) {
							final String message = "Could not mark slot " + jobId + " active.";
							jobMasterGateway.failSlot(
								getResourceID(),
								acceptedSlot.getAllocationId(),
								new FlinkException(message));
						}

						offeredSlots.remove(acceptedSlot);
					}

					final Exception e = new Exception("The slot was rejected by the JobManager.");

					for (SlotOffer rejectedSlot : offeredSlots) {
						freeSlotInternal(rejectedSlot.getAllocationId(), e);
					}
				} else {
					// discard the response since there is a new leader for the job
					log.debug("Discard offer slot response since there is a new leader " +
						"for the job {}.", jobId);
				}
			}
		};
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
		GlobalAggregateManager aggregateManager = new RpcGlobalAggregateManager(jobMasterGateway);

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
			aggregateManager,
			libraryCacheManager,
			resultPartitionConsumableNotifier,
			partitionStateChecker);
	}

	private void disassociateFromJobManager(JobManagerConnection jobManagerConnection, Exception cause) throws IOException {
		checkNotNull(jobManagerConnection);

		final JobID jobId = jobManagerConnection.getJobID();

		// cleanup remaining partitions once all tasks for this job have completed
		scheduleResultPartitionCleanup(jobId);

		final KvStateRegistry kvStateRegistry = kvStateService.getKvStateRegistry();

		if (kvStateRegistry != null) {
			kvStateRegistry.unregisterListener(jobId);
		}

		final KvStateClientProxy kvStateClientProxy = kvStateService.getKvStateClientProxy();

		if (kvStateClientProxy != null) {
			kvStateClientProxy.updateKvStateLocationOracle(jobManagerConnection.getJobID(), null);
		}

		JobMasterGateway jobManagerGateway = jobManagerConnection.getJobManagerGateway();
		jobManagerGateway.disconnectTaskManager(getResourceID(), cause);
		jobManagerConnection.getLibraryCacheManager().shutdown();
	}

	private void scheduleResultPartitionCleanup(JobID jobId) {
		final Collection<CompletableFuture<ExecutionState>> taskTerminationFutures = taskResultPartitionCleanupFuturesPerJob.remove(jobId);
		if (taskTerminationFutures != null) {
			FutureUtils.waitForAll(taskTerminationFutures)
				.thenRunAsync(() -> {
					partitionTracker.stopTrackingAndReleaseJobPartitionsFor(jobId);
				}, getMainThreadExecutor());
		}
	}

	private void registerQueryableState(JobID jobId, JobMasterGateway jobMasterGateway) {
		final KvStateServer kvStateServer = kvStateService.getKvStateServer();
		final KvStateRegistry kvStateRegistry = kvStateService.getKvStateRegistry();

		if (kvStateServer != null && kvStateRegistry != null) {
			kvStateRegistry.registerListener(
				jobId,
				new RpcKvStateRegistryListener(
					jobMasterGateway,
					kvStateServer.getServerAddress()));
		}

		final KvStateClientProxy kvStateProxy = kvStateService.getKvStateClientProxy();

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
				task.getExecutionState(), task.getTaskInfo().getTaskNameWithSubtasks(), task.getExecutionId());

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
					ResourceManagerGateway resourceManagerGateway = establishedResourceManagerConnection.getResourceManagerGateway();

					resourceManagerGateway.notifySlotAvailable(
						establishedResourceManagerConnection.getTaskExecutorRegistrationId(),
						new SlotID(getResourceID(), slotIndex),
						allocationId);
				}

				if (jobId != null) {
					closeJobManagerConnectionIfNoAllocatedResources(jobId);
				}
			}
		} catch (SlotNotFoundException e) {
			log.debug("Could not free slot for allocation id {}.", allocationId, e);
		}

		localStateStoresManager.releaseLocalStateForAllocationId(allocationId);
	}

	private void closeJobManagerConnectionIfNoAllocatedResources(JobID jobId) {
		// check whether we still have allocated slots for the same job
		if (taskSlotTable.getAllocationIdsPerJob(jobId).isEmpty() && !partitionTracker.isTrackingPartitionsFor(jobId)) {
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

	private void timeoutSlot(AllocationID allocationId, UUID ticket) {
		checkNotNull(allocationId);
		checkNotNull(ticket);

		if (taskSlotTable.isValidTimeout(allocationId, ticket)) {
			freeSlotInternal(allocationId, new Exception("The slot " + allocationId + " has timed out."));
		} else {
			log.debug("Received an invalid timeout for allocation id {} with ticket {}.", allocationId, ticket);
		}
	}

	/**
	 * Syncs the TaskExecutor's view on its allocated slots with the JobMaster's view.
	 * Slots which are no longer reported by the JobMaster are being freed.
	 * Slots which the JobMaster thinks it still owns but which are no longer allocated to it
	 * will be failed via {@link JobMasterGateway#failSlot}.
	 *
	 * @param allocatedSlotReport represents the JobMaster's view on the current slot allocation state
	 */
	private void syncSlotsWithSnapshotFromJobMaster(AllocatedSlotReport allocatedSlotReport) {
		JobManagerConnection jobManagerConnection = jobManagerTable.get(allocatedSlotReport.getJobId());
		if (jobManagerConnection != null) {
			final JobMasterGateway jobMasterGateway = jobManagerConnection.getJobManagerGateway();

			failNoLongerAllocatedSlots(allocatedSlotReport, jobMasterGateway);

			freeNoLongerUsedSlots(allocatedSlotReport);
		} else {
			log.debug("Ignoring allocated slot report from job {} because there is no active leader.",
					allocatedSlotReport.getJobId());
		}
	}

	private void failNoLongerAllocatedSlots(AllocatedSlotReport allocatedSlotReport, JobMasterGateway jobMasterGateway) {
		for (AllocatedSlotInfo allocatedSlotInfo : allocatedSlotReport.getAllocatedSlotInfos()) {
			final AllocationID allocationId = allocatedSlotInfo.getAllocationId();
			if (!taskSlotTable.isAllocated(
					allocatedSlotInfo.getSlotIndex(),
					allocatedSlotReport.getJobId(),
					allocationId)) {
				jobMasterGateway.failSlot(
						getResourceID(),
						allocationId,
						new FlinkException(
							String.format(
								"Slot %s on TaskExecutor %s is not allocated by job %s.",
								allocatedSlotInfo.getSlotIndex(),
								getResourceID(),
								allocatedSlotReport.getJobId())));
			}
		}
	}

	private void freeNoLongerUsedSlots(AllocatedSlotReport allocatedSlotReport) {
		final Iterator<AllocationID> slotsTaskManagerSide = taskSlotTable.getActiveSlots(allocatedSlotReport.getJobId());
		final Set<AllocationID> activeSlots = Sets.newHashSet(slotsTaskManagerSide);
		final Set<AllocationID> reportedSlots = allocatedSlotReport.getAllocatedSlotInfos().stream()
				.map(AllocatedSlotInfo::getAllocationId).collect(Collectors.toSet());

		final Sets.SetView<AllocationID> difference = Sets.difference(activeSlots, reportedSlots);

		for (AllocationID allocationID : difference) {
			freeSlotInternal(
				allocationID,
				new FlinkException(
					String.format("%s is no longer allocated by job %s.", allocationID, allocatedSlotReport.getJobId())));
		}
	}

	// ------------------------------------------------------------------------
	//  Internal utility methods
	// ------------------------------------------------------------------------

	private boolean isConnectedToResourceManager() {
		return establishedResourceManagerConnection != null;
	}

	private boolean isConnectedToResourceManager(ResourceManagerId resourceManagerId) {
		return establishedResourceManagerConnection != null && resourceManagerAddress != null && resourceManagerAddress.getResourceManagerId().equals(resourceManagerId);
	}

	private boolean isJobManagerConnectionValid(JobID jobId, JobMasterId jobMasterId) {
		JobManagerConnection jmConnection = jobManagerTable.get(jobId);

		return jmConnection != null && Objects.equals(jmConnection.getJobMasterId(), jobMasterId);
	}

	private CompletableFuture<TransientBlobKey> requestFileUploadByFilePath(String filePath, String fileTag) {
		log.debug("Received file upload request for file {}", fileTag);
		if (!StringUtils.isNullOrWhitespaceOnly(filePath)) {
			final File file = new File(filePath);
			if (file.exists()) {
				final TransientBlobCache transientBlobService = blobCacheService.getTransientBlobService();
				final TransientBlobKey transientBlobKey;
				try (FileInputStream fileInputStream = new FileInputStream(file)) {
					transientBlobKey = transientBlobService.putTransient(fileInputStream);
				} catch (IOException e) {
					log.debug("Could not upload file {}.", fileTag, e);
					return FutureUtils.completedExceptionally(new FlinkException("Could not upload file " + fileTag + '.', e));
				}

				return CompletableFuture.completedFuture(transientBlobKey);
			} else {
				log.debug("The file {} does not exist on the TaskExecutor {}.", fileTag, getResourceID());
				return FutureUtils.completedExceptionally(new FlinkException("The file " + fileTag + " does not exist on the TaskExecutor."));
			}
		} else {
			log.debug("The file {} is unavailable on the TaskExecutor {}.", fileTag, getResourceID());
			return FutureUtils.completedExceptionally(new FlinkException("The file " + fileTag + " is not available on the TaskExecutor."));
		}
	}

	// ------------------------------------------------------------------------
	//  Properties
	// ------------------------------------------------------------------------

	public ResourceID getResourceID() {
		return unresolvedTaskManagerLocation.getResourceID();
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
	HeartbeatManager<Void, TaskExecutorHeartbeatPayload> getResourceManagerHeartbeatManager() {
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

	private final class ResourceManagerRegistrationListener implements RegistrationConnectionListener<TaskExecutorToResourceManagerConnection, TaskExecutorRegistrationSuccess> {

		@Override
		public void onRegistrationSuccess(TaskExecutorToResourceManagerConnection connection, TaskExecutorRegistrationSuccess success) {
			final ResourceID resourceManagerId = success.getResourceManagerId();
			final InstanceID taskExecutorRegistrationId = success.getRegistrationId();
			final ClusterInformation clusterInformation = success.getClusterInformation();
			final ResourceManagerGateway resourceManagerGateway = connection.getTargetGateway();

			runAsync(
				() -> {
					// filter out outdated connections
					//noinspection ObjectEquality
					if (resourceManagerConnection == connection) {
						establishResourceManagerConnection(
							resourceManagerGateway,
							resourceManagerId,
							taskExecutorRegistrationId,
							clusterInformation);
					}
				});
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
			if (taskExecutionState.getExecutionState().isTerminal()) {
				runAsync(() -> unregisterTaskAndNotifyFinalState(jobMasterGateway, taskExecutionState.getID()));
			} else {
				TaskExecutor.this.updateTaskExecutionState(jobMasterGateway, taskExecutionState);
			}
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

	private class JobManagerHeartbeatListener implements HeartbeatListener<AllocatedSlotReport, AccumulatorReport> {

		@Override
		public void notifyHeartbeatTimeout(final ResourceID resourceID) {
			validateRunsInMainThread();
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
		}

		@Override
		public void reportPayload(ResourceID resourceID, AllocatedSlotReport allocatedSlotReport) {
			validateRunsInMainThread();
			syncSlotsWithSnapshotFromJobMaster(allocatedSlotReport);
		}

		@Override
		public AccumulatorReport retrievePayload(ResourceID resourceID) {
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
				return new AccumulatorReport(accumulatorSnapshots);
			} else {
				return new AccumulatorReport(Collections.emptyList());
			}
		}
	}

	private class ResourceManagerHeartbeatListener implements HeartbeatListener<Void, TaskExecutorHeartbeatPayload> {

		@Override
		public void notifyHeartbeatTimeout(final ResourceID resourceId) {
			validateRunsInMainThread();
			// first check whether the timeout is still valid
			if (establishedResourceManagerConnection != null && establishedResourceManagerConnection.getResourceManagerResourceId().equals(resourceId)) {
				log.info("The heartbeat of ResourceManager with id {} timed out.", resourceId);

				reconnectToResourceManager(new TaskManagerException(
					String.format("The heartbeat of ResourceManager with id %s timed out.", resourceId)));
			} else {
				log.debug("Received heartbeat timeout for outdated ResourceManager id {}. Ignoring the timeout.", resourceId);
			}
		}

		@Override
		public void reportPayload(ResourceID resourceID, Void payload) {
			// nothing to do since the payload is of type Void
		}

		@Override
		public TaskExecutorHeartbeatPayload retrievePayload(ResourceID resourceID) {
			validateRunsInMainThread();
			return new TaskExecutorHeartbeatPayload(taskSlotTable.createSlotReport(getResourceID()), partitionTracker.createClusterPartitionReport());
		}
	}
}
