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

import org.apache.flink.runtime.accumulators.AccumulatorSnapshot;
import org.apache.flink.runtime.blob.BlobCache;
import org.apache.flink.runtime.broadcast.BroadcastVariableManager;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.concurrent.ApplyFunction;
import org.apache.flink.runtime.concurrent.Future;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.execution.librarycache.BlobLibraryCacheManager;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.PartitionInfo;
import org.apache.flink.runtime.filecache.FileCache;
import org.apache.flink.runtime.io.network.netty.PartitionStateChecker;
import org.apache.flink.runtime.io.network.partition.ResultPartitionConsumableNotifier;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.resourcemanager.messages.taskexecutor.TMSlotRequestRegistered;
import org.apache.flink.runtime.resourcemanager.messages.taskexecutor.TMSlotRequestRejected;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.runtime.resourcemanager.messages.taskexecutor.TMSlotRequestReply;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.taskexecutor.exceptions.CheckpointException;
import org.apache.flink.runtime.taskexecutor.exceptions.PartitionException;
import org.apache.flink.runtime.taskexecutor.exceptions.TaskException;
import org.apache.flink.runtime.taskexecutor.exceptions.TaskSubmissionException;
import org.apache.flink.runtime.taskexecutor.rpc.RpcCheckpointResponder;
import org.apache.flink.runtime.taskexecutor.rpc.RpcInputSplitProvider;
import org.apache.flink.runtime.taskexecutor.rpc.RpcPartitionStateChecker;
import org.apache.flink.runtime.taskexecutor.rpc.RpcResultPartitionConsumableNotifier;
import org.apache.flink.runtime.taskmanager.CheckpointResponder;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.taskmanager.TaskManagerActions;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.NetworkEnvironment;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcMethod;
import org.apache.flink.runtime.rpc.RpcService;

import org.apache.flink.util.Preconditions;

import java.util.HashSet;
import java.util.Set;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * TaskExecutor implementation. The task executor is responsible for the execution of multiple
 * {@link Task}.
 */
public class TaskExecutor extends RpcEndpoint<TaskExecutorGateway> {

	/** The connection information of this task manager */
	private final TaskManagerLocation taskManagerLocation;

	/** The access to the leader election and retrieval services */
	private final HighAvailabilityServices haServices;

	/** The task manager configuration */
	private final TaskManagerConfiguration taskManagerConfiguration;

	/** The I/O manager component in the task manager */
	private final IOManager ioManager;

	/** The memory manager component in the task manager */
	private final MemoryManager memoryManager;

	/** The network component in the task manager */
	private final NetworkEnvironment networkEnvironment;

	/** The metric registry in the task manager */
	private final MetricRegistry metricRegistry;

	/** The fatal error handler to use in case of a fatal error */
	private final FatalErrorHandler fatalErrorHandler;

	private final TaskManagerMetricGroup taskManagerMetricGroup;

	private final BroadcastVariableManager broadcastVariableManager;
	
	/** Slots which have become available but haven't been confirmed by the RM */
	private final Set<SlotID> unconfirmedFreeSlots;


	private final FileCache fileCache;

	// --------- resource manager --------

	private TaskExecutorToResourceManagerConnection resourceManagerConnection;

	// --------- job manager connections -----------

	private Map<ResourceID, JobManagerConnection> jobManagerConnections;

	// --------- Slot allocation table --------

	private Map<AllocationID, TaskSlot> taskSlots;

	// --------- Slot allocation table --------

	private Map<ExecutionAttemptID, TaskSlotMapping> taskSlotMappings;

	// ------------------------------------------------------------------------

	public TaskExecutor(
		TaskManagerConfiguration taskManagerConfiguration,
		TaskManagerLocation taskManagerLocation,
		RpcService rpcService,
		MemoryManager memoryManager,
		IOManager ioManager,
		NetworkEnvironment networkEnvironment,
		HighAvailabilityServices haServices,
		MetricRegistry metricRegistry,
		TaskManagerMetricGroup taskManagerMetricGroup,
		BroadcastVariableManager broadcastVariableManager,
		FileCache fileCache,
		FatalErrorHandler fatalErrorHandler) {

		super(rpcService);

		checkArgument(taskManagerConfiguration.getNumberSlots() > 0, "The number of slots has to be larger than 0.");

		this.taskManagerConfiguration = checkNotNull(taskManagerConfiguration);
		this.taskManagerLocation = checkNotNull(taskManagerLocation);
		this.memoryManager = checkNotNull(memoryManager);
		this.ioManager = checkNotNull(ioManager);
		this.networkEnvironment = checkNotNull(networkEnvironment);
		this.haServices = checkNotNull(haServices);
		this.metricRegistry = checkNotNull(metricRegistry);
		this.fatalErrorHandler = checkNotNull(fatalErrorHandler);
		this.taskManagerMetricGroup = checkNotNull(taskManagerMetricGroup);
		this.broadcastVariableManager = checkNotNull(broadcastVariableManager);
		this.fileCache = checkNotNull(fileCache);

		this.jobManagerConnections = new HashMap<>(4);

		this.unconfirmedFreeSlots = new HashSet<>();
		this.taskSlots = new HashMap<>(taskManagerConfiguration.getNumberSlots());
		this.taskSlotMappings = new HashMap<>(taskManagerConfiguration.getNumberSlots() * 2);
	}

	// ------------------------------------------------------------------------
	//  Life cycle
	// ------------------------------------------------------------------------

	@Override
	public void start() {
		super.start();

		// start by connecting to the ResourceManager
		try {
			haServices.getResourceManagerLeaderRetriever().start(new ResourceManagerLeaderListener());
		} catch (Exception e) {
			onFatalErrorAsync(e);
		}
	}

	/**
	 * Called to shut down the TaskManager. The method closes all TaskManager services.
	 */
	@Override
	public void shutDown() {
		log.info("Stopping TaskManager {}.", getAddress());

		if (resourceManagerConnection.isConnected()) {
			try {
				resourceManagerConnection.close();
			} catch (Exception e) {
				log.error("Could not cleanly close the ResourceManager connection.", e);
			}
		}

		try {
			ioManager.shutdown();
		} catch (Exception e) {
			log.error("IOManager did not shut down properly.", e);
		}

		try {
			memoryManager.shutdown();
		} catch (Exception e) {
			log.error("MemoryManager did not shut down properly.", e);
		}

		try {
			networkEnvironment.shutdown();
		} catch (Exception e) {
			log.error("Network environment did not shut down properly.", e);
		}

		try {
			fileCache.shutdown();
		} catch (Exception e) {
			log.error("File cache did not shut down properly.", e);
		}

		try {
			metricRegistry.shutdown();
		} catch (Exception e) {
			log.error("MetricRegistry did not shut down properly.", e);
		}

		log.info("Stopped TaskManager {}.", getAddress());
	}

	// ========================================================================
	//  RPC methods
	// ========================================================================

	// ----------------------------------------------------------------------
	// Task lifecycle RPCs
	// ----------------------------------------------------------------------

	@RpcMethod
	public Acknowledge submitTask(TaskDeploymentDescriptor tdd, ResourceID jobManagerID) throws TaskSubmissionException {

		JobManagerConnection jobManagerConnection = getJobManagerConnection(jobManagerID);

		if (jobManagerConnection == null) {
			final String message = "Could not submit task because JobManager " + jobManagerID +
				" was not associated.";

			log.debug(message);
			throw new TaskSubmissionException(message);
		}

		TaskSlot taskSlot = taskSlots.get(tdd.getAllocationID());

		if (taskSlot == null) {
			final String message = "No task slot allocated for allocation ID " + tdd.getAllocationID() + '.';
			log.debug(message);
			throw new TaskSubmissionException(message);
		}

		TaskMetricGroup taskMetricGroup = taskManagerMetricGroup.addTaskForJob(tdd);

		InputSplitProvider inputSplitProvider = new RpcInputSplitProvider(
			jobManagerConnection.getJobManagerGateway(),
			tdd.getJobID(),
			tdd.getVertexID(),
			tdd.getExecutionId(),
			taskManagerConfiguration.getTimeout());

		TaskManagerActions taskManagerActions = jobManagerConnection.getTaskManagerActions();
		CheckpointResponder checkpointResponder = jobManagerConnection.getCheckpointResponder();
		LibraryCacheManager libraryCache = jobManagerConnection.getLibraryCacheManager();
		ResultPartitionConsumableNotifier resultPartitionConsumableNotifier = jobManagerConnection.getResultPartitionConsumableNotifier();
		PartitionStateChecker partitionStateChecker = jobManagerConnection.getPartitionStateChecker();

		Task task = new Task(
			tdd,
			memoryManager,
			ioManager,
			networkEnvironment,
			broadcastVariableManager,
			taskManagerActions,
			inputSplitProvider,
			checkpointResponder,
			libraryCache,
			fileCache,
			taskManagerConfiguration,
			taskMetricGroup,
			resultPartitionConsumableNotifier,
			partitionStateChecker,
			getRpcService().getExecutor());

		log.info("Received task {}.", task.getTaskInfo().getTaskNameWithSubtasks());

		if(taskSlot.add(task)) {
			TaskSlotMapping taskSlotMapping = new TaskSlotMapping(task, taskSlot);

			taskSlotMappings.put(task.getExecutionId(), taskSlotMapping);
			task.startTaskThread();

			return Acknowledge.get();
		} else {
			final String message = "TaskManager already contains a task for id " +
				task.getExecutionId() + '.';

			log.debug(message);
			throw new TaskSubmissionException(message);
		}
	}

	@RpcMethod
	public Acknowledge cancelTask(ExecutionAttemptID executionAttemptID) throws TaskException {
		final Task task = getTask(executionAttemptID);

		if (task != null) {
			try {
				task.cancelExecution();
				return Acknowledge.get();
			} catch (Throwable t) {
				throw new TaskException("Cannot cancel task for execution " + executionAttemptID + '.', t);
			}
		} else {
			final String message = "Cannot find task to stop for execution " + executionAttemptID + '.';

			log.debug(message);
			throw new TaskException(message);
		}
	}

	@RpcMethod
	public Acknowledge stopTask(ExecutionAttemptID executionAttemptID) throws TaskException {
		final Task task = getTask(executionAttemptID);

		if (task != null) {
			try {
				task.stopExecution();
				return Acknowledge.get();
			} catch (Throwable t) {
				throw new TaskException("Cannot stop task for execution " + executionAttemptID + '.', t);
			}
		} else {
			final String message = "Cannot find task to stop for execution " + executionAttemptID + '.';

			log.debug(message);
			throw new TaskException(message);
		}
	}

	// ----------------------------------------------------------------------
	// Partition lifecycle RPCs
	// ----------------------------------------------------------------------

	@RpcMethod
	public Acknowledge updatePartitions(final ExecutionAttemptID executionAttemptID, Collection<PartitionInfo> partitionInfos) throws PartitionException {
		final Task task = getTask(executionAttemptID);

		if (task != null) {
			for (final PartitionInfo partitionInfo: partitionInfos) {
				IntermediateDataSetID intermediateResultPartitionID = partitionInfo.getIntermediateDataSetID();

				final SingleInputGate singleInputGate = task.getInputGateById(intermediateResultPartitionID);

				if (singleInputGate != null) {
					// Run asynchronously because it might be blocking
					getRpcService().execute(new Runnable() {
						@Override
						public void run() {
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
						}
					});
				} else {
					throw new PartitionException("No reader with ID " +
						intermediateResultPartitionID + " for task " + executionAttemptID +
						" was found.");
				}
			}

			return Acknowledge.get();
		} else {
			log.debug("Discard update for input partitions of task {}. Task is no longer running.", executionAttemptID);
			return Acknowledge.get();
		}
	}

	@RpcMethod
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
	// Checkpointing RPCs
	// ----------------------------------------------------------------------

	@RpcMethod
	public Acknowledge triggerCheckpoint(ExecutionAttemptID executionAttemptID, long checkpointId, long checkpointTimestamp) throws CheckpointException {
		log.debug("Trigger checkpoint {}@{} for {}.", checkpointId, checkpointTimestamp, executionAttemptID);

		final Task task = getTask(executionAttemptID);

		if (task != null) {
			task.triggerCheckpointBarrier(checkpointId, checkpointTimestamp);

			return Acknowledge.get();
		} else {
			final String message = "TaskManager received a checkpoint request for unknown task " + executionAttemptID + '.';

			log.debug(message);
			throw new CheckpointException(message);
		}
	}

	@RpcMethod
	public Acknowledge confirmCheckpoint(ExecutionAttemptID executionAttemptID, long checkpointId, long checkpointTimestamp) throws CheckpointException {
		log.debug("Confirm checkpoint {}@{} for {}.", checkpointId, checkpointTimestamp, executionAttemptID);

		final Task task = getTask(executionAttemptID);

		if (task != null) {
			task.notifyCheckpointComplete(checkpointId);

			return Acknowledge.get();
		} else {
			final String message = "TaskManager received a checkpoint confirmation for unknown task " + executionAttemptID + '.';

			log.debug(message);
			throw new CheckpointException(message);
		}
	}

	/**
	 * Requests a slot from the TaskManager
	 *
	 * @param slotID Slot id for the request
	 * @param allocationID id for the request
	 * @param resourceManagerLeaderID current leader id of the ResourceManager
	 * @return answer to the slot request
	 */
	@RpcMethod
	public TMSlotRequestReply requestSlot(SlotID slotID, AllocationID allocationID, UUID resourceManagerLeaderID) {
		if (!resourceManagerConnection.getTargetLeaderId().equals(resourceManagerLeaderID)) {
			return new TMSlotRequestRejected(
				resourceManagerConnection.getRegistrationId(), getResourceID(), allocationID);
		}
		if (unconfirmedFreeSlots.contains(slotID)) {
			// check if request has not been blacklisted because the notification of a free slot
			// has not been confirmed by the ResourceManager
			return new TMSlotRequestRejected(
				resourceManagerConnection.getRegistrationId(), getResourceID(), allocationID);
		}
		return new TMSlotRequestRegistered(new InstanceID(), ResourceID.generate(), allocationID);

	}

	// ------------------------------------------------------------------------
	//  Internal methods
	// ------------------------------------------------------------------------

	private JobManagerConnection getJobManagerConnection(ResourceID jobManagerID) {
		return jobManagerConnections.get(jobManagerID);
	}

	private Task getTask(ExecutionAttemptID executionAttemptID) {
		TaskSlotMapping taskSlotMapping = taskSlotMappings.get(executionAttemptID);

		if (taskSlotMapping != null) {
			return taskSlotMapping.getTask();
		} else {
			return null;
		}
	}

	private Task removeTask(ExecutionAttemptID executionAttemptID) {
		TaskSlotMapping taskSlotMapping = taskSlotMappings.remove(executionAttemptID);

		if (taskSlotMapping != null) {
			final Task task = taskSlotMapping.getTask();
			final TaskSlot taskSlot = taskSlotMapping.getTaskSlot();

			taskSlot.remove(task);

			return task;
		} else {
			return null;
		}
	}

	private Iterable<Task> getAllTasks() {
		final Iterator<TaskSlotMapping> taskEntryIterator = taskSlotMappings.values().iterator();
		final Iterator<Task> iterator = new Iterator<Task>() {
			@Override
			public boolean hasNext() {
				return taskEntryIterator.hasNext();
			}

			@Override
			public Task next() {
				return taskEntryIterator.next().getTask();
			}

			@Override
			public void remove() {
				taskEntryIterator.remove();
			}
		};

		return new Iterable<Task>() {
			@Override
			public Iterator<Task> iterator() {
				return iterator;
			}
		};
	}

	private void clearTasks() {
		taskSlotMappings.clear();

		for (TaskSlot taskSlot: taskSlots.values()) {
			taskSlot.clear();
		}
	}

	private void failTask(final ExecutionAttemptID executionAttemptID, final Throwable cause) {
		final Task task = getTask(executionAttemptID);

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

	private void cancelAndClearAllTasks(Throwable cause) {
		log.info("Cancellaing all computations and discarding all cached data.");

		Iterable<Task> tasks = getAllTasks();

		for (Task task: tasks) {
			task.failExternally(cause);
		}

		clearTasks();
	}

	private void updateTaskExecutionState(final JobMasterGateway jobMasterGateway, final TaskExecutionState taskExecutionState) {
		final ExecutionAttemptID executionAttemptID = taskExecutionState.getID();

		Future<Acknowledge> futureAcknowledge = jobMasterGateway.updateTaskExecutionState(taskExecutionState);

		futureAcknowledge.exceptionallyAsync(new ApplyFunction<Throwable, Void>() {
			@Override
			public Void apply(Throwable value) {
				failTask(executionAttemptID, value);

				return null;
			}
		}, getMainThreadExecutor());
	}

	private void unregisterTaskAndNotifyFinalState(final JobMasterGateway jobMasterGateway, ExecutionAttemptID executionAttemptID) {
		Task task = removeTask(executionAttemptID);

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
					accumulatorSnapshot));
		} else {
			log.error("Cannot find task with ID {} to unregister.", executionAttemptID);
		}
	}

	private void notifyOfNewResourceManagerLeader(String newLeaderAddress, UUID newLeaderId) {
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
			if (resourceManagerConnection != null) {
				resourceManagerConnection.close();
				resourceManagerConnection = null;
			}
		}

		unconfirmedFreeSlots.clear();

		// establish a connection to the new leader
		if (newLeaderAddress != null) {
			log.info("Attempting to register at ResourceManager {}", newLeaderAddress);
			resourceManagerConnection =
				new TaskExecutorToResourceManagerConnection(
					log,
					this,
					newLeaderAddress,
					newLeaderId,
					getMainThreadExecutor());
			resourceManagerConnection.start();
		}
	}

	private JobManagerConnection associateWithJobManager(JobMasterGateway jobMasterGateway, int blobPort) {
		Preconditions.checkNotNull(jobMasterGateway);
		Preconditions.checkArgument(blobPort > 0 || blobPort <= 65535, "Blob port is out of range.");

		TaskManagerActions taskManagerActions = new TaskManagerActionsImpl(jobMasterGateway);

		CheckpointResponder checkpointResponder = new RpcCheckpointResponder(jobMasterGateway);

		InetSocketAddress address = new InetSocketAddress(jobMasterGateway.getAddress(), blobPort);

		BlobCache blobCache = new BlobCache(address, taskManagerConfiguration.getConfiguration());

		LibraryCacheManager libraryCacheManager = new BlobLibraryCacheManager(
			blobCache,
			taskManagerConfiguration.getCleanupInterval());

		ResultPartitionConsumableNotifier resultPartitionConsumableNotifier = new RpcResultPartitionConsumableNotifier(
			jobMasterGateway,
			getRpcService().getExecutor(),
			taskManagerConfiguration.getTimeout());

		PartitionStateChecker partitionStateChecker = new RpcPartitionStateChecker(jobMasterGateway);

		return new JobManagerConnection(
			jobMasterGateway,
			taskManagerActions,
			checkpointResponder,
			libraryCacheManager,
			resultPartitionConsumableNotifier,
			partitionStateChecker);
	}

	private void disassociateFromJobManager(JobManagerConnection jobManagerConnection) throws IOException {
		if (jobManagerConnection != null) {
			JobMasterGateway jobManagerGateway = jobManagerConnection.getJobManagerGateway();

			jobManagerGateway.disconnectTaskManager(getResourceID());

			jobManagerConnection.getLibraryCacheManager().shutdown();
		}
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
	 * This method should be used when asynchronous threads want to notify the
	 * TaskExecutor of a fatal error.
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
	 * Notifies the TaskExecutor that a fatal error has occurred and it cannot proceed.
	 * This method must only be called from within the TaskExecutor's main thread.
	 *
	 * @param t The exception describing the fatal error
	 */
	void onFatalError(Throwable t) {
		log.error("Fatal error occurred.", t);
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
	public void addUnconfirmedFreeSlotNotification(SlotID slotID) {
		unconfirmedFreeSlots.add(slotID);
	}

	// ------------------------------------------------------------------------
	//  Utility classes
	// ------------------------------------------------------------------------

	/**
	 * The listener for leader changes of the resource manager
	 */
	private class ResourceManagerLeaderListener implements LeaderRetrievalListener {

		@Override
		public void notifyLeaderAddress(final String leaderAddress, final UUID leaderSessionID) {
			runAsync(new Runnable() {
				@Override
				public void run() {
					notifyOfNewResourceManagerLeader(leaderAddress, leaderSessionID);
				}
			});
		}

		@Override
		public void handleError(Exception exception) {
			onFatalErrorAsync(exception);
		}
	}

	private class TaskManagerActionsImpl implements TaskManagerActions {
		private final JobMasterGateway jobMasterGateway;

		private TaskManagerActionsImpl(JobMasterGateway jobMasterGateway) {
			this.jobMasterGateway = Preconditions.checkNotNull(jobMasterGateway);
		}

		@Override
		public void notifyFinalState(final ExecutionAttemptID executionAttemptID) {
			runAsync(new Runnable() {
				@Override
				public void run() {
					unregisterTaskAndNotifyFinalState(jobMasterGateway, executionAttemptID);
				}
			});
		}

		@Override
		public void notifyFatalError(String message, Throwable cause) {
			log.error(message, cause);
			fatalErrorHandler.onFatalError(cause);
		}

		@Override
		public void failTask(final ExecutionAttemptID executionAttemptID, final Throwable cause) {
			runAsync(new Runnable() {
				@Override
				public void run() {
					TaskExecutor.this.failTask(executionAttemptID, cause);
				}
			});
		}

		@Override
		public void updateTaskExecutionState(final TaskExecutionState taskExecutionState) {
			TaskExecutor.this.updateTaskExecutionState(jobMasterGateway, taskExecutionState);
		}
	}

}
