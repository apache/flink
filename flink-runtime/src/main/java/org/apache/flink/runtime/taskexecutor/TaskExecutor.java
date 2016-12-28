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
import org.apache.flink.runtime.accumulators.AccumulatorSnapshot;
import org.apache.flink.runtime.blob.BlobCache;
import org.apache.flink.runtime.broadcast.BroadcastVariableManager;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.concurrent.AcceptFunction;
import org.apache.flink.runtime.concurrent.ApplyFunction;
import org.apache.flink.runtime.concurrent.Future;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.execution.librarycache.BlobLibraryCacheManager;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.JobInformation;
import org.apache.flink.runtime.executiongraph.PartitionInfo;
import org.apache.flink.runtime.executiongraph.TaskInformation;
import org.apache.flink.runtime.filecache.FileCache;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.NetworkEnvironment;
import org.apache.flink.runtime.io.network.netty.PartitionProducerStateChecker;
import org.apache.flink.runtime.io.network.partition.ResultPartitionConsumableNotifier;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.jobmaster.JMTMRegistrationSuccess;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.runtime.resourcemanager.messages.taskexecutor.TMSlotRequestRegistered;
import org.apache.flink.runtime.resourcemanager.messages.taskexecutor.TMSlotRequestReply;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcMethod;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.taskexecutor.exceptions.CheckpointException;
import org.apache.flink.runtime.taskexecutor.exceptions.PartitionException;
import org.apache.flink.runtime.taskexecutor.exceptions.SlotAllocationException;
import org.apache.flink.runtime.taskexecutor.exceptions.TaskException;
import org.apache.flink.runtime.taskexecutor.exceptions.TaskSubmissionException;
import org.apache.flink.runtime.taskexecutor.rpc.RpcCheckpointResponder;
import org.apache.flink.runtime.taskexecutor.rpc.RpcInputSplitProvider;
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
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * TaskExecutor implementation. The task executor is responsible for the execution of multiple
 * {@link Task}.
 */
public class TaskExecutor extends RpcEndpoint<TaskExecutorGateway> {

	/** The connection information of this task manager */
	private final TaskManagerLocation taskManagerLocation;

	/** Max blob port which is accepted */
	public static final int MAX_BLOB_PORT = 65536;

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

	private final FileCache fileCache;

	// --------- resource manager --------

	private TaskExecutorToResourceManagerConnection resourceManagerConnection;

	// --------- job manager connections -----------

	private Map<ResourceID, JobManagerConnection> jobManagerConnections;

	// --------- task slot allocation table -----------

	private final TaskSlotTable taskSlotTable;

	private final JobManagerTable jobManagerTable;

	private final JobLeaderService jobLeaderService;

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
		TaskSlotTable taskSlotTable,
		JobManagerTable jobManagerTable,
		JobLeaderService jobLeaderService,
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
		this.taskSlotTable = checkNotNull(taskSlotTable);
		this.fatalErrorHandler = checkNotNull(fatalErrorHandler);
		this.taskManagerMetricGroup = checkNotNull(taskManagerMetricGroup);
		this.broadcastVariableManager = checkNotNull(broadcastVariableManager);
		this.fileCache = checkNotNull(fileCache);
		this.jobManagerTable = checkNotNull(jobManagerTable);
		this.jobLeaderService = checkNotNull(jobLeaderService);

		this.jobManagerConnections = new HashMap<>(4);
	}

	// ------------------------------------------------------------------------
	//  Life cycle
	// ------------------------------------------------------------------------

	@Override
	public void start() throws Exception {
		super.start();

		// start by connecting to the ResourceManager
		try {
			haServices.getResourceManagerLeaderRetriever().start(new ResourceManagerLeaderListener());
		} catch (Exception e) {
			onFatalErrorAsync(e);
		}

		// tell the task slot table who's responsible for the task slot actions
		taskSlotTable.start(new SlotActionsImpl());

		// start the job leader service
		jobLeaderService.start(getAddress(), getRpcService(), haServices, new JobLeaderListenerImpl());
	}

	/**
	 * Called to shut down the TaskManager. The method closes all TaskManager services.
	 */
	@Override
	public void shutDown() throws Exception {
		log.info("Stopping TaskManager {}.", getAddress());

		Exception exception = null;

		taskSlotTable.stop();

		if (isConnectedToResourceManager()) {
			resourceManagerConnection.close();
		}

		ioManager.shutdown();

		memoryManager.shutdown();

		networkEnvironment.shutdown();

		fileCache.shutdown();

		try {
			super.shutDown();
		} catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(e, exception);
		}

		if (exception != null) {
			ExceptionUtils.rethrowException(exception, "Error while shutting the TaskExecutor down.");
		}

		log.info("Stopped TaskManager {}.", getAddress());
	}

	// ======================================================================
	//  RPC methods
	// ======================================================================

	// ----------------------------------------------------------------------
	// Task lifecycle RPCs
	// ----------------------------------------------------------------------

	@RpcMethod
	public Acknowledge submitTask(TaskDeploymentDescriptor tdd, UUID jobManagerLeaderId) throws TaskSubmissionException {

		// first, deserialize the pre-serialized information
		final JobInformation jobInformation;
		final TaskInformation taskInformation;
		try {
			jobInformation = tdd.getSerializedJobInformation().deserializeValue(getClass().getClassLoader());
			taskInformation = tdd.getSerializedTaskInformation().deserializeValue(getClass().getClassLoader());
		}
		catch (IOException | ClassNotFoundException e) {
			throw new TaskSubmissionException("Could not deserialize the job or task information.", e);
		}

		final JobID jobId = jobInformation.getJobId();
		final JobManagerConnection jobManagerConnection = jobManagerTable.get(jobId);

		if (jobManagerConnection == null) {
			final String message = "Could not submit task because there is no JobManager " +
				"associated for the job " + jobId + '.';

			log.debug(message);
			throw new TaskSubmissionException(message);
		}

		if (!jobManagerConnection.getLeaderId().equals(jobManagerLeaderId)) {
			final String message = "Rejecting the task submission because the job manager leader id " +
				jobManagerLeaderId + " does not match the expected job manager leader id " +
				jobManagerConnection.getLeaderId() + '.';

			log.debug(message);
			throw new TaskSubmissionException(message);
		}

		if (!taskSlotTable.existsActiveSlot(jobId, tdd.getAllocationId())) {
			final String message = "No task slot allocated for job ID " + jobId +
				" and allocation ID " + tdd.getAllocationId() + '.';
			log.debug(message);
			throw new TaskSubmissionException(message);
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
				jobManagerConnection.getLeaderId(),
				jobManagerConnection.getJobManagerGateway(),
				jobInformation.getJobId(),
				taskInformation.getJobVertexId(),
				tdd.getExecutionAttemptId(),
				taskManagerConfiguration.getTimeout());

		TaskManagerActions taskManagerActions = jobManagerConnection.getTaskManagerActions();
		CheckpointResponder checkpointResponder = jobManagerConnection.getCheckpointResponder();
		LibraryCacheManager libraryCache = jobManagerConnection.getLibraryCacheManager();
		ResultPartitionConsumableNotifier resultPartitionConsumableNotifier = jobManagerConnection.getResultPartitionConsumableNotifier();
		PartitionProducerStateChecker partitionStateChecker = jobManagerConnection.getPartitionStateChecker();

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
				tdd.getTaskStateHandles(),
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

		boolean taskAdded;

		try {
			taskAdded = taskSlotTable.addTask(task);
		} catch (SlotNotFoundException | SlotNotActiveException e) {
			throw new TaskSubmissionException("Could not submit task.", e);
		}

		if (taskAdded) {
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
		final Task task = taskSlotTable.getTask(executionAttemptID);

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
		final Task task = taskSlotTable.getTask(executionAttemptID);

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
	public Acknowledge updatePartitions(final ExecutionAttemptID executionAttemptID, Iterable<PartitionInfo> partitionInfos) throws PartitionException {
		final Task task = taskSlotTable.getTask(executionAttemptID);

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

		final Task task = taskSlotTable.getTask(executionAttemptID);

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

		final Task task = taskSlotTable.getTask(executionAttemptID);

		if (task != null) {
			task.notifyCheckpointComplete(checkpointId);

			return Acknowledge.get();
		} else {
			final String message = "TaskManager received a checkpoint confirmation for unknown task " + executionAttemptID + '.';

			log.debug(message);
			throw new CheckpointException(message);
		}
	}

	// ----------------------------------------------------------------------
	// Slot allocation RPCs
	// ----------------------------------------------------------------------

	/**
	 * /**
	 * Requests a slot from the TaskManager
	 *
	 * @param slotId identifying the requested slot
	 * @param jobId identifying the job for which the request is issued
	 * @param allocationId id for the request
	 * @param targetAddress of the job manager requesting the slot
	 * @param rmLeaderId current leader id of the ResourceManager
	 * @throws SlotAllocationException if the slot allocation fails
	 * @return answer to the slot request
	 */
	@RpcMethod
	public TMSlotRequestReply requestSlot(
		final SlotID slotId,
		final JobID jobId,
		final AllocationID allocationId,
		final String targetAddress,
		final UUID rmLeaderId) throws SlotAllocationException {
		log.info("Receive slot request {} for job {} from resource manager with leader id {}.",
			allocationId, jobId, rmLeaderId);

		if (resourceManagerConnection == null) {
			final String message = "TaskManager is not connected to a resource manager.";
			log.debug(message);
			throw new SlotAllocationException(message);
		}

		if (!resourceManagerConnection.getTargetLeaderId().equals(rmLeaderId)) {
			final String message = "The leader id " + rmLeaderId +
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
			throw new SlotAllocationException(message);
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

				// sanity check
				if (!taskSlotTable.isSlotFree(slotId.getSlotNumber())) {
					onFatalError(new Exception("Could not free slot " + slotId));
				}

				throw new SlotAllocationException("Could not add job to job leader service.", e);
			}
		}

		return new TMSlotRequestRegistered(resourceManagerConnection.getRegistrationId(), getResourceID(), allocationId);
	}

	// ======================================================================
	//  Internal methods
	// ======================================================================

	// ------------------------------------------------------------------------
	//  Internal resource manager connection methods
	// ------------------------------------------------------------------------

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

		// establish a connection to the new leader
		if (newLeaderAddress != null) {
			log.info("Attempting to register at ResourceManager {}", newLeaderAddress);
			resourceManagerConnection =
				new TaskExecutorToResourceManagerConnection(
					log,
					getRpcService(),
					getAddress(),
					getResourceID(),
					taskSlotTable.createSlotReport(getResourceID()),
					newLeaderAddress,
					newLeaderId,
					getMainThreadExecutor(),
					new ForwardingFatalErrorHandler());
			resourceManagerConnection.start();
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
				final UUID leaderId = jobManagerConnection.getLeaderId();

				final Collection<SlotOffer> reservedSlots = new HashSet<>(2);

				while (reservedSlotsIterator.hasNext()) {
					reservedSlots.add(reservedSlotsIterator.next().generateSlotOffer());
				}

				Future<Iterable<SlotOffer>> acceptedSlotsFuture = jobMasterGateway.offerSlots(
					getResourceID(),
					reservedSlots,
					leaderId,
					taskManagerConfiguration.getTimeout());

				acceptedSlotsFuture.thenAcceptAsync(new AcceptFunction<Iterable<SlotOffer>>() {
					@Override
					public void accept(Iterable<SlotOffer> acceptedSlots) {
						// check if the response is still valid
						if (isJobManagerConnectionValid(jobId, leaderId)) {
							// mark accepted slots active
							for (SlotOffer acceptedSlot: acceptedSlots) {
								try {
									if (!taskSlotTable.markSlotActive(acceptedSlot.getAllocationId())) {
										// the slot is either free or releasing at the moment
										final String message = "Could not mark slot " + jobId + " active.";
										log.debug(message);
										jobMasterGateway.failSlot(getResourceID(), acceptedSlot.getAllocationId(),
												leaderId, new Exception(message));
									}

									// remove the assigned slots so that we can free the left overs
									reservedSlots.remove(acceptedSlot);
								} catch (SlotNotFoundException e) {
									log.debug("Could not mark slot {} active.", acceptedSlot,  e);
									jobMasterGateway.failSlot(getResourceID(), acceptedSlot.getAllocationId(), leaderId, e);
								}
							}

							final Exception e = new Exception("The slot was rejected by the JobManager.");

							for (SlotOffer rejectedSlot: reservedSlots) {
								freeSlot(rejectedSlot.getAllocationId(), e);
							}
						} else {
							// discard the response since there is a new leader for the job
							log.debug("Discard offer slot response since there is a new leader " +
								"for the job {}.", jobId);
						}
					}
				}, getMainThreadExecutor());

				acceptedSlotsFuture.exceptionallyAsync(new ApplyFunction<Throwable, Void>() {
					@Override
					public Void apply(Throwable throwable) {
						if (throwable instanceof TimeoutException) {
							// We ran into a timeout. Try again.
							offerSlotsToJobManager(jobId);
						} else {
							// We encountered an exception. Free the slots and return them to the RM.
							for (SlotOffer reservedSlot: reservedSlots) {
								freeSlot(reservedSlot.getAllocationId(), throwable);
							}
						}

						return null;
					}
				}, getMainThreadExecutor());
			} else {
				log.debug("There are no unassigned slots for the job {}.", jobId);
			}
		}
	}

	private void establishJobManagerConnection(JobID jobId, JobMasterGateway jobMasterGateway, UUID jobManagerLeaderId, JMTMRegistrationSuccess registrationSuccess) {
		log.info("Establish JobManager connection for job {}.", jobId);

		if (jobManagerTable.contains(jobId)) {
			JobManagerConnection oldJobManagerConnection = jobManagerTable.get(jobId);

			if (!oldJobManagerConnection.getLeaderId().equals(jobManagerLeaderId)) {
				closeJobManagerConnection(jobId);
				jobManagerTable.put(jobId, associateWithJobManager(jobMasterGateway, jobManagerLeaderId, registrationSuccess.getBlobPort()));
			}
		} else {
			jobManagerTable.put(jobId, associateWithJobManager(jobMasterGateway, jobManagerLeaderId, registrationSuccess.getBlobPort()));
		}

		offerSlotsToJobManager(jobId);
	}

	private void closeJobManagerConnection(JobID jobId) {
		log.info("Close JobManager connection for job {}.", jobId);

		// 1. fail tasks running under this JobID
		Iterator<Task> tasks = taskSlotTable.getTasks(jobId);

		while (tasks.hasNext()) {
			tasks.next().failExternally(new Exception("JobManager responsible for " + jobId +
				" lost the leadership."));
		}

		// 2. Move the active slots to state allocated (possible to time out again)
		Iterator<AllocationID> activeSlots = taskSlotTable.getActiveSlots(jobId);

		while (activeSlots.hasNext()) {
			AllocationID activeSlot = activeSlots.next();

			try {
				if (!taskSlotTable.markSlotInactive(activeSlot, taskManagerConfiguration.getTimeout())) {
					freeSlot(activeSlot, new Exception("Slot could not be marked inactive."));
				}
			} catch (SlotNotFoundException e) {
				log.debug("Could not mark the slot {} inactive.", jobId, e);
			}
		}

		// 3. Disassociate from the JobManager
		JobManagerConnection jobManagerConnection = jobManagerTable.remove(jobId);

		if (jobManagerConnection != null) {
			try {
				disassociateFromJobManager(jobManagerConnection);
			} catch (IOException e) {
				log.warn("Could not properly disassociate from JobManager {}.",
					jobManagerConnection.getJobManagerGateway().getAddress(), e);
			}
		}
	}

	private JobManagerConnection associateWithJobManager(JobMasterGateway jobMasterGateway, UUID jobManagerLeaderId, int blobPort) {
		Preconditions.checkNotNull(jobManagerLeaderId);
		Preconditions.checkNotNull(jobMasterGateway);
		Preconditions.checkArgument(blobPort > 0 || blobPort < MAX_BLOB_PORT, "Blob port is out of range.");

		TaskManagerActions taskManagerActions = new TaskManagerActionsImpl(jobManagerLeaderId, jobMasterGateway);

		CheckpointResponder checkpointResponder = new RpcCheckpointResponder(jobMasterGateway);

		InetSocketAddress address = new InetSocketAddress(jobMasterGateway.getAddress(), blobPort);

		BlobCache blobCache = new BlobCache(address, taskManagerConfiguration.getConfiguration());

		LibraryCacheManager libraryCacheManager = new BlobLibraryCacheManager(
			blobCache,
			taskManagerConfiguration.getCleanupInterval());

		ResultPartitionConsumableNotifier resultPartitionConsumableNotifier = new RpcResultPartitionConsumableNotifier(
			jobManagerLeaderId,
			jobMasterGateway,
			getRpcService().getExecutor(),
			taskManagerConfiguration.getTimeout());

		PartitionProducerStateChecker partitionStateChecker = new RpcPartitionStateChecker(jobManagerLeaderId, jobMasterGateway);

		return new JobManagerConnection(
			jobMasterGateway,
			jobManagerLeaderId,
			taskManagerActions,
			checkpointResponder,
			libraryCacheManager,
			resultPartitionConsumableNotifier,
			partitionStateChecker);
	}

	private void disassociateFromJobManager(JobManagerConnection jobManagerConnection) throws IOException {
		Preconditions.checkNotNull(jobManagerConnection);
		JobMasterGateway jobManagerGateway = jobManagerConnection.getJobManagerGateway();
		jobManagerGateway.disconnectTaskManager(getResourceID());
		jobManagerConnection.getLibraryCacheManager().shutdown();
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
			final UUID jobMasterLeaderId,
			final JobMasterGateway jobMasterGateway,
			final TaskExecutionState taskExecutionState)
	{
		final ExecutionAttemptID executionAttemptID = taskExecutionState.getID();

		Future<Acknowledge> futureAcknowledge = jobMasterGateway.updateTaskExecutionState(
				jobMasterLeaderId, taskExecutionState);

		futureAcknowledge.exceptionallyAsync(new ApplyFunction<Throwable, Void>() {
			@Override
			public Void apply(Throwable value) {
				failTask(executionAttemptID, value);

				return null;
			}
		}, getMainThreadExecutor());
	}

	private void unregisterTaskAndNotifyFinalState(
			final UUID jobMasterLeaderId,
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
					jobMasterLeaderId,
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

	private void freeSlot(AllocationID allocationId, Throwable cause) {
		Preconditions.checkNotNull(allocationId);

		try {
			int freedSlotIndex = taskSlotTable.freeSlot(allocationId, cause);

			if (freedSlotIndex != -1 && isConnectedToResourceManager()) {
				// the slot was freed. Tell the RM about it
				ResourceManagerGateway resourceManagerGateway = resourceManagerConnection.getTargetGateway();

				resourceManagerGateway.notifySlotAvailable(
					resourceManagerConnection.getTargetLeaderId(),
					resourceManagerConnection.getRegistrationId(),
					new SlotID(getResourceID(), freedSlotIndex));
			}
		} catch (SlotNotFoundException e) {
			log.debug("Could not free slot for allocation id {}.", allocationId, e);
		}
	}

	private void freeSlot(AllocationID allocationId) {
		freeSlot(allocationId, new Exception("The slot " + allocationId + " is beeing freed."));
	}

	private void timeoutSlot(AllocationID allocationId, UUID ticket) {
		Preconditions.checkNotNull(allocationId);
		Preconditions.checkNotNull(ticket);

		if (taskSlotTable.isValidTimeout(allocationId, ticket)) {
			freeSlot(allocationId, new Exception("The slot " + allocationId + " has timed out."));
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

	private boolean isJobManagerConnectionValid(JobID jobId, UUID leaderId) {
		JobManagerConnection jmConnection = jobManagerTable.get(jobId);

		return jmConnection != null && jmConnection.getLeaderId().equals(leaderId);
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

	// ------------------------------------------------------------------------
	//  Utility classes
	// ------------------------------------------------------------------------

	/**
	 * The listener for leader changes of the resource manager
	 */
	private final class ResourceManagerLeaderListener implements LeaderRetrievalListener {

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

	private final class JobLeaderListenerImpl implements JobLeaderListener {

		@Override
		public void jobManagerGainedLeadership(
			final JobID jobId,
			final JobMasterGateway jobManagerGateway,
			final UUID jobLeaderId,
			final JMTMRegistrationSuccess registrationMessage) {
			runAsync(new Runnable() {
				@Override
				public void run() {
					establishJobManagerConnection(
						jobId,
						jobManagerGateway,
						jobLeaderId,
						registrationMessage);
				}
			});
		}

		@Override
		public void jobManagerLostLeadership(final JobID jobId, final UUID jobLeaderId) {
			log.info("JobManager for job {} with leader id {} lost leadership.", jobId, jobLeaderId);

			runAsync(new Runnable() {
				@Override
				public void run() {
					closeJobManagerConnection(jobId);
				}
			});
		}

		@Override
		public void handleError(Throwable throwable) {
			onFatalErrorAsync(throwable);
		}
	}

	private final class ForwardingFatalErrorHandler implements FatalErrorHandler {

		@Override
		public void onFatalError(Throwable exception) {
			onFatalErrorAsync(exception);
		}
	}

	private final class TaskManagerActionsImpl implements TaskManagerActions {
		private final UUID jobMasterLeaderId;
		private final JobMasterGateway jobMasterGateway;

		private TaskManagerActionsImpl(UUID jobMasterLeaderId, JobMasterGateway jobMasterGateway) {
			this.jobMasterLeaderId = Preconditions.checkNotNull(jobMasterLeaderId);
			this.jobMasterGateway = Preconditions.checkNotNull(jobMasterGateway);
		}

		@Override
		public void notifyFinalState(final ExecutionAttemptID executionAttemptID) {
			runAsync(new Runnable() {
				@Override
				public void run() {
					unregisterTaskAndNotifyFinalState(jobMasterLeaderId, jobMasterGateway, executionAttemptID);
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
			TaskExecutor.this.updateTaskExecutionState(jobMasterLeaderId, jobMasterGateway, taskExecutionState);
		}
	}

	private class SlotActionsImpl implements SlotActions {

		@Override
		public void freeSlot(final AllocationID allocationId) {
			runAsync(new Runnable() {
				@Override
				public void run() {
					TaskExecutor.this.freeSlot(allocationId);
				}
			});
		}

		@Override
		public void timeoutSlot(final AllocationID allocationId, final UUID ticket) {
			runAsync(new Runnable() {
				@Override
				public void run() {
					TaskExecutor.this.timeoutSlot(allocationId, ticket);
				}
			});
		}
	}
}
