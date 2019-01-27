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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.Archiveable;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.resources.CommonExtendedResource;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.checkpoint.JobManagerTaskRestore;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.deployment.InputChannelDeploymentDescriptor;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.PartialInputChannelDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionLocationTrackerProxy;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.instance.SimpleSlot;
import org.apache.flink.runtime.io.network.partition.BlockingShuffleType;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.ExecutionVertexID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationConstraint;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroup;
import org.apache.flink.runtime.jobmanager.scheduler.LocationPreferenceConstraint;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.TaskNetworkMemoryUtil;
import org.apache.flink.runtime.jobmaster.failover.ResultDescriptor;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.schedule.ExecutionVertexStatus;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.util.EvictingBoundedList;
import org.apache.flink.types.Either;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.runtime.execution.ExecutionState.FINISHED;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The ExecutionVertex is a parallel subtask of the execution. It may be executed once, or several times, each of
 * which time it spawns an {@link Execution}.
 */
public class ExecutionVertex implements AccessExecutionVertex, Archiveable<ArchivedExecutionVertex> {

	private static final Logger LOG = ExecutionGraph.LOG;

	private static final int MAX_DISTINCT_LOCATIONS_TO_CONSIDER = 8;

	// --------------------------------------------------------------------------------------------

	private final ExecutionJobVertex jobVertex;

	private Map<IntermediateResultPartitionID, IntermediateResultPartition> resultPartitions;

	private final ExecutionEdge[][] inputEdges;

	private final int subTaskIndex;

	private final ExecutionVertexID executionVertexID;

	private final EvictingBoundedList<Execution> priorExecutions;

	private final Time timeout;

	/** The name in the format "myTask (2/7)", cached to avoid frequent string concatenations. */
	private final String taskNameWithSubtask;

	private volatile CoLocationConstraint locationConstraint;

	/** The current or latest execution attempt of this vertex's task. */
	private volatile Execution currentExecution;	// this field must never be null

	/** The create timestamp of execution. */
	private long createTimestamp;

	/** The location of the last execution. */
	private TaskManagerLocation latestPriorLocation = null;

	private final Map<OperatorID, List<InputSplit>> assignedInputSplitsMap = new HashMap<>();

	private final Map<OperatorID, Integer> inputSplitIndexMap = new HashMap<>();

	// --------------------------------------------------------------------------------------------

	/**
	 * Convenience constructor for tests. Sets various fields to default values.
	 */
	@VisibleForTesting
	ExecutionVertex(
			ExecutionJobVertex jobVertex,
			int subTaskIndex,
			IntermediateResult[] producedDataSets,
			Time timeout) {

		this(
				jobVertex,
				subTaskIndex,
				producedDataSets,
				timeout,
				1L,
				System.currentTimeMillis(),
				JobManagerOptions.MAX_ATTEMPTS_HISTORY_SIZE.defaultValue());
	}

	/**
	 *
	 * @param timeout
	 *            The RPC timeout to use for deploy / cancel calls
	 * @param initialGlobalModVersion
	 *            The global modification version to initialize the first Execution with.
	 * @param createTimestamp
	 *            The timestamp for the vertex creation, used to initialize the first Execution with.
	 * @param maxPriorExecutionHistoryLength
	 *            The number of prior Executions (= execution attempts) to keep.
	 */
	public ExecutionVertex(
			ExecutionJobVertex jobVertex,
			int subTaskIndex,
			IntermediateResult[] producedDataSets,
			Time timeout,
			long initialGlobalModVersion,
			long createTimestamp,
			int maxPriorExecutionHistoryLength) {

		this.jobVertex = jobVertex;
		this.subTaskIndex = subTaskIndex;
		this.executionVertexID = new ExecutionVertexID(jobVertex.getJobVertexId(), subTaskIndex);
		this.taskNameWithSubtask = String.format("%s (%d/%d)",
				jobVertex.getJobVertex().getName(), subTaskIndex + 1, jobVertex.getParallelism());

		this.resultPartitions = new LinkedHashMap<>(producedDataSets.length, 1);

		for (IntermediateResult result : producedDataSets) {
			IntermediateResultPartition irp = new IntermediateResultPartition(result, this, subTaskIndex);
			result.setPartition(subTaskIndex, irp);

			resultPartitions.put(irp.getPartitionId(), irp);
		}

		this.inputEdges = new ExecutionEdge[jobVertex.getJobVertex().getInputs().size()][];

		this.priorExecutions = new EvictingBoundedList<>(maxPriorExecutionHistoryLength);

		this.createTimestamp = createTimestamp;

		this.currentExecution = new Execution(
			getExecutionGraph().getFutureExecutor(),
			this,
			0,
			initialGlobalModVersion,
			createTimestamp,
			timeout);

		// create a co-location scheduling hint, if necessary
		final CoLocationGroup clg = jobVertex.getCoLocationGroup();
		if (clg != null) {
			synchronized (clg) {
				this.locationConstraint = clg.getLocationConstraint(subTaskIndex);
			}
		}
		else {
			this.locationConstraint = null;
		}

		getExecutionGraph().registerExecution(currentExecution);

		this.timeout = timeout;
	}


	// --------------------------------------------------------------------------------------------
	//  Properties
	// --------------------------------------------------------------------------------------------

	public JobID getJobId() {
		return this.jobVertex.getJobId();
	}

	public ExecutionJobVertex getJobVertex() {
		return jobVertex;
	}

	public JobVertexID getJobvertexId() {
		return this.jobVertex.getJobVertexId();
	}

	public ExecutionVertexID getExecutionVertexID() {
		return executionVertexID;
	}

	public String getTaskName() {
		return this.jobVertex.getJobVertex().getName();
	}

	/**
	 * Creates a simple name representation in the style 'taskname (x/y)', where
	 * 'taskname' is the name as returned by {@link #getTaskName()}, 'x' is the parallel
	 * subtask index as returned by {@link #getParallelSubtaskIndex()}{@code + 1}, and 'y' is the total
	 * number of tasks, as returned by {@link #getTotalNumberOfParallelSubtasks()}.
	 *
	 * @return A simple name representation in the form 'myTask (2/7)'
	 */
	@Override
	public String getTaskNameWithSubtaskIndex() {
		return this.taskNameWithSubtask;
	}

	public int getTotalNumberOfParallelSubtasks() {
		return this.jobVertex.getParallelism();
	}

	public int getMaxParallelism() {
		return this.jobVertex.getMaxParallelism();
	}

	@Override
	public int getParallelSubtaskIndex() {
		return this.subTaskIndex;
	}

	public int getNumberOfInputs() {
		return this.inputEdges.length;
	}

	public ExecutionEdge[] getInputEdges(int input) {
		if (input < 0 || input >= this.inputEdges.length) {
			throw new IllegalArgumentException(String.format("Input %d is out of range [0..%d)", input, this.inputEdges.length));
		}
		return inputEdges[input];
	}

	public CoLocationConstraint getLocationConstraint() {
		return locationConstraint;
	}

	@Override
	public Execution getCurrentExecutionAttempt() {
		return currentExecution;
	}

	@Override
	public ExecutionState getExecutionState() {
		return currentExecution.getState();
	}

	@Override
	public long getStateTimestamp(ExecutionState state) {
		return currentExecution.getStateTimestamp(state);
	}

	@Override
	public String getFailureCauseAsString() {
		return ExceptionUtils.stringifyException(getFailureCause());
	}

	public Throwable getFailureCause() {
		return currentExecution.getFailureCause();
	}

	public CompletableFuture<TaskManagerLocation> getCurrentTaskManagerLocationFuture() {
		return currentExecution.getTaskManagerLocationFuture();
	}

	public LogicalSlot getCurrentAssignedResource() {
		return currentExecution.getAssignedResource();
	}

	@Override
	public TaskManagerLocation getCurrentAssignedResourceLocation() {
		return currentExecution.getAssignedResourceLocation();
	}

	@Override
	public Execution getPriorExecutionAttempt(int attemptNumber) {
		synchronized (priorExecutions) {
			if (attemptNumber >= 0 && attemptNumber < priorExecutions.size()) {
				return priorExecutions.get(attemptNumber);
			} else {
				throw new IllegalArgumentException("attempt does not exist");
			}
		}
	}

	public Execution getLatestPriorExecution() {
		synchronized (priorExecutions) {
			final int size = priorExecutions.size();
			if (size > 0) {
				return priorExecutions.get(size - 1);
			}
			else {
				return null;
			}
		}
	}

	/**
	 * Gets the location where the latest completed/canceled/failed execution of the vertex's
	 * task happened.
	 *
	 * @return The latest prior execution location, or null, if there is none, yet.
	 */
	public TaskManagerLocation getLatestPriorLocation() {
		return latestPriorLocation;
	}

	public void setLatestPriorLocation(TaskManagerLocation location) {
		this.latestPriorLocation = location;
	}

	public AllocationID getLatestPriorAllocation() {
		Execution latestPriorExecution = getLatestPriorExecution();
		return latestPriorExecution != null ? latestPriorExecution.getAssignedAllocationID() : null;
	}

	EvictingBoundedList<Execution> getCopyOfPriorExecutionsList() {
		synchronized (priorExecutions) {
			return new EvictingBoundedList<>(priorExecutions);
		}
	}

	public ExecutionGraph getExecutionGraph() {
		return this.jobVertex.getGraph();
	}

	public Map<IntermediateResultPartitionID, IntermediateResultPartition> getProducedPartitions() {
		return resultPartitions;
	}

	public ExecutionVertexStatus getCurrentStatus() {
		return new ExecutionVertexStatus(executionVertexID, getExecutionState());
	}

	// --------------------------------------------------------------------------------------------
	//  Graph building
	// --------------------------------------------------------------------------------------------

	public void setInputExecutionEdges(ExecutionEdge[] edges, int inputNumber) {
		this.inputEdges[inputNumber] = edges;
	}

	/**
	 * Gets the overall preferred execution location for this vertex's current execution.
	 * The preference is determined as follows:
	 *
	 * <ol>
	 *     <li>If the task execution has state to load (from a checkpoint), then the location preference
	 *         is the location of the previous execution (if there is a previous execution attempt).
	 *     <li>If the task execution has no state or no previous location, then the location preference
	 *         is based on the task's inputs.
	 * </ol>
	 *
	 * These rules should result in the following behavior:
	 *
	 * <ul>
	 *     <li>Stateless tasks are always scheduled based on co-location with inputs.
	 *     <li>Stateful tasks are on their initial attempt executed based on co-location with inputs.
	 *     <li>Repeated executions of stateful tasks try to co-locate the execution with its state.
	 * </ul>
	 *
	 * @return The preferred execution locations for the execution attempt.
	 *
	 * @see #getPreferredLocationsBasedOnState()
	 * @see #getPreferredLocationsBasedOnInputs()
	 */
	public Collection<CompletableFuture<TaskManagerLocation>> getPreferredLocations() {
		Collection<CompletableFuture<TaskManagerLocation>> basedOnState = getPreferredLocationsBasedOnState();
		return basedOnState != null ? basedOnState : getPreferredLocationsBasedOnInputs();
	}

	/**
	 * Gets the preferred location to execute the current task execution attempt, based on the state
	 * that the execution attempt will resume.
	 *
	 * @return A size-one collection with the location preference, or null, if there is no
	 *         location preference based on the state.
	 */
	public Collection<CompletableFuture<TaskManagerLocation>> getPreferredLocationsBasedOnState() {
		TaskManagerLocation priorLocation;
		if (currentExecution.getTaskRestore() != null && (priorLocation = getLatestPriorLocation()) != null) {
			return Collections.singleton(CompletableFuture.completedFuture(priorLocation));
		}
		else {
			return null;
		}
	}

	/**
	 * Gets the location preferences of the vertex's current task execution, as determined by the locations
	 * of the predecessors from which it receives input data.
	 * If there are more than MAX_DISTINCT_LOCATIONS_TO_CONSIDER different locations of source data, this
	 * method returns {@code null} to indicate no location preference.
	 *
	 * @return The preferred locations based in input streams, or an empty iterable,
	 *         if there is no input-based preference.
	 */
	public Collection<CompletableFuture<TaskManagerLocation>> getPreferredLocationsBasedOnInputs() {
		// otherwise, base the preferred locations on the input connections
		if (inputEdges == null) {
			return Collections.emptySet();
		}
		else {
			Set<CompletableFuture<TaskManagerLocation>> locations = new HashSet<>(getTotalNumberOfParallelSubtasks());
			Set<CompletableFuture<TaskManagerLocation>> inputLocations = new HashSet<>(getTotalNumberOfParallelSubtasks());

			// go over all inputs
			for (int i = 0; i < inputEdges.length; i++) {
				inputLocations.clear();
				ExecutionEdge[] sources = inputEdges[i];
				if (sources != null) {
					// go over all input sources
					for (int k = 0; k < sources.length; k++) {
						// look-up assigned slot of input source
						CompletableFuture<TaskManagerLocation> locationFuture = sources[k].getSource().getProducer().getCurrentTaskManagerLocationFuture();
						// add input location
						inputLocations.add(locationFuture);
						// inputs which have too many distinct sources are not considered
						if (inputLocations.size() > MAX_DISTINCT_LOCATIONS_TO_CONSIDER) {
							inputLocations.clear();
							break;
						}
					}
				}
				// keep the locations of the input with the least preferred locations
				if (locations.isEmpty() || // nothing assigned yet
						(!inputLocations.isEmpty() && inputLocations.size() < locations.size())) {
					// current input has fewer preferred locations
					locations.clear();
					locations.addAll(inputLocations);
				}
			}

			return locations.isEmpty() ? Collections.emptyList() : locations;
		}
	}

	// --------------------------------------------------------------------------------------------
	//   Resources
	// --------------------------------------------------------------------------------------------

	public ResourceProfile calculateResourceProfile() {
		if (jobVertex.getJobVertex().getMinResources().equals(ResourceSpec.DEFAULT)) {
			return ResourceProfile.UNKNOWN;
		} else {
			int networkMemory = calculateTaskNetworkMemory();

			int additionalManagedMemory = calculateTaskExtraManagedMemory();
			ResourceSpec additionalResourceSpec = ResourceSpec.newBuilder().addExtendedResource(
				new CommonExtendedResource(ResourceSpec.MANAGED_MEMORY_NAME, additionalManagedMemory))
				.build();

			return ResourceProfile.fromResourceSpec(
				getJobVertex().getJobVertex().getMinResources()
					.merge(additionalResourceSpec), networkMemory);
		}
	}

	@VisibleForTesting
	int calculateTaskNetworkMemory() {
		Configuration config = jobVertex.getGraph().getJobManagerConfiguration();

		BlockingShuffleType shuffleType =
			BlockingShuffleType.getBlockingShuffleTypeFromConfiguration(config, LOG);
		int numInternalSubpartitions = 0;
		int numInternalResultPartitions = 0;
		for (IntermediateResultPartition irp : getProducedPartitions().values()) {
			if (!(shuffleType == BlockingShuffleType.YARN && irp.getIntermediateResult().getResultType().isBlocking())) {
				for (List<ExecutionEdge> consumer : irp.getConsumers()) {
					numInternalSubpartitions += consumer.size();
				}
				++numInternalResultPartitions;
			}
		}

		final int maxBlockingRequestsInFlight = config.getInteger(TaskManagerOptions.TASK_EXTERNAL_SHUFFLE_MAX_CONCURRENT_REQUESTS);

		int numPipelineChannels = 0;
		int numPipelineGates = 0;
		int numExternalBlockingChannels = 0;
		int numExternalBlockingGates = 0;
		for (int j = 0; j < getNumberOfInputs(); ++j) {
			ExecutionEdge[] edges = getInputEdges(j);

			checkState(edges.length > 0, "There should be at least on edge for each input");

			// Check the result type by viewing the first edge
			boolean isExternalBlocking = edges[0].getSource().getIntermediateResult().getResultType().isBlocking()
				&& shuffleType == BlockingShuffleType.YARN;

			if (isExternalBlocking) {
				numExternalBlockingChannels += edges.length;
				numExternalBlockingGates++;
			} else {
				numPipelineChannels += edges.length;
				numPipelineGates++;
			}
		}

		if (maxBlockingRequestsInFlight > 0) {
			numExternalBlockingChannels = Math.min(numExternalBlockingChannels, maxBlockingRequestsInFlight);
			// each blocking input gate should monopolize at least one piece of resource to
			// support input selection by operator
			numExternalBlockingChannels = Math.max(numExternalBlockingChannels, numExternalBlockingGates);
		}

		return TaskNetworkMemoryUtil.calculateTaskNetworkMemory(config,
			numInternalSubpartitions, numInternalResultPartitions, numPipelineChannels,
			numPipelineGates, numExternalBlockingChannels, numExternalBlockingGates);
	}

	private int calculateTaskExtraManagedMemory() {
		Configuration config = getJobVertex().getGraph().getJobManagerConfiguration();

		// Calculates managed memory for external result partition.
		BlockingShuffleType shuffleType =
			BlockingShuffleType.getBlockingShuffleTypeFromConfiguration(config, LOG);
		int numExternalResultPartitions = 0;
		for (IntermediateResultPartition irp : getProducedPartitions().values()) {
			if (shuffleType == BlockingShuffleType.YARN && irp.getIntermediateResult().getResultType().isBlocking()) {
				numExternalResultPartitions++;
			}
		}
		int mapOutputMemoryInMB = config.getInteger(TaskManagerOptions.TASK_MANAGER_OUTPUT_MEMORY_MB);
		return mapOutputMemoryInMB * numExternalResultPartitions;
	}

	// --------------------------------------------------------------------------------------------
	//   Actions
	// --------------------------------------------------------------------------------------------

	/**
	 * Archives the current Execution and creates a new Execution for this vertex.
	 *
	 * <p>This method atomically checks if the ExecutionGraph is still of an expected
	 * global mod. version and replaces the execution if that is the case. If the ExecutionGraph
	 * has increased its global mod. version in the meantime, this operation fails.
	 *
	 * <p>This mechanism can be used to prevent conflicts between various concurrent recovery and
	 * reconfiguration actions in a similar way as "optimistic concurrency control".
	 *
	 * @param timestamp
	 *             The creation timestamp for the new Execution
	 * @param originatingGlobalModVersion
	 *             The
	 * @return Returns the new created Execution.
	 *
	 * @throws GlobalModVersionMismatch Thrown, if the execution graph has a new global mod
	 *                                  version than the one passed to this message.
	 */
	public Execution resetForNewExecution(final long timestamp, final long originatingGlobalModVersion)
			throws GlobalModVersionMismatch {
		LOG.debug("Resetting execution vertex {} for new execution.", getTaskNameWithSubtaskIndex());

		synchronized (priorExecutions) {
			// check if another global modification has been triggered since the
			// action that originally caused this reset/restart happened
			final long actualModVersion = getExecutionGraph().getGlobalModVersion();
			if (actualModVersion > originatingGlobalModVersion) {
				// global change happened since, reject this action
				throw new GlobalModVersionMismatch(originatingGlobalModVersion, actualModVersion);
			}

			final Execution oldExecution = currentExecution;
			final ExecutionState oldState = oldExecution.getState();

			if (oldState.isTerminal() || getExecutionGraph().getGraphManager().isReplaying()) {
				priorExecutions.add(oldExecution);
				latestPriorLocation = oldExecution.getAssignedResourceLocation();

				final Execution newExecution = new Execution(
					getExecutionGraph().getFutureExecutor(),
					this,
					oldExecution.getAttemptNumber() + 1,
					originatingGlobalModVersion,
					timestamp,
					timeout);

				this.currentExecution = newExecution;

				CoLocationGroup grp = jobVertex.getCoLocationGroup();
				if (grp != null) {
					this.locationConstraint = grp.getLocationConstraint(subTaskIndex);
				}

				// register this execution at the execution graph, to receive call backs
				getExecutionGraph().registerExecution(newExecution);

				// if the execution was 'FINISHED' before, tell the ExecutionGraph that
				// we take one step back on the road to reaching global FINISHED
				if (oldState == FINISHED) {
					getExecutionGraph().vertexUnFinished();
				}

				//TODO: set this index according to checkpoint when batch support checkpoint.
				inputSplitIndexMap.clear();

				// Reset intermediate results
				for (IntermediateResultPartition resultPartition : resultPartitions.values()) {
					resultPartition.resetForNewExecution();
				}

				return newExecution;
			}
			else {
				throw new IllegalStateException("Cannot reset a vertex that is in non-terminal state " + oldState);
			}
		}
	}

	/**
	 * Schedules the current execution of this ExecutionVertex.
	 *
	 * @param slotProvider to allocate the slots from
	 * @param queued if the allocation can be queued
	 * @param locationPreferenceConstraint constraint for the location preferences
	 * @return Future which is completed once the execution is deployed. The future
	 * can also completed exceptionally.
	 */
	public CompletableFuture<Void> scheduleForExecution(
			SlotProvider slotProvider,
			boolean queued,
			LocationPreferenceConstraint locationPreferenceConstraint) {
		return this.currentExecution.scheduleForExecution(
			slotProvider,
			queued,
			locationPreferenceConstraint);
	}

	@VisibleForTesting
	public void deployToSlot(SimpleSlot slot) throws JobException {
		if (this.currentExecution.tryAssignResource(slot)) {
			this.currentExecution.deploy();
		} else {
			throw new IllegalStateException("Could not assign resource " + slot + " to current execution " +
				currentExecution + '.');
		}
	}

	/**
	 *
	 * @return A future that completes once the execution has reached its final state.
	 */
	public CompletableFuture<?> cancel() {
		// to avoid any case of mixup in the presence of concurrent calls,
		// we copy a reference to the stack to make sure both calls go to the same Execution.
		final Execution exec = this.currentExecution;
		exec.cancel();
		return exec.getReleaseFuture();
	}

	public void stop() {
		this.currentExecution.stop();
	}

	public void fail(Throwable t) {
		this.currentExecution.fail(t);
	}

	/**
	 * Schedules or updates the consumer tasks of the result partition with the given ID.
	 */
	void scheduleOrUpdateConsumers(ResultPartitionID partitionId) {

		final Execution execution = currentExecution;

		// Abort this request if there was a concurrent reset
		if (!partitionId.getProducerId().equals(execution.getAttemptId())) {
			return;
		}

		final IntermediateResultPartition partition = resultPartitions.get(partitionId.getPartitionId());

		if (partition == null) {
			throw new IllegalStateException("Unknown partition " + partitionId + ".");
		}

		if (partition.getIntermediateResult().getResultType().isPipelined()) {
			// Schedule or update receivers of this partition
			partition.markDataProduced();
			// Notify the scheduler to handle the consumable partition
			notifyAndUpdateConsumers(partition);
		}
		else {
			throw new IllegalArgumentException("ScheduleOrUpdateConsumers msg is only valid for" +
					"pipelined partitions.");
		}
	}

	protected void notifyAndUpdateConsumers(IntermediateResultPartition partition) {

		getExecutionGraph().getGraphManager().notifyResultPartitionConsumable(
				getExecutionVertexID(),
				partition.getIntermediateResult().getId(),
				partition.getPartitionNumber(),
				getCurrentAssignedResourceLocation());

		getExecutionGraph().getFutureExecutor().execute(() -> {
			currentExecution.updateConsumers(partition.getConsumers());
		});
	}

	public void cachePartitionInfo(PartialInputChannelDeploymentDescriptor partitionInfo){
		getCurrentExecutionAttempt().cachePartitionInfo(partitionInfo);
	}

	void clearAssignedInputSplits() {
		assignedInputSplitsMap.clear();
		inputSplitIndexMap.clear();
	}

	/**
	 * Finish all blocking result partitions whose receivers can be scheduled/updated and notify.
	 */
	void finishPartitionsAndNotify() {
		for (IntermediateResultPartition partition : resultPartitions.values()) {
			partition.markFinished();

			// Blocking partitions are consumable on finished
			if (partition.getResultType().isBlocking()) {
				notifyAndUpdateConsumers(partition);
			}
		}
	}

	void resetResultPartitionID(ResultPartitionID[] partitionIds) {

		Map<IntermediateResultPartitionID, IntermediateResultPartition> newResultPartitions =
				new LinkedHashMap<>(resultPartitions.size());
		Iterator<IntermediateResultPartition> iterator = resultPartitions.values().iterator();
		for (int i = 0; i < resultPartitions.size(); i++) {
			IntermediateResultPartition resultPartition = iterator.next();
			IntermediateResultPartitionID originId = resultPartition.getPartitionId();
			resultPartition.setPartitionId(partitionIds[i].getPartitionId());
			resultPartition.getIntermediateResult().resetLookupHelper(originId, partitionIds[i].getPartitionId());
			newResultPartitions.put(resultPartition.getPartitionId(), resultPartition);
		}
		this.resultPartitions = newResultPartitions;
	}

	// the following two method is added for region failover
	// record the input split assigned to this task
	public void inputSplitAssigned(OperatorID operatorID, InputSplit inputSplit) {
		assignedInputSplitsMap.putIfAbsent(operatorID, new LinkedList<>());

		assignedInputSplitsMap.get(operatorID).add(inputSplit);
		inputSplitIndexMap.put(operatorID, inputSplitIndexMap.getOrDefault(operatorID, 0) + 1);
		Preconditions.checkArgument(inputSplitIndexMap.get(operatorID) == assignedInputSplitsMap.get(operatorID).size());
	}

	public InputSplit getNextInputSplitFromAssgined(OperatorID operatorID) {
		List<InputSplit> assignedInputSplits = assignedInputSplitsMap.getOrDefault(operatorID, Collections.emptyList());
		Integer inputSplitIndex = inputSplitIndexMap.getOrDefault(operatorID, 0);

		if (assignedInputSplits.isEmpty() || inputSplitIndex >= assignedInputSplits.size()) {
			return null;
		}

		InputSplit split = assignedInputSplits.get(inputSplitIndex++);
		inputSplitIndexMap.put(operatorID, inputSplitIndex);

		return split;
	}

	public Map<OperatorID, List<InputSplit>> getAssignedInputSplits() {
		return Collections.unmodifiableMap(assignedInputSplitsMap);
	}

	/**
	 * Recover the execution vertex status after job master failover.
	 *
	 * @param state The state in the log.
	 * @param assignedInputSplits The assigned input splits of a finished executions.
	 * @param resultDescriptor The result information of a finished executions.
	 */
	public void recoverStatus(
			ExecutionState state,
			Map<OperatorID, List<InputSplit>> assignedInputSplits,
			ResultDescriptor resultDescriptor) {
		if (!ExecutionState.FINISHED.equals(state) &&
				(assignedInputSplits != null && resultDescriptor != null)) {
			throw new FlinkRuntimeException("Can not assign input split or result partion when execution is " + state);
		}
		switch (state) {
			case FINISHED:
				currentExecution.getTaskManagerLocationFuture().complete(resultDescriptor.getTaskManagerLocation());
				resetResultPartitionID(resultDescriptor.getResultPartitionIds());
				currentExecution.markFinished();

				if (assignedInputSplits != null) {
					assignedInputSplitsMap.clear();
					assignedInputSplitsMap.putAll(assignedInputSplits);
					for (Map.Entry<OperatorID, List<InputSplit>> opToInputs : assignedInputSplits.entrySet()) {
						getJobVertex().getSplitAssigner(opToInputs.getKey()).inputSplitsAssigned(subTaskIndex, opToInputs.getValue());
					}
				}
				break;
			case RUNNING:
				currentExecution.switchToRunning();
				break;
			case DEPLOYING:
				currentExecution.recoverState(state);
				break;
			default:
				throw new FlinkRuntimeException("Unsupported replaying the state " + state);
		}
	}

	/**
	 * Recover the pipelined result partition consume status after job master failover.
	 *
	 * @param resultId The intermediate data set id in the log.
	 */
	public void recoverResultPartitionStatus(
			IntermediateDataSetID resultId,
			TaskManagerLocation location) {

		IntermediateResultPartition partitionToRecover = null;
		for (IntermediateResultPartition irp : getProducedPartitions().values()) {
			if (irp.getIntermediateResult().getId().equals(resultId)) {
				partitionToRecover = irp;
			}
		}
		if (partitionToRecover == null) {
			throw new FlinkRuntimeException("Can not find the intermediate result " + resultId + " on " + getTaskNameWithSubtaskIndex());
		}
		if (!(ExecutionState.RUNNING.equals(currentExecution.getState()) &&
						partitionToRecover.getResultType().isPipelined())) {
			throw new FlinkRuntimeException("Invalid state " + currentExecution.getState() + " for " + getTaskNameWithSubtaskIndex());
		}
		currentExecution.getTaskManagerLocationFuture().complete(location);

		scheduleOrUpdateConsumers(new ResultPartitionID(partitionToRecover.getPartitionId(), currentExecution.getAttemptId()));
	}

	/**
	 * Reconcile the execution with the running info reported by task executor.
	 * @param executionId
	 * @param attemptNumber
	 * @param startTimestamp
	 * @param partitionIds
	 */
	public boolean reconcileExecution(
			ExecutionState state,
			ExecutionAttemptID executionId,
			int attemptNumber,
			long startTimestamp,
			ResultPartitionID[] partitionIds,
			boolean[] partitionsConsumable,
			Map<OperatorID, List<InputSplit>> assignedInputSplits,
			LogicalSlot slot) {

		LOG.debug("Reconcile execution vertex {} for current execution.", getTaskNameWithSubtaskIndex());
		if (resultPartitions.size() != partitionIds.length) {
			LOG.info("Reconcile execution failed due to partition number with actual {}, expect {}.",
					partitionIds.length, resultPartitions.size());
			return false;
		}

		// first, update the IntermediateResultPartition and reset the map
		resetResultPartitionID(partitionIds);

		// second, update the pipelined partition info to its consumers.
		for (int i = 0; i < partitionIds.length; i++) {
			IntermediateResultPartition partition = resultPartitions.get(partitionIds[i].getPartitionId());
			if (partition.getResultType().isPipelined()) {
				if (partition.hasDataProduced() != partitionsConsumable[i]) {
					LOG.info("Reconcile execution {} failed due to partition {} consumable not equals to {}.",
							getTaskNameWithSubtaskIndex(), partition.getPartitionId(), partition.hasDataProduced());

					currentExecution.getReconcileFuture().complete(currentExecution.getAttemptId());
					return false;
				}
			}
		}

		// third, reset execution basic information
		getExecutionGraph().deregisterExecution(currentExecution);
		if (currentExecution.reconcileStatus(state, executionId, attemptNumber, startTimestamp, slot)) {
			getExecutionGraph().registerExecution(currentExecution);

			// forth, build the input split map
			inputSplitIndexMap.clear();
			assignedInputSplitsMap.clear();
			for (Map.Entry<OperatorID, List<InputSplit>> opToInputs : assignedInputSplits.entrySet()) {
				for (InputSplit inputSplit : opToInputs.getValue()) {
					inputSplitAssigned(opToInputs.getKey(), inputSplit);
				}
				getJobVertex().getSplitAssigner(opToInputs.getKey()).inputSplitsAssigned(subTaskIndex, opToInputs.getValue());
			}
			return true;
		}
		else {
			getExecutionGraph().registerExecution(currentExecution);
			return false;
		}
	}

	// --------------------------------------------------------------------------------------------
	//   Notifications from the Execution Attempt
	// --------------------------------------------------------------------------------------------

	void executionFinished(Execution execution) {
		getExecutionGraph().vertexFinished();
	}

	void executionCanceled(Execution execution) {
		// nothing to do
	}

	void executionFailed(Execution execution, Throwable cause) {
		// nothing to do
	}

	// --------------------------------------------------------------------------------------------
	//   Miscellaneous
	// --------------------------------------------------------------------------------------------

	/**
	 * Simply forward this notification.
	 */
	void notifyStateTransition(Execution execution, ExecutionState newState, Throwable error) {
		// only forward this notification if the execution is still the current execution
		// otherwise we have an outdated execution
		if (currentExecution == execution) {
			getExecutionGraph().notifyExecutionChange(execution, newState, error);
		}
	}

	/**
	 * Creates a task deployment descriptor to deploy a subtask to the given target slot.
	 *
	 * TODO: This should actually be in the EXECUTION
	 */
	TaskDeploymentDescriptor createDeploymentDescriptor(
			ExecutionAttemptID executionId,
			LogicalSlot targetSlot,
			@Nullable JobManagerTaskRestore taskRestore,
			int attemptNumber) throws ExecutionGraphException {

		// Produced intermediate results
		List<ResultPartitionDeploymentDescriptor> producedPartitions = new ArrayList<>(resultPartitions.size());

		// Consumed intermediate results
		List<InputGateDeploymentDescriptor> consumedPartitions = new ArrayList<>(inputEdges.length);

		boolean lazyScheduling = getExecutionGraph().isLazyDeploymentAllowed();

		for (IntermediateResultPartition partition : resultPartitions.values()) {

			List<List<ExecutionEdge>> consumers = partition.getConsumers();

			if (consumers.isEmpty()) {
				//TODO this case only exists for test, currently there has to be exactly one consumer in real jobs!
				producedPartitions.add(ResultPartitionDeploymentDescriptor.from(
						partition,
						KeyGroupRangeAssignment.UPPER_BOUND_MAX_PARALLELISM,
						lazyScheduling));
			} else {
				Preconditions.checkState(1 == consumers.size(),
						"Only one consumer supported in the current implementation! Found: " + consumers.size());

				List<ExecutionEdge> consumer = consumers.get(0);
				ExecutionJobVertex vertex = consumer.get(0).getTarget().getJobVertex();
				int maxParallelism = vertex.getMaxParallelism();
				producedPartitions.add(ResultPartitionDeploymentDescriptor.from(partition, maxParallelism, lazyScheduling));
			}
		}

		ResultPartitionLocationTrackerProxy resultPartitionLocationTrackerProxy =
			currentExecution.getVertex().getExecutionGraph().getResultPartitionLocationTrackerProxy();

		for (ExecutionEdge[] edges : inputEdges) {
			InputChannelDeploymentDescriptor[] partitions = InputChannelDeploymentDescriptor.fromEdges(
				resultPartitionLocationTrackerProxy,
				edges,
				targetSlot.getTaskManagerLocation(),
				lazyScheduling);

			// If the produced partition has multiple consumers registered, we
			// need to request the one matching our sub task index.
			// TODO Refactor after removing the consumers from the intermediate result partitions
			int numConsumerEdges = edges[0].getSource().getConsumers().get(0).size();

			int queueToRequest = subTaskIndex % numConsumerEdges;

			IntermediateResult consumedIntermediateResult = edges[0].getSource().getIntermediateResult();
			final IntermediateDataSetID resultId = consumedIntermediateResult.getId();
			final ResultPartitionType partitionType = consumedIntermediateResult.getResultType();

			consumedPartitions.add(new InputGateDeploymentDescriptor(resultId, partitionType, queueToRequest, partitions));
		}

		final Either<SerializedValue<JobInformation>, PermanentBlobKey> jobInformationOrBlobKey = getExecutionGraph().getJobInformationOrBlobKey();

		final TaskDeploymentDescriptor.MaybeOffloaded<JobInformation> serializedJobInformation;

		if (jobInformationOrBlobKey.isLeft()) {
			serializedJobInformation = new TaskDeploymentDescriptor.NonOffloaded<>(jobInformationOrBlobKey.left());
		} else {
			serializedJobInformation = new TaskDeploymentDescriptor.Offloaded<>(jobInformationOrBlobKey.right());
		}

		final Either<SerializedValue<TaskInformation>, PermanentBlobKey> taskInformationOrBlobKey;

		try {
			taskInformationOrBlobKey = jobVertex.getTaskInformationOrBlobKey();
		} catch (IOException e) {
			throw new ExecutionGraphException(
				"Could not create a serialized JobVertexInformation for " +
					jobVertex.getJobVertexId(), e);
		}

		final TaskDeploymentDescriptor.MaybeOffloaded<TaskInformation> serializedTaskInformation;

		if (taskInformationOrBlobKey.isLeft()) {
			serializedTaskInformation = new TaskDeploymentDescriptor.NonOffloaded<>(taskInformationOrBlobKey.left());
		} else {
			serializedTaskInformation = new TaskDeploymentDescriptor.Offloaded<>(taskInformationOrBlobKey.right());
		}

		return new TaskDeploymentDescriptor(
			getJobId(),
			serializedJobInformation,
			serializedTaskInformation,
			executionId,
			targetSlot.getAllocationId(),
			subTaskIndex,
			attemptNumber,
			targetSlot.getPhysicalSlotNumber(),
			createTimestamp,
			taskRestore,
			producedPartitions,
			consumedPartitions);
	}

	// --------------------------------------------------------------------------------------------
	//  Utilities
	// --------------------------------------------------------------------------------------------

	@Override
	public String toString() {
		return getTaskNameWithSubtaskIndex();
	}

	@Override
	public ArchivedExecutionVertex archive() {
		return new ArchivedExecutionVertex(this);
	}
}
