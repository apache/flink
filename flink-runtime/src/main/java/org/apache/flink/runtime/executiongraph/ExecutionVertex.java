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

import org.apache.flink.api.common.Archiveable;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.deployment.InputChannelDeploymentDescriptor;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.PartialInputChannelDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.instance.SimpleSlot;
import org.apache.flink.runtime.instance.SlotProvider;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationConstraint;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroup;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.TaskStateHandles;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.util.EvictingBoundedList;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;

import org.slf4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.runtime.execution.ExecutionState.CANCELED;
import static org.apache.flink.runtime.execution.ExecutionState.FAILED;
import static org.apache.flink.runtime.execution.ExecutionState.FINISHED;

/**
 * The ExecutionVertex is a parallel subtask of the execution. It may be executed once, or several times, each of
 * which time it spawns an {@link Execution}.
 */
public class ExecutionVertex implements AccessExecutionVertex, Archiveable<ArchivedExecutionVertex> {

	private static final Logger LOG = ExecutionGraph.LOG;

	private static final int MAX_DISTINCT_LOCATIONS_TO_CONSIDER = 8;

	// --------------------------------------------------------------------------------------------

	private final ExecutionJobVertex jobVertex;

	private final Map<IntermediateResultPartitionID, IntermediateResultPartition> resultPartitions;

	private final ExecutionEdge[][] inputEdges;

	private final int subTaskIndex;

	private final EvictingBoundedList<Execution> priorExecutions;

	private final Time timeout;

	/** The name in the format "myTask (2/7)", cached to avoid frequent string concatenations */
	private final String taskNameWithSubtask;

	private volatile CoLocationConstraint locationConstraint;

	/** The current or latest execution attempt of this vertex's task */
	private volatile Execution currentExecution;	// this field must never be null

	// --------------------------------------------------------------------------------------------

	public ExecutionVertex(
			ExecutionJobVertex jobVertex,
			int subTaskIndex,
			IntermediateResult[] producedDataSets,
			Time timeout) {

		this(
				jobVertex,
				subTaskIndex,
				producedDataSets,
				timeout,
				System.currentTimeMillis(),
				JobManagerOptions.MAX_ATTEMPTS_HISTORY_SIZE.defaultValue());
	}

	public ExecutionVertex(
			ExecutionJobVertex jobVertex,
			int subTaskIndex,
			IntermediateResult[] producedDataSets,
			Time timeout,
			int maxPriorExecutionHistoryLength) {
		this(jobVertex, subTaskIndex, producedDataSets, timeout, System.currentTimeMillis(), maxPriorExecutionHistoryLength);
	}

	public ExecutionVertex(
			ExecutionJobVertex jobVertex,
			int subTaskIndex,
			IntermediateResult[] producedDataSets,
			Time timeout,
			long createTimestamp,
			int maxPriorExecutionHistoryLength) {

		this.jobVertex = jobVertex;
		this.subTaskIndex = subTaskIndex;
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

		this.currentExecution = new Execution(
			getExecutionGraph().getFutureExecutor(),
			this,
			0,
			createTimestamp,
			timeout);

		// create a co-location scheduling hint, if necessary
		CoLocationGroup clg = jobVertex.getCoLocationGroup();
		if (clg != null) {
			this.locationConstraint = clg.getLocationConstraint(subTaskIndex);
		}
		else {
			this.locationConstraint = null;
		}

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

	public SimpleSlot getCurrentAssignedResource() {
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

	/**
	 * Gets the location where the latest completed/canceled/failed execution of the vertex's
	 * task happened.
	 * 
	 * @return The latest prior execution location, or null, if there is none, yet.
	 */
	public TaskManagerLocation getLatestPriorLocation() {
		synchronized (priorExecutions) {
			final int size = priorExecutions.size();
			if (size > 0) {
				return priorExecutions.get(size - 1).getAssignedResourceLocation();
			}
			else {
				return null;
			}
		}
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

	// --------------------------------------------------------------------------------------------
	//  Graph building
	// --------------------------------------------------------------------------------------------

	public void connectSource(int inputNumber, IntermediateResult source, JobEdge edge, int consumerNumber) {

		final DistributionPattern pattern = edge.getDistributionPattern();
		final IntermediateResultPartition[] sourcePartitions = source.getPartitions();

		ExecutionEdge[] edges;

		switch (pattern) {
			case POINTWISE:
				edges = connectPointwise(sourcePartitions, inputNumber);
				break;

			case ALL_TO_ALL:
				edges = connectAllToAll(sourcePartitions, inputNumber);
				break;

			default:
				throw new RuntimeException("Unrecognized distribution pattern.");

		}

		this.inputEdges[inputNumber] = edges;

		// add the consumers to the source
		// for now (until the receiver initiated handshake is in place), we need to register the
		// edges as the execution graph
		for (ExecutionEdge ee : edges) {
			ee.getSource().addConsumer(ee, consumerNumber);
		}
	}

	private ExecutionEdge[] connectAllToAll(IntermediateResultPartition[] sourcePartitions, int inputNumber) {
		ExecutionEdge[] edges = new ExecutionEdge[sourcePartitions.length];

		for (int i = 0; i < sourcePartitions.length; i++) {
			IntermediateResultPartition irp = sourcePartitions[i];
			edges[i] = new ExecutionEdge(irp, this, inputNumber);
		}

		return edges;
	}

	private ExecutionEdge[] connectPointwise(IntermediateResultPartition[] sourcePartitions, int inputNumber) {
		final int numSources = sourcePartitions.length;
		final int parallelism = getTotalNumberOfParallelSubtasks();

		// simple case same number of sources as targets
		if (numSources == parallelism) {
			return new ExecutionEdge[] { new ExecutionEdge(sourcePartitions[subTaskIndex], this, inputNumber) };
		}
		else if (numSources < parallelism) {

			int sourcePartition;

			// check if the pattern is regular or irregular
			// we use int arithmetics for regular, and floating point with rounding for irregular
			if (parallelism % numSources == 0) {
				// same number of targets per source
				int factor = parallelism / numSources;
				sourcePartition = subTaskIndex / factor;
			}
			else {
				// different number of targets per source
				float factor = ((float) parallelism) / numSources;
				sourcePartition = (int) (subTaskIndex / factor);
			}

			return new ExecutionEdge[] { new ExecutionEdge(sourcePartitions[sourcePartition], this, inputNumber) };
		}
		else {
			if (numSources % parallelism == 0) {
				// same number of targets per source
				int factor = numSources / parallelism;
				int startIndex = subTaskIndex * factor;

				ExecutionEdge[] edges = new ExecutionEdge[factor];
				for (int i = 0; i < factor; i++) {
					edges[i] = new ExecutionEdge(sourcePartitions[startIndex + i], this, inputNumber);
				}
				return edges;
			}
			else {
				float factor = ((float) numSources) / parallelism;

				int start = (int) (subTaskIndex * factor);
				int end = (subTaskIndex == getTotalNumberOfParallelSubtasks() - 1) ?
						sourcePartitions.length :
						(int) ((subTaskIndex + 1) * factor);

				ExecutionEdge[] edges = new ExecutionEdge[end - start];
				for (int i = 0; i < edges.length; i++) {
					edges[i] = new ExecutionEdge(sourcePartitions[start + i], this, inputNumber);
				}

				return edges;
			}
		}
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
	 * @return The preferred excution locations for the execution attempt.
	 * 
	 * @see #getPreferredLocationsBasedOnState()
	 * @see #getPreferredLocationsBasedOnInputs() 
	 */
	public Iterable<TaskManagerLocation> getPreferredLocations() {
		Iterable<TaskManagerLocation> basedOnState = getPreferredLocationsBasedOnState();
		return basedOnState != null ? basedOnState : getPreferredLocationsBasedOnInputs();
	}
	
	/**
	 * Gets the preferred location to execute the current task execution attempt, based on the state
	 * that the execution attempt will resume.
	 * 
	 * @return A size-one iterable with the location preference, or null, if there is no
	 *         location preference based on the state.
	 */
	public Iterable<TaskManagerLocation> getPreferredLocationsBasedOnState() {
		TaskManagerLocation priorLocation;
		if (currentExecution.getTaskStateHandles() != null && (priorLocation = getLatestPriorLocation()) != null) {
			return Collections.singleton(priorLocation);
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
	public Iterable<TaskManagerLocation> getPreferredLocationsBasedOnInputs() {
		// otherwise, base the preferred locations on the input connections
		if (inputEdges == null) {
			return Collections.emptySet();
		}
		else {
			Set<TaskManagerLocation> locations = new HashSet<>();
			Set<TaskManagerLocation> inputLocations = new HashSet<>();

			// go over all inputs
			for (int i = 0; i < inputEdges.length; i++) {
				inputLocations.clear();
				ExecutionEdge[] sources = inputEdges[i];
				if (sources != null) {
					// go over all input sources
					for (int k = 0; k < sources.length; k++) {
						// look-up assigned slot of input source
						SimpleSlot sourceSlot = sources[k].getSource().getProducer().getCurrentAssignedResource();
						if (sourceSlot != null) {
							// add input location
							inputLocations.add(sourceSlot.getTaskManagerLocation());
							// inputs which have too many distinct sources are not considered
							if (inputLocations.size() > MAX_DISTINCT_LOCATIONS_TO_CONSIDER) {
								inputLocations.clear();
								break;
							}
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

			return locations.isEmpty() ? Collections.<TaskManagerLocation>emptyList() : locations;
		}
	}

	// --------------------------------------------------------------------------------------------
	//   Actions
	// --------------------------------------------------------------------------------------------

	public void resetForNewExecution() {

		LOG.debug("Resetting execution vertex {} for new execution.", getTaskNameWithSubtaskIndex());

		synchronized (priorExecutions) {
			Execution execution = currentExecution;
			ExecutionState state = execution.getState();

			if (state == FINISHED || state == CANCELED || state == FAILED) {
				priorExecutions.add(execution);
				currentExecution = new Execution(
					getExecutionGraph().getFutureExecutor(),
					this,
					execution.getAttemptNumber()+1,
					System.currentTimeMillis(),
					timeout);

				CoLocationGroup grp = jobVertex.getCoLocationGroup();
				if (grp != null) {
					this.locationConstraint = grp.getLocationConstraint(subTaskIndex);
				}
			}
			else {
				throw new IllegalStateException("Cannot reset a vertex that is in state " + state);
			}
		}
	}

	public boolean scheduleForExecution(SlotProvider slotProvider, boolean queued) {
		return this.currentExecution.scheduleForExecution(slotProvider, queued);
	}

	public void deployToSlot(SimpleSlot slot) throws JobException {
		this.currentExecution.deployToSlot(slot);
	}

	public void cancel() {
		this.currentExecution.cancel();
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
			execution.scheduleOrUpdateConsumers(partition.getConsumers());
		}
		else {
			throw new IllegalArgumentException("ScheduleOrUpdateConsumers msg is only valid for" +
					"pipelined partitions.");
		}
	}

	public void cachePartitionInfo(PartialInputChannelDeploymentDescriptor partitionInfo){
		getCurrentExecutionAttempt().cachePartitionInfo(partitionInfo);
	}

	void sendPartitionInfos() {
		currentExecution.sendPartitionInfos();
	}

	/**
	 * Returns all blocking result partitions whose receivers can be scheduled/updated.
	 */
	List<IntermediateResultPartition> finishAllBlockingPartitions() {
		List<IntermediateResultPartition> finishedBlockingPartitions = null;

		for (IntermediateResultPartition partition : resultPartitions.values()) {
			if (partition.getResultType().isBlocking() && partition.markFinished()) {
				if (finishedBlockingPartitions == null) {
					finishedBlockingPartitions = new LinkedList<IntermediateResultPartition>();
				}

				finishedBlockingPartitions.add(partition);
			}
		}

		if (finishedBlockingPartitions == null) {
			return Collections.emptyList();
		}
		else {
			return finishedBlockingPartitions;
		}
	}

	// --------------------------------------------------------------------------------------------
	//   Notifications from the Execution Attempt
	// --------------------------------------------------------------------------------------------

	void executionFinished() {
		jobVertex.vertexFinished(subTaskIndex);
	}

	void executionCanceled() {
		jobVertex.vertexCancelled(subTaskIndex);
	}

	void executionFailed(Throwable t) {
		jobVertex.vertexFailed(subTaskIndex, t);
	}

	// --------------------------------------------------------------------------------------------
	//   Miscellaneous
	// --------------------------------------------------------------------------------------------

	/**
	 * Simply forward this notification. This is for logs and event archivers.
	 */
	void notifyStateTransition(ExecutionAttemptID executionId, ExecutionState newState, Throwable error) {
		getExecutionGraph().notifyExecutionChange(getJobvertexId(), subTaskIndex, executionId, newState, error);
	}

	/**
	 * Creates a task deployment descriptor to deploy a subtask to the given target slot.
	 *
	 * TODO: This should actually be in the EXECUTION
	 */
	TaskDeploymentDescriptor createDeploymentDescriptor(
			ExecutionAttemptID executionId,
			SimpleSlot targetSlot,
			TaskStateHandles taskStateHandles,
			int attemptNumber) throws ExecutionGraphException {
		
		// Produced intermediate results
		List<ResultPartitionDeploymentDescriptor> producedPartitions = new ArrayList<>(resultPartitions.size());
		
		// Consumed intermediate results
		List<InputGateDeploymentDescriptor> consumedPartitions = new ArrayList<>(inputEdges.length);
		
		boolean lazyScheduling = getExecutionGraph().getScheduleMode().allowLazyDeployment();

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
		
		
		for (ExecutionEdge[] edges : inputEdges) {
			InputChannelDeploymentDescriptor[] partitions = InputChannelDeploymentDescriptor
					.fromEdges(edges, targetSlot, lazyScheduling);

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

		SerializedValue<JobInformation> serializedJobInformation = getExecutionGraph().getSerializedJobInformation();
		SerializedValue<TaskInformation> serializedJobVertexInformation = null;

		try {
			serializedJobVertexInformation = jobVertex.getSerializedTaskInformation();
		} catch (IOException e) {
			throw new ExecutionGraphException(
					"Could not create a serialized JobVertexInformation for " + jobVertex.getJobVertexId(), e);
		}

		return new TaskDeploymentDescriptor(
			serializedJobInformation,
			serializedJobVertexInformation,
			executionId,
			targetSlot.getAllocatedSlot().getSlotAllocationId(),
			subTaskIndex,
			attemptNumber,
			targetSlot.getRoot().getSlotNumber(),
			taskStateHandles,
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
