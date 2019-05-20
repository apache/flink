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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionEdge;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobmanager.scheduler.ScheduledUnit;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.jobmaster.slotpool.Scheduler;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.runtime.executiongraph.ExecutionVertex.MAX_DISTINCT_LOCATIONS_TO_CONSIDER;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Default {@link ExecutionSlotAllocator} which will use {@link SlotProvider} to allocate slots and
 * keep the unfulfilled requests for further cancellation.
 */
public class DefaultExecutionSlotAllocator implements ExecutionSlotAllocator {

	static final Logger LOG = LoggerFactory.getLogger(DefaultExecutionSlotAllocator.class);

	/**
	 * Store the uncompleted slot assignments.
	 */
	private final Map<ExecutionVertexID, SlotExecutionVertexAssignment> pendingSlotAssignments;

	private final SlotProvider slotProvider;

	private final ExecutionGraph executionGraph;

	public DefaultExecutionSlotAllocator(SlotProvider slotProvider, ExecutionGraph executionGraph) {
		this.slotProvider = checkNotNull(slotProvider);
		this.executionGraph = checkNotNull(executionGraph);

		pendingSlotAssignments = new HashMap<>();
	}

	@Override
	public Collection<SlotExecutionVertexAssignment> allocateSlotsFor(
			Collection<ExecutionVertexSchedulingRequirements> executionVertexSchedulingRequirements) {

		List<SlotExecutionVertexAssignment> slotExecutionVertexAssignments =
				new ArrayList<>(executionVertexSchedulingRequirements.size());

		Set<AllocationID> allPreviousAllocationIds =
				computeAllPriorAllocationIdsIfRequiredByScheduling(executionVertexSchedulingRequirements);

		Set<ExecutionVertexID> verticesScheduledTogether = getAllExecutionVerticesId(executionVertexSchedulingRequirements);

		for (ExecutionVertexSchedulingRequirements schedulingRequirements : executionVertexSchedulingRequirements) {
			final ExecutionVertexID executionVertexId = schedulingRequirements.getExecutionVertexId();
			final SlotRequestId slotRequestId = new SlotRequestId();
			final SlotSharingGroupId slotSharingGroupId = schedulingRequirements.getSlotSharingGroupId();

			Execution execution = null;
			ExecutionJobVertex ejv = executionGraph.getJobVertex(executionVertexId.getJobVertexId());
			if (ejv != null && ejv.getParallelism() > executionVertexId.getSubtaskIndex()) {
				execution = ejv.getTaskVertices()[executionVertexId.getSubtaskIndex()].getCurrentExecutionAttempt();

			}
			if (execution == null) {
				throw new IllegalArgumentException("Fail to get execution for vertex id " + executionVertexId);
			}

			if (LOG.isDebugEnabled()) {
				LOG.debug("Allocate slot with id {} for execution {}", slotRequestId, executionVertexId);
			}

			// TODO: the calculation of preferred location should be refined so we can use the preferredLocations
			// in ExecutionVertexSchedulingRequirements instead of calculating it here
			CompletableFuture<LogicalSlot> slotFuture =
					calculatePreferredLocations(execution, verticesScheduledTogether).thenCompose(
							(Collection<TaskManagerLocation> preferredLocations) ->
								slotProvider.allocateSlot(
									slotRequestId,
									new ScheduledUnit(
											null,
											executionVertexId.getJobVertexId(),
											slotSharingGroupId,
											schedulingRequirements.getCoLocationConstraint()),
									new SlotProfile(
											schedulingRequirements.getResourceProfile(),
											preferredLocations,
											Arrays.asList(schedulingRequirements.getPreviousAllocationId()),
											allPreviousAllocationIds),
									executionGraph.isQueuedSchedulingAllowed(),
									executionGraph.getAllocationTimeout()));

			SlotExecutionVertexAssignment slotExecutionVertexAssignment =
					new SlotExecutionVertexAssignment(executionVertexId, slotFuture);
			// add to map first to avoid the future completed before added.
			pendingSlotAssignments.put(executionVertexId, slotExecutionVertexAssignment);

			slotFuture.whenComplete(
					(ignored, throwable) -> {
						pendingSlotAssignments.remove(executionVertexId);
						if (throwable != null) {
							slotProvider.cancelSlotRequest(slotRequestId, slotSharingGroupId, throwable);
						}
					});

			slotExecutionVertexAssignments.add(slotExecutionVertexAssignment);
		}

		return slotExecutionVertexAssignments;
	}

	@Override
	public void cancel(ExecutionVertexID executionVertexId) {
		SlotExecutionVertexAssignment slotExecutionVertexAssignment = pendingSlotAssignments.get(executionVertexId);
		if (slotExecutionVertexAssignment != null) {
			slotExecutionVertexAssignment.getLogicalSlotFuture().cancel(false);
		}
	}

	@Override
	public CompletableFuture<Void> stop() {
		List<ExecutionVertexID> executionVertexIds = new ArrayList<>(pendingSlotAssignments.keySet());
		executionVertexIds.stream().forEach(executionVertexID -> cancel(executionVertexID));

		return CompletableFuture.completedFuture(null);
	}

	private static Set<ExecutionVertexID> getAllExecutionVerticesId(
			Collection<ExecutionVertexSchedulingRequirements> executionVertexSchedulingRequirements) {
		Set<ExecutionVertexID> executionVertexIds = new HashSet<>(executionVertexSchedulingRequirements.size());
		for (ExecutionVertexSchedulingRequirements schedulingRequirements : executionVertexSchedulingRequirements) {
			executionVertexIds.add(schedulingRequirements.getExecutionVertexId());
		}
		return executionVertexIds;
	}

	/**
	 * Calculates the preferred locations for an execution.
	 * It will first try to use preferred locations based on state,
	 * if null, will use the preferred locations based on inputs.
	 */
	private static CompletableFuture<Collection<TaskManagerLocation>> calculatePreferredLocations(
			Execution execution,
			Collection<ExecutionVertexID> verticesScheduledTogether) {

		Collection<CompletableFuture<TaskManagerLocation>> preferredLocations =
				execution.getVertex().getPreferredLocationsBasedOnState();

		if (preferredLocations == null) {
			preferredLocations = getPreferredLocationsBasedOnInputs(execution, verticesScheduledTogether);
		}
		return FutureUtils.combineAll(preferredLocations);
	}

	/**
	 * Gets the location preferences of the execution, as determined by the locations
	 * of the predecessors from which it receives input data.
	 * If there are more than {@link MAX_DISTINCT_LOCATIONS_TO_CONSIDER} different locations of source data,
	 * or neither the sources have not been started nor will be started with the execution together,
	 * this method returns an empty collection to indicate no location preference.
	 *
	 * @return The preferred locations based in input streams, or an empty iterable,
	 *         if there is no input-based preference.
	 */
	@VisibleForTesting
	static Collection<CompletableFuture<TaskManagerLocation>> getPreferredLocationsBasedOnInputs(
			Execution execution,
			Collection<ExecutionVertexID> verticesScheduledTogether) {
		Collection<CompletableFuture<TaskManagerLocation>> preferredLocations = new HashSet<>();

		Set<CompletableFuture<TaskManagerLocation>> inputLocations = new HashSet<>();
		for (int i = 0; i < execution.getVertex().getNumberOfInputs(); i++) {
			ExecutionEdge[] inputEdges = execution.getVertex().getInputEdges(i);
			if (inputEdges.length <= MAX_DISTINCT_LOCATIONS_TO_CONSIDER) {
				for (ExecutionEdge inputEdge : inputEdges) {
					ExecutionVertex producer = inputEdge.getSource().getProducer();
					if (verticesScheduledTogether.contains(
							new ExecutionVertexID(producer.getJobvertexId(), producer.getParallelSubtaskIndex()))
							|| producer.getExecutionState() != ExecutionState.CREATED) {
						inputLocations.add(producer.getCurrentTaskManagerLocationFuture());
					} else {
						inputLocations.clear();
						break;
					}
				}
				if (preferredLocations.isEmpty() ||
						(!inputLocations.isEmpty() && inputLocations.size() < preferredLocations.size())) {
					preferredLocations.clear();
					preferredLocations.addAll(inputLocations);
				}
				inputLocations.clear();
			}
		}
		return preferredLocations;
	}

	/**
	 * Returns the result of {@link #computeAllPriorAllocationIds}, but only if the scheduling really requires it.
	 * Otherwise this method simply returns an empty set.
	 */
	private Set<AllocationID> computeAllPriorAllocationIdsIfRequiredByScheduling(
			Collection<ExecutionVertexSchedulingRequirements> executionVertexSchedulingRequirements) {
		// This is a temporary optimization to avoid computing all previous allocations if not required
		// This can go away when we progress with the implementation of the Scheduler.
		if (slotProvider instanceof Scheduler && ((Scheduler) slotProvider).requiresPreviousExecutionGraphAllocations()) {
			return computeAllPriorAllocationIds(executionVertexSchedulingRequirements);
		} else {
			return Collections.emptySet();
		}
	}

	/**
	 * Computes and returns a set with the prior allocation ids from all execution vertices scheduled together.
	 *
	 * @param executionVertexSchedulingRequirements contains the execution vertices which are scheduled together
	 */
	@VisibleForTesting
	static Set<AllocationID> computeAllPriorAllocationIds(
			Collection<ExecutionVertexSchedulingRequirements> executionVertexSchedulingRequirements) {
		HashSet<AllocationID> allPreviousAllocationIds = new HashSet<>(executionVertexSchedulingRequirements.size());
		for (ExecutionVertexSchedulingRequirements schedulingRequirements : executionVertexSchedulingRequirements) {
			AllocationID priorAllocation = schedulingRequirements.getPreviousAllocationId();
			if (priorAllocation != null) {
				allPreviousAllocationIds.add(priorAllocation);
			}
		}
		return allPreviousAllocationIds;
	}

	@VisibleForTesting
	Map<ExecutionVertexID, SlotExecutionVertexAssignment> getPendingSlotAssignments() {
		return Collections.unmodifiableMap(pendingSlotAssignments);
	}
}
