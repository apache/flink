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
import org.apache.flink.runtime.executiongraph.SlotProviderStrategy;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobmanager.scheduler.ScheduledUnit;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Default {@link ExecutionSlotAllocator} which will use {@link SlotProvider} to allocate slots and
 * keep the unfulfilled requests for further cancellation.
 */
public class DefaultExecutionSlotAllocator implements ExecutionSlotAllocator {

	private static final Logger LOG = LoggerFactory.getLogger(DefaultExecutionSlotAllocator.class);

	/**
	 * Store the uncompleted slot assignments.
	 */
	private final Map<ExecutionVertexID, SlotExecutionVertexAssignment> pendingSlotAssignments;

	private final SlotProviderStrategy slotProviderStrategy;

	private final PreferredLocationsRetriever preferredLocationsRetriever;

	public DefaultExecutionSlotAllocator(
			final SlotProviderStrategy slotProviderStrategy,
			final PreferredLocationsRetriever preferredLocationsRetriever) {
		this.slotProviderStrategy = checkNotNull(slotProviderStrategy);
		this.preferredLocationsRetriever = checkNotNull(preferredLocationsRetriever);

		pendingSlotAssignments = new HashMap<>();
	}

	@Override
	public List<SlotExecutionVertexAssignment> allocateSlotsFor(
			List<ExecutionVertexSchedulingRequirements> executionVertexSchedulingRequirements) {

		validateSchedulingRequirements(executionVertexSchedulingRequirements);

		List<SlotExecutionVertexAssignment> slotExecutionVertexAssignments =
				new ArrayList<>(executionVertexSchedulingRequirements.size());

		Set<AllocationID> allPreviousAllocationIds = computeAllPriorAllocationIds(executionVertexSchedulingRequirements);

		for (ExecutionVertexSchedulingRequirements schedulingRequirements : executionVertexSchedulingRequirements) {
			final ExecutionVertexID executionVertexId = schedulingRequirements.getExecutionVertexId();
			final SlotRequestId slotRequestId = new SlotRequestId();
			final SlotSharingGroupId slotSharingGroupId = schedulingRequirements.getSlotSharingGroupId();

			LOG.debug("Allocate slot with id {} for execution {}", slotRequestId, executionVertexId);

			CompletableFuture<LogicalSlot> slotFuture = calculatePreferredLocations(
					executionVertexId).thenCompose(
							(Collection<TaskManagerLocation> preferredLocations) ->
								slotProviderStrategy.allocateSlot(
									slotRequestId,
									new ScheduledUnit(
										executionVertexId,
										slotSharingGroupId,
										schedulingRequirements.getCoLocationConstraint()),
									SlotProfile.priorAllocation(
										schedulingRequirements.getTaskResourceProfile(),
										schedulingRequirements.getPhysicalSlotResourceProfile(),
										preferredLocations,
										Collections.singletonList(schedulingRequirements.getPreviousAllocationId()),
										allPreviousAllocationIds)));

			SlotExecutionVertexAssignment slotExecutionVertexAssignment =
					new SlotExecutionVertexAssignment(executionVertexId, slotFuture);
			// add to map first to avoid the future completed before added.
			pendingSlotAssignments.put(executionVertexId, slotExecutionVertexAssignment);

			slotFuture.whenComplete(
					(ignored, throwable) -> {
						pendingSlotAssignments.remove(executionVertexId);
						if (throwable != null) {
							slotProviderStrategy.cancelSlotRequest(slotRequestId, slotSharingGroupId, throwable);
						}
					});

			slotExecutionVertexAssignments.add(slotExecutionVertexAssignment);
		}

		return slotExecutionVertexAssignments;
	}

	private void validateSchedulingRequirements(Collection<ExecutionVertexSchedulingRequirements> schedulingRequirements) {
		schedulingRequirements.stream()
			.map(ExecutionVertexSchedulingRequirements::getExecutionVertexId)
			.forEach(id -> checkState(
				!pendingSlotAssignments.containsKey(id),
				"BUG: vertex %s tries to allocate a slot when its previous slot request is still pending", id));
	}

	@Override
	public void cancel(ExecutionVertexID executionVertexId) {
		SlotExecutionVertexAssignment slotExecutionVertexAssignment = pendingSlotAssignments.get(executionVertexId);
		if (slotExecutionVertexAssignment != null) {
			slotExecutionVertexAssignment.getLogicalSlotFuture().cancel(false);
		}
	}

	/**
	 * Calculates the preferred locations for an execution.
	 * It will first try to use preferred locations based on state,
	 * if null, will use the preferred locations based on inputs.
	 */
	private CompletableFuture<Collection<TaskManagerLocation>> calculatePreferredLocations(
			ExecutionVertexID executionVertexId) {
		return preferredLocationsRetriever.getPreferredLocations(executionVertexId, Collections.emptySet());
	}

	/**
	 * Computes and returns a set with the prior allocation ids from all execution vertices scheduled together.
	 *
	 * @param executionVertexSchedulingRequirements contains the execution vertices which are scheduled together
	 */
	@VisibleForTesting
	static Set<AllocationID> computeAllPriorAllocationIds(
			Collection<ExecutionVertexSchedulingRequirements> executionVertexSchedulingRequirements) {
		return executionVertexSchedulingRequirements
			.stream()
			.map(ExecutionVertexSchedulingRequirements::getPreviousAllocationId)
			.filter(Objects::nonNull)
			.collect(Collectors.toSet());
	}

	@VisibleForTesting
	int getNumberOfPendingSlotAssignments() {
		return pendingSlotAssignments.size();
	}
}
