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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.jobmanager.scheduler.Locality;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotOwner;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.jobmaster.slotpool.BulkSlotProvider;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotRequest;
import org.apache.flink.runtime.jobmaster.slotpool.SingleLogicalSlot;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.util.FlinkException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * This slot allocator will request one physical slot for each single execution vertex.
 * The slots will be requested in bulks so that the {@link SlotProvider} can check
 * whether this bulk of slot requests can be fulfilled at the same time.
 * It has several limitations:
 *
 * <p>1. Slot sharing will be ignored.
 *
 * <p>2. Co-location constraints are not allowed.
 *
 * <p>3. Intra-bulk input location preferences will be ignored.
 */
class OneSlotPerExecutionSlotAllocator extends AbstractExecutionSlotAllocator implements SlotOwner {

	private static final Logger LOG = LoggerFactory.getLogger(OneSlotPerExecutionSlotAllocator.class);

	private final BulkSlotProvider slotProvider;

	private final boolean slotWillBeOccupiedIndefinitely;

	private final Time allocationTimeout;

	OneSlotPerExecutionSlotAllocator(
			final BulkSlotProvider slotProvider,
			final PreferredLocationsRetriever preferredLocationsRetriever,
			final boolean slotWillBeOccupiedIndefinitely,
			final Time allocationTimeout) {

		super(preferredLocationsRetriever);

		this.slotProvider = checkNotNull(slotProvider);
		this.slotWillBeOccupiedIndefinitely = slotWillBeOccupiedIndefinitely;
		this.allocationTimeout = checkNotNull(allocationTimeout);
	}

	@Override
	public List<SlotExecutionVertexAssignment> allocateSlotsFor(
			final List<ExecutionVertexSchedulingRequirements> executionVertexSchedulingRequirements) {

		validateSchedulingRequirements(executionVertexSchedulingRequirements);

		validateNoCoLocationConstraint(executionVertexSchedulingRequirements);

		// LinkedHashMap is needed to retain the given order
		final LinkedHashMap<SlotRequestId, SlotExecutionVertexAssignment> slotExecutionVertexAssignments =
			createSlotExecutionVertexAssignments(executionVertexSchedulingRequirements);

		final Map<ExecutionVertexID, SlotRequestId> executionVertexSlotRequestIds = slotExecutionVertexAssignments
			.entrySet()
			.stream()
			.collect(Collectors.toMap(e -> e.getValue().getExecutionVertexId(), Map.Entry::getKey));

		final List<CompletableFuture<PhysicalSlotRequest>> physicalSlotRequestFutures =
			createPhysicalSlotRequestFutures(
				executionVertexSchedulingRequirements,
				executionVertexSlotRequestIds);

		allocateSlotsForAssignments(
			physicalSlotRequestFutures,
			slotExecutionVertexAssignments);

		return Collections.unmodifiableList(new ArrayList<>(slotExecutionVertexAssignments.values()));
	}

	private static void validateNoCoLocationConstraint(
			final Collection<ExecutionVertexSchedulingRequirements> schedulingRequirements) {

		final boolean hasCoLocationConstraint = schedulingRequirements.stream()
			.anyMatch(r -> r.getCoLocationConstraint() != null);
		checkState(
			!hasCoLocationConstraint,
			"Jobs with co-location constraints are not allowed to run with pipelined region scheduling strategy.");
	}

	private LinkedHashMap<SlotRequestId, SlotExecutionVertexAssignment> createSlotExecutionVertexAssignments(
			final List<ExecutionVertexSchedulingRequirements> executionVertexSchedulingRequirements) {

		final LinkedHashMap<SlotRequestId, SlotExecutionVertexAssignment> assignments = new LinkedHashMap<>();
		for (ExecutionVertexSchedulingRequirements schedulingRequirements : executionVertexSchedulingRequirements) {
			final ExecutionVertexID executionVertexId = schedulingRequirements.getExecutionVertexId();

			final SlotRequestId slotRequestId = new SlotRequestId();
			final SlotExecutionVertexAssignment slotExecutionVertexAssignment =
				createAndRegisterSlotExecutionVertexAssignment(
					executionVertexId,
					new CompletableFuture<>(),
					throwable -> slotProvider.cancelSlotRequest(slotRequestId, throwable));
			assignments.put(slotRequestId, slotExecutionVertexAssignment);
		}

		return assignments;
	}

	private List<CompletableFuture<PhysicalSlotRequest>> createPhysicalSlotRequestFutures(
			final List<ExecutionVertexSchedulingRequirements> executionVertexSchedulingRequirements,
			final Map<ExecutionVertexID, SlotRequestId> executionVertexSlotRequestIds) {

		final Set<AllocationID> allPreviousAllocationIds =
			computeAllPriorAllocationIds(executionVertexSchedulingRequirements);

		final List<CompletableFuture<PhysicalSlotRequest>> physicalSlotRequestFutures =
			new ArrayList<>(executionVertexSchedulingRequirements.size());
		for (ExecutionVertexSchedulingRequirements schedulingRequirements : executionVertexSchedulingRequirements) {
			final ExecutionVertexID executionVertexId = schedulingRequirements.getExecutionVertexId();
			final SlotRequestId slotRequestId = executionVertexSlotRequestIds.get(executionVertexId);

			LOG.debug("Allocate slot with id {} for execution {}", slotRequestId, executionVertexId);

			// use the task resource profile as the physical slot resource requirement since slot sharing is ignored
			final CompletableFuture<SlotProfile> slotProfileFuture = getSlotProfileFuture(
				schedulingRequirements,
				schedulingRequirements.getTaskResourceProfile(),
				executionVertexSlotRequestIds.keySet(),
				allPreviousAllocationIds);

			final CompletableFuture<PhysicalSlotRequest> physicalSlotRequestFuture =
				slotProfileFuture.thenApply(
					slotProfile -> createPhysicalSlotRequest(slotRequestId, slotProfile));
			physicalSlotRequestFutures.add(physicalSlotRequestFuture);
		}

		return physicalSlotRequestFutures;
	}

	private PhysicalSlotRequest createPhysicalSlotRequest(
			final SlotRequestId slotRequestId,
			final SlotProfile slotProfile) {
		return new PhysicalSlotRequest(slotRequestId, slotProfile, slotWillBeOccupiedIndefinitely);
	}

	private void allocateSlotsForAssignments(
			final List<CompletableFuture<PhysicalSlotRequest>> physicalSlotRequestFutures,
			final Map<SlotRequestId, SlotExecutionVertexAssignment> slotExecutionVertexAssignments) {

		FutureUtils.combineAll(physicalSlotRequestFutures)
			.thenCompose(physicalSlotRequests -> slotProvider.allocatePhysicalSlots(physicalSlotRequests, allocationTimeout))
			.thenAccept(physicalSlotRequestResults -> {
				for (PhysicalSlotRequest.Result result : physicalSlotRequestResults) {
					final SlotRequestId slotRequestId = result.getSlotRequestId();
					final SlotExecutionVertexAssignment assignment = slotExecutionVertexAssignments.get(slotRequestId);

					checkState(assignment != null);

					final LogicalSlot logicalSlot = SingleLogicalSlot.allocateFromPhysicalSlot(
						slotRequestId,
						result.getPhysicalSlot(),
						Locality.UNKNOWN,
						this,
						slotWillBeOccupiedIndefinitely);
					assignment.getLogicalSlotFuture().complete(logicalSlot);
				}
			})
			.exceptionally(ex -> {
				slotExecutionVertexAssignments.values().forEach(
					assignment -> assignment.getLogicalSlotFuture().completeExceptionally(ex));
				return null;
			});
	}

	@Override
	public void returnLogicalSlot(LogicalSlot logicalSlot) {
		slotProvider.cancelSlotRequest(
			logicalSlot.getSlotRequestId(),
			new FlinkException("Slot is being returned to OneSlotPerExecutionSlotAllocator."));
	}
}
