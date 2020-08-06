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

import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.jobmanager.scheduler.Locality;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotOwner;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlot;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotProvider;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotRequest;
import org.apache.flink.runtime.jobmaster.slotpool.SingleLogicalSlot;
import org.apache.flink.runtime.scheduler.SharedSlotProfileRetriever.SharedSlotProfileRetrieverFactory;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.util.DualKeyLinkedMap;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Allocates {@link LogicalSlot}s from physical shared slots.
 *
 * <p>The allocator maintains a shared slot for each {@link ExecutionSlotSharingGroup}.
 * It allocates a physical slot for the shared slot and then allocates logical slots from it for scheduled tasks.
 * The physical slot is lazily allocated for a shared slot, upon any hosted subtask asking for the shared slot.
 * Each subsequent sharing subtask allocates a logical slot from the existing shared slot. The shared/physical slot
 * can be released only if all the requested logical slots are released or canceled.
 */
class SlotSharingExecutionSlotAllocator implements ExecutionSlotAllocator {
	private static final Logger LOG = LoggerFactory.getLogger(SlotSharingExecutionSlotAllocator.class);

	private final PhysicalSlotProvider slotProvider;

	private final boolean slotWillBeOccupiedIndefinitely;

	private final SlotSharingStrategy slotSharingStrategy;

	private final Map<ExecutionSlotSharingGroup, SharedSlot> sharedSlots;

	private final SharedSlotProfileRetrieverFactory sharedSlotProfileRetrieverFactory;

	SlotSharingExecutionSlotAllocator(
			PhysicalSlotProvider slotProvider,
			boolean slotWillBeOccupiedIndefinitely,
			SlotSharingStrategy slotSharingStrategy,
			SharedSlotProfileRetrieverFactory sharedSlotProfileRetrieverFactory) {
		this.slotProvider = slotProvider;
		this.slotWillBeOccupiedIndefinitely = slotWillBeOccupiedIndefinitely;
		this.slotSharingStrategy = slotSharingStrategy;
		this.sharedSlotProfileRetrieverFactory = sharedSlotProfileRetrieverFactory;
		this.sharedSlots = new IdentityHashMap<>();
	}

	/**
	 * Creates logical {@link SlotExecutionVertexAssignment}s from physical shared slots.
	 *
	 * <p>The allocation has the following steps:
	 * <ol>
	 *  <li>Map the executions to {@link ExecutionSlotSharingGroup}s using {@link SlotSharingStrategy}</li>
	 *  <li>Check which {@link ExecutionSlotSharingGroup}s already have shared slot</li>
	 *  <li>For all involved {@link ExecutionSlotSharingGroup}s which do not have a shared slot yet:</li>
	 *  <li>Create a {@link SlotProfile} future using {@link SharedSlotProfileRetriever} and then</li>
	 *  <li>Allocate a physical slot from the {@link PhysicalSlotProvider}</li>
	 *  <li>Create a shared slot based on the returned physical slot futures</li>
	 *  <li>Allocate logical slot futures for the executions from all corresponding shared slots.</li>
	 *  <li>If a physical slot request fails, associated logical slot requests are canceled within the shared slot</li>
	 *  <li>Generate {@link SlotExecutionVertexAssignment}s based on the logical slot futures and returns the results.</li>
	 * </ol>
	 *
	 * @param executionVertexSchedulingRequirements the requirements for scheduling the executions.
	 */
	@Override
	public List<SlotExecutionVertexAssignment> allocateSlotsFor(
			List<ExecutionVertexSchedulingRequirements> executionVertexSchedulingRequirements) {
		List<ExecutionVertexID> executionVertexIds = executionVertexSchedulingRequirements
			.stream()
			.map(ExecutionVertexSchedulingRequirements::getExecutionVertexId)
			.collect(Collectors.toList());

		SharedSlotProfileRetriever sharedSlotProfileRetriever = sharedSlotProfileRetrieverFactory
			.createFromBulk(new HashSet<>(executionVertexIds));
		Map<ExecutionVertexID, SlotExecutionVertexAssignment> assignments = executionVertexIds
			.stream()
			.collect(Collectors.groupingBy(slotSharingStrategy::getExecutionSlotSharingGroup))
			.entrySet()
			.stream()
			.flatMap(entry -> allocateLogicalSlotsFromSharedSlot(sharedSlotProfileRetriever, entry.getKey(), entry.getValue()))
			.collect(Collectors.toMap(SlotExecutionVertexAssignment::getExecutionVertexId, a -> a));

		return executionVertexIds.stream().map(assignments::get).collect(Collectors.toList());
	}

	@Override
	public void cancel(ExecutionVertexID executionVertexId) {
		ExecutionSlotSharingGroup executionSlotSharingGroup =
			slotSharingStrategy.getExecutionSlotSharingGroup(executionVertexId);
		Preconditions.checkNotNull(
			executionSlotSharingGroup,
			"There is no ExecutionSlotSharingGroup for ExecutionVertexID " + executionVertexId);
		SharedSlot slot = sharedSlots.get(executionSlotSharingGroup);
		if (slot != null) {
			slot.cancelLogicalSlotRequest(executionVertexId);
		} else {
			LOG.debug("There is no slot for ExecutionSlotSharingGroup of ExecutionVertexID {}", executionVertexId);
		}
	}

	private Stream<SlotExecutionVertexAssignment> allocateLogicalSlotsFromSharedSlot(
			SharedSlotProfileRetriever sharedSlotProfileRetriever,
			ExecutionSlotSharingGroup executionSlotSharingGroup,
			Collection<ExecutionVertexID> executions) {
		SharedSlot sharedSlot = getOrAllocateSharedSlot(executionSlotSharingGroup, sharedSlotProfileRetriever);
		return executions
			.stream()
			.map(execution -> new SlotExecutionVertexAssignment(execution, sharedSlot.allocateLogicalSlot(execution)));
	}

	private SharedSlot getOrAllocateSharedSlot(
			ExecutionSlotSharingGroup executionSlotSharingGroup,
			SharedSlotProfileRetriever sharedSlotProfileRetriever) {
		return sharedSlots
			.computeIfAbsent(executionSlotSharingGroup, group -> {
				SlotRequestId physicalSlotRequestId = new SlotRequestId();
				CompletableFuture<PhysicalSlot> physicalSlotFuture = sharedSlotProfileRetriever
					.getSlotProfileFuture(group)
					.thenCompose(slotProfile -> slotProvider.allocatePhysicalSlot(
						new PhysicalSlotRequest(physicalSlotRequestId, slotProfile, slotWillBeOccupiedIndefinitely)))
					.thenApply(PhysicalSlotRequest.Result::getPhysicalSlot);
				return new SharedSlot(physicalSlotRequestId, group, physicalSlotFuture);
			});
	}

	private class SharedSlot implements SlotOwner, PhysicalSlot.Payload {
		private final SlotRequestId physicalSlotRequestId;

		private final ExecutionSlotSharingGroup executionSlotSharingGroup;

		private final CompletableFuture<PhysicalSlot> slotContextFuture;

		private final DualKeyLinkedMap<ExecutionVertexID, SlotRequestId, CompletableFuture<SingleLogicalSlot>> requestedLogicalSlots;

		private SharedSlot(
				SlotRequestId physicalSlotRequestId,
				ExecutionSlotSharingGroup executionSlotSharingGroup,
				CompletableFuture<PhysicalSlot> slotContextFuture) {
			this.physicalSlotRequestId = physicalSlotRequestId;
			this.executionSlotSharingGroup = executionSlotSharingGroup;
			this.slotContextFuture = slotContextFuture.thenApply(physicalSlot -> {
				Preconditions.checkState(
					physicalSlot.tryAssignPayload(this),
					"Unexpected physical slot payload assignment failure!");
				return physicalSlot;
			});
			this.requestedLogicalSlots = new DualKeyLinkedMap<>(executionSlotSharingGroup.getExecutionVertexIds().size());
		}

		private CompletableFuture<LogicalSlot> allocateLogicalSlot(ExecutionVertexID executionVertexId) {
			Preconditions.checkArgument(executionSlotSharingGroup.getExecutionVertexIds().contains(executionVertexId));
			CompletableFuture<SingleLogicalSlot> logicalSlotFuture = requestedLogicalSlots.getValueByKeyA(executionVertexId);
			if (logicalSlotFuture != null) {
				LOG.debug("Request for {} already exists", getLogicalSlotString(executionVertexId));
			} else {
				logicalSlotFuture = allocateNonExistentLogicalSlot(executionVertexId);
			}
			return logicalSlotFuture.thenApply(Function.identity());
		}

		private CompletableFuture<SingleLogicalSlot> allocateNonExistentLogicalSlot(ExecutionVertexID executionVertexId) {
			CompletableFuture<SingleLogicalSlot> logicalSlotFuture;
			SlotRequestId logicalSlotRequestId = new SlotRequestId();
			String logMessageBase = getLogicalSlotString(logicalSlotRequestId, executionVertexId);
			LOG.debug("Request a {}", logMessageBase);

			logicalSlotFuture = slotContextFuture
				.thenApply(physicalSlot -> {
					LOG.debug("Allocated {}", logMessageBase);
					return createLogicalSlot(physicalSlot, logicalSlotRequestId);
				});
			requestedLogicalSlots.put(executionVertexId, logicalSlotRequestId, logicalSlotFuture);

			// If the physical slot request fails (slotContextFuture), it will also fail the logicalSlotFuture.
			// Therefore, the next `exceptionally` callback will cancelLogicalSlotRequest and do the cleanup
			// in requestedLogicalSlots and eventually in sharedSlots
			logicalSlotFuture.exceptionally(cause -> {
				LOG.debug("Failed {}", logMessageBase);
				cancelLogicalSlotRequest(logicalSlotRequestId);
				return null;
			});
			return logicalSlotFuture;
		}

		private SingleLogicalSlot createLogicalSlot(PhysicalSlot physicalSlot, SlotRequestId logicalSlotRequestId) {
			return new SingleLogicalSlot(
				logicalSlotRequestId,
				physicalSlot,
				null,
				Locality.UNKNOWN,
				this,
				slotWillBeOccupiedIndefinitely);
		}

		void cancelLogicalSlotRequest(ExecutionVertexID executionVertexID) {
			cancelLogicalSlotRequest(requestedLogicalSlots.getKeyBByKeyA(executionVertexID));
		}

		void cancelLogicalSlotRequest(SlotRequestId logicalSlotRequestId) {
			CompletableFuture<SingleLogicalSlot> logicalSlotFuture = requestedLogicalSlots.getValueByKeyB(logicalSlotRequestId);
			if (logicalSlotFuture != null) {
				LOG.debug("Cancel {}", getLogicalSlotString(logicalSlotRequestId));
				logicalSlotFuture.cancel(false);
				requestedLogicalSlots.removeKeyB(logicalSlotRequestId);
			} else {
				LOG.debug("No SlotExecutionVertexAssignment for logical {} from physical %s", logicalSlotRequestId);
			}
			removeSharedSlotIfAllLogicalDone();
		}

		private void removeSharedSlotIfAllLogicalDone() {
			if (requestedLogicalSlots.values().isEmpty()) {
				sharedSlots.remove(executionSlotSharingGroup);
				slotProvider.cancelSlotRequest(
					physicalSlotRequestId,
					new FlinkException("Slot is being returned from SlotSharingExecutionSlotAllocator."));
			}
		}

		@Override
		public void returnLogicalSlot(LogicalSlot logicalSlot) {
			cancelLogicalSlotRequest(logicalSlot.getSlotRequestId());
		}

		@Override
		public void release(Throwable cause) {
			Preconditions.checkState(
				slotContextFuture.isDone(),
				"Releasing of the shared slot is expected only from its successfully allocated physical slot ({})",
				physicalSlotRequestId);
			for (ExecutionVertexID executionVertexId : requestedLogicalSlots.keySetA()) {
				LOG.debug("Release {}", getLogicalSlotString(executionVertexId));
				CompletableFuture<SingleLogicalSlot> logicalSlotFuture =
					requestedLogicalSlots.getValueByKeyA(executionVertexId);
				Preconditions.checkNotNull(logicalSlotFuture);
				Preconditions.checkState(
					logicalSlotFuture.isDone(),
					"Logical slot future must already done when release call comes from the successfully allocated physical slot ({})",
					physicalSlotRequestId);
				logicalSlotFuture.thenAccept(logicalSlot -> logicalSlot.release(cause));
			}
		}

		@Override
		public boolean willOccupySlotIndefinitely() {
			return slotWillBeOccupiedIndefinitely;
		}

		private String getLogicalSlotString(SlotRequestId logicalSlotRequestId) {
			return getLogicalSlotString(logicalSlotRequestId, requestedLogicalSlots.getKeyAByKeyB(logicalSlotRequestId));
		}

		private String getLogicalSlotString(ExecutionVertexID executionVertexId) {
			return getLogicalSlotString(requestedLogicalSlots.getKeyBByKeyA(executionVertexId), executionVertexId);
		}

		private String getLogicalSlotString(SlotRequestId logicalSlotRequestId, ExecutionVertexID executionVertexId) {
			return String.format(
				"logical slot (%s) for execution vertex (id %s) from the physical slot (%s)",
				logicalSlotRequestId,
				executionVertexId,
				physicalSlotRequestId);
		}
	}
}
