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

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.executiongraph.SlotProviderStrategy;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobmanager.scheduler.ScheduledUnit;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Default {@link ExecutionSlotAllocator} which will use {@link SlotProvider} to allocate slots and
 * keep the unfulfilled requests for further cancellation.
 */
class DefaultExecutionSlotAllocator extends AbstractExecutionSlotAllocator {

	private static final Logger LOG = LoggerFactory.getLogger(DefaultExecutionSlotAllocator.class);

	private final SlotProviderStrategy slotProviderStrategy;

	DefaultExecutionSlotAllocator(
			final SlotProviderStrategy slotProviderStrategy,
			final PreferredLocationsRetriever preferredLocationsRetriever) {

		super(preferredLocationsRetriever);
		this.slotProviderStrategy = checkNotNull(slotProviderStrategy);
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
			final SlotSharingGroupId slotSharingGroupId = schedulingRequirements.getSlotSharingGroupId();

			final SlotRequestId slotRequestId = new SlotRequestId();

			final CompletableFuture<LogicalSlot> slotFuture = allocateSlot(
				schedulingRequirements,
				slotRequestId,
				allPreviousAllocationIds);

			final SlotExecutionVertexAssignment slotExecutionVertexAssignment =
				createAndRegisterSlotExecutionVertexAssignment(
					executionVertexId,
					slotFuture,
					throwable -> slotProviderStrategy.cancelSlotRequest(slotRequestId, slotSharingGroupId, throwable));

			slotExecutionVertexAssignments.add(slotExecutionVertexAssignment);
		}

		return slotExecutionVertexAssignments;
	}

	private CompletableFuture<LogicalSlot> allocateSlot(
			final ExecutionVertexSchedulingRequirements schedulingRequirements,
			final SlotRequestId slotRequestId,
			final Set<AllocationID> allPreviousAllocationIds) {

		final ExecutionVertexID executionVertexId = schedulingRequirements.getExecutionVertexId();

		LOG.debug("Allocate slot with id {} for execution {}", slotRequestId, executionVertexId);

		final CompletableFuture<SlotProfile> slotProfileFuture = getSlotProfileFuture(
			schedulingRequirements,
			schedulingRequirements.getPhysicalSlotResourceProfile(),
			Collections.emptySet(),
			allPreviousAllocationIds);

		return slotProfileFuture.thenCompose(
			slotProfile -> slotProviderStrategy.allocateSlot(
				slotRequestId,
				new ScheduledUnit(
					executionVertexId,
					schedulingRequirements.getSlotSharingGroupId(),
					schedulingRequirements.getCoLocationConstraint()),
				slotProfile));
	}
}
