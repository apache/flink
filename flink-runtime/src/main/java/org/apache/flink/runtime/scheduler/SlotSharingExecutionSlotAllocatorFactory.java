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
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotProvider;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import java.util.function.Function;

/**
 * Factory for {@link SlotSharingExecutionSlotAllocator}.
 */
class SlotSharingExecutionSlotAllocatorFactory implements ExecutionSlotAllocatorFactory {
	private final PhysicalSlotProvider slotProvider;

	private final boolean slotWillBeOccupiedIndefinitely;

	private final SlotSharingStrategy slotSharingStrategy;

	private final Function<ExecutionVertexID, ResourceProfile> resourceProfileRetriever;

	private final Function<ExecutionVertexID, AllocationID> priorAllocationIdRetriever;

	SlotSharingExecutionSlotAllocatorFactory(
			PhysicalSlotProvider slotProvider,
			boolean slotWillBeOccupiedIndefinitely,
			SlotSharingStrategy slotSharingStrategy,
			Function<ExecutionVertexID, ResourceProfile> resourceProfileRetriever,
			Function<ExecutionVertexID, AllocationID> priorAllocationIdRetriever) {
		this.slotProvider = slotProvider;
		this.slotWillBeOccupiedIndefinitely = slotWillBeOccupiedIndefinitely;
		this.slotSharingStrategy = slotSharingStrategy;
		this.resourceProfileRetriever = resourceProfileRetriever;
		this.priorAllocationIdRetriever = priorAllocationIdRetriever;
	}

	@Override
	public ExecutionSlotAllocator createInstance(PreferredLocationsRetriever preferredLocationsRetriever) {
		return new SlotSharingExecutionSlotAllocator(
			slotProvider,
			slotWillBeOccupiedIndefinitely,
			slotSharingStrategy,
			new MergingSharedSlotProfileRetrieverFactory(
				preferredLocationsRetriever,
				resourceProfileRetriever,
				priorAllocationIdRetriever));
	}
}
