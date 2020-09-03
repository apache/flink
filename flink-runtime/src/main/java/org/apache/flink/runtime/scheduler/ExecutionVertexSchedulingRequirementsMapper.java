/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.scheduler;

import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

/**
 * Class that creates {@link ExecutionVertexSchedulingRequirements} for an {@link ExecutionVertex}.
 */
public final class ExecutionVertexSchedulingRequirementsMapper {

	public static ExecutionVertexSchedulingRequirements from(final ExecutionVertex executionVertex) {

		final ExecutionVertexID executionVertexId = executionVertex.getID();

		final AllocationID latestPriorAllocation = executionVertex.getLatestPriorAllocation();
		final SlotSharingGroup slotSharingGroup = executionVertex.getJobVertex().getSlotSharingGroup();

		return new ExecutionVertexSchedulingRequirements.Builder()
			.withExecutionVertexId(executionVertexId)
			.withPreviousAllocationId(latestPriorAllocation)
			.withTaskResourceProfile(executionVertex.getResourceProfile())
			.withPhysicalSlotResourceProfile(getPhysicalSlotResourceProfile(executionVertex))
			.withSlotSharingGroupId(slotSharingGroup.getSlotSharingGroupId())
			.withCoLocationConstraint(executionVertex.getLocationConstraint())
			.build();
	}

	/**
	 * Get resource profile of the physical slot to allocate a logical slot in for the given vertex.
	 * If the vertex is in a slot sharing group, the physical slot resource profile should be the
	 * resource profile of the slot sharing group. Otherwise it should be the resource profile of
	 * the vertex itself since the physical slot would be used by this vertex only in this case.
	 *
	 * @return resource profile of the physical slot to allocate a logical slot for the given vertex
	 */
	public static ResourceProfile getPhysicalSlotResourceProfile(final ExecutionVertex executionVertex) {
		final SlotSharingGroup slotSharingGroup = executionVertex.getJobVertex().getSlotSharingGroup();
		return ResourceProfile.fromResourceSpec(slotSharingGroup.getResourceSpec(), MemorySize.ZERO);
	}

	private ExecutionVertexSchedulingRequirementsMapper() {
	}
}
