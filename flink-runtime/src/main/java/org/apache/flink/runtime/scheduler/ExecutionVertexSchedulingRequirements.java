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
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationConstraint;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import java.util.Collection;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The requirements for scheduling a {@link ExecutionVertex}.
 */
public class ExecutionVertexSchedulingRequirements {

	private final ExecutionVertexID executionVertexId;

	private final AllocationID previousAllocationId;

	private final ResourceProfile resourceProfile;

	private final SlotSharingGroupId slotSharingGroupId;

	private final CoLocationConstraint coLocationConstraint;

	private final Collection<TaskManagerLocation> preferredLocations;

	public ExecutionVertexSchedulingRequirements(
			ExecutionVertexID executionVertexId,
			AllocationID previousAllocationId,
			ResourceProfile resourceProfile,
			SlotSharingGroupId slotSharingGroupId,
			CoLocationConstraint coLocationConstraint,
			Collection<TaskManagerLocation> preferredLocations) {
		this.executionVertexId = checkNotNull(executionVertexId);
		this.previousAllocationId = previousAllocationId;
		this.resourceProfile = checkNotNull(resourceProfile);
		this.slotSharingGroupId = slotSharingGroupId;
		this.coLocationConstraint = coLocationConstraint;
		this.preferredLocations = preferredLocations;
	}

	public ExecutionVertexID getExecutionVertexId() {
		return executionVertexId;
	}

	public AllocationID getPreviousAllocationId() {
		return previousAllocationId;
	}

	public ResourceProfile getResourceProfile() {
		return resourceProfile;
	}

	public SlotSharingGroupId getSlotSharingGroupId() {
		return slotSharingGroupId;
	}

	public CoLocationConstraint getCoLocationConstraint() {
		return coLocationConstraint;
	}

	public Collection<TaskManagerLocation> getPreferredLocations() {
		return preferredLocations;
	}
}
