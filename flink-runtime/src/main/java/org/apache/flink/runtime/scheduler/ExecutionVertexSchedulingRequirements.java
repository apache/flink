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

import javax.annotation.Nullable;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The requirements for scheduling a {@link ExecutionVertex}.
 */
public class ExecutionVertexSchedulingRequirements {

	private final ExecutionVertexID executionVertexId;

	@Nullable
	private final AllocationID previousAllocationId;

	private final ResourceProfile taskResourceProfile;

	private final ResourceProfile physicalSlotResourceProfile;

	@Nullable
	private final SlotSharingGroupId slotSharingGroupId;

	@Nullable
	private final CoLocationConstraint coLocationConstraint;

	private ExecutionVertexSchedulingRequirements(
			ExecutionVertexID executionVertexId,
			@Nullable AllocationID previousAllocationId,
			ResourceProfile taskResourceProfile,
			ResourceProfile physicalSlotResourceProfile,
			@Nullable SlotSharingGroupId slotSharingGroupId,
			@Nullable CoLocationConstraint coLocationConstraint) {
		this.executionVertexId = checkNotNull(executionVertexId);
		this.previousAllocationId = previousAllocationId;
		this.taskResourceProfile = checkNotNull(taskResourceProfile);
		this.physicalSlotResourceProfile = checkNotNull(physicalSlotResourceProfile);
		this.slotSharingGroupId = slotSharingGroupId;
		this.coLocationConstraint = coLocationConstraint;
	}

	public ExecutionVertexID getExecutionVertexId() {
		return executionVertexId;
	}

	@Nullable
	public AllocationID getPreviousAllocationId() {
		return previousAllocationId;
	}

	public ResourceProfile getTaskResourceProfile() {
		return taskResourceProfile;
	}

	public ResourceProfile getPhysicalSlotResourceProfile() {
		return physicalSlotResourceProfile;
	}

	@Nullable
	public SlotSharingGroupId getSlotSharingGroupId() {
		return slotSharingGroupId;
	}

	@Nullable
	public CoLocationConstraint getCoLocationConstraint() {
		return coLocationConstraint;
	}

	/**
	 * Builder for {@link ExecutionVertexSchedulingRequirements}.
	 */
	public static class Builder {

		private ExecutionVertexID executionVertexId;

		private AllocationID previousAllocationId;

		private ResourceProfile taskResourceProfile = ResourceProfile.UNKNOWN;

		private ResourceProfile physicalSlotResourceProfile = ResourceProfile.UNKNOWN;

		private SlotSharingGroupId slotSharingGroupId;

		private CoLocationConstraint coLocationConstraint;

		public Builder withExecutionVertexId(final ExecutionVertexID executionVertexId) {
			this.executionVertexId = executionVertexId;
			return this;
		}

		public Builder withPreviousAllocationId(final AllocationID previousAllocationId) {
			this.previousAllocationId = previousAllocationId;
			return this;
		}

		public Builder withTaskResourceProfile(final ResourceProfile taskResourceProfile) {
			this.taskResourceProfile = taskResourceProfile;
			return this;
		}

		public Builder withPhysicalSlotResourceProfile(final ResourceProfile physicalSlotResourceProfile) {
			this.physicalSlotResourceProfile = physicalSlotResourceProfile;
			return this;
		}

		public Builder withSlotSharingGroupId(final SlotSharingGroupId slotSharingGroupId) {
			this.slotSharingGroupId = slotSharingGroupId;
			return this;
		}

		public Builder withCoLocationConstraint(final CoLocationConstraint coLocationConstraint) {
			this.coLocationConstraint = coLocationConstraint;
			return this;
		}

		public ExecutionVertexSchedulingRequirements build() {
			checkState(
				physicalSlotResourceProfile.isMatching(taskResourceProfile),
				"The physical slot resources must fulfill the task slot requirements");

			return new ExecutionVertexSchedulingRequirements(
				executionVertexId,
				previousAllocationId,
				taskResourceProfile,
				physicalSlotResourceProfile,
				slotSharingGroupId,
				coLocationConstraint);
		}
	}
}
