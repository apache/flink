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

package org.apache.flink.runtime.clusterframework.types;

import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.runtime.resourcemanager.slotmanager.PendingSlotRequest;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A TaskManagerSlot represents a slot located in a TaskManager. It has a unique identification and
 * resource profile associated.
 */
public class TaskManagerSlot {

	/** The unique identification of this slot */
	private final SlotID slotId;

	/** The resource profile of this slot */
	private final ResourceProfile resourceProfile;

	/** Gateway to the TaskExecutor which owns the slot */
	private final TaskExecutorConnection taskManagerConnection;

	/** Allocation id for which this slot has been allocated */
	private AllocationID allocationId;

	/** Assigned slot request if there is currently an ongoing request */
	private PendingSlotRequest assignedSlotRequest;

	public TaskManagerSlot(
			SlotID slotId,
			ResourceProfile resourceProfile,
			TaskExecutorConnection taskManagerConnection,
			AllocationID allocationId) {
		this.slotId = checkNotNull(slotId);
		this.resourceProfile = checkNotNull(resourceProfile);
		this.taskManagerConnection = checkNotNull(taskManagerConnection);

		this.allocationId = allocationId;
		this.assignedSlotRequest = null;
	}

	public SlotID getSlotId() {
		return slotId;
	}

	public ResourceProfile getResourceProfile() {
		return resourceProfile;
	}

	public TaskExecutorConnection getTaskManagerConnection() {
		return taskManagerConnection;
	}

	public AllocationID getAllocationId() {
		return allocationId;
	}

	public void setAllocationId(AllocationID allocationId) {
		this.allocationId = allocationId;
	}

	public PendingSlotRequest getAssignedSlotRequest() {
		return assignedSlotRequest;
	}

	public void setAssignedSlotRequest(PendingSlotRequest assignedSlotRequest) {
		this.assignedSlotRequest = assignedSlotRequest;
	}

	public InstanceID getInstanceId() {
		return taskManagerConnection.getInstanceID();
	}

	/**
	 * Check whether required resource profile can be matched by this slot.
	 *
	 * @param required The required resource profile
	 * @return true if requirement can be matched
	 */
	public boolean isMatchingRequirement(ResourceProfile required) {
		return resourceProfile.isMatching(required);
	}

	public boolean isFree() {
		return !isAllocated() && !hasPendingSlotRequest();
	}

	public boolean isAllocated() {
		return null != allocationId;
	}

	public boolean hasPendingSlotRequest() {
		return null != assignedSlotRequest;
	}
}
