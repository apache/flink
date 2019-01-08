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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This describes the slot current status which located in TaskManager.
 */
public class SlotStatus implements Serializable {

	private static final long serialVersionUID = 5099191707339664493L;

	/** slotID to identify a slot */
	private final SlotID slotID;

	/** the resource profile of the slot */
	private final ResourceProfile resourceProfile;

	/** if the slot is allocated, allocationId identify its allocation; else, allocationId is null */
	private final AllocationID allocationID;

	/** if the slot is allocated, jobId identify which job this slot is allocated to; else, jobId is null */
	private final JobID jobID;

	public SlotStatus(SlotID slotID, ResourceProfile resourceProfile) {
		this(slotID, resourceProfile, null, null);
	}

	public SlotStatus(
			SlotID slotID,
			ResourceProfile resourceProfile,
			JobID jobID,
			AllocationID allocationID) {
		this.slotID = checkNotNull(slotID, "slotID cannot be null");
		this.resourceProfile = checkNotNull(resourceProfile, "profile cannot be null");
		this.allocationID = allocationID;
		this.jobID = jobID;
	}

	/**
	 * Get the unique identification of this slot
	 *
	 * @return The slot id
	 */
	public SlotID getSlotID() {
		return slotID;
	}

	/**
	 * Get the resource profile of this slot
	 *
	 * @return The resource profile
	 */
	public ResourceProfile getResourceProfile() {
		return resourceProfile;
	}

	/**
	 * Get the allocation id of this slot
	 *
	 * @return The allocation id if this slot is allocated, otherwise null
	 */
	public AllocationID getAllocationID() {
		return allocationID;
	}

	/**
	 * Get the job id of the slot allocated for
	 *
	 * @return The job id if this slot is allocated, otherwise null
	 */
	public JobID getJobID() {
		return jobID;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		SlotStatus that = (SlotStatus) o;

		if (!slotID.equals(that.slotID)) {
			return false;
		}
		if (!resourceProfile.equals(that.resourceProfile)) {
			return false;
		}
		if (allocationID != null ? !allocationID.equals(that.allocationID) : that.allocationID != null) {
			return false;
		}
		return jobID != null ? jobID.equals(that.jobID) : that.jobID == null;

	}

	@Override
	public int hashCode() {
		int result = slotID.hashCode();
		result = 31 * result + resourceProfile.hashCode();
		result = 31 * result + (allocationID != null ? allocationID.hashCode() : 0);
		result = 31 * result + (jobID != null ? jobID.hashCode() : 0);
		return result;
	}

}
