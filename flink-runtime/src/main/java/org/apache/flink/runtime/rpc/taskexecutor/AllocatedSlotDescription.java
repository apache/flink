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
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.flink.runtime.rpc.taskexecutor;


import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;

import java.io.Serializable;

public class AllocatedSlotDescription extends SlotDescription {
	private static final long serialVersionUID = -832874308969287695L;
	private final AllocationID allocationID;
	private final JobID jobID;
	private final transient ResourceProfile profiler;

	public AllocatedSlotDescription(
		SlotID slotID,
		AllocationID allocationID,
		JobID jobID,
		ResourceProfile profiler) {
		super(slotID);
		this.allocationID = allocationID;
		this.jobID = jobID;
		this.profiler = profiler;
	}

	public SlotDescription free() {
		return new AvailableSlotDescription(this.getSlotID(), profiler);
	}

	public AllocationID getAllocationID() {
		return allocationID;
	}

	public JobID getJobID() {
		return jobID;
	}

	public ResourceProfile getProfiler() {
		return profiler;
	}

	@Override
	public String toString() {
		return "AllocatedSlotDescription{" +
		       "slotID=" + getSlotID() +
		       ", allocationID=" + allocationID +
		       ", jobID=" + jobID +
		       ", profiler=" + profiler +
		       '}';
	}
}
