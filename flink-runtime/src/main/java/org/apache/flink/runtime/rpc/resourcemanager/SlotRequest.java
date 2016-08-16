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

package org.apache.flink.runtime.rpc.resourcemanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;

import java.io.Serializable;

/**
 * Slot allocation request from jobManager to resourceManager
 */
public class SlotRequest implements Serializable {
	private static final long serialVersionUID = -6586877187990445986L;
	// jobId to identify which job send the request
	private final JobID jobID;
	// allocationId to identify slot allocation, created by JobManager when requesting a sot
	private final AllocationID allocationID;
	// the resource profile of the desired slot
	private final ResourceProfile profile;

	public SlotRequest(JobID jobID, AllocationID allocationID) {
		this(jobID, allocationID, null);
	}

	public SlotRequest(JobID jobID, AllocationID allocationID, ResourceProfile profile) {
		this.jobID = jobID;
		this.allocationID = allocationID;
		this.profile = profile;
	}

	public ResourceProfile getProfile() {
		return profile;
	}

	public AllocationID getAllocationID() {
		return allocationID;
	}

	public JobID getJobID() {
		return jobID;
	}

	@Override
	public String toString() {
		return "SlotRequest{" +
			   "jobID=" + jobID +
			   ", allocationID=" + allocationID +
			   ", profile=" + profile +
			   '}';
	}
}

