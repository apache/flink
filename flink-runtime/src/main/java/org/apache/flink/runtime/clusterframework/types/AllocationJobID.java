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

package org.apache.flink.runtime.clusterframework.types;

import org.apache.flink.api.common.JobID;

public class AllocationJobID {
	private final AllocationID allocationID;
	private final JobID jobID;

	public AllocationJobID(AllocationID allocationID, JobID jobID) {
		this.allocationID = allocationID;
		this.jobID = jobID;
	}


	public AllocationID getAllocationID() {
		return allocationID;
	}

	public JobID getJobID() {
		return jobID;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		AllocationJobID that = (AllocationJobID) o;

		if (!allocationID.equals(that.allocationID)) return false;
		return jobID.equals(that.jobID);

	}

	@Override
	public int hashCode() {
		int result = allocationID.hashCode();
		result = 31 * result + jobID.hashCode();
		return result;
	}

	@Override
	public String toString() {
		return "AllocationJobID{" +
			"allocationID=" + allocationID +
			", jobID=" + jobID +
			'}';
	}
}
