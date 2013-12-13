/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.instance.cluster;

import eu.stratosphere.nephele.instance.AllocationID;
import eu.stratosphere.nephele.instance.InstanceType;
import eu.stratosphere.nephele.jobgraph.JobID;

/**
 * An allocated slice is a part of an instance which is assigned to a job.
 * <p>
 * This class is thread-safe.
 * 
 * @author Dominic Battre
 */
class AllocatedSlice {

	/**
	 * The allocation ID which identifies the resources occupied by this slice.
	 */
	private final AllocationID allocationID;

	/**
	 * The machine hosting the slice.
	 */
	private final ClusterInstance hostingInstance;

	/**
	 * The type describing the characteristics of the allocated slice.
	 */
	private final InstanceType type;

	/**
	 * The ID of the job this slice belongs to.
	 */
	private final JobID jobID;

	/**
	 * Time when this machine has been allocation in milliseconds, {@see currentTimeMillis()}.
	 */
	private final long allocationTime;

	/**
	 * Creates a new allocated slice on the given hosting instance.
	 * 
	 * @param hostingInstance
	 *        the instance hosting the slice
	 * @param type
	 *        the type describing the characteristics of the allocated slice
	 * @param jobID
	 *        the ID of the job this slice belongs to
	 * @param allocationTime
	 *        the time the instance was allocated
	 */
	public AllocatedSlice(final ClusterInstance hostingInstance, final InstanceType type, final JobID jobID,
			final long allocationTime) {

		this.allocationID = new AllocationID();
		this.hostingInstance = hostingInstance;
		this.type = type;
		this.jobID = jobID;
		this.allocationTime = allocationTime;
	}

	/**
	 * Returns the allocation ID of this slice.
	 * 
	 * @return the allocation ID of this slice
	 */
	public AllocationID getAllocationID() {
		return this.allocationID;
	}

	/**
	 * The type describing the characteristics of
	 * this allocated slice.
	 * 
	 * @return the type describing the characteristics of the slice
	 */
	public InstanceType getType() {
		return this.type;
	}

	/**
	 * Returns the time the instance was allocated.
	 * 
	 * @return the time the instance was allocated
	 */
	public long getAllocationTime() {
		return this.allocationTime;
	}

	/**
	 * Returns the ID of the job this allocated slice belongs to.
	 * 
	 * @return the ID of the job this allocated slice belongs to
	 */
	public JobID getJobID() {
		return this.jobID;
	}

	/**
	 * Returns the instance hosting this slice.
	 * 
	 * @return the instance hosting this slice
	 */
	public ClusterInstance getHostingInstance() {
		return this.hostingInstance;
	}
}
