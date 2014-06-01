/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.nephele.instance;

import eu.stratosphere.nephele.jobgraph.JobID;

/**
 * An allocated slot is a part of an instance which is assigned to a job.
 * <p>
 * This class is thread-safe.
 * 
 */
public class AllocatedSlot {

	/**
	 * The allocation ID which identifies the resources occupied by this slot.
	 */
	private final AllocationID allocationID;

	/**
	 * The ID of the job this slice belongs to.
	 */
	private final JobID jobID;

	/**
	 * Creates a new allocated slice on the given hosting instance.
	 * 
	 * @param jobID
	 *        the ID of the job this slice belongs to
	 */
	public AllocatedSlot(final JobID jobID) {

		this.allocationID = new AllocationID();
		this.jobID = jobID;
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
	 * Returns the ID of the job this allocated slice belongs to.
	 * 
	 * @return the ID of the job this allocated slice belongs to
	 */
	public JobID getJobID() {
		return this.jobID;
	}
}
