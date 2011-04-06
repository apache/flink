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

package eu.stratosphere.nephele.instance.local;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import eu.stratosphere.nephele.instance.AllocatedResource;
import eu.stratosphere.nephele.instance.InstanceListener;
import eu.stratosphere.nephele.jobgraph.JobID;

/**
 * This class implements an instance listener that can be used for unit tests.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 */
public final class TestInstanceListener implements InstanceListener {

	/**
	 * Stores the resources allocates
	 */
	final Map<JobID, List<AllocatedResource>> resourcesOfJobs = new HashMap<JobID, List<AllocatedResource>>();

	/**
	 * {@inheritDoc}
	 */
	@Override
	public synchronized void resourceAllocated(JobID jobID, AllocatedResource allocatedResource) {

		List<AllocatedResource> allocatedResources = this.resourcesOfJobs.get(jobID);
		if (allocatedResources == null) {
			allocatedResources = new ArrayList<AllocatedResource>();
			this.resourcesOfJobs.put(jobID, allocatedResources);
		}

		if (allocatedResources.contains(allocatedResource)) {
			throw new IllegalStateException("Resource " + allocatedResource.getAllocationID()
				+ " is already allocated by job " + jobID);
		}

		allocatedResources.add(allocatedResource);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public synchronized void allocatedResourceDied(JobID jobID, AllocatedResource allocatedResource) {

		List<AllocatedResource> allocatedResources = this.resourcesOfJobs.get(jobID);
		if (allocatedResources == null) {
			throw new IllegalStateException("Unable to find allocated resources for job with ID " + jobID);
		}

		if (!allocatedResources.remove(allocatedResource)) {
			throw new IllegalStateException("Resource " + allocatedResource.getAllocationID()
				+ " is not assigned to job " + jobID);
		}

		if (allocatedResources.isEmpty()) {
			this.resourcesOfJobs.remove(jobID);
		}
	}

	/**
	 * Returns the number of allocated resources for the job with the given job ID.
	 * 
	 * @param jobID
	 *        the job ID specifying the job
	 * @return the number of allocated resources for the job
	 */
	public synchronized int getNumberOfAllocatedResourcesForJob(JobID jobID) {

		final List<AllocatedResource> allocatedResources = this.resourcesOfJobs.get(jobID);
		if (allocatedResources == null) {
			return 0;
		}

		return allocatedResources.size();
	}

	/**
	 * Returns a list of resources allocated for the given job.
	 * 
	 * @param jobID
	 *        the job ID specifying the job
	 * @return the (possibly empty) list of resource allocated for the job
	 */
	public synchronized List<AllocatedResource> getAllocatedResourcesForJob(JobID jobID) {

		final List<AllocatedResource> allocatedResources = this.resourcesOfJobs.get(jobID);
		if (allocatedResources == null) {
			return new ArrayList<AllocatedResource>();
		}

		return new ArrayList<AllocatedResource>(allocatedResources);
	}
}
