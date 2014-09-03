/**
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


package org.apache.flink.runtime.instance;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.runtime.instance.AllocatedResource;
import org.apache.flink.runtime.instance.InstanceListener;
import org.apache.flink.runtime.jobgraph.JobID;

/**
 * This class implements an instance listener that can be used for unit tests.
 * <p>
 * This class is thread-safe.
 * 
 */
public class TestInstanceListener implements InstanceListener {

	/**
	 * Stores the resources allocates
	 */
	final Map<JobID, List<AllocatedResource>> resourcesOfJobs = new HashMap<JobID, List<AllocatedResource>>();


	@Override
	public synchronized void resourcesAllocated(final JobID jobID, final List<AllocatedResource> allocatedResources) {

		List<AllocatedResource> allocatedResourcesOfJob = this.resourcesOfJobs.get(jobID);
		if (allocatedResourcesOfJob == null) {
			allocatedResourcesOfJob = new ArrayList<AllocatedResource>();
			this.resourcesOfJobs.put(jobID, allocatedResourcesOfJob);
		}

		for (final AllocatedResource allocatedResource : allocatedResources) {
			if (allocatedResourcesOfJob.contains(allocatedResource)) {
				throw new IllegalStateException("Resource " + allocatedResource.getAllocationID()
					+ " is already allocated by job " + jobID);
			}

			allocatedResourcesOfJob.add(allocatedResource);
		}
	}


	@Override
	public synchronized void allocatedResourcesDied(final JobID jobID, final List<AllocatedResource> allocatedResources) {

		List<AllocatedResource> allocatedResourcesOfJob = this.resourcesOfJobs.get(jobID);
		if (allocatedResourcesOfJob == null) {
			throw new IllegalStateException("Unable to find allocated resources for job with ID " + jobID);
		}

		for (final AllocatedResource allocatedResource : allocatedResources) {
			if (!allocatedResourcesOfJob.remove(allocatedResource)) {
				throw new IllegalStateException("Resource " + allocatedResource.getAllocationID()
					+ " is not assigned to job " + jobID);
			}
		}

		if (allocatedResourcesOfJob.isEmpty()) {
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
