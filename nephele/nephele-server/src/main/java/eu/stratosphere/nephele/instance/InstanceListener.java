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

package eu.stratosphere.nephele.instance;

import eu.stratosphere.nephele.jobgraph.JobID;

/**
 * Classes implementing the {@link InstanceListener} interface can be notified about
 * the availability or the unexpected failure of an instance.
 * 
 * @author warneke
 */
public interface InstanceListener {

	/**
	 * Called if a requested resource has become available.
	 * 
	 * @param jobID
	 *        the ID of the job the initial request has been triggered for
	 * @param allocatedResource
	 *        the resource which have been allocated as a response to the initial request
	 */
	void resourceAllocated(JobID jobID, AllocatedResource allocatedResource);

	/**
	 * Called if an allocated resource assigned to at least one job has died unexpectedly.
	 * 
	 * @param jobID
	 *        the ID of the job the instance is used for
	 * @param allocatedResource
	 *        the allocated resource which is affected by the instance death
	 */
	void allocatedResourceDied(JobID jobID, AllocatedResource allocatedResource);

}
