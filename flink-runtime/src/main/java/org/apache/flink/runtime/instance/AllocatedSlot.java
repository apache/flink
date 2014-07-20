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

import org.apache.flink.runtime.executiongraph.ExecutionVertex2;
import org.apache.flink.runtime.jobgraph.JobID;
import org.apache.flink.runtime.jobmanager.scheduler.ResourceId;

/**
 * An allocated slot is the unit in which resources are allocated on instances.
 */
public class AllocatedSlot {

	/** The ID which identifies the resources occupied by this slot. */
	private final ResourceId resourceId;

	/** The ID of the job this slice belongs to. */
	private final JobID jobID;
	
	/** The instance on which the slot is allocated */
	private final Instance instance;
	
	/** The number of the slot on which the task is deployed */
	private final int slotNumber;


	public AllocatedSlot(JobID jobID, ResourceId resourceId, Instance instance, int slotNumber) {
		this.resourceId = resourceId;
		this.jobID = jobID;
		this.instance = instance;
		this.slotNumber = slotNumber;
	}

	// --------------------------------------------------------------------------------------------
	
	/**
	 * Returns the ID of the job this allocated slot belongs to.
	 * 
	 * @return the ID of the job this allocated slot belongs to
	 */
	public JobID getJobID() {
		return this.jobID;
	}
	
	public ResourceId getResourceId() {
		return resourceId;
	}
	
	public Instance getInstance() {
		return instance;
	}
	
	public int getSlotNumber() {
		return slotNumber;
	}
	
	// --------------------------------------------------------------------------------------------
	
	public void runTask(ExecutionVertex2 vertex) {
		
	}
	
	public void cancelResource() {
		
	}
}
