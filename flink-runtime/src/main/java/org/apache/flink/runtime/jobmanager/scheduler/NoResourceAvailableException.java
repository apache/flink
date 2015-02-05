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

package org.apache.flink.runtime.jobmanager.scheduler;

import org.apache.flink.runtime.JobException;

public class NoResourceAvailableException extends JobException {

	private static final long serialVersionUID = -2249953165298717803L;
	
	private static final String BASE_MESSAGE = "Not enough free slots available to run the job. "
			+ "You can decrease the operator parallelism or increase the number of slots per TaskManager in the configuration.";

	public NoResourceAvailableException() {
		super(BASE_MESSAGE);
	}
	
	public NoResourceAvailableException(ScheduledUnit unit) {
		super("No resource available to schedule unit " + unit
				+ ". You can decrease the operator parallelism or increase the number of slots per TaskManager in the configuration.");
	}

	public NoResourceAvailableException(int numInstances, int numSlotsTotal, int availableSlots) {
		super(String.format("%s Resources available to scheduler: Number of instances=%d, total number of slots=%d, available slots=%d",
				BASE_MESSAGE, numInstances, numSlotsTotal, availableSlots));
	}
	
	NoResourceAvailableException(ScheduledUnit task, int numInstances, int numSlotsTotal, int availableSlots) {
		super(String.format("%s Task to schedule: < %s > with groupID < %s > in sharing group < %s >. Resources available to scheduler: Number of instances=%d, total number of slots=%d, available slots=%d",
				BASE_MESSAGE, task.getTaskToExecute(),
				task.getLocationConstraint() == null ? task.getTaskToExecute().getVertex().getJobvertexId() : task.getLocationConstraint().getGroupId(),
				task.getSlotSharingGroup(),
				numInstances,
				numSlotsTotal,
				availableSlots));
	}

	public NoResourceAvailableException(String message) {
		super(message);
	}
	
	// --------------------------------------------------------------------------------------------
	
	@Override
	public boolean equals(Object obj){
		if(obj == null){
			return false;
		}

		if(!(obj instanceof NoResourceAvailableException)){
			return false;
		}else{
			return getMessage().equals(((NoResourceAvailableException)obj).getMessage());
		}
	}
	
	@Override
	public int hashCode() {
		return getMessage().hashCode();
	}
}
