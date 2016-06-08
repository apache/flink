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

import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.util.Preconditions;

public class ScheduledUnit {
	
	private final Execution vertexExecution;
	
	private final SlotSharingGroup sharingGroup;
	
	private final CoLocationConstraint locationConstraint;
	
	// --------------------------------------------------------------------------------------------
	
	public ScheduledUnit(Execution task) {
		Preconditions.checkNotNull(task);
		
		this.vertexExecution = task;
		this.sharingGroup = null;
		this.locationConstraint = null;
	}
	
	public ScheduledUnit(Execution task, SlotSharingGroup sharingUnit) {
		Preconditions.checkNotNull(task);
		
		this.vertexExecution = task;
		this.sharingGroup = sharingUnit;
		this.locationConstraint = null;
	}
	
	public ScheduledUnit(Execution task, SlotSharingGroup sharingUnit, CoLocationConstraint locationConstraint) {
		Preconditions.checkNotNull(task);
		Preconditions.checkNotNull(sharingUnit);
		Preconditions.checkNotNull(locationConstraint);
		
		this.vertexExecution = task;
		this.sharingGroup = sharingUnit;
		this.locationConstraint = locationConstraint;
	}

	// --------------------------------------------------------------------------------------------
	
	public JobVertexID getJobVertexId() {
		return this.vertexExecution.getVertex().getJobvertexId();
	}
	
	public Execution getTaskToExecute() {
		return vertexExecution;
	}
	
	public SlotSharingGroup getSlotSharingGroup() {
		return sharingGroup;
	}
	
	public CoLocationConstraint getLocationConstraint() {
		return locationConstraint;
	}

	// --------------------------------------------------------------------------------------------
	
	@Override
	public String toString() {
		return "{task=" + vertexExecution.getVertexWithAttempt() + ", sharingUnit=" + sharingGroup + 
				", locationConstraint=" + locationConstraint + '}';
	}
}
