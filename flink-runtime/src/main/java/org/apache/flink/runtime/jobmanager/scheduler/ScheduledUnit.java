/***********************************************************************************************************************
 *
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
 *
 **********************************************************************************************************************/

package org.apache.flink.runtime.jobmanager.scheduler;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.flink.runtime.executiongraph.ExecutionVertex2;
import org.apache.flink.runtime.jobgraph.JobID;

public class ScheduledUnit {
	
	private final JobID jobId;
	
	private final ExecutionVertex2 taskVertex;
	
	private final ResourceId resourceId;
	
	private final AtomicBoolean scheduled = new AtomicBoolean(false);
	
	
	public ScheduledUnit(JobID jobId, ExecutionVertex2 taskVertex) {
		this(jobId, taskVertex, new ResourceId());
	}
	
	public ScheduledUnit(JobID jobId, ExecutionVertex2 taskVertex, ResourceId resourceId) {
		if (jobId == null || taskVertex == null || resourceId == null) {
			throw new NullPointerException();
		}
		
		this.jobId = jobId;
		this.taskVertex = taskVertex;
		this.resourceId = resourceId;
	}
	
	ScheduledUnit() {
		this.jobId = null;
		this.taskVertex = null;
		this.resourceId = null;
	}

	
	public JobID getJobId() {
		return jobId;
	}
	
	public ExecutionVertex2 getTaskVertex() {
		return taskVertex;
	}
	
	public ResourceId getSharedResourceId() {
		return resourceId;
	}
	
	@Override
	public String toString() {
		return "(job=" + jobId + ", resourceId=" + resourceId + ", vertex=" + taskVertex + ')';
	}
}
