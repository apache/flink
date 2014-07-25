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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.jobgraph.JobID;
import org.apache.flink.runtime.jobgraph.JobVertexID;

public class ExecutionVertex2 {

	private final JobVertexID jobVertexId;
	
			
			
	public ExecutionVertex2() {
		this(new JobVertexID());
	}
	
	public ExecutionVertex2(JobVertexID jobVertexId) {
		this.jobVertexId = jobVertexId;
	}
	
	
	
	public JobID getJobId() {
		return new JobID();
	}
	
	
	public JobVertexID getJobvertexId() {
		return this.jobVertexId;
	}
	
	public String getTaskName() {
		return "task";
	}
	
	public int getTotalNumberOfParallelSubtasks() {
		return 1;
	}
	
	public int getParallelSubtaskIndex() {
		return 0;
	}

	
	// --------------------------------------------------------------------------------------------
	//  Scheduling
	// --------------------------------------------------------------------------------------------
	
	public Iterable<Instance> getPreferredLocations() {
		return null;
	}
	
	
	// --------------------------------------------------------------------------------------------
	//  Utilities
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Creates a simple name representation in the style 'taskname (x/y)', where
	 * 'taskname' is the name as returned by {@link #getTaskName()}, 'x' is the parallel
	 * subtask index as returned by {@link #getParallelSubtaskIndex()}{@code + 1}, and 'y' is the total
	 * number of tasks, as returned by {@link #getTotalNumberOfParallelSubtasks()}.
	 * 
	 * @return A simple name representation.
	 */
	public String getSimpleName() {
		return getTaskName() + " (" + (getParallelSubtaskIndex()+1) + '/' + getTotalNumberOfParallelSubtasks() + ')';
	}
}
