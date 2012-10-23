/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.nephele.taskmanager;

import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.jobgraph.JobID;

/**
 * This class can be used to propagate updates about a task's execution state from the
 * task manager to the job manager.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 */
public final class TaskExecutionState {

	private final JobID jobID;

	private final ExecutionVertexID executionVertexID;

	private final ExecutionState executionState;

	private final String description;

	/**
	 * Creates a new task execution state.
	 * 
	 * @param jobID
	 *        the ID of the job the task belongs to
	 * @param id
	 *        the ID of the task whose state is to be reported
	 * @param executionState
	 *        the execution state to be reported
	 * @param description
	 *        an optional description
	 */
	public TaskExecutionState(final JobID jobID, final ExecutionVertexID id, final ExecutionState executionState,
			final String description) {
		this.jobID = jobID;
		this.executionVertexID = id;
		this.executionState = executionState;
		this.description = description;
	}

	/**
	 * Creates an empty task execution state.
	 */
	public TaskExecutionState() {
		this.jobID = null;
		this.executionVertexID = null;
		this.executionState = null;
		this.description = null;
	}

	/**
	 * Returns the description of this task execution state.
	 * 
	 * @return the description of this task execution state or <code>null</code> if there is no description available
	 */
	public String getDescription() {
		return this.description;
	}

	/**
	 * Returns the ID of the task this result belongs to
	 * 
	 * @return the ID of the task this result belongs to
	 */
	public ExecutionVertexID getID() {
		return this.executionVertexID;
	}

	/**
	 * Returns the new execution state of the task.
	 * 
	 * @return the new execution state of the task
	 */
	public ExecutionState getExecutionState() {
		return this.executionState;
	}

	/**
	 * The ID of the job the task belongs to
	 * 
	 * @return the ID of the job the task belongs to
	 */
	public JobID getJobID() {
		return this.jobID;
	}
}
