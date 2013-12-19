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

package eu.stratosphere.nephele.taskmanager;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.core.io.IOReadableWritable;
import eu.stratosphere.core.io.StringRecord;
import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.util.EnumUtils;

/**
 * This class can be used to propagate updates about a task's execution state from the
 * task manager to the job manager.
 * 
 */
public class TaskExecutionState implements IOReadableWritable {

	private JobID jobID = null;

	private ExecutionVertexID executionVertexID = null;

	private ExecutionState executionState = null;

	private String description = null;

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


	@Override
	public void read(final DataInput in) throws IOException {

		boolean isNotNull = in.readBoolean();

		if (isNotNull) {
			this.jobID = new JobID();
			this.jobID.read(in);
		} else {
			this.jobID = null;
		}

		isNotNull = in.readBoolean();

		// Read the execution vertex ID
		if (isNotNull) {
			this.executionVertexID = new ExecutionVertexID();
			this.executionVertexID.read(in);
		} else {
			this.executionVertexID = null;
		}

		// Read execution state
		this.executionState = EnumUtils.readEnum(in, ExecutionState.class);

		// Read description
		this.description = StringRecord.readString(in);
	}


	@Override
	public void write(final DataOutput out) throws IOException {

		if (this.jobID == null) {
			out.writeBoolean(false);
		} else {
			out.writeBoolean(true);
			this.jobID.write(out);
		}

		// Write the execution vertex ID
		if (this.executionVertexID == null) {
			out.writeBoolean(false);
		} else {
			out.writeBoolean(true);
			this.executionVertexID.write(out);
		}

		// Write execution state
		EnumUtils.writeEnum(out, this.executionState);

		// Write description
		StringRecord.writeString(out, this.description);

	}

}
