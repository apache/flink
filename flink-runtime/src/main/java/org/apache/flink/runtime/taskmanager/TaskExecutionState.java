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

package org.apache.flink.runtime.taskmanager;

import java.io.IOException;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.execution.ExecutionState2;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.JobID;
import org.apache.flink.types.StringValue;

/**
 * This class represents an update about a task's execution state.
 */
public class TaskExecutionState implements IOReadableWritable , java.io.Serializable {

	private static final long serialVersionUID = 1L;

	private JobID jobID;

	private ExecutionAttemptID executionId;

	private ExecutionState2 executionState;

	private String description;

	/**
	 * Creates a new task execution state.
	 * 
	 * @param jobID
	 *        the ID of the job the task belongs to
	 * @param executionId
	 *        the ID of the task execution whose state is to be reported
	 * @param executionState
	 *        the execution state to be reported
	 * @param description
	 *        an optional description
	 */
	public TaskExecutionState(JobID jobID, ExecutionAttemptID executionId, ExecutionState2 executionState, String description) {
		if (jobID == null || executionId == null || executionState == null) {
			throw new NullPointerException();
		}
		
		this.jobID = jobID;
		this.executionId = executionId;
		this.executionState = executionState;
		this.description = description;
	}

	/**
	 * Creates an empty task execution state.
	 */
	public TaskExecutionState() {
		this.jobID = new JobID();
		this.executionId = new ExecutionAttemptID();
	}

	// --------------------------------------------------------------------------------------------
	
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
	public ExecutionAttemptID getID() {
		return this.executionId;
	}

	/**
	 * Returns the new execution state of the task.
	 * 
	 * @return the new execution state of the task
	 */
	public ExecutionState2 getExecutionState() {
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

	// --------------------------------------------------------------------------------------------

	@Override
	public void read(DataInputView in) throws IOException {
		this.jobID.read(in);
		this.executionId.read(in);
		this.executionState = ExecutionState2.values()[in.readInt()];

		if (in.readBoolean()) {
			this.description = StringValue.readString(in);
		} else {
			this.description = null;
		}
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		this.jobID.write(out);
		this.executionId.write(out);
		out.writeInt(this.executionState.ordinal());

		if (description != null) {
			out.writeBoolean(true);
			StringValue.writeString(description, out);
		} else {
			out.writeBoolean(false);
		}
	}
	
	// --------------------------------------------------------------------------------------------

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof TaskExecutionState) {
			TaskExecutionState other = (TaskExecutionState) obj;
			return other.jobID.equals(this.jobID) &&
					other.executionId.equals(this.executionId) &&
					other.executionState == this.executionState &&
					(other.description == null ? this.description == null :
						(this.description != null && other.description.equals(this.description)));
		}
		else {
			return false;
		}
	}
	
	@Override
	public int hashCode() {
		return jobID.hashCode() + executionId.hashCode() + executionState.ordinal();
	}
	
	@Override
	public String toString() {
		return String.format("TaskState jobId=%s, executionId=%s, state=%s, description=%s", 
				jobID, executionId, executionState, description == null ? "(null)" : description);
	}
}
