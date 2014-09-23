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

package org.apache.flink.runtime.taskmanager;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.JobID;

/**
 * This class represents an update about a task's execution state.
 */
public class TaskExecutionState implements IOReadableWritable , java.io.Serializable {

	private static final long serialVersionUID = 1L;

	private JobID jobID;

	private ExecutionAttemptID executionId;

	private ExecutionState executionState;

	private Throwable error;

	
	public TaskExecutionState(JobID jobID, ExecutionAttemptID executionId, ExecutionState executionState) {
		this(jobID, executionId, executionState, null);
	}
	
	/**
	 * Creates a new task execution state.
	 * 
	 * @param jobID
	 *        the ID of the job the task belongs to
	 * @param executionId
	 *        the ID of the task execution whose state is to be reported
	 * @param executionState
	 *        the execution state to be reported
	 * @param error
	 *        an optional error
	 */
	public TaskExecutionState(JobID jobID, ExecutionAttemptID executionId, ExecutionState executionState, Throwable error) {
		if (jobID == null || executionId == null || executionState == null) {
			throw new NullPointerException();
		}
		
		this.jobID = jobID;
		this.executionId = executionId;
		this.executionState = executionState;
		this.error = error;
	}

	/**
	 * Creates an empty task execution state.
	 */
	public TaskExecutionState() {
		this.jobID = new JobID();
		this.executionId = new ExecutionAttemptID();
	}

	// --------------------------------------------------------------------------------------------
	
	public Throwable getError() {
		return this.error;
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

	// --------------------------------------------------------------------------------------------

	@Override
	public void read(DataInputView in) throws IOException {
		this.jobID.read(in);
		this.executionId.read(in);
		this.executionState = ExecutionState.values()[in.readInt()];

		// read the exception
		int errorDataLen = in.readInt();
		if (errorDataLen > 0) {
			byte[] data = new byte[errorDataLen];
			in.readFully(data);
			try {
				ByteArrayInputStream bis = new ByteArrayInputStream(data);
				ObjectInputStream ois = new ObjectInputStream(bis);
				this.error = (Throwable) ois.readObject();
				ois.close();
			} catch (Throwable t) {
				this.error = new Exception("An error occurred, but the exception could not be transfered through the RPC");
			}
		}
		else {
			this.error = null;
		}
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		this.jobID.write(out);
		this.executionId.write(out);
		out.writeInt(this.executionState.ordinal());

		// transfer the exception
		if (this.error != null) {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(baos);
			oos.writeObject(error);
			oos.flush();
			oos.close();
			
			byte[] data = baos.toByteArray();
			out.writeInt(data.length);
			out.write(data);
		}
		else {
			out.writeInt(0);
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
					(other.error == null ? this.error == null :
						(this.error != null && other.error.getClass() == this.error.getClass()));
			
			// NOTE: exception equality does not work, so we can only check for same error class
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
		return String.format("TaskState jobId=%s, executionId=%s, state=%s, error=%s", 
				jobID, executionId, executionState, error == null ? "(null)" : error.getClass().getName() + ": " + error.getMessage());
	}
}
