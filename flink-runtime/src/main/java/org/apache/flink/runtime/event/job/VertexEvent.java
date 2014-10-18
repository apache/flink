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

package org.apache.flink.runtime.event.job;

import java.io.IOException;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.util.StringUtils;

import com.google.common.base.Preconditions;

/**
 * Vertex events are transmitted from the job manager to the job client in order to inform the user about
 * changes in terms of a tasks execution state.
 */
public class VertexEvent extends AbstractEvent {

	private static final long serialVersionUID = -521556020344465262L;

	/** The ID of the job vertex this event belongs to. */
	private JobVertexID jobVertexID;

	/** The name of the job vertex this event belongs to. */
	private String jobVertexName;

	/** The number of subtasks the corresponding vertex has been split into at runtime. */
	private int totalNumberOfSubtasks;

	/** The index of the subtask that this event belongs to. */
	private int indexOfSubtask;
	
	/** The id of the execution attempt */
	private ExecutionAttemptID executionAttemptId;

	/** The current execution state of the subtask this event belongs to. */
	private ExecutionState currentExecutionState;

	/** An optional more detailed description of the event. */
	private String description;

	/**
	 * Constructs a new vertex event object.
	 * 
	 * @param timestamp
	 *        the timestamp of the event
	 * @param jobVertexID
	 *        the ID of the job vertex this event belongs to
	 * @param jobVertexName
	 *        the name of the job vertex this event belongs to
	 * @param totalNumberOfSubtasks
	 *        the number of subtasks the corresponding vertex has been split into at runtime
	 * @param indexOfSubtask
	 *        the index of the subtask that this event belongs to
	 * @param currentExecutionState
	 *        the current execution state of the subtask this event belongs to
	 * @param description
	 *        an optional description
	 */
	public VertexEvent(long timestamp, JobVertexID jobVertexID, String jobVertexName,
			int totalNumberOfSubtasks, int indexOfSubtask, ExecutionAttemptID executionAttemptId,
			ExecutionState currentExecutionState, String description)
	{
		super(timestamp);
		
		Preconditions.checkNotNull(jobVertexID);
		Preconditions.checkNotNull(currentExecutionState);
		
		this.jobVertexID = jobVertexID;
		this.jobVertexName = jobVertexName;
		this.totalNumberOfSubtasks = totalNumberOfSubtasks;
		this.indexOfSubtask = indexOfSubtask;
		this.executionAttemptId = executionAttemptId;
		this.currentExecutionState = currentExecutionState;
		this.description = description;
	}

	/**
	 * Constructs a new vertex event object. This constructor is
	 * required for the deserialization process and is not supposed
	 * to be called directly.
	 */
	public VertexEvent() {
		super();

		this.jobVertexID = new JobVertexID();
		this.totalNumberOfSubtasks = -1;
		this.indexOfSubtask = -1;
		this.executionAttemptId = new ExecutionAttemptID();
		this.currentExecutionState = ExecutionState.CREATED;
	}

	/**
	 * Returns the ID of the job vertex this event belongs to.
	 * 
	 * @return the ID of the job vertex this event belongs to
	 */
	public JobVertexID getJobVertexID() {
		return jobVertexID;
	}

	/**
	 * Returns the name of the job vertex this event belongs to.
	 * 
	 * @return the name of the job vertex, possibly <code>null</code>
	 */
	public String getJobVertexName() {
		return jobVertexName;
	}

	/**
	 * Returns the number of subtasks the corresponding vertex has been
	 * split into at runtime.
	 * 
	 * @return the number of subtasks
	 */
	public int getTotalNumberOfSubtasks() {
		return totalNumberOfSubtasks;
	}

	/**
	 * Returns the index of the subtask that this event belongs to.
	 * 
	 * @return the index of the subtask
	 */
	public int getIndexOfSubtask() {
		return indexOfSubtask;
	}

	/**
	 * Returns the current execution state of the subtask this event
	 * belongs to.
	 * 
	 * @return the current execution state of the subtask this event belongs to
	 */
	public ExecutionState getCurrentExecutionState() {
		return currentExecutionState;
	}

	/**
	 * Returns an optional description which may be attached to this event.
	 * 
	 * @return the description attached to this event, possibly <code>null</code>.
	 */
	public String getDescription() {
		return description;
	}
	
	public ExecutionAttemptID getExecutionAttemptId() {
		return executionAttemptId;
	}

	// --------------------------------------------------------------------------------------------
	// Utilities
	// --------------------------------------------------------------------------------------------
	
	@Override
	public void read(DataInputView in) throws IOException {
		super.read(in);

		this.jobVertexID.read(in);
		this.executionAttemptId.read(in);
		this.totalNumberOfSubtasks = in.readInt();
		this.indexOfSubtask = in.readInt();
		this.currentExecutionState = ExecutionState.values()[in.readInt()];
		this.jobVertexName = StringUtils.readNullableString(in);
		this.description = StringUtils.readNullableString(in);
	}


	@Override
	public void write(DataOutputView out) throws IOException {
		super.write(out);

		this.jobVertexID.write(out);
		this.executionAttemptId.write(out);
		out.writeInt(this.totalNumberOfSubtasks);
		out.writeInt(this.indexOfSubtask);
		out.writeInt(this.currentExecutionState.ordinal());
		StringUtils.writeNullableString(jobVertexName, out);
		StringUtils.writeNullableString(description, out);
	}

	// --------------------------------------------------------------------------------------------
	// Utilities
	// --------------------------------------------------------------------------------------------

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof VertexEvent) {
			final VertexEvent other = (VertexEvent) obj;
			
			return super.equals(other) &&
					this.jobVertexID.equals(other.jobVertexID) && 
					this.totalNumberOfSubtasks == other.totalNumberOfSubtasks &&
					this.indexOfSubtask == other.indexOfSubtask &&
					this.currentExecutionState == other.currentExecutionState &&
					
					(this.jobVertexName == null ? other.jobVertexName == null :
						(other.jobVertexName != null && this.jobVertexName.equals(other.jobVertexName))) &&
						
					(this.description == null ? other.description == null :
						(other.description != null && this.description.equals(other.description)));
		}
		else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return super.hashCode() ^ jobVertexID.hashCode() ^ (31*indexOfSubtask) ^ (17*currentExecutionState.ordinal());
	}
	
	public String toString() {
		return timestampToString(getTimestamp()) + ":\t" + this.jobVertexName + " (" + (this.indexOfSubtask + 1) + "/"
			+ this.totalNumberOfSubtasks + ") switched to " + this.currentExecutionState
			+ ((this.description != null) ? ("\n" + this.description) : "");
	}
}
