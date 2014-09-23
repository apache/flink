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

import com.google.common.base.Preconditions;

/**
 * An ecutionStateChangeEvent can be used to notify other objects about an execution state change of a vertex. 
 */
public final class ExecutionStateChangeEvent extends AbstractEvent implements ManagementEvent {

	private static final long serialVersionUID = 1L;

	private JobVertexID vertexId;
	
	private int subtask;
	
	private ExecutionAttemptID executionAttemptId;

	private ExecutionState newExecutionState;

	/**
	 * Constructs a new vertex event object.
	 * 
	 * @param timestamp
	 *        the timestamp of the event
	 * @param executionAttemptId
	 *        identifies the vertex this event refers to
	 * @param newExecutionState
	 *        the new execution state of the vertex this event refers to
	 */
	public ExecutionStateChangeEvent(long timestamp, JobVertexID vertexId, int subtask,
			ExecutionAttemptID executionAttemptId, ExecutionState newExecutionState)
	{
		super(timestamp);
		
		Preconditions.checkNotNull(vertexId);
		Preconditions.checkArgument(subtask >= 0);
		Preconditions.checkNotNull(executionAttemptId);
		Preconditions.checkNotNull(newExecutionState);
		
		this.vertexId = vertexId;
		this.subtask = subtask;
		this.executionAttemptId = executionAttemptId;
		this.newExecutionState = newExecutionState;
	}

	/**
	 * Constructs a new execution state change event object. This constructor is
	 * required for the deserialization process and is not supposed
	 * to be called directly.
	 */
	public ExecutionStateChangeEvent() {
		super();

		this.vertexId = new JobVertexID();
		this.executionAttemptId = new ExecutionAttemptID();
		this.newExecutionState = ExecutionState.CREATED;
	}

	// --------------------------------------------------------------------------------------------
	
	public JobVertexID getVertexId() {
		return vertexId;
	}
	
	public int getSubtaskIndex() {
		return subtask;
	}
	
	/**
	 * Returns the ID of the vertex this event refers to.
	 * 
	 * @return the ID of the vertex this event refers to
	 */
	public ExecutionAttemptID getExecutionAttemptID() {
		return this.executionAttemptId;
	}

	/**
	 * Returns the new execution state of the vertex this event refers to.
	 * 
	 * @return the new execution state of the vertex this event refers to
	 */
	public ExecutionState getNewExecutionState() {
		return this.newExecutionState;
	}

	// --------------------------------------------------------------------------------------------
	
	@Override
	public void read(DataInputView in) throws IOException {
		super.read(in);
		this.vertexId.read(in);
		this.executionAttemptId.read(in);
		this.subtask = in.readInt();
		this.newExecutionState = ExecutionState.values()[in.readInt()];
	}


	@Override
	public void write(DataOutputView out) throws IOException {
		super.write(out);
		this.vertexId.write(out);
		this.executionAttemptId.write(out);
		out.writeInt(subtask);
		out.writeInt(this.newExecutionState.ordinal());
	}

	// --------------------------------------------------------------------------------------------
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof ExecutionStateChangeEvent) {
			ExecutionStateChangeEvent other = (ExecutionStateChangeEvent) obj;
			
			return other.newExecutionState == this.newExecutionState &&
					other.executionAttemptId.equals(this.executionAttemptId) &&
					super.equals(obj);
			
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return super.hashCode() ^ 
				(127 * newExecutionState.ordinal()) ^ 
				this.executionAttemptId.hashCode();
	}
	
	@Override
	public String toString() {
		return String.format("ExecutionStateChangeEvent %d at %d , executionAttempt=%s, newState=%s", getSequenceNumber(), getTimestamp(),
				executionAttemptId, newExecutionState);
	}
}
