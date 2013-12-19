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

package eu.stratosphere.nephele.event.job;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.core.io.StringRecord;
import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.jobgraph.JobVertexID;
import eu.stratosphere.nephele.util.EnumUtils;

/**
 * Vertex events are transmitted from the job manager
 * to the job client in order to inform the user about
 * changes in terms of a tasks execution state.
 * 
 */
public class VertexEvent extends AbstractEvent {

	/**
	 * The ID of the job vertex this event belongs to.
	 */
	private JobVertexID jobVertexID;

	/**
	 * The name of the job vertex this event belongs to.
	 */
	private String jobVertexName;

	/**
	 * The number of subtasks the corresponding vertex has been split into at runtime.
	 */
	private int totalNumberOfSubtasks;

	/**
	 * The index of the subtask that this event belongs to.
	 */
	private int indexOfSubtask;

	/**
	 * The current execution state of the subtask this event belongs to.
	 */
	private ExecutionState currentExecutionState;

	/**
	 * An optional more detailed description of the event.
	 */
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
	public VertexEvent(final long timestamp, final JobVertexID jobVertexID, final String jobVertexName,
			final int totalNumberOfSubtasks, final int indexOfSubtask, final ExecutionState currentExecutionState,
			final String description) {
		super(timestamp);
		this.jobVertexID = jobVertexID;
		this.jobVertexName = jobVertexName;
		this.totalNumberOfSubtasks = totalNumberOfSubtasks;
		this.indexOfSubtask = indexOfSubtask;
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
		this.jobVertexName = null;
		this.totalNumberOfSubtasks = -1;
		this.indexOfSubtask = -1;
		this.currentExecutionState = ExecutionState.CREATED;
		this.description = null;
	}


	@Override
	public void read(final DataInput in) throws IOException {

		super.read(in);

		this.jobVertexID.read(in);
		this.jobVertexName = StringRecord.readString(in);
		this.totalNumberOfSubtasks = in.readInt();
		this.indexOfSubtask = in.readInt();
		this.currentExecutionState = EnumUtils.readEnum(in, ExecutionState.class);
		this.description = StringRecord.readString(in);
	}


	@Override
	public void write(final DataOutput out) throws IOException {

		super.write(out);

		this.jobVertexID.write(out);
		StringRecord.writeString(out, this.jobVertexName);
		out.writeInt(this.totalNumberOfSubtasks);
		out.writeInt(this.indexOfSubtask);
		EnumUtils.writeEnum(out, this.currentExecutionState);
		StringRecord.writeString(out, this.description);
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


	public String toString() {

		return timestampToString(getTimestamp()) + ":\t" + this.jobVertexName + " (" + (this.indexOfSubtask + 1) + "/"
			+ this.totalNumberOfSubtasks + ") switched to " + this.currentExecutionState
			+ ((this.description != null) ? ("\n" + this.description) : "");
	}


	@Override
	public boolean equals(final Object obj) {

		if (!super.equals(obj)) {
			return false;
		}

		if (!(obj instanceof VertexEvent)) {
			return false;
		}

		final VertexEvent vertexEvent = (VertexEvent) obj;

		if (!this.jobVertexID.equals(vertexEvent.getJobVertexID())) {
			return false;
		}

		if (this.jobVertexName != null && vertexEvent.getJobVertexName() != null) {
			if (!this.jobVertexName.equals(vertexEvent.getJobVertexName())) {
				return false;
			}
		} else {
			if (this.jobVertexName != vertexEvent.getJobVertexName()) {
				return false;
			}
		}

		if (this.totalNumberOfSubtasks != vertexEvent.getTotalNumberOfSubtasks()) {
			return false;
		}

		if (this.indexOfSubtask != vertexEvent.getIndexOfSubtask()) {
			return false;
		}

		if (!this.currentExecutionState.equals(vertexEvent.getCurrentExecutionState())) {
			return false;
		}

		if (this.description != null && vertexEvent.getDescription() != null) {

			if (!this.description.equals(vertexEvent.getDescription())) {
				return false;
			}
		} else {
			if (this.description != vertexEvent.getDescription()) {
				return false;
			}
		}

		return true;
	}


	@Override
	public int hashCode() {

		return super.hashCode();
	}
}
