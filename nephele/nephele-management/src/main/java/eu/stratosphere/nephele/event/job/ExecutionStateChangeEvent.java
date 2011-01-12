/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.nephele.event.job;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.event.job.AbstractEvent;
import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.managementgraph.ManagementVertexID;
import eu.stratosphere.nephele.util.EnumUtils;

public class ExecutionStateChangeEvent extends AbstractEvent implements ManagementEvent {

	/**
	 * The ID identifies the vertex this events refers to.
	 */
	private ManagementVertexID managementVertexID;

	/**
	 * The new execution state of the vertex this event refers to.
	 */
	private ExecutionState newExecutionState;

	/**
	 * Constructs a new vertex event object.
	 * 
	 * @param timestamp
	 *        the timestamp of the event
	 * @param managementVertexID
	 *        identifies the vertex this event refers to
	 * @param newExecutionState
	 *        the new execution state of the vertex this event refers to
	 */
	public ExecutionStateChangeEvent(long timestamp, ManagementVertexID managementVertexID,
			ExecutionState newExecutionState) {
		super(timestamp);
		this.managementVertexID = managementVertexID;
		this.newExecutionState = newExecutionState;
	}

	/**
	 * Constructs a new execution state change event object. This constructor is
	 * required for the deserialization process and is not supposed
	 * to be called directly.
	 */
	public ExecutionStateChangeEvent() {
		super();

		this.managementVertexID = new ManagementVertexID();
		this.newExecutionState = ExecutionState.CREATED;
	}

	public ManagementVertexID getVertexID() {
		return this.managementVertexID;
	}

	public ExecutionState getNewExecutionState() {
		return this.newExecutionState;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(DataInput in) throws IOException {

		super.read(in);

		this.managementVertexID.read(in);
		this.newExecutionState = EnumUtils.readEnum(in, ExecutionState.class);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(DataOutput out) throws IOException {

		super.write(out);

		this.managementVertexID.write(out);
		EnumUtils.writeEnum(out, this.newExecutionState);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean equals(Object obj) {

		if (!super.equals(obj)) {
			return false;
		}

		if (!(obj instanceof ExecutionStateChangeEvent)) {
			return false;
		}

		ExecutionStateChangeEvent stateChangeEvent = (ExecutionStateChangeEvent) obj;
		if (!stateChangeEvent.getNewExecutionState().equals(this.newExecutionState)) {
			return false;
		}

		if (!stateChangeEvent.getVertexID().equals(this.managementVertexID)) {
			return false;
		}

		return true;
	}
}
