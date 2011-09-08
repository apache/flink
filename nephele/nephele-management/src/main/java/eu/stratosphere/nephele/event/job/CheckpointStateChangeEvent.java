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

import eu.stratosphere.nephele.managementgraph.ManagementVertexID;
import eu.stratosphere.nephele.types.StringRecord;

/**
 * An {@link CheckpointStateChangeEvent} can be used to notify other objects about changes of a vertex's checkpoint
 * state.
 * 
 * @author warneke
 */
public class CheckpointStateChangeEvent extends AbstractEvent implements ManagementEvent {

	/**
	 * The ID identifies the vertex this events refers to.
	 */
	private ManagementVertexID managementVertexID;

	/**
	 * The new state of the vertex's checkpoint.
	 */
	private String newCheckpointState;

	/**
	 * Constructs a checkpoint state change event object.
	 * 
	 * @param timestamp
	 *        the timestamp of the event
	 * @param managementVertexID
	 *        identifies the vertex this event refers to
	 * @param newCheckpointState
	 *        the new state of the vertex's checkpoint
	 */
	public CheckpointStateChangeEvent(final long timestamp, final ManagementVertexID managementVertexID,
			final String newCheckpointState) {

		super(timestamp);
		this.managementVertexID = managementVertexID;
		this.newCheckpointState = newCheckpointState;
	}

	/**
	 * Constructs a new checkpoint state change event object. This constructor is
	 * required for the deserialization process and is not supposed
	 * to be called directly.
	 */
	public CheckpointStateChangeEvent() {

		this.managementVertexID = new ManagementVertexID();
		this.newCheckpointState = null;
	}

	/**
	 * Returns the ID of the vertex this event refers to.
	 * 
	 * @return the ID of the vertex this event refers to
	 */
	public ManagementVertexID getVertexID() {
		return this.managementVertexID;
	}

	/**
	 * Returns the new state of the vertex's checkpoint.
	 * 
	 * @return the new state of the vertex's checkpoint
	 */
	public String getNewCheckpointState() {
		return this.newCheckpointState;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(final DataInput in) throws IOException {

		super.read(in);

		this.managementVertexID.read(in);
		this.newCheckpointState = StringRecord.readString(in);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput out) throws IOException {

		super.write(out);

		this.managementVertexID.write(out);
		StringRecord.writeString(out, this.newCheckpointState);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean equals(final Object obj) {

		if (!super.equals(obj)) {
			return false;
		}

		if (!(obj instanceof CheckpointStateChangeEvent)) {
			return false;
		}

		CheckpointStateChangeEvent stateChangeEvent = (CheckpointStateChangeEvent) obj;
		if (!stateChangeEvent.getNewCheckpointState().equals(this.newCheckpointState)) {
			return false;
		}

		if (!stateChangeEvent.getVertexID().equals(this.managementVertexID)) {
			return false;
		}

		return true;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int hashCode() {

		if (this.newCheckpointState != null) {
			return this.newCheckpointState.hashCode();
		}

		if (this.managementVertexID != null) {
			return this.managementVertexID.hashCode();
		}

		return super.hashCode();
	}
}
