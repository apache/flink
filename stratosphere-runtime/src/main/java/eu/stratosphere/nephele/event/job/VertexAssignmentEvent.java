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

import eu.stratosphere.core.io.StringRecord;
import eu.stratosphere.nephele.managementgraph.ManagementVertexID;

/**
 * A {@link VertexAssignmentEvent} can be used to notify other objects about changes in the assignment of vertices to
 * instances.
 * 
 * @author warneke
 */
public final class VertexAssignmentEvent extends AbstractEvent implements ManagementEvent {

	/**
	 * The ID identifies the vertex this events refers to.
	 */
	private ManagementVertexID managementVertexID;

	/**
	 * The name of the instance the vertex is now assigned to.
	 */
	private String instanceName;

	/**
	 * The type of the instance the vertex is now assigned to.
	 */
	private String instanceType;

	/**
	 * Constructs a new event.
	 * 
	 * @param timestamp
	 *        the timestamp of the event
	 * @param managementVertexID
	 *        identifies the vertex this event refers to
	 * @param instanceName
	 *        the name of the instance the vertex is now assigned to
	 * @param instanceType
	 *        the type of the instance the vertex is now assigned to
	 */
	public VertexAssignmentEvent(final long timestamp, final ManagementVertexID managementVertexID,
			final String instanceName, final String instanceType) {
		super(timestamp);

		this.managementVertexID = managementVertexID;
		this.instanceName = instanceName;
		this.instanceType = instanceType;
	}

	/**
	 * Constructor for serialization/deserialization. Should not be called on other occasions.
	 */
	public VertexAssignmentEvent() {
		super();

		this.managementVertexID = new ManagementVertexID();
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
	 * Returns the name of the instance the vertex is now assigned to.
	 * 
	 * @return the name of the instance the vertex is now assigned to
	 */
	public String getInstanceName() {
		return this.instanceName;
	}

	/**
	 * Returns the type of the instance the vertex is now assigned to.
	 * 
	 * @return the type of the instance the vertex is now assigned to
	 */
	public String getInstanceType() {
		return this.instanceType;
	}


	@Override
	public void read(final DataInput in) throws IOException {

		super.read(in);

		this.managementVertexID.read(in);
		this.instanceName = StringRecord.readString(in);
		this.instanceType = StringRecord.readString(in);
	}


	@Override
	public void write(final DataOutput out) throws IOException {

		super.write(out);

		this.managementVertexID.write(out);
		StringRecord.writeString(out, this.instanceName);
		StringRecord.writeString(out, this.instanceType);
	}


	@Override
	public boolean equals(final Object obj) {

		if (!super.equals(obj)) {
			return false;
		}

		if (!(obj instanceof VertexAssignmentEvent)) {
			return false;
		}

		final VertexAssignmentEvent vae = (VertexAssignmentEvent) obj;

		if (!this.managementVertexID.equals(vae.getVertexID())) {
			return false;
		}

		if (this.instanceName == null) {
			if (vae.getInstanceName() != null) {
				return false;
			}
		} else {
			if (!this.instanceName.equals(vae.getInstanceName())) {
				return false;
			}
		}

		if (this.instanceType == null) {
			if (vae.getInstanceType() != null) {
				return false;
			}
		} else {
			if (!this.instanceType.equals(vae.getInstanceType())) {
				return false;
			}
		}

		return true;
	}


	@Override
	public int hashCode() {

		if (this.managementVertexID != null) {
			return this.managementVertexID.hashCode();
		}

		return super.hashCode();
	}
}
