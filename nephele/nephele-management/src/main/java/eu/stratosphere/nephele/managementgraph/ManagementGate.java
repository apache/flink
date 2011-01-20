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

package eu.stratosphere.nephele.managementgraph;

import java.util.ArrayList;
import java.util.List;

/**
 * This class implements an input or output gate of a {@link ManagementVertex}. The gate is derived an input or output
 * gate of the actual execution vertex.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 */
public final class ManagementGate {

	/**
	 * The management vertex this gate belongs to.
	 */
	private final ManagementVertex managementVertex;

	/**
	 * The name of the record type transported through this gate.
	 */
	private final String recordType;

	/**
	 * <code>true</code> if this gate represents an input gate in the actual execution graph, <code>false</code>
	 * otherwise.
	 */
	private final boolean isInputGate;

	/**
	 * The index of this gate.
	 */
	private final int index;

	/**
	 * A possible attachment to this gate.
	 */
	private volatile Object attachment = null;

	/**
	 * A list of edges originating from this gate.
	 */
	private final List<ManagementEdge> forwardEdges = new ArrayList<ManagementEdge>();

	/**
	 * A list of edges arriving at this gate.
	 */
	private final List<ManagementEdge> backwardEdges = new ArrayList<ManagementEdge>();

	/**
	 * Constructs a new management gate.
	 * 
	 * @param managementVertex
	 *        the management vertex this gate belongs to
	 * @param index
	 *        the index of this gate
	 * @param isInputGate
	 *        <code>true</code> if this gate represents an input gate in the actual execution graph, <code>false</code>
	 *        otherwise
	 * @param recordType
	 *        the name of the record type transported through this gate
	 */
	public ManagementGate(final ManagementVertex managementVertex, final int index, final boolean isInputGate,
			final String recordType) {
		this.isInputGate = isInputGate;
		this.managementVertex = managementVertex;
		this.recordType = recordType;
		this.index = index;

		managementVertex.addGate(this);
	}

	/**
	 * Checks if this gate represents an input gate.
	 * 
	 * @return <code>true</code> if this gate represents an input gate in the actual execution graph, <code>false</code>
	 *         otherwise
	 */
	public boolean isInputGate() {
		return this.isInputGate;
	}

	/**
	 * Adds a new edge which originates at this gate.
	 * 
	 * @param managementEdge
	 *        the edge to be added
	 * @param index
	 *        the index at which the edge shall be added
	 */
	synchronized void insertForwardEdge(final ManagementEdge managementEdge, final int index) {

		while (index >= this.forwardEdges.size()) {
			this.forwardEdges.add(null);
		}

		this.forwardEdges.set(index, managementEdge);
	}

	/**
	 * Adds a new edge which arrives at this gate.
	 * 
	 * @param managementEdge
	 *        the edge to be added
	 * @param index
	 *        the index at which the edge shall be added
	 */
	synchronized void insertBackwardEdge(final ManagementEdge managementEdge, final int index) {

		while (index >= this.backwardEdges.size()) {
			this.backwardEdges.add(null);
		}

		this.backwardEdges.set(index, managementEdge);
	}

	/**
	 * Returns the name of the record type transported through this gate.
	 * 
	 * @return the name of the record type transported through this gate
	 */
	public String getRecordType() {
		return this.recordType;
	}

	/**
	 * Returns the {@link ManagementGraph} this gate belongs to.
	 * 
	 * @return the management graph this gate belongs to
	 */
	public ManagementGraph getGraph() {
		return this.managementVertex.getGraph();
	}

	/**
	 * Returns the number of edges originating at this gate.
	 * 
	 * @return the number of edges originating at this gate
	 */
	public synchronized int getNumberOfForwardEdges() {
		return this.forwardEdges.size();
	}

	/**
	 * Returns the number of edges arriving at this gate.
	 * 
	 * @return the number of edges arriving at this gate
	 */
	public synchronized int getNumberOfBackwardEdges() {
		return this.backwardEdges.size();
	}

	/**
	 * Returns the index of this gate.
	 * 
	 * @return the index of this gate
	 */
	public int getIndex() {
		return this.index;
	}

	/**
	 * Returns the edge originating at the given index.
	 * 
	 * @param index
	 *        the index of the edge to be returned
	 * @return the edge at the given index or <code>null</code> if no such edge exists
	 */
	public synchronized ManagementEdge getForwardEdge(final int index) {

		if (index < this.forwardEdges.size()) {
			return this.forwardEdges.get(index);
		}

		return null;
	}

	/**
	 * Returns the edge arriving at the given index.
	 * 
	 * @param index
	 *        the index of the edge to be returned
	 * @return the edge at the given index or <code>null</code> if no such edge exists
	 */
	public synchronized ManagementEdge getBackwardEdge(final int index) {

		if (index < this.backwardEdges.size()) {
			return this.backwardEdges.get(index);
		}

		return null;
	}

	/**
	 * Returns the vertex this gate belongs to.
	 * 
	 * @return the vertex this gate belongs to
	 */
	public ManagementVertex getVertex() {
		return this.managementVertex;
	}

	/**
	 * Sets an attachment for this gate.
	 * 
	 * @param attachment
	 *        the attachment for this gate
	 */
	public void setAttachment(final Object attachment) {
		this.attachment = attachment;
	}

	/**
	 * Returns the attachment of this gate.
	 * 
	 * @return the attachment of this gate or <code>null</code> if this gate has no attachment
	 */
	public Object getAttachment() {
		return this.attachment;
	}
}
