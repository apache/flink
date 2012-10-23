/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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
 * This class is not thread-safe.
 * 
 * @author warneke
 */
public final class ManagementGate extends ManagementAttachment {

	/**
	 * The management vertex this gate belongs to.
	 */
	private final ManagementVertex managementVertex;

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
	 * A list of edges originating from this gate.
	 */
	private final List<ManagementEdge> forwardEdges = new ArrayList<ManagementEdge>();

	/**
	 * A list of edges arriving at this gate.
	 */
	private final List<ManagementEdge> backwardEdges = new ArrayList<ManagementEdge>();

	/**
	 * The id of the management gate.
	 */
	private ManagementGateID gateID;

	/**
	 * Constructs a new management gate.
	 * 
	 * @param managementVertex
	 *        the management vertex this gate belongs to
	 * @param index
	 *        the index of this gate
	 * @param gateID
	 *        The id of the new management gate
	 * @param isInputGate
	 *        <code>true</code> if this gate represents an input gate in the actual execution graph, <code>false</code>
	 *        otherwise
	 */
	public ManagementGate(final ManagementVertex managementVertex, final ManagementGateID gateID,
			final int index, final boolean isInputGate) {
		this.isInputGate = isInputGate;
		this.managementVertex = managementVertex;
		this.gateID = gateID;
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
	void insertForwardEdge(final ManagementEdge managementEdge, final int index) {

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
	void insertBackwardEdge(final ManagementEdge managementEdge, final int index) {

		while (index >= this.backwardEdges.size()) {
			this.backwardEdges.add(null);
		}

		this.backwardEdges.set(index, managementEdge);
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
	public int getNumberOfForwardEdges() {

		return this.forwardEdges.size();
	}

	/**
	 * Returns the number of edges arriving at this gate.
	 * 
	 * @return the number of edges arriving at this gate
	 */
	public int getNumberOfBackwardEdges() {

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
	public ManagementEdge getForwardEdge(final int index) {

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
	public ManagementEdge getBackwardEdge(final int index) {

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
	 * Returns the id of the management gate.
	 * 
	 * @return the id of the management gate
	 */
	public ManagementGateID getManagementGateID() {
		return gateID;
	}
}
