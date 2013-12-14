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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import eu.stratosphere.core.io.IOReadableWritable;
import eu.stratosphere.core.io.StringRecord;
import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.util.EnumUtils;
import eu.stratosphere.util.StringUtils;

/**
 * This class implements a management vertex of a {@link ManagementGraph}. A management vertex is derived from the type
 * of vertices Nephele uses in its internal scheduling structures.
 * <p>
 * This class is not thread-safe.
 * 
 * @author warneke
 */
public final class ManagementVertex extends ManagementAttachment implements IOReadableWritable {

	/**
	 * The management group vertex this vertex belongs to.
	 */
	private final ManagementGroupVertex groupVertex;

	/**
	 * The ID of this management group vertex.
	 */
	private final ManagementVertexID id;

	/**
	 * A list of input gates which belong to this vertex.
	 */
	private final List<ManagementGate> inputGates = new ArrayList<ManagementGate>();

	/**
	 * A list of output gates which belong to this vertex.
	 */
	private final List<ManagementGate> outputGates = new ArrayList<ManagementGate>();

	/**
	 * The current execution state of the vertex represented by this management vertex.
	 */
	private ExecutionState executionState = ExecutionState.CREATED;

	/**
	 * The name of the instance the vertex represented by this management vertex currently runs on.
	 */
	private String instanceName;

	/**
	 * The type of the instance the vertex represented by this management vertex currently runs on.
	 */
	private String instanceType;

	/**
	 * The index of this vertex in the management group vertex it belongs to.
	 */
	private final int indexInGroup;

	/**
	 * Constructs a new management vertex.
	 * 
	 * @param groupVertex
	 *        the management group vertex the new vertex belongs to
	 * @param id
	 *        the ID of the new management vertex
	 * @param instanceName
	 *        the name of the instance the vertex represented by this new management vertex currently runs on
	 * @param instanceType
	 *        the type of the instance the vertex represented by this new management vertex currently runs on
	 * @param checkpointState
	 *        the state of the vertex's checkpoint
	 * @param indexInGroup
	 *        the index of this vertex in the management group vertex it belongs to
	 */
	public ManagementVertex(final ManagementGroupVertex groupVertex, final ManagementVertexID id,
			final String instanceName, final String instanceType, final int indexInGroup) {
		this.groupVertex = groupVertex;
		this.id = id;
		this.instanceName = instanceName;
		this.instanceType = instanceType;

		this.indexInGroup = indexInGroup;

		groupVertex.addGroupMember(this);
	}

	/**
	 * Adds a management gate to this vertex.
	 * 
	 * @param gate
	 *        the management gate to be added
	 */
	void addGate(final ManagementGate gate) {

		if (gate.isInputGate()) {
			this.inputGates.add(gate);
		} else {
			this.outputGates.add(gate);
		}
	}

	/**
	 * Returns the name of the instance the vertex represented by this management vertex currently runs on.
	 * 
	 * @return the name of the instance the vertex represented by this management vertex currently runs on
	 */
	public String getInstanceName() {
		return this.instanceName;
	}

	/**
	 * Returns the type of the instance the vertex represented by this management vertex currently runs on.
	 * 
	 * @return the type of the instance the vertex represented by this management vertex currently runs on
	 */
	public String getInstanceType() {
		return this.instanceType;
	}

	/**
	 * Returns the number of input gates this management vertex contains.
	 * 
	 * @return the number of input gates this management vertex contains
	 */
	public int getNumberOfInputGates() {
		return this.inputGates.size();
	}

	/**
	 * Returns the number of output gates this management vertex contains.
	 * 
	 * @return the number of output gates this management vertex contains
	 */
	public int getNumberOfOutputGates() {
		return this.outputGates.size();
	}

	/**
	 * Returns the input gate at the given index.
	 * 
	 * @param index
	 *        the index of the input gate to be returned
	 * @return the input gate at the given index or <code>null</code> if no such input gate exists
	 */
	public ManagementGate getInputGate(final int index) {

		if (index < this.inputGates.size()) {
			return this.inputGates.get(index);
		}

		return null;
	}

	/**
	 * Returns the output gate at the given index.
	 * 
	 * @param index
	 *        the index of the output gate to be returned
	 * @return the output gate at the given index or <code>null</code> if no such output gate exists
	 */
	public ManagementGate getOutputGate(final int index) {

		if (index < this.outputGates.size()) {
			return this.outputGates.get(index);
		}

		return null;
	}

	/**
	 * Returns the group vertex this management vertex belongs to.
	 * 
	 * @return the group vertex this management vertex belongs to
	 */
	public ManagementGroupVertex getGroupVertex() {
		return this.groupVertex;
	}

	/**
	 * The management graph this management vertex belongs to.
	 * 
	 * @return the management graph this management vertex belongs to
	 */
	public ManagementGraph getGraph() {
		return this.groupVertex.getGraph();
	}

	/**
	 * Returns the ID of this management vertex.
	 * 
	 * @return the ID of this management vertex
	 */
	public ManagementVertexID getID() {
		return this.id;
	}

	/**
	 * Returns the name of this management vertex.
	 * 
	 * @return the name of this management vertex, possibly <code>null</code>
	 */
	public String getName() {

		return this.groupVertex.getName();
	}

	/**
	 * Returns the number of vertices which belong to the same group vertex as this management vertex.
	 * 
	 * @return the number of vertices which belong to the same group vertex as this management vertex
	 */
	public int getNumberOfVerticesInGroup() {

		return this.groupVertex.getNumberOfGroupMembers();
	}

	/**
	 * Returns the index at which this vertex is stored inside its corresponding group vertex.
	 * 
	 * @return the index at which this vertex is stored inside its corresponding group vertex
	 */
	public int getIndexInGroup() {

		return this.indexInGroup;
	}

	/**
	 * Sets the current execution state of this management vertex.
	 * 
	 * @param executionState
	 *        the current execution state of this vertex
	 */
	public void setExecutionState(final ExecutionState executionState) {
		this.executionState = executionState;
	}

	/**
	 * Returns the current execution state of this management vertex.
	 * 
	 * @return the current execution state of this management vertex
	 */
	public ExecutionState getExecutionState() {
		return this.executionState;
	}

	/**
	 * Sets the name of the instance this vertex currently runs on.
	 * 
	 * @param instanceName
	 *        the name of the instance this vertex currently runs on
	 */
	public void setInstanceName(final String instanceName) {
		this.instanceName = instanceName;
	}

	/**
	 * Sets the type of instance this vertex currently runs on.
	 * 
	 * @param instanceType
	 *        the type of instance this vertex currently runs on
	 */
	public void setInstanceType(final String instanceType) {
		this.instanceType = instanceType;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(final DataInput in) throws IOException {

		// Read the execution state
		this.executionState = EnumUtils.readEnum(in, ExecutionState.class);

		// Read number of input gates
		int numberOfInputGates = in.readInt();
		for (int i = 0; i < numberOfInputGates; i++) {
			new ManagementGate(this, new ManagementGateID(), i, true);
		}

		// Read number of input gates
		int numberOfOutputGates = in.readInt();
		for (int i = 0; i < numberOfOutputGates; i++) {
			new ManagementGate(this, new ManagementGateID(), i, false);
		}

		this.instanceName = StringRecord.readString(in);
		this.instanceType = StringRecord.readString(in);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput out) throws IOException {

		// Write the execution state
		EnumUtils.writeEnum(out, this.executionState);

		// Write out number of input gates
		out.writeInt(this.inputGates.size());

		// Write out number of output gates
		out.writeInt(this.outputGates.size());

		StringRecord.writeString(out, this.instanceName);
		StringRecord.writeString(out, this.instanceType);
	}
	
	@Override
	public String toString() {
		return String.format("%s_%d", getGroupVertex().getName(), indexInGroup);
	}
	
	/**
	 * Returns Json representation of this ManagementVertex
	 * 
	 * @return
	 */
	public String toJson() {
		StringBuilder json = new StringBuilder("");
		json.append("{");
		json.append("\"vertexid\": \"" + this.getID() + "\",");
		json.append("\"vertexname\": \"" + StringUtils.escapeHtml(this.toString()) + "\",");
		json.append("\"vertexstatus\": \"" + this.getExecutionState() + "\",");
		json.append("\"vertexinstancename\": \"" + this.getInstanceName() + "\",");
		json.append("\"vertexinstancetype\": \"" + this.getInstanceType() + "\"");
		json.append("}");
		return json.toString();
	}
} 
