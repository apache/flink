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

package eu.stratosphere.nephele.managementgraph;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import eu.stratosphere.core.io.IOReadableWritable;
import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.util.EnumUtils;
import eu.stratosphere.util.StringUtils;

/**
 * This class implements a management group vertex of a {@link ManagementGraph}. A management group vertex is derived
 * from the type of group vertices Nephele uses in its internal scheduling structures.
 * <p>
 * This class is not thread-safe.
 * 
 * @author warneke
 */
public final class ManagementGroupVertex extends ManagementAttachment implements IOReadableWritable {

	/**
	 * The ID of the management group vertex.
	 */
	private final ManagementGroupVertexID id;

	/**
	 * The name of the management group vertex.
	 */
	private final String name;

	/**
	 * The stage this management group vertex belongs to.
	 */
	private final ManagementStage stage;

	/**
	 * The list of {@link ManagementVertex} contained in this group vertex.
	 */
	private final List<ManagementVertex> groupMembers = new ArrayList<ManagementVertex>();

	/**
	 * The list of group edges which originate from this group vertex.
	 */
	private final List<ManagementGroupEdge> forwardEdges = new ArrayList<ManagementGroupEdge>();

	/**
	 * The list of group edges which arrive at this group vertex.
	 */
	private final List<ManagementGroupEdge> backwardEdges = new ArrayList<ManagementGroupEdge>();

	/**
	 * Constructs a new management group vertex.
	 * 
	 * @param stage
	 *        the stage this group vertex belongs to
	 * @param name
	 *        the name of the new management group vertex
	 */
	public ManagementGroupVertex(final ManagementStage stage, final String name) {
		this(stage, new ManagementGroupVertexID(), name);
	}

	/**
	 * Constructs a new management group vertex.
	 * 
	 * @param stage
	 *        the stage this group vertex belongs to
	 * @param id
	 *        the ID of the management group vertex
	 * @param name
	 *        the name of the new management group vertex
	 */
	public ManagementGroupVertex(final ManagementStage stage, final ManagementGroupVertexID id, final String name) {

		this.stage = stage;
		this.id = id;
		this.name = name;

		stage.addGroupVertex(this);
	}

	/**
	 * Returns the ID of this management group vertex.
	 * 
	 * @return the ID of this management group vertex
	 */
	public ManagementGroupVertexID getID() {
		return this.id;
	}

	/**
	 * Returns the name of this management group vertex.
	 * 
	 * @return the anme of this management group vertex, possibly <code>null</code>
	 */
	public String getName() {
		return name;
	}

	/**
	 * Inserts a new edge starting at this group vertex at the given index.
	 * 
	 * @param edge
	 *        the edge to be added
	 * @param index
	 *        the index at which the edge shall be added
	 */
	void insertForwardEdge(final ManagementGroupEdge edge, final int index) {

		while (index >= this.forwardEdges.size()) {
			this.forwardEdges.add(null);
		}

		this.forwardEdges.set(index, edge);
	}

	/**
	 * Inserts a new edge arriving at this group vertex at the given index.
	 * 
	 * @param edge
	 *        the edge to be added
	 * @param index
	 *        the index at which the edge shall be added
	 */
	void insertBackwardEdge(final ManagementGroupEdge edge, final int index) {

		while (index >= this.backwardEdges.size()) {
			this.backwardEdges.add(null);
		}

		this.backwardEdges.set(index, edge);
	}

	/**
	 * Returns the number of edges originating at this group vertex.
	 * 
	 * @return the number of edges originating at this group vertex
	 */
	public int getNumberOfForwardEdges() {
		return this.forwardEdges.size();
	}

	/**
	 * Returns the number of edges arriving at this group vertex.
	 * 
	 * @return the number of edges arriving at this group vertex
	 */
	public int getNumberOfBackwardEdges() {
		return this.backwardEdges.size();
	}

	/**
	 * Returns the group edge which leaves this group vertex at the given index.
	 * 
	 * @param index
	 *        the index of the group edge
	 * @return the group edge which leaves this group vertex at the given index or <code>null</code> if no such group
	 *         edge exists
	 */
	public ManagementGroupEdge getForwardEdge(final int index) {

		if (index < this.forwardEdges.size()) {
			return this.forwardEdges.get(index);
		}

		return null;
	}

	/**
	 * Returns the group edge which arrives at this group vertex at the given index.
	 * 
	 * @param index
	 *        the index of the group edge
	 * @return the group edge which arrives at this group vertex at the given index or <code>null</code> if no such
	 *         group edge exists
	 */
	public ManagementGroupEdge getBackwardEdge(final int index) {

		if (index < this.backwardEdges.size()) {
			return this.backwardEdges.get(index);
		}

		return null;
	}

	/**
	 * Adds a {@link ManagementVertex} to this group vertex.
	 * 
	 * @param vertex
	 *        the vertex to be added
	 */
	void addGroupMember(final ManagementVertex vertex) {

		while (this.groupMembers.size() <= vertex.getIndexInGroup()) {
			this.groupMembers.add(null);
		}

		this.groupMembers.set(vertex.getIndexInGroup(), vertex);

		this.getGraph().addVertex(vertex.getID(), vertex);
	}

	/**
	 * Adds all management vertices which are included in this group vertex to the given list.
	 * 
	 * @param vertices
	 *        the list to which the vertices shall be added
	 */
	void collectVertices(final List<ManagementVertex> vertices) {

		final Iterator<ManagementVertex> it = this.groupMembers.iterator();
		while (it.hasNext()) {
			vertices.add(it.next());
		}
	}

	/**
	 * Returns the stage this management group vertex belongs to.
	 * 
	 * @return the stage this management group vertex belongs to
	 */
	public ManagementStage getStage() {
		return this.stage;
	}

	/**
	 * Returns the management graph this group vertex is part of.
	 * 
	 * @return the management graph this group vertex is part of
	 */
	public ManagementGraph getGraph() {
		return this.stage.getGraph();
	}

	/**
	 * Returns the number of management vertices included in this group vertex.
	 * 
	 * @return the number of management vertices included in this group vertex
	 */
	public int getNumberOfGroupMembers() {
		return this.groupMembers.size();
	}

	/**
	 * Returns the management vertex with the given index.
	 * 
	 * @param index
	 *        the index of the management vertex to be returned
	 * @return the management vertex with the given index or <code>null</code> if no such vertex exists
	 */
	public ManagementVertex getGroupMember(final int index) {

		if (index < this.groupMembers.size()) {
			return this.groupMembers.get(index);
		}

		return null;
	}
	
	/**
	 * Returns alle management vertices containted in this group
	 * 
	 * @return the management vertex with the given index or <code>null</code> if no such vertex exists
	 */
	public List<ManagementVertex> getGroupMembers() {

		return this.groupMembers;
	}

	/**
	 * Checks if this vertex is an input vertex in its stage, i.e. has either no
	 * incoming connections or only incoming connections to group vertices in a lower stage.
	 * 
	 * @return <code>true</code> if this vertex is an input vertex, <code>false</code> otherwise
	 */
	public boolean isInputVertex() {

		if (this.backwardEdges.size() == 0) {
			return true;
		}

		final Iterator<ManagementGroupEdge> it = this.backwardEdges.iterator();
		while (it.hasNext()) {
			if (it.next().getSource().getStageNumber() == this.getStageNumber()) {
				return false;
			}
		}

		return true;
	}

	/**
	 * Checks if this vertex is an output vertex in its stage, i.e. has either no
	 * outgoing connections or only outgoing connections to group vertices in a higher stage.
	 * 
	 * @return <code>true</code> if this vertex is an output vertex, <code>false</code> otherwise
	 */
	public boolean isOutputVertex() {

		if (this.forwardEdges.size() == 0) {
			return true;
		}

		final Iterator<ManagementGroupEdge> it = this.forwardEdges.iterator();
		while (it.hasNext()) {
			if (it.next().getTarget().getStageNumber() == this.getStageNumber()) {
				return false;
			}
		}

		return true;
	}

	/**
	 * Returns the number of the management stage this group vertex belongs to.
	 * 
	 * @return the number of the management stage this group vertex belongs to
	 */
	public int getStageNumber() {

		return this.stage.getStageNumber();
	}


	@Override
	public void read(final DataInput in) throws IOException {

		int numberOfForwardLinks = in.readInt();
		for (int i = 0; i < numberOfForwardLinks; i++) {
			final ManagementGroupVertexID targetGroupVertexID = new ManagementGroupVertexID();
			targetGroupVertexID.read(in);
			final ManagementGroupVertex targetGroupVertex = getGraph().getGroupVertexByID(targetGroupVertexID);
			final int sourceIndex = in.readInt();
			final int targetIndex = in.readInt();
			final ChannelType channelType = EnumUtils.readEnum(in, ChannelType.class);
			new ManagementGroupEdge(this, sourceIndex, targetGroupVertex, targetIndex, channelType);
		}

	}


	@Override
	public void write(final DataOutput out) throws IOException {

		// Write the number of forward links
		out.writeInt(this.forwardEdges.size());
		final Iterator<ManagementGroupEdge> it = this.forwardEdges.iterator();
		while (it.hasNext()) {
			final ManagementGroupEdge groupEdge = it.next();
			groupEdge.getTarget().getID().write(out);
			out.writeInt(groupEdge.getSourceIndex());
			out.writeInt(groupEdge.getTargetIndex());
			EnumUtils.writeEnum(out, groupEdge.getChannelType());
		}
	}

	/**
	 * Returns the list of successors of this group vertex. A successor is a group vertex which can be reached via a
	 * group edge originating at this group vertex.
	 * 
	 * @return the list of successors of this group vertex.
	 */
	public List<ManagementGroupVertex> getSuccessors() {

		final List<ManagementGroupVertex> successors = new ArrayList<ManagementGroupVertex>();

		for (ManagementGroupEdge edge : this.forwardEdges) {
			successors.add(edge.getTarget());
		}

		return successors;
	}

	/**
	 * Returns the list of predecessors of this group vertex. A predecessors is a group vertex which can be reached via
	 * a group edge arriving at this group vertex.
	 * 
	 * @return the list of predecessors of this group vertex.
	 */
	public List<ManagementGroupVertex> getPredecessors() {

		final List<ManagementGroupVertex> predecessors = new ArrayList<ManagementGroupVertex>();

		for (ManagementGroupEdge edge : this.backwardEdges) {
			predecessors.add(edge.getSource());
		}

		return predecessors;
	}
	
	@Override
	public String toString() {
		return String.format("ManagementGroupVertex(%s)", getName());
	}
	
	/**
	 * Returns Json representation of this ManagementGroupVertex
	 * 
	 * @return
	 */
	public String toJson() {
		StringBuilder json = new StringBuilder("");
		
		json.append("{");
		json.append("\"groupvertexid\": \"" + this.getID() + "\",");
		json.append("\"groupvertexname\": \"" + StringUtils.escapeHtml(this.getName()) + "\",");
		json.append("\"numberofgroupmembers\": " + this.getNumberOfGroupMembers() + ",");
		json.append("\"groupmembers\": [");
		
		// Count state status of group members
		Map<ExecutionState, Integer> stateCounts = new HashMap<ExecutionState, Integer>();
		
		// initialize with 0
		for (ExecutionState state : ExecutionState.values()) {
			stateCounts.put(state, new Integer(0));
		}
		
		for(int j = 0; j < this.getNumberOfGroupMembers(); j++) {
			ManagementVertex vertex = this.getGroupMember(j);
			
			json.append(vertex.toJson());
			
			// print delimiter
			if(j != this.getNumberOfGroupMembers() - 1) {
				json.append(",");
			}
			
			// Increment state status count
			Integer count =  stateCounts.get(vertex.getExecutionState()) + new Integer(1);
			stateCounts.put(vertex.getExecutionState(), count);
		}
		json.append("],");
		json.append("\"backwardEdges\": [");
		
		for(int edgeIndex = 0; edgeIndex < this.getNumberOfBackwardEdges(); edgeIndex++) {
			ManagementGroupEdge edge = this.getBackwardEdge(edgeIndex);
			
			json.append("{");
			json.append("\"groupvertexid\": \"" + edge.getSource().getID() + "\",");
			json.append("\"groupvertexname\": \"" +  StringUtils.escapeHtml(edge.getSource().getName()) + "\",");
			json.append("\"channelType\": \"" +  edge.getChannelType() + "\"");
			json.append("}");
			
			// print delimiter
			if(edgeIndex != this.getNumberOfBackwardEdges() - 1) {
				json.append(",");
			}
		}
		json.append("]");
		
		// list number of members for each status
		for (Map.Entry<ExecutionState, Integer> stateCount : stateCounts.entrySet()) {
			json.append(",\""+stateCount.getKey()+"\": " + stateCount.getValue());
		}
		
		json.append("}");
		
		return json.toString();
	}
}
