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
import java.util.Iterator;
import java.util.List;

import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.util.EnumUtils;

public class ManagementGroupVertex implements IOReadableWritable {

	private final ManagementGroupVertexID id;

	private final String name;

	private final ManagementStage stage;

	private final List<ManagementVertex> groupMembers = new ArrayList<ManagementVertex>();

	private final List<ManagementGroupEdge> forwardEdges = new ArrayList<ManagementGroupEdge>();

	private final List<ManagementGroupEdge> backwardEdges = new ArrayList<ManagementGroupEdge>();

	private Object attachment = null;

	public ManagementGroupVertex(ManagementStage stage, String name) {
		this(stage, new ManagementGroupVertexID(), name);
	}

	public ManagementGroupVertex(ManagementStage stage, ManagementGroupVertexID id, String name) {

		this.stage = stage;
		this.id = id;
		this.name = name;

		stage.addGroupVertex(this);
	}

	public ManagementGroupVertexID getID() {
		return this.id;
	}

	public String getName() {
		return name;
	}

	void insertForwardEdge(ManagementGroupEdge edge, int index) {

		while (index >= this.forwardEdges.size()) {
			this.forwardEdges.add(null);
		}

		this.forwardEdges.set(index, edge);
	}

	void insertBackwardEdge(ManagementGroupEdge edge, int index) {

		while (index >= this.backwardEdges.size()) {
			this.backwardEdges.add(null);
		}

		this.backwardEdges.set(index, edge);
	}

	public int getNumberOfForwardEdges() {
		return this.forwardEdges.size();
	}

	public int getNumberOfBackwardEdges() {
		return this.backwardEdges.size();
	}

	public ManagementGroupEdge getForwardEdge(int index) {

		if (index < this.forwardEdges.size()) {
			return this.forwardEdges.get(index);
		}

		return null;
	}

	public ManagementGroupEdge getBackwardEdge(int index) {

		if (index < this.backwardEdges.size()) {
			return this.backwardEdges.get(index);
		}

		return null;
	}

	void addGroupMember(ManagementVertex vertex) {

		while (this.groupMembers.size() <= vertex.getIndexInGroup()) {
			this.groupMembers.add(null);
		}

		this.groupMembers.set(vertex.getIndexInGroup(), vertex);

		this.getGraph().addVertex(vertex.getID(), vertex);
	}

	void collectVertices(List<ManagementVertex> vertices) {

		final Iterator<ManagementVertex> it = this.groupMembers.iterator();
		while (it.hasNext()) {
			vertices.add(it.next());
		}
	}

	public ManagementStage getStage() {
		return this.stage;
	}

	public ManagementGraph getGraph() {
		return this.stage.getGraph();
	}

	public int getNumberOfGroupMembers() {
		return this.groupMembers.size();
	}

	public ManagementVertex getGroupMember(int index) {

		if (index < this.groupMembers.size()) {
			return this.groupMembers.get(index);
		}

		return null;
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

	public int getStageNumber() {

		return this.stage.getStageNumber();
	}

	public int getIndexOf(ManagementVertex managementVertex) {

		return this.groupMembers.indexOf(managementVertex);
	}

	@Override
	public void read(DataInput in) throws IOException {

		int numberOfForwardLinks = in.readInt();
		for (int i = 0; i < numberOfForwardLinks; i++) {
			final ManagementGroupVertexID targetGroupVertexID = new ManagementGroupVertexID();
			targetGroupVertexID.read(in);
			final ManagementGroupVertex targetGroupVertex = getGraph().getGroupVertexByID(targetGroupVertexID);
			final int sourceIndex = in.readInt();
			final int targetIndex = in.readInt();
			final ChannelType channelType = EnumUtils.readEnum(in, ChannelType.class);
			final CompressionLevel compressionLevel = EnumUtils.readEnum(in, CompressionLevel.class);
			new ManagementGroupEdge(this, sourceIndex, targetGroupVertex, targetIndex, channelType, compressionLevel);
		}

	}

	@Override
	public void write(DataOutput out) throws IOException {

		// Write the number of forward links
		out.writeInt(this.forwardEdges.size());
		final Iterator<ManagementGroupEdge> it = this.forwardEdges.iterator();
		while (it.hasNext()) {
			final ManagementGroupEdge groupEdge = it.next();
			groupEdge.getTarget().getID().write(out);
			out.writeInt(groupEdge.getSourceIndex());
			out.writeInt(groupEdge.getTargetIndex());
			EnumUtils.writeEnum(out, groupEdge.getChannelType());
			EnumUtils.writeEnum(out, groupEdge.getCompressionLevel());
		}
	}

	public void setAttachment(Object attachment) {
		this.attachment = attachment;
	}

	public Object getAttachment() {
		return this.attachment;
	}

	public List<ManagementGroupVertex> getSuccessors() {

		final List<ManagementGroupVertex> successors = new ArrayList<ManagementGroupVertex>();

		for (ManagementGroupEdge edge : this.forwardEdges) {
			successors.add(edge.getTarget());
		}

		return successors;
	}

	public List<ManagementGroupVertex> getPredecessors() {

		final List<ManagementGroupVertex> predecessors = new ArrayList<ManagementGroupVertex>();

		for (ManagementGroupEdge edge : this.backwardEdges) {
			predecessors.add(edge.getSource());
		}

		return predecessors;
	}
}
