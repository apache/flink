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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.types.StringRecord;
import eu.stratosphere.nephele.util.EnumUtils;

public class ManagementGraph implements IOReadableWritable {

	private final List<ManagementStage> stages = new ArrayList<ManagementStage>();

	private final JobID jobID;

	private Object attachment = null;

	private final Map<ManagementVertexID, ManagementVertex> vertices = new HashMap<ManagementVertexID, ManagementVertex>();

	private final Map<ManagementGroupVertexID, ManagementGroupVertex> groupVertices = new HashMap<ManagementGroupVertexID, ManagementGroupVertex>();

	public ManagementGraph(JobID jobID) {
		this.jobID = jobID;
	}

	public ManagementGraph() {
		this.jobID = new JobID();
	}

	void addStage(ManagementStage mangementStage) {
		this.stages.add(mangementStage);
	}

	public JobID getJobID() {
		return this.jobID;
	}

	void addVertex(ManagementVertexID id, ManagementVertex vertex) {
		this.vertices.put(id, vertex);
	}

	public ManagementVertex getVertexByID(ManagementVertexID id) {
		return this.vertices.get(id);
	}

	public ManagementGroupVertex getGroupVertexByID(ManagementGroupVertexID id) {
		return this.groupVertices.get(id);
	}

	void addGroupVertex(ManagementGroupVertexID id, ManagementGroupVertex groupVertex) {
		this.groupVertices.put(id, groupVertex);
	}

	public int getNumberOfStages() {
		return this.stages.size();
	}

	public ManagementStage getStage(int index) {

		if (index < this.stages.size()) {
			return this.stages.get(index);
		}

		return null;
	}

	public int getNumberOfInputGroupVertices(int stage) {

		if (stage >= this.stages.size()) {
			return 0;
		}

		return this.stages.get(stage).getNumberOfInputGroupVertices();
	}

	public int getNumberOfOutputGroupVertices(int stage) {

		if (stage >= this.stages.size()) {
			return 0;
		}

		return this.stages.get(stage).getNumberOfOutputGroupVertices();
	}

	public ManagementGroupVertex getInputGroupVertex(int stage, int index) {

		if (stage >= this.stages.size()) {
			return null;
		}

		return this.stages.get(stage).getInputGroupVertex(index);
	}

	public ManagementGroupVertex getOutputGroupVertex(int stage, int index) {

		if (stage >= this.stages.size()) {
			return null;
		}

		return this.stages.get(stage).getOutputGroupVertex(index);
	}

	/**
	 * Returns the number of input vertices for the given stage.
	 * 
	 * @param stage
	 *        the index of the management stage
	 * @return the number of input vertices for the given stage
	 */
	public int getNumberOfInputVertices(int stage) {

		if (stage >= this.stages.size()) {
			return 0;
		}

		return this.stages.get(stage).getNumberOfInputManagementVertices();
	}

	/**
	 * Returns the number of output vertices for the given stage.
	 * 
	 * @param stage
	 *        the index of the management stage
	 * @return the number of input vertices for the given stage
	 */
	public int getNumberOfOutputVertices(int stage) {

		if (stage >= this.stages.size()) {
			return 0;
		}

		return this.stages.get(stage).getNumberOfInputManagementVertices();
	}

	/**
	 * Returns the input vertex with the specified index for the given stage
	 * 
	 * @param stage
	 *        the index of the stage
	 * @param index
	 *        the index of the input vertex to return
	 * @return the input vertex with the specified index or <code>null</code> if no input vertex with such an index
	 *         exists in that stage
	 */
	public ManagementVertex getInputVertex(int stage, int index) {

		if (stage >= this.stages.size()) {
			return null;
		}

		return this.stages.get(stage).getInputManagementVertex(index);
	}

	/**
	 * Returns the output vertex with the specified index for the given stage.
	 * 
	 * @param stage
	 *        the index of the stage
	 * @param index
	 *        the index of the output vertex to return
	 * @return the output vertex with the specified index or <code>null</code> if no output vertex with such an index
	 *         exists in that stage
	 */
	public ManagementVertex getOutputVertex(int stage, int index) {

		if (stage >= this.stages.size()) {
			return null;
		}

		return this.stages.get(stage).getOutputManagementVertex(index);
	}

	public void setAttachment(Object attachment) {
		this.attachment = attachment;
	}

	public Object getAttachment() {
		return this.attachment;
	}

	public List<ManagementGroupVertex> getGroupVerticesInTopologicalOrder() {

		final List<ManagementGroupVertex> topologicalSort = new ArrayList<ManagementGroupVertex>();
		final Deque<ManagementGroupVertex> noIncomingEdges = new ArrayDeque<ManagementGroupVertex>();
		final Map<ManagementGroupVertex, Integer> indegrees = new HashMap<ManagementGroupVertex, Integer>();

		final Iterator<ManagementGroupVertex> it = new ManagementGroupVertexIterator(this, true, -1);
		while (it.hasNext()) {
			final ManagementGroupVertex groupVertex = it.next();
			indegrees.put(groupVertex, new Integer(groupVertex.getNumberOfBackwardEdges()));
			if (groupVertex.getNumberOfBackwardEdges() == 0) {
				noIncomingEdges.add(groupVertex);
			}
		}

		while (!noIncomingEdges.isEmpty()) {

			final ManagementGroupVertex groupVertex = noIncomingEdges.removeFirst();
			topologicalSort.add(groupVertex);
			// Decrease indegree of connected vertices
			for (int i = 0; i < groupVertex.getNumberOfForwardEdges(); i++) {
				final ManagementGroupVertex targetVertex = groupVertex.getForwardEdge(i).getTarget();
				Integer indegree = indegrees.get(targetVertex);
				indegree = new Integer(indegree.intValue() - 1);
				indegrees.put(targetVertex, indegree);
				if (indegree.intValue() == 0) {
					noIncomingEdges.add(targetVertex);
				}

			}
		}

		return topologicalSort;
	}

	public List<ManagementGroupVertex> getGroupVerticesInReverseTopologicalOrder() {

		final List<ManagementGroupVertex> reverseTopologicalSort = new ArrayList<ManagementGroupVertex>();
		final Deque<ManagementGroupVertex> noOutgoingEdges = new ArrayDeque<ManagementGroupVertex>();
		final Map<ManagementGroupVertex, Integer> outdegrees = new HashMap<ManagementGroupVertex, Integer>();

		final Iterator<ManagementGroupVertex> it = new ManagementGroupVertexIterator(this, false, -1);
		while (it.hasNext()) {
			final ManagementGroupVertex groupVertex = it.next();
			outdegrees.put(groupVertex, new Integer(groupVertex.getNumberOfForwardEdges()));
			if (groupVertex.getNumberOfForwardEdges() == 0) {
				noOutgoingEdges.add(groupVertex);
			}
		}

		while (!noOutgoingEdges.isEmpty()) {

			final ManagementGroupVertex groupVertex = noOutgoingEdges.removeFirst();
			reverseTopologicalSort.add(groupVertex);
			// Decrease indegree of connected vertices
			for (int i = 0; i < groupVertex.getNumberOfBackwardEdges(); i++) {
				final ManagementGroupVertex sourceVertex = groupVertex.getBackwardEdge(i).getSource();
				Integer outdegree = outdegrees.get(sourceVertex);
				outdegree = new Integer(outdegree.intValue() - 1);
				outdegrees.put(sourceVertex, outdegree);
				if (outdegree.intValue() == 0) {
					noOutgoingEdges.add(sourceVertex);
				}

			}
		}

		return reverseTopologicalSort;
	}

	/**
	 * Returns all list of all possible paths though the management graph, starting
	 * at the input vertices. Each path is represented by a list of vertices.
	 * 
	 * @return a list of all possible paths through the management graph
	 */
	/*
	 * public List<List<ManagementGroupVertex>> getListOfAllPathsThroughGraph() {
	 * final List<List<ManagementGroupVertex>> listOfAllPaths = new ArrayList<List<ManagementGroupVertex>>();
	 * final int numberOfInputGroupVertices = getNumberOfInputGroupVertices(0);
	 * for(int i = 0; i < numberOfInputGroupVertices; i++) {
	 * final ManagementGroupVertex startVertex = getInputGroupVertex(0, i);
	 * final List<ManagementGroupVertex> currentPath = new ArrayList<ManagementGroupVertex>();
	 * currentPath.add(startVertex);
	 * constructListOfAllPaths(listOfAllPaths, currentPath);
	 * }
	 * return listOfAllPaths;
	 * }
	 */

	/*
	 * private static void constructListOfAllPaths(List<List<ManagementGroupVertex>> listOfAllPaths,
	 * List<ManagementGroupVertex> currentPath) {
	 * final ManagementGroupVertex currentVertex = currentPath.get(currentPath.size()-1);
	 * if(currentVertex.getNumberOfForwardEdges() == 0) {
	 * //Output vertex, add current path to list of all paths
	 * listOfAllPaths.add(currentPath);
	 * } else {
	 * for(int i = 0; i < currentVertex.getNumberOfForwardEdges(); i++) {
	 * final ManagementGroupVertex targetVertex = currentVertex.getForwardEdge(i).getTarget();
	 * if(i < (currentVertex.getNumberOfForwardEdges()-1)) {
	 * //Duplicate list
	 * List<ManagementGroupVertex> newPath = new ArrayList<ManagementGroupVertex>();
	 * newPath.addAll(currentPath);
	 * newPath.add(targetVertex);
	 * constructListOfAllPaths(listOfAllPaths, newPath);
	 * } else {
	 * currentPath.add(targetVertex);
	 * constructListOfAllPaths(listOfAllPaths, currentPath);
	 * }
	 * }
	 * }
	 * }
	 */

	@Override
	public void read(DataInput in) throws IOException {

		// Read job ID
		this.jobID.read(in);

		// Recreate stages
		final int numberOfStages = in.readInt();
		for (int i = 0; i < numberOfStages; i++) {
			new ManagementStage(this, i);
		}

		// Read number of group vertices and their corresponding IDs
		final int numberOfGroupVertices = in.readInt();
		for (int i = 0; i < numberOfGroupVertices; i++) {

			final ManagementGroupVertexID groupVertexID = new ManagementGroupVertexID();
			groupVertexID.read(in);
			final ManagementStage stage = this.stages.get(in.readInt());
			final String groupVertexName = StringRecord.readString(in);
			new ManagementGroupVertex(stage, groupVertexID, groupVertexName);
		}

		for (int i = 0; i < numberOfGroupVertices; i++) {
			final ManagementGroupVertexID groupVertexID = new ManagementGroupVertexID();
			groupVertexID.read(in);
			final ManagementGroupVertex groupVertex = this.groupVertices.get(groupVertexID);
			groupVertex.read(in);
		}

		// Read the management vertices
		int numberOfVertices = in.readInt();
		for (int i = 0; i < numberOfVertices; i++) {

			final ManagementVertexID vertexID = new ManagementVertexID();
			vertexID.read(in);
			final ManagementGroupVertexID groupVertexID = new ManagementGroupVertexID();
			groupVertexID.read(in);
			final ManagementGroupVertex groupVertex = this.getGroupVertexByID(groupVertexID);
			final String instanceName = StringRecord.readString(in);
			final String instanceType = StringRecord.readString(in);
			final int indexInGroup = in.readInt();
			final ManagementVertex vertex = new ManagementVertex(groupVertex, vertexID, instanceName, instanceType,
				indexInGroup);
			vertex.read(in);
		}

		for (int i = 0; i < numberOfVertices; i++) {

			final ManagementVertexID sourceID = new ManagementVertexID();
			sourceID.read(in);
			final ManagementVertex sourceVertex = getVertexByID(sourceID);
			for (int j = 0; j < sourceVertex.getNumberOfOutputGates(); j++) {
				final ManagementGate sourceGate = sourceVertex.getOutputGate(j);
				int numberOfForwardEdges = in.readInt();
				for (int k = 0; k < numberOfForwardEdges; k++) {
					final ManagementVertexID targetID = new ManagementVertexID();
					targetID.read(in);
					final ManagementVertex targetVertex = getVertexByID(targetID);
					final int targetGateIndex = in.readInt();
					final ManagementGate targetGate = targetVertex.getInputGate(targetGateIndex);

					final int sourceIndex = in.readInt();
					final int targetIndex = in.readInt();

					final ChannelType channelType = EnumUtils.readEnum(in, ChannelType.class);
					final CompressionLevel compressionLevel = EnumUtils.readEnum(in, CompressionLevel.class);
					new ManagementEdge(sourceGate, sourceIndex, targetGate, targetIndex, channelType, compressionLevel);
				}

			}
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {

		// Write job ID
		this.jobID.write(out);

		// Write number of stages
		out.writeInt(this.stages.size());

		// Write number of group vertices and their corresponding IDs
		out.writeInt(this.groupVertices.size());

		Iterator<ManagementGroupVertex> it = groupVertices.values().iterator();
		while (it.hasNext()) {

			final ManagementGroupVertex groupVertex = it.next();
			groupVertex.getID().write(out);
			out.writeInt(groupVertex.getStage().getStageNumber());
			StringRecord.writeString(out, groupVertex.getName());
		}

		it = groupVertices.values().iterator();
		while (it.hasNext()) {

			final ManagementGroupVertex groupVertex = it.next();
			groupVertex.getID().write(out);
			groupVertex.write(out);
		}

		// Write out the management vertices and their corresponding IDs
		out.writeInt(this.vertices.size());
		Iterator<ManagementVertex> it2 = vertices.values().iterator();
		while (it2.hasNext()) {

			final ManagementVertex managementVertex = it2.next();
			managementVertex.getID().write(out);
			managementVertex.getGroupVertex().getID().write(out);
			StringRecord.writeString(out, managementVertex.getInstanceName());
			StringRecord.writeString(out, managementVertex.getInstanceType());
			out.writeInt(managementVertex.getIndexInGroup());
			managementVertex.write(out);
		}

		// Finally, serialize the edges between the management vertices
		it2 = vertices.values().iterator();
		while (it2.hasNext()) {

			final ManagementVertex managementVertex = it2.next();
			managementVertex.getID().write(out);
			for (int i = 0; i < managementVertex.getNumberOfOutputGates(); i++) {
				final ManagementGate outputGate = managementVertex.getOutputGate(i);
				out.writeInt(outputGate.getNumberOfForwardEdges());
				for (int j = 0; j < outputGate.getNumberOfForwardEdges(); j++) {
					final ManagementEdge edge = outputGate.getForwardEdge(j);
					// This identifies the target gate
					edge.getTarget().getVertex().getID().write(out);
					out.writeInt(edge.getTarget().getIndex());

					out.writeInt(edge.getSourceIndex());
					out.writeInt(edge.getTargetIndex());

					EnumUtils.writeEnum(out, edge.getChannelType());
					EnumUtils.writeEnum(out, edge.getCompressionLevel());
				}
			}
		}
	}
}
