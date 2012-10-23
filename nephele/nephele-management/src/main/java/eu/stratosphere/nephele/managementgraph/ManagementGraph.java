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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.util.EnumUtils;

/**
 * A management graph is structurally equal to the graph Nephele uses internally for scheduling jobs. Management graphs
 * are intended to provide more fine-grained information about a job at runtime than available through the regular
 * client interface, however, without exposing Nephele's internal scheduling data structures.
 * <p>
 * This class is not thread-safe.
 * 
 * @author warneke
 */
public final class ManagementGraph extends ManagementAttachment implements KryoSerializable {

	/**
	 * List of stages the graph is divided into.
	 */
	private final List<ManagementStage> stages = new ArrayList<ManagementStage>();

	/**
	 * The ID of the job this graph describes.
	 */
	private JobID jobID;

	/**
	 * A map of vertices this graph consists of.
	 */
	private final Map<ManagementVertexID, ManagementVertex> vertices = new HashMap<ManagementVertexID, ManagementVertex>();

	/**
	 * A map of group vertices this graph consists of.
	 */
	private final Map<ManagementGroupVertexID, ManagementGroupVertex> groupVertices = new HashMap<ManagementGroupVertexID, ManagementGroupVertex>();

	/**
	 * Constructs a new management graph with the given job ID.
	 * 
	 * @param jobID
	 *        the job ID of the graph.
	 */
	public ManagementGraph(final JobID jobID) {
		this.jobID = jobID;
	}

	/**
	 * Constructs a new management graph with a random job ID.
	 */
	public ManagementGraph() {
		this.jobID = JobID.generate();
	}

	/**
	 * Adds a new management stage to the graph.
	 * 
	 * @param mangementStage
	 *        the management stage to be added.
	 */
	void addStage(final ManagementStage mangementStage) {

		this.stages.add(mangementStage);
	}

	/**
	 * Returns the ID of the job this graph describes.
	 * 
	 * @return the ID of the job this graph describes
	 */
	public JobID getJobID() {
		return this.jobID;
	}

	/**
	 * Adds the given vertex to the graph's internal vertex map.
	 * 
	 * @param id
	 *        the ID of the vertex to be added
	 * @param vertex
	 *        the vertex to be added
	 */
	void addVertex(final ManagementVertexID id, final ManagementVertex vertex) {

		this.vertices.put(id, vertex);
	}

	/**
	 * Returns the vertex with the given ID from the graph's internal vertex map.
	 * 
	 * @param id
	 *        the ID of the vertex to be returned
	 * @return the vertex with the given ID or <code>null</code> if no such vertex exists
	 */
	public ManagementVertex getVertexByID(final ManagementVertexID id) {

		return this.vertices.get(id);
	}

	/**
	 * Returns the group vertex with the given ID from the graph's internal group vertex map.
	 * 
	 * @param id
	 *        the ID of the group vertex to be returned
	 * @return the group vertex with the given ID or <code>null</code> if no such group vertex exists
	 */
	public ManagementGroupVertex getGroupVertexByID(final ManagementGroupVertexID id) {

		return this.groupVertices.get(id);
	}

	/**
	 * Adds the given group vertex to the graph's internal group vertex map.
	 * 
	 * @param id
	 *        the ID of the group vertex to be added
	 * @param groupVertex
	 *        the group vertex to be added
	 */
	void addGroupVertex(final ManagementGroupVertexID id, final ManagementGroupVertex groupVertex) {

		this.groupVertices.put(id, groupVertex);
	}

	/**
	 * Returns the number of stages in this management graph.
	 * 
	 * @return the number of stages in this management graph
	 */
	public int getNumberOfStages() {

		return this.stages.size();
	}

	/**
	 * Returns the management stage with the given index.
	 * 
	 * @param index
	 *        the index of the management stage to be returned
	 * @return the management stage with the given index or <code>null</code> if no such management stage exists
	 */
	public ManagementStage getStage(final int index) {

		if (index >= 0 && index < this.stages.size()) {
			return this.stages.get(index);
		}

		return null;
	}

	/**
	 * Returns the number of input group vertices in the management stage with the given index.
	 * 
	 * @param stage
	 *        the index to the management stage
	 * @return the number of input group vertices in this stage, possibly 0.
	 */
	public int getNumberOfInputGroupVertices(final int stage) {

		if (stage < 0 || stage >= this.stages.size()) {
			return 0;
		}

		return this.stages.get(stage).getNumberOfInputGroupVertices();
	}

	/**
	 * Returns the number of output group vertices in the management stage with the given index.
	 * 
	 * @param stage
	 *        the index to the management stage
	 * @return the number of output group vertices in this stage, possibly 0.
	 */
	public int getNumberOfOutputGroupVertices(final int stage) {

		if (stage >= this.stages.size()) {
			return 0;
		}

		return this.stages.get(stage).getNumberOfOutputGroupVertices();
	}

	/**
	 * Returns the input group vertex at the given index in the given stage.
	 * 
	 * @param stage
	 *        the index to the management stage
	 * @param index
	 *        the index to the input group vertex
	 * @return the input group vertex at the given index in the given stage or <code>null</code> if either the stage
	 *         does not exists or the given index is invalid in this stage
	 */
	public ManagementGroupVertex getInputGroupVertex(final int stage, final int index) {

		if (stage >= this.stages.size()) {
			return null;
		}

		return this.stages.get(stage).getInputGroupVertex(index);
	}

	/**
	 * Returns the output group vertex at the given index in the given stage.
	 * 
	 * @param stage
	 *        the index to the management stage
	 * @param index
	 *        the index to the output group vertex
	 * @return the output group vertex at the given index in the given stage or <code>null</code> if either the stage
	 *         does not exists or the given index is invalid in this stage
	 */
	public ManagementGroupVertex getOutputGroupVertex(final int stage, final int index) {

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
	public int getNumberOfInputVertices(final int stage) {

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
	public int getNumberOfOutputVertices(final int stage) {

		if (stage >= this.stages.size()) {
			return 0;
		}

		return this.stages.get(stage).getNumberOfInputManagementVertices();
	}

	/**
	 * Returns the input vertex with the specified index for the given stage.
	 * 
	 * @param stage
	 *        the index of the stage
	 * @param index
	 *        the index of the input vertex to return
	 * @return the input vertex with the specified index or <code>null</code> if no input vertex with such an index
	 *         exists in that stage
	 */
	public ManagementVertex getInputVertex(final int stage, final int index) {

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
	public ManagementVertex getOutputVertex(final int stage, final int index) {

		if (stage >= this.stages.size()) {
			return null;
		}

		return this.stages.get(stage).getOutputManagementVertex(index);
	}

	/**
	 * Returns an unmodifiable collection of all group vertices with no guarantees on their order.
	 * 
	 * @return an unmodifiable collection of all group vertices with no guarantees on their order
	 */
	public Collection<ManagementGroupVertex> getGroupVertices() {
		return Collections.unmodifiableCollection(groupVertices.values());
	}

	/**
	 * Returns a list of group vertices sorted in topological order.
	 * 
	 * @return a list of group vertices sorted in topological order
	 */
	public List<ManagementGroupVertex> getGroupVerticesInTopologicalOrder() {

		final List<ManagementGroupVertex> topologicalSort = new ArrayList<ManagementGroupVertex>();
		final Deque<ManagementGroupVertex> noIncomingEdges = new ArrayDeque<ManagementGroupVertex>();
		final Map<ManagementGroupVertex, Integer> indegrees = new HashMap<ManagementGroupVertex, Integer>();

		final Iterator<ManagementGroupVertex> it = new ManagementGroupVertexIterator(this, true, -1);
		while (it.hasNext()) {
			final ManagementGroupVertex groupVertex = it.next();
			indegrees.put(groupVertex, Integer.valueOf(groupVertex.getNumberOfBackwardEdges()));
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
				indegree = Integer.valueOf(indegree.intValue() - 1);
				indegrees.put(targetVertex, indegree);
				if (indegree.intValue() == 0) {
					noIncomingEdges.add(targetVertex);
				}

			}
		}

		return topologicalSort;
	}

	/**
	 * Returns a list of group vertices sorted in reverse topological order.
	 * 
	 * @return a list of group vertices sorted in reverse topological order
	 */
	public List<ManagementGroupVertex> getGroupVerticesInReverseTopologicalOrder() {

		final List<ManagementGroupVertex> reverseTopologicalSort = new ArrayList<ManagementGroupVertex>();
		final Deque<ManagementGroupVertex> noOutgoingEdges = new ArrayDeque<ManagementGroupVertex>();
		final Map<ManagementGroupVertex, Integer> outdegrees = new HashMap<ManagementGroupVertex, Integer>();

		final Iterator<ManagementGroupVertex> it = new ManagementGroupVertexIterator(this, false, -1);
		while (it.hasNext()) {
			final ManagementGroupVertex groupVertex = it.next();
			outdegrees.put(groupVertex, Integer.valueOf(groupVertex.getNumberOfForwardEdges()));
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
				outdegree = Integer.valueOf(outdegree.intValue() - 1);
				outdegrees.put(sourceVertex, outdegree);
				if (outdegree.intValue() == 0) {
					noOutgoingEdges.add(sourceVertex);
				}

			}
		}

		return reverseTopologicalSort;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(final Kryo kryo, final Input input) {

		// Read job ID
		this.jobID = kryo.readObject(input, JobID.class);

		// Recreate stages
		final int numberOfStages = input.readInt();
		for (int i = 0; i < numberOfStages; i++) {
			new ManagementStage(this, i);
		}

		// Read number of group vertices and their corresponding IDs
		final int numberOfGroupVertices = input.readInt();
		for (int i = 0; i < numberOfGroupVertices; i++) {

			final ManagementGroupVertexID groupVertexID = kryo.readObject(input, ManagementGroupVertexID.class);
			final ManagementStage stage = this.stages.get(input.readInt());
			final String groupVertexName = input.readString();
			new ManagementGroupVertex(stage, groupVertexID, groupVertexName);
		}

		for (int i = 0; i < numberOfGroupVertices; i++) {
			final ManagementGroupVertexID groupVertexID = kryo.readObject(input, ManagementGroupVertexID.class);
			final ManagementGroupVertex groupVertex = this.groupVertices.get(groupVertexID);
			groupVertex.read(kryo, input);
		}

		// Read the management vertices
		int numberOfVertices = input.readInt();
		for (int i = 0; i < numberOfVertices; i++) {

			final ManagementVertexID vertexID = kryo.readObject(input, ManagementVertexID.class);
			final ManagementGroupVertexID groupVertexID = kryo.readObject(input, ManagementGroupVertexID.class);
			final ManagementGroupVertex groupVertex = this.getGroupVertexByID(groupVertexID);
			final String instanceName = input.readString();
			final String instanceType = input.readString();
			final String checkpointState = input.readString();
			final int indexInGroup = input.readInt();
			final ManagementVertex vertex = new ManagementVertex(groupVertex, vertexID, instanceName, instanceType,
				checkpointState, indexInGroup);
			vertex.read(kryo, input);
		}

		for (int i = 0; i < numberOfVertices; i++) {

			final ManagementVertexID sourceID = kryo.readObject(input, ManagementVertexID.class);
			final ManagementVertex sourceVertex = getVertexByID(sourceID);
			for (int j = 0; j < sourceVertex.getNumberOfOutputGates(); j++) {
				final ManagementGate sourceGate = sourceVertex.getOutputGate(j);
				int numberOfForwardEdges = input.readInt();
				for (int k = 0; k < numberOfForwardEdges; k++) {
					final ManagementEdgeID sourceEdgeID = kryo.readObject(input, ManagementEdgeID.class);
					final ManagementEdgeID targetEdgeID = kryo.readObject(input, ManagementEdgeID.class);

					final ManagementVertexID targetID = kryo.readObject(input, ManagementVertexID.class);
					final ManagementVertex targetVertex = getVertexByID(targetID);
					final int targetGateIndex = input.readInt();
					final ManagementGate targetGate = targetVertex.getInputGate(targetGateIndex);

					final int sourceIndex = input.readInt();
					final int targetIndex = input.readInt();

					final ChannelType channelType = EnumUtils.readEnum(input, ChannelType.class);
					final CompressionLevel compressionLevel = EnumUtils.readEnum(input, CompressionLevel.class);
					new ManagementEdge(sourceEdgeID, targetEdgeID, sourceGate, sourceIndex, targetGate, targetIndex,
						channelType, compressionLevel);
				}

			}
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final Kryo kryo, final Output output) {

		// Write job ID
		kryo.writeObject(output, this.jobID);

		// Write number of stages
		output.writeInt(this.stages.size());

		// Write number of group vertices and their corresponding IDs
		output.writeInt(this.groupVertices.size());
		Iterator<ManagementGroupVertex> it = new ManagementGroupVertexIterator(this, true, -1);

		while (it.hasNext()) {

			final ManagementGroupVertex groupVertex = it.next();
			kryo.writeObject(output, groupVertex.getID());
			output.writeInt(groupVertex.getStage().getStageNumber());
			output.writeString(groupVertex.getName());
		}

		it = new ManagementGroupVertexIterator(this, true, -1);
		while (it.hasNext()) {

			final ManagementGroupVertex groupVertex = it.next();
			kryo.writeObject(output, groupVertex.getID());
			groupVertex.write(kryo, output);
		}

		// Write out the management vertices and their corresponding IDs
		output.writeInt(this.vertices.size());
		Iterator<ManagementVertex> it2 = new ManagementGraphIterator(this, true);
		while (it2.hasNext()) {

			final ManagementVertex managementVertex = it2.next();
			kryo.writeObject(output, managementVertex.getID());
			kryo.writeObject(output, managementVertex.getGroupVertex().getID());
			output.writeString(managementVertex.getInstanceName());
			output.writeString(managementVertex.getInstanceType());
			output.writeString(managementVertex.getCheckpointState());
			output.writeInt(managementVertex.getIndexInGroup());
			managementVertex.write(kryo, output);
		}

		// Finally, serialize the edges between the management vertices
		it2 = vertices.values().iterator();
		while (it2.hasNext()) {

			final ManagementVertex managementVertex = it2.next();
			kryo.writeObject(output, managementVertex.getID());
			for (int i = 0; i < managementVertex.getNumberOfOutputGates(); i++) {
				final ManagementGate outputGate = managementVertex.getOutputGate(i);
				output.writeInt(outputGate.getNumberOfForwardEdges());
				for (int j = 0; j < outputGate.getNumberOfForwardEdges(); j++) {
					final ManagementEdge edge = outputGate.getForwardEdge(j);

					kryo.writeObject(output, edge.getSourceEdgeID());
					kryo.writeObject(output, edge.getTargetEdgeID());

					// This identifies the target gate
					kryo.writeObject(output, edge.getTarget().getVertex().getID());
					output.writeInt(edge.getTarget().getIndex());

					output.writeInt(edge.getSourceIndex());
					output.writeInt(edge.getTargetIndex());

					EnumUtils.writeEnum(output, edge.getChannelType());
					EnumUtils.writeEnum(output, edge.getCompressionLevel());
				}
			}
		}
	}
}
