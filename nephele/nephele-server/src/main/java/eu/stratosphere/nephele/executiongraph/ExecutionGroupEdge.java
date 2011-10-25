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

package eu.stratosphere.nephele.executiongraph;

import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.compression.CompressionLevel;

/**
 * An execution group edge represents an edge between two execution group vertices.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 */
public class ExecutionGroupEdge {

	/**
	 * Stores a reference to the execution graph this edge belongs to.
	 */
	private final ExecutionGraph executionGraph;

	/**
	 * Stores if the channel type has been specified by the user.
	 */
	private final boolean userDefinedChannelType;

	/**
	 * Stores if the compression level has been specified by the user.
	 */
	private final boolean userDefinedCompressionLevel;

	/**
	 * The channel type to be used between the execution vertices of the two connected group vertices.
	 */
	private volatile ChannelType channelType;

	/**
	 * The compression level to be used between the execution vertices of the two connected group vertices.
	 */
	private volatile CompressionLevel compressionLevel;

	/**
	 * The group vertex connected to this edge.
	 */
	private final ExecutionGroupVertex targetVertex;

	/**
	 * The group vertex this edge starts from.
	 */
	private final ExecutionGroupVertex sourceVertex;

	/**
	 * The index of the producing task's output gate.
	 */
	private final int indexOfOutputGate;

	/**
	 * The index of the consuming task's input gate.
	 */
	private final int indexOfInputGate;

	/**
	 * Constructs a new group edge.
	 * 
	 * @param executionGraph
	 *        the execution graph this edge belongs to
	 * @param sourceVertex
	 *        the source vertex this edge originates from
	 * @param indexOfOutputGate
	 *        the index of the source vertex's output gate this edge originates from
	 * @param targetVertex
	 *        the group vertex to be connected
	 * @param indexOfInputGate
	 *        the index of the consuming task's input gate
	 * @param channelType
	 *        the channel type for the edge
	 * @param userDefinedChannelType
	 *        <code>true</code> if the channel type has been specified by the user, <code>false</code> otherwise
	 * @param compressionLevel
	 *        the compression level for the edge
	 * @param userDefinedCompressionLevel
	 *        <code>true</code> if the compression level has been specified by the user, <code>false</code> otherwise
	 */
	public ExecutionGroupEdge(final ExecutionGraph executionGraph, final ExecutionGroupVertex sourceVertex,
			final int indexOfOutputGate, final ExecutionGroupVertex targetVertex, final int indexOfInputGate,
			final ChannelType channelType, final boolean userDefinedChannelType,
			final CompressionLevel compressionLevel, final boolean userDefinedCompressionLevel) {
		this.executionGraph = executionGraph;
		this.sourceVertex = sourceVertex;
		this.indexOfOutputGate = indexOfOutputGate;
		this.channelType = channelType;
		this.indexOfInputGate = indexOfInputGate;
		this.userDefinedChannelType = userDefinedChannelType;
		this.compressionLevel = compressionLevel;
		this.userDefinedCompressionLevel = userDefinedCompressionLevel;
		this.targetVertex = targetVertex;
	}

	/**
	 * Returns the channel type assigned to this edge.
	 * 
	 * @return the channel type assigned to this edge
	 */
	public ChannelType getChannelType() {
		return this.channelType;
	}

	/**
	 * Changes the channel type for this edge.
	 * 
	 * @param newChannelType
	 *        the channel type for this edge
	 * @throws GraphConversionException
	 *         thrown if the new channel type violates a user setting
	 */
	void changeChannelType(final ChannelType newChannelType) throws GraphConversionException {

		if (!this.channelType.equals(newChannelType) && this.userDefinedChannelType) {
			throw new GraphConversionException("Cannot overwrite user defined channel type");
		}

		this.executionGraph.unwire(this.sourceVertex, this.indexOfOutputGate, this.targetVertex, this.indexOfInputGate);
		this.executionGraph.wire(this.sourceVertex, this.indexOfOutputGate, this.targetVertex, this.indexOfInputGate,
			newChannelType, this.compressionLevel);

		// TODO: This code breaks graphs which have multiple edges with different channel between a pair of vertices
		// It should probably be removed
		// Make sure update is applied to all edges connecting the two vertices
		/*
		 * final List<ExecutionGroupEdge> edges = this.getSourceVertex().getForwardEdges(this.getTargetVertex());
		 * final Iterator<ExecutionGroupEdge> it = edges.iterator();
		 * while (it.hasNext()) {
		 * final ExecutionGroupEdge edge = it.next();
		 * // Update channel type
		 * CompressionLevel cl = null;
		 * synchronized(edge) {
		 * System.out.println("Chaning channel2 type from " + edge.channelType + " to " + newChannelType);
		 * edge.channelType = newChannelType;
		 * cl = edge.compressionLevel;
		 * }
		 * this.executionGraph.unwire(edge.sourceVertex, edge.indexOfOutputGate, edge.targetVertex,
		 * edge.indexOfInputGate);
		 * }
		 */

		// Changing the channels may require to reassign the stages
		this.executionGraph.repairStages();
		// It may also be necessary to repair the instance assignment
		this.executionGraph.repairInstanceAssignment();
	}

	/**
	 * Returns the group vertex connected to this edge.
	 * 
	 * @return the group vertex connected to this edge
	 */
	public ExecutionGroupVertex getTargetVertex() {
		return this.targetVertex;
	}

	/**
	 * Returns the compression level assigned to this edge.
	 * 
	 * @return the compression level assigned to this edge
	 */
	public CompressionLevel getCompressionLevel() {
		return this.compressionLevel;
	}

	/**
	 * Changes the compression level for this edge.
	 * 
	 * @param newCompressionLevel
	 *        the compression type for this edge
	 * @throws GraphConversionException
	 *         thrown if the new compression level violates a user setting
	 */
	void changeCompressionLevel(final CompressionLevel newCompressionLevel) throws GraphConversionException {

		if (!this.compressionLevel.equals(newCompressionLevel)) {

			if (this.userDefinedCompressionLevel) {
				throw new GraphConversionException("Cannot overwrite user defined compression level");
			}

			// TODO: Implement propagation of compression level to channels

			// Update compression level
			this.compressionLevel = newCompressionLevel;
		}
	}

	/**
	 * Returns if the edge's channel type is user defined.
	 * 
	 * @return <code>true</code> if the channel type is user defined, <code>false</code> otherwise
	 */
	public boolean isChannelTypeUserDefined() {
		return this.userDefinedChannelType;
	}

	/**
	 * Returns if the edge's compression level is user defined.
	 * 
	 * @return <code>true</code> if the compression level is user defined, <code>false</code> otherwise
	 */
	public boolean isCompressionLevelUserDefined() {
		return this.userDefinedCompressionLevel;
	}

	/**
	 * Returns the index of the input gate this edge starts from.
	 * 
	 * @return the index of the input gate this edge starts from
	 */
	public int getIndexOfInputGate() {
		return this.indexOfInputGate;
	}

	/**
	 * Returns the source vertex this edge starts from.
	 * 
	 * @return the source vertex this edge starts from
	 */
	public ExecutionGroupVertex getSourceVertex() {
		return this.sourceVertex;
	}

	/**
	 * Returns the index of the output gate this edge arrives at.
	 * 
	 * @return the index of the output gate this edge arrives at
	 */
	public int getIndexOfOutputGate() {
		return this.indexOfOutputGate;
	}
}
