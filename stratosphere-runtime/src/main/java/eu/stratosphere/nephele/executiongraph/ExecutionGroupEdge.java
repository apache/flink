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

package eu.stratosphere.nephele.executiongraph;

import eu.stratosphere.nephele.io.DistributionPattern;
import eu.stratosphere.nephele.io.channels.ChannelType;

/**
 * An execution group edge represents an edge between two execution group vertices.
 * <p>
 * This class is thread-safe.
 * 
 */
public class ExecutionGroupEdge {

	/**
	 * Stores if the channel type has been specified by the user.
	 */
	private final boolean userDefinedChannelType;

	/**
	 * The channel type to be used between the execution vertices of the two connected group vertices.
	 */
	private volatile ChannelType channelType;

	/**
	 * The edge's connection ID. The connection ID determines to which physical TCP connection channels represented by
	 * this edge will be mapped in case the edge's channel type is NETWORK.
	 */
	private volatile int connectionID;

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
	 * The distribution pattern used to connect the vertices within two groups.
	 */
	private final DistributionPattern distributionPattern;

	/**
	 * Stores if the edge is part of a broadcast group.
	 */
	private final boolean isBroadcast;

	/**
	 * Constructs a new group edge.
	 * 
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
	 * @param distributionPattern
	 *        the distribution pattern to create the wiring
	 * @param isBroadcast
	 *        indicates that the edge is part of a broadcast group
	 */
	public ExecutionGroupEdge(final ExecutionGroupVertex sourceVertex, final int indexOfOutputGate,
			final ExecutionGroupVertex targetVertex, final int indexOfInputGate, final ChannelType channelType,
			final boolean userDefinedChannelType, final DistributionPattern distributionPattern,
			final boolean isBroadcast) {
		this.sourceVertex = sourceVertex;
		this.indexOfOutputGate = indexOfOutputGate;
		this.channelType = channelType;
		this.indexOfInputGate = indexOfInputGate;
		this.userDefinedChannelType = userDefinedChannelType;
		this.targetVertex = targetVertex;
		this.distributionPattern = distributionPattern;
		this.isBroadcast = isBroadcast;
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

		this.channelType = newChannelType;
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
	 * Sets the edge's connection ID.
	 * 
	 * @param connectionID
	 *        the edge's connection ID
	 */
	void setConnectionID(final int connectionID) {
		this.connectionID = connectionID;
	}

	/**
	 * Returns the edge's connection ID.
	 * 
	 * @return the edge's connection ID
	 */
	public int getConnectionID() {
		return this.connectionID;
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

	/**
	 * Returns the distribution pattern to create the wiring between the group members.
	 * 
	 * @return the distribution pattern to create the wiring between the group members
	 */
	public DistributionPattern getDistributionPattern() {
		return this.distributionPattern;
	}

	/**
	 * Checks if the edge is part of a broadcast group.
	 * 
	 * @return <code>true</code> if the edge is part of a broadcast group, <code>false</code> otherwise
	 */
	public boolean isBroadcast() {
		return this.isBroadcast;
	}
}
