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

import eu.stratosphere.runtime.io.channels.ChannelType;

/**
 * This class implements a directed edge of a {@link ManagementGraph}. The edge is derived from a channel of the actual
 * execution graph.
 * <p>
 * This class is not thread-safe.
 * 
 */
public final class ManagementEdge extends ManagementAttachment {

	/**
	 * The source of the edge referring to the output gate of an execution vertex.
	 */
	private final ManagementGate source;

	/**
	 * The target of the edge referring to the input gate of an execution vertex.
	 */
	private final ManagementGate target;

	/**
	 * The edge's index in the source gate.
	 */
	private final int sourceIndex;

	/**
	 * The edge's index in the target gate.
	 */
	private final int targetIndex;

	/**
	 * The type of the channel this edge refers to.
	 */
	private final ChannelType channelType;

	/**
	 * The source ID of the management edge.
	 */
	private final ManagementEdgeID sourceEdgeID;

	/**
	 * The target ID of the management edge.
	 */
	private final ManagementEdgeID targetEdgeID;

	/**
	 * Constructs a new edge object.
	 * 
	 * @param sourceEdgeID
	 *        source ID of the management edge
	 * @param targetEdgeID
	 *        target ID of the management edge
	 * @param source
	 *        the source of the edge referring to the output gate of an execution vertex
	 * @param sourceIndex
	 *        the edge's index in the source gate
	 * @param target
	 *        the target of the edge referring to the input gate of an execution vertex
	 * @param targetIndex
	 *        the edge's index in the target gate
	 * @param channelType
	 *        the type of the channel this edge refers to
	 * @param compressionLevel
	 *        the compression level of the channel this edge refers to
	 */
	public ManagementEdge(final ManagementEdgeID sourceEdgeID, final ManagementEdgeID targetEdgeID,
			final ManagementGate source, final int sourceIndex, final ManagementGate target, final int targetIndex,
			final ChannelType channelType) {

		this.sourceEdgeID = sourceEdgeID;
		this.targetEdgeID = targetEdgeID;
		this.source = source;
		this.target = target;
		this.sourceIndex = sourceIndex;
		this.targetIndex = targetIndex;
		this.channelType = channelType;

		this.source.insertForwardEdge(this, sourceIndex);
		this.target.insertBackwardEdge(this, targetIndex);
	}

	/**
	 * Returns the type of the channel this edge refers to.
	 * 
	 * @return the type of the channel this edge refers to
	 */
	public ChannelType getChannelType() {
		return this.channelType;
	}

	/**
	 * Returns the source of the edge referring to the output gate of an execution vertex.
	 * 
	 * @return the source of the edge referring to the output gate of an execution vertex
	 */
	public ManagementGate getSource() {
		return this.source;
	}

	/**
	 * Returns the target of the edge referring to the input gate of an execution vertex.
	 * 
	 * @return the target of the edge referring to the input gate of an execution vertex
	 */
	public ManagementGate getTarget() {
		return this.target;
	}

	/**
	 * Returns the edge's index in the source gate.
	 * 
	 * @return the edge's index in the source gate
	 */
	public int getSourceIndex() {
		return this.sourceIndex;
	}

	/**
	 * Returns the edge's index in the target gate.
	 * 
	 * @return the edge's index in the target gate
	 */
	public int getTargetIndex() {
		return this.targetIndex;
	}

	/**
	 * Returns the source ID of the edge.
	 * 
	 * @return The source ID of the edge
	 */
	public ManagementEdgeID getSourceEdgeID() {
		return sourceEdgeID;
	}

	/**
	 * Returns the target ID of the edge.
	 * 
	 * @return The target ID of the edge
	 */
	public ManagementEdgeID getTargetEdgeID() {
		return targetEdgeID;
	}
}
