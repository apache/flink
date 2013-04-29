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

import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.compression.CompressionLevel;

/**
 * This class implements a directed edge of between two {@link ManagementGroupVertex} objects. The edge is derived from
 * a group edge of the actual execution graph.
 * <p>
 * This class is not thread-safe.
 * 
 * @author warneke
 */
public final class ManagementGroupEdge extends ManagementAttachment {

	/**
	 * The source vertex of this group edge.
	 */
	private final ManagementGroupVertex source;

	/**
	 * The target vertex of this group edge.
	 */
	private final ManagementGroupVertex target;

	/**
	 * The edge's index at the source vertex.
	 */
	private final int sourceIndex;

	/**
	 * The edge's index at the target vertex.
	 */
	private final int targetIndex;

	/**
	 * The type of the channels represented by this group edge.
	 */
	private final ChannelType channelType;

	/**
	 * The compression level of the channels represented by this group edge.
	 */
	private final CompressionLevel compressionLevel;

	/**
	 * Constructs a new management group edge.
	 * 
	 * @param source
	 *        the source vertex of the new group edge
	 * @param sourceIndex
	 *        the index at the source vertex
	 * @param target
	 *        the target vertex of the new group edge
	 * @param targetIndex
	 *        the index at the target vertex
	 * @param channelType
	 *        the type of the channels represented by the new group edge
	 * @param compressionLevel
	 *        the compression level of the channels represented by the new group edge
	 */
	public ManagementGroupEdge(final ManagementGroupVertex source, final int sourceIndex,
			final ManagementGroupVertex target, final int targetIndex, final ChannelType channelType,
			final CompressionLevel compressionLevel) {
		this.source = source;
		this.target = target;
		this.sourceIndex = sourceIndex;
		this.targetIndex = targetIndex;
		this.channelType = channelType;
		this.compressionLevel = compressionLevel;

		source.insertForwardEdge(this, sourceIndex);
		target.insertBackwardEdge(this, targetIndex);
	}

	/**
	 * Returns the type of the channels represented by this group edge.
	 * 
	 * @return the type of the channels represented by this group edge
	 */
	public ChannelType getChannelType() {
		return this.channelType;
	}

	/**
	 * Returns the compression level of the channels represented by this group edge.
	 * 
	 * @return the compression level of the channels represented by this group edge
	 */
	public CompressionLevel getCompressionLevel() {
		return this.compressionLevel;
	}

	/**
	 * Returns the source vertex of this group edge.
	 * 
	 * @return the source vertex of this group edge
	 */
	public ManagementGroupVertex getSource() {
		return this.source;
	}

	/**
	 * Returns the target vertex of this group edge.
	 * 
	 * @return the target vertex of this group edge
	 */
	public ManagementGroupVertex getTarget() {
		return this.target;
	}

	/**
	 * Returns the edge's index at the source vertex.
	 * 
	 * @return the edge's index at the source vertex
	 */
	public int getSourceIndex() {
		return this.sourceIndex;
	}

	/**
	 * Returns the edge's index at the target vertex.
	 * 
	 * @return the edges's index at the target vertex
	 */
	public int getTargetIndex() {
		return this.targetIndex;
	}
}
