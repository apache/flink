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
 * This class implements a directed edge of a {@link ManagementGraph}. The edge is derived from a channel of the actual
 * execution graph.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 */
public final class ManagementEdge {

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
	 * The compression level of the channel this edge refers to.
	 */
	private final CompressionLevel compressionLevel;

	/**
	 * A possible attachment to this object.
	 */
	private volatile Object attachment = null;

	/**
	 * Constructs a new edge object.
	 * 
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
	public ManagementEdge(final ManagementGate source, final int sourceIndex, final ManagementGate target,
			final int targetIndex,
			final ChannelType channelType, final CompressionLevel compressionLevel) {
		this.source = source;
		this.target = target;
		this.sourceIndex = sourceIndex;
		this.targetIndex = targetIndex;
		this.channelType = channelType;
		this.compressionLevel = compressionLevel;

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
	 * Returns the compression level of the channel this edge refers to.
	 * 
	 * @return the compression level of the channel this edge refers to
	 */
	public CompressionLevel getCompressionLevel() {
		return this.compressionLevel;
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
	 * Sets an attachment for this edge.
	 * 
	 * @param attachment
	 *        the attachment for this edge
	 */
	public void setAttachment(final Object attachment) {
		this.attachment = attachment;
	}

	/**
	 * Returns the attachment of this edge.
	 * 
	 * @return the attachment of this edge or <code>null</code> if this edge has no attachment
	 */
	public Object getAttachment() {
		return this.attachment;
	}
}
