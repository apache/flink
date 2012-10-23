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

package eu.stratosphere.nephele.jobgraph;

import eu.stratosphere.nephele.io.DistributionPattern;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.compression.CompressionLevel;

/**
 * Objects of this class represent edges in the user's job graph.
 * The edges can be annotated by a specific channel and compression level.
 * 
 * @author warneke
 */
public class JobEdge {

	/**
	 * The channel type to be used for the resulting channel.
	 */
	private final ChannelType channelType;

	/**
	 * The compression level to apply to the resulting channel.
	 */
	private final CompressionLevel compressionLevel;

	/**
	 * The vertex connected to this edge.
	 */
	private final AbstractJobVertex connectedVertex;

	/**
	 * The index of the consuming task's input gate.
	 */
	private final int indexOfInputGate;

	/**
	 * The distribution pattern that should be used for this job edge.
	 */
	private final DistributionPattern distributionPattern;

	/**
	 * Stores whether spanning records are allowed or not.
	 */
	private final boolean allowSpanningRecords;

	/**
	 * Constructs a new job edge.
	 * 
	 * @param connectedVertex
	 *        the vertex this edge should connect to
	 * @param channelType
	 *        the channel type this edge should be translated to at runtime
	 * @param compressionLevel
	 *        the compression level the corresponding channel should have at runtime
	 * @param indexOfInputGate
	 *        index of the consuming task's input gate that this edge connects to
	 * @param distributionPattern
	 *        the distribution patter according to which the subtasks of the connected vertices shall be connected
	 * @param allowSpanningRecords
	 *        <code>true</code> to allow spanning records, <code>false</code> otherwise
	 */
	public JobEdge(final AbstractJobVertex connectedVertex, final ChannelType channelType,
			final CompressionLevel compressionLevel, final int indexOfInputGate,
			final DistributionPattern distributionPattern, final boolean allowSpanningRecords) {

		this.connectedVertex = connectedVertex;
		this.channelType = channelType;
		this.compressionLevel = compressionLevel;
		this.indexOfInputGate = indexOfInputGate;
		this.distributionPattern = distributionPattern;
		this.allowSpanningRecords = allowSpanningRecords;
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
	 * Returns the compression level assigned to this edge.
	 * 
	 * @return the compression level assigned to this edge
	 */
	public CompressionLevel getCompressionLevel() {
		return this.compressionLevel;
	}

	/**
	 * Returns the vertex this edge is connected to.
	 * 
	 * @return the vertex this edge is connected to
	 */
	public AbstractJobVertex getConnectedVertex() {
		return this.connectedVertex;
	}

	/**
	 * Returns the index of the consuming task's input gate that this edge connects to.
	 * 
	 * @return the index of the consuming task's input gate that this edge connects to
	 */
	public int getIndexOfInputGate() {
		return this.indexOfInputGate;
	}

	/**
	 * Returns the distribution pattern used for this edge.
	 * 
	 * @return the distribution pattern used for this edge
	 */
	public DistributionPattern getDistributionPattern() {
		return this.distributionPattern;
	}

	/**
	 * Checks if spanning records are allowed for this edge.
	 * 
	 * @return <code>true</code> if spanning records are allowed for this edge, <code>false</code> otherwise
	 */
	public boolean spanningRecordsAllowed() {

		return this.allowSpanningRecords;
	}
}
