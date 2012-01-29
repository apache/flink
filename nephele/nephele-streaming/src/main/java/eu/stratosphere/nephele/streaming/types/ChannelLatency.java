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

package eu.stratosphere.nephele.streaming.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.jobgraph.JobID;

/**
 * This class stores information about the latency of a specific channel from a source to a sink vertex.
 * 
 * @author warneke
 */
public final class ChannelLatency extends AbstractStreamingData {

	/**
	 * The ID of the vertex representing the source of the channel.
	 */
	private final ExecutionVertexID sourceVertexID;

	/**
	 * The ID of the vertex representing the sink of the channel.
	 */
	private final ExecutionVertexID sinkVertexID;

	/**
	 * The channel latency in milliseconds
	 */
	private double channelLatency;

	/**
	 * Constructs a new path latency object.
	 * 
	 * @param jobID
	 *        the ID of the job this channel latency information refers to
	 * @param sourceVertexID
	 *        the ID of the vertex representing the source of the channel
	 * @param sinkVertexID
	 *        the ID of the vertex representing the sink of the channel
	 * @param pathLatency
	 *        the path latency in milliseconds
	 */
	public ChannelLatency(final JobID jobID, final ExecutionVertexID sourceVertexID,
			final ExecutionVertexID sinkVertexID, final double channelLatency) {

		super(jobID);

		if (sourceVertexID == null) {
			throw new IllegalArgumentException("sourceVertexID must not be null");
		}

		if (sinkVertexID == null) {
			throw new IllegalArgumentException("sinkVertexID must not be null");
		}

		this.sourceVertexID = sourceVertexID;
		this.sinkVertexID = sinkVertexID;
		this.channelLatency = channelLatency;
	}

	/**
	 * Default constructor for the deserialization of the object.
	 */
	public ChannelLatency() {
		super(new JobID());
		this.sourceVertexID = new ExecutionVertexID();
		this.sinkVertexID = new ExecutionVertexID();
		this.channelLatency = 0.0;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput out) throws IOException {
		super.write(out);
		this.sourceVertexID.write(out);
		this.sinkVertexID.write(out);
		out.writeDouble(this.channelLatency);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(final DataInput in) throws IOException {
		super.read(in);
		this.sourceVertexID.read(in);
		this.sinkVertexID.read(in);
		this.channelLatency = in.readDouble();
	}

	/**
	 * Returns the ID of the vertex representing the source of the channel.
	 * 
	 * @return the ID of the vertex representing the source of the channel
	 */
	public ExecutionVertexID getSourceVertexID() {

		return this.sourceVertexID;
	}

	/**
	 * Returns the ID of the vertex representing the sink of the channel.
	 * 
	 * @return the ID of the vertex representing the sink of the channel
	 */
	public ExecutionVertexID getSinkVertexID() {

		return this.sinkVertexID;
	}

	/**
	 * Returns the channel latency in milliseconds.
	 * 
	 * @return the channel latency in milliseconds
	 */
	public double getChannelLatency() {

		return this.channelLatency;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {

		final StringBuilder str = new StringBuilder();
		str.append(this.sourceVertexID.toString());
		str.append(" -> ");
		str.append(this.sinkVertexID.toString());
		str.append(": ");
		str.append(this.channelLatency);

		return str.toString();
	}
}
