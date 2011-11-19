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

package eu.stratosphere.nephele.streaming;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.jobgraph.JobID;

/**
 * This class stores information about the latency of a specific (sub) path from a start to an end vertex.
 * 
 * @author warneke
 */
public final class ChannelLatency extends AbstractStreamingData {

	/**
	 * The ID of the vertex representing the start of the path.
	 */
	private final ExecutionVertexID startVertexID;

	/**
	 * The ID of the vertex representing the end of the path.
	 */
	private final ExecutionVertexID endVertexID;

	/**
	 * The path latency in milliseconds
	 */
	private double pathLatency;

	/**
	 * Constructs a new path latency object.
	 * 
	 * @param jobID
	 *        the ID of the job this path latency information refers to
	 * @param startVertexID
	 *        the ID of the vertex representing the start of the path
	 * @param endVertexID
	 *        the ID of the vertex representing the end of the path
	 * @param pathLatency
	 *        the path latency in milliseconds
	 */
	public ChannelLatency(final JobID jobID, final ExecutionVertexID startVertexID, final ExecutionVertexID endVertexID,
			final double pathLatency) {

		super(jobID);

		if (startVertexID == null) {
			throw new IllegalArgumentException("sourceID must not be null");
		}

		if (endVertexID == null) {
			throw new IllegalArgumentException("targetID must not be null");
		}

		this.startVertexID = startVertexID;
		this.endVertexID = endVertexID;
		this.pathLatency = pathLatency;
	}

	/**
	 * Default constructor for the deserialization of the object.
	 */
	public ChannelLatency() {
		super(new JobID());
		this.startVertexID = new ExecutionVertexID();
		this.endVertexID = new ExecutionVertexID();
		this.pathLatency = 0.0;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput out) throws IOException {
		super.write(out);
		this.startVertexID.write(out);
		this.endVertexID.write(out);
		out.writeDouble(this.pathLatency);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(final DataInput in) throws IOException {
		super.read(in);
		this.startVertexID.read(in);
		this.endVertexID.read(in);
		this.pathLatency = in.readDouble();
	}



	/**
	 * Returns the ID of the vertex representing the start of the path.
	 * 
	 * @return the ID of the vertex representing the start of the path
	 */
	public ExecutionVertexID getStartVertexID() {

		return this.startVertexID;
	}

	/**
	 * Returns the ID of the vertex representing the end of the path.
	 * 
	 * @return the ID of the vertex representing the end of the path
	 */
	public ExecutionVertexID getEndVertexID() {

		return this.endVertexID;
	}

	/**
	 * Returns the path latency in milliseconds.
	 * 
	 * @return the path latency in milliseconds
	 */
	public double getPathLatency() {

		return this.pathLatency;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {

		final StringBuilder str = new StringBuilder();
		str.append(this.startVertexID.toString());
		str.append(" -> ");
		str.append(this.endVertexID.toString());
		str.append(": ");
		str.append(this.pathLatency);

		return str.toString();
	}
}
