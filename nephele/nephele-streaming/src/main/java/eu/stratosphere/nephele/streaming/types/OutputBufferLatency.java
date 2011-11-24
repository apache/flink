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
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.jobgraph.JobID;

public final class OutputBufferLatency extends AbstractStreamingData {

	private final ExecutionVertexID vertexID;

	private final ChannelID sourceChannelID;

	private int bufferLatency;

	public OutputBufferLatency(final JobID jobID, final ExecutionVertexID vertexID, final ChannelID sourceChannelID,
			final int bufferLatency) {
		super(jobID);

		if (vertexID == null) {
			throw new IllegalArgumentException("Argument vertexID must not be null");
		}

		if (sourceChannelID == null) {
			throw new IllegalArgumentException("Argument sourceChannelID must not be null");
		}

		if (bufferLatency <= 0) {
			throw new IllegalArgumentException("Argument bufferLatency must be greater than zero");
		}

		this.vertexID = vertexID;
		this.sourceChannelID = sourceChannelID;
		this.bufferLatency = bufferLatency;
	}

	public OutputBufferLatency() {
		super();

		this.vertexID = new ExecutionVertexID();
		this.sourceChannelID = new ChannelID();
		this.bufferLatency = 0;
	}

	public ExecutionVertexID getVertexID() {

		return this.vertexID;
	}

	public ChannelID getSourceChannelID() {

		return this.sourceChannelID;
	}

	public int getBufferLatency() {

		return this.bufferLatency;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput out) throws IOException {
		super.write(out);

		this.vertexID.write(out);
		this.sourceChannelID.write(out);
		out.writeInt(this.bufferLatency);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(final DataInput in) throws IOException {
		super.read(in);

		this.vertexID.read(in);
		this.sourceChannelID.read(in);
		this.bufferLatency = in.readInt();
	}
}
