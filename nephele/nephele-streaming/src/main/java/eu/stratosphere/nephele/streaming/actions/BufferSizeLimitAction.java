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

package eu.stratosphere.nephele.streaming.actions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.jobgraph.JobID;

public final class BufferSizeLimitAction implements IOReadableWritable {

	private final JobID jobID;

	private final ExecutionVertexID vertexID;

	private final ChannelID sourceChannelID;

	private int bufferSize;

	public BufferSizeLimitAction(final JobID jobID, final ExecutionVertexID vertexID, final ChannelID sourceChannelID,
			final int bufferSize) {

		if (jobID == null) {
			throw new IllegalArgumentException("Argument jobID must not be null");
		}

		if (vertexID == null) {
			throw new IllegalArgumentException("Argument vertexID must not be null");
		}

		if (sourceChannelID == null) {
			throw new IllegalArgumentException("Argument sourceChannelID must not be null");
		}

		if (bufferSize <= 0) {
			throw new IllegalArgumentException("Argument bufferSize must be greather than zero");
		}

		this.jobID = jobID;
		this.vertexID = vertexID;
		this.sourceChannelID = sourceChannelID;
		this.bufferSize = bufferSize;
	}

	/**
	 * Default constructor for deserialization.
	 */
	public BufferSizeLimitAction() {
		this.jobID = new JobID();
		this.vertexID = new ExecutionVertexID();
		this.sourceChannelID = new ChannelID();
		this.bufferSize = 0;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput out) throws IOException {

		this.jobID.write(out);
		this.vertexID.write(out);
		this.sourceChannelID.write(out);
		out.writeInt(this.bufferSize);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(final DataInput in) throws IOException {

		this.jobID.read(in);
		this.vertexID.read(in);
		this.sourceChannelID.read(in);
		this.bufferSize = in.readInt();
	}
}
