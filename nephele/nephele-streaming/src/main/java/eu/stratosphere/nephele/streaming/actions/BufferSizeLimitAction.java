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
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.jobgraph.JobID;

/**
 * This class implements an action to limit the buffer size of a particular output channel.
 * 
 * @author warneke
 */
public final class BufferSizeLimitAction extends AbstractAction {

	/**
	 * The ID of the output channel whose buffer size shall be limited.
	 */
	private final ChannelID sourceChannelID;

	/**
	 * The new buffer size in bytes.
	 */
	private int bufferSize;

	/**
	 * Constructs a new buffer size limit action object.
	 * 
	 * @param jobID
	 *        the ID of the job the action applies to
	 * @param vertexID
	 *        the ID of the vertex the action applies to
	 * @param sourceChannelID
	 *        the ID of the output channel whose buffer size shall be limited
	 * @param bufferSize
	 *        the new buffer size in bytes
	 */
	public BufferSizeLimitAction(final JobID jobID, final ExecutionVertexID vertexID, final ChannelID sourceChannelID,
			final int bufferSize) {
		super(jobID, vertexID);

		if (sourceChannelID == null) {
			throw new IllegalArgumentException("Argument sourceChannelID must not be null");
		}

		if (bufferSize <= 0) {
			throw new IllegalArgumentException("Argument bufferSize must be greather than zero");
		}

		this.sourceChannelID = sourceChannelID;
		this.bufferSize = bufferSize;
	}

	/**
	 * Default constructor for deserialization.
	 */
	public BufferSizeLimitAction() {
		super();
		this.sourceChannelID = new ChannelID();
		this.bufferSize = 0;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput out) throws IOException {

		this.sourceChannelID.write(out);
		out.writeInt(this.bufferSize);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(final DataInput in) throws IOException {

		this.sourceChannelID.read(in);
		this.bufferSize = in.readInt();
	}

	/**
	 * Returns the ID of the output channel whose buffer size shall be limited.
	 * 
	 * @return the ID of the output channel whose buffer size shall be limited
	 */
	public ChannelID getSourceChannelID() {

		return this.sourceChannelID;
	}

	/**
	 * Returns the new buffer size in bytes.
	 * 
	 * @return the new buffer size in bytes
	 */
	public int getBufferSize() {

		return this.bufferSize;
	}
}
