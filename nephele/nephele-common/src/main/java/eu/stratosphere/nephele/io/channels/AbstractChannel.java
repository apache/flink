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

package eu.stratosphere.nephele.io.channels;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.event.task.AbstractEvent;
import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.jobgraph.JobID;

/**
 * An abstract base class for channel objects.
 * 
 * @author warneke
 */
public abstract class AbstractChannel implements IOReadableWritable {

	/**
	 * The ID of the channel.
	 */
	private final ChannelID id;

	/**
	 * The ID of the connected channel.
	 */
	private ChannelID connectedChannelId;

	/**
	 * The compression level to be used for the channel.
	 */
	private CompressionLevel compressionLevel = null;

	private final int channelIndex;

	/**
	 * Auxiliary constructor for channels
	 * 
	 * @param channelID
	 *        the channel ID to assign to the new channel, <code>null</code> to generate a new ID
	 * @param compressionLevel
	 *        the level of compression to be used for this channel
	 */
	protected AbstractChannel(int channelIndex, ChannelID channelID, CompressionLevel compressionLevel) {
		if (channelID == null) {
			this.id = new ChannelID();
		} else {
			this.id = channelID;
		}

		this.compressionLevel = compressionLevel;
		this.channelIndex = channelIndex;
	}

	/**
	 * Returns the ID of the channel.
	 * 
	 * @return the ID of the channel.
	 */
	public ChannelID getID() {
		return this.id;
	}

	/**
	 * Returns the channel's input at the associated gate.
	 * 
	 * @return the channel's input at the associated gate
	 */
	public int getChannelIndex() {
		return this.channelIndex;
	}

	/**
	 * Returns the type of the channel.
	 * 
	 * @return the type of the channel.
	 */
	public abstract ChannelType getType();

	/**
	 * Checks if the channel is closed, i.e. no more records can be transported through the channel.
	 * 
	 * @return <code>true</code> if the channel is closed, <code>false</code> otherwise
	 * @throws IOException
	 *         thrown if an error occurred while closing the channel
	 */
	public abstract boolean isClosed() throws IOException;

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(DataInput in) throws IOException {

		// Read connected channel id
		final boolean isNotNull = in.readBoolean();
		if (isNotNull) {
			this.connectedChannelId = new ChannelID();
			this.connectedChannelId.read(in);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(DataOutput out) throws IOException {

		// Write connected channel id
		if (this.connectedChannelId == null) {
			out.writeBoolean(false);
		} else {
			out.writeBoolean(true);
			this.connectedChannelId.write(out);
		}
	}

	public ChannelID getConnectedChannelID() {
		return this.connectedChannelId;
	}

	public void setConnectedChannelID(ChannelID connectedChannelId) {
		this.connectedChannelId = connectedChannelId;
	}

	/**
	 * Returns the channel's current compression level.
	 * 
	 * @return the channel's current compression level
	 */
	public CompressionLevel getCompressionLevel() {
		return this.compressionLevel;
	}

	/**
	 * Returns the ID of the job this channel belongs to.
	 * 
	 * @return the ID of the job this channel belongs to
	 */
	public abstract JobID getJobID();

	public abstract boolean isInputChannel();

	public abstract void transferEvent(AbstractEvent event) throws IOException;

	public abstract void processEvent(AbstractEvent event);
}
