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

import java.io.IOException;

import eu.stratosphere.nephele.event.task.AbstractEvent;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.jobgraph.JobID;

/**
 * An abstract base class for channel objects.
 * 
 * @author warneke
 */
public abstract class AbstractChannel implements Channel {

	/**
	 * The ID of the channel.
	 */
	private final ChannelID channelID;

	/**
	 * The ID of the connected channel.
	 */
	private final ChannelID connectedChannelID;

	/**
	 * The compression level to be used for the channel.
	 */
	private final CompressionLevel compressionLevel;

	private final int channelIndex;

	/**
	 * Auxiliary constructor for channels
	 * 
	 * @param channelIndex
	 *        the index of the channel in either the output or input gate
	 * @param channelID
	 *        the ID of the channel
	 * @param connectedChannelID
	 *        the ID of the channel this channel is connected to
	 * @param compressionLevel
	 *        the level of compression to be used for this channel
	 */
	protected AbstractChannel(final int channelIndex, final ChannelID channelID, final ChannelID connectedChannelID,
			final CompressionLevel compressionLevel) {

		this.channelIndex = channelIndex;
		this.channelID = channelID;
		this.connectedChannelID = connectedChannelID;
		this.compressionLevel = compressionLevel;
	}

	/**
	 * Returns the ID of the channel.
	 * 
	 * @return the ID of the channel.
	 */
	public ChannelID getID() {

		return this.channelID;
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
	 * Checks if the channel is closed, i.e. no more records can be transported through the channel.
	 * 
	 * @return <code>true</code> if the channel is closed, <code>false</code> otherwise
	 * @throws IOException
	 *         thrown if an error occurred while closing the channel
	 * @throws InterruptedException
	 *         thrown if the channel is interrupted while waiting for this operation to complete
	 */
	public abstract boolean isClosed() throws IOException, InterruptedException;

	public ChannelID getConnectedChannelID() {

		return this.connectedChannelID;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public CompressionLevel getCompressionLevel() {
		return this.compressionLevel;
	}

	/**
	 * Returns the ID of the job this channel belongs to.
	 * 
	 * @return the ID of the job this channel belongs to
	 */
	public abstract JobID getJobID();

	/**
	 * Returns <code>true</code> if this channel is an input channel, <code>false</code> otherwise.
	 * 
	 * @return <code>true</code> if this channel is an input channel, <code>false</code> otherwise
	 */
	public abstract boolean isInputChannel();

	public abstract void transferEvent(AbstractEvent event) throws IOException, InterruptedException;

	public abstract void processEvent(AbstractEvent event);

	/**
	 * Releases all resources (especially buffers) which are currently allocated by this channel. This method should be
	 * called in case of a task error or as a result of a cancel operation.
	 */
	public abstract void releaseAllResources();

	/**
	 * Returns the number of bytes which have been transmitted through this channel since its instantiation.
	 * 
	 * @return the number of bytes which have been transmitted through this channel since its instantiation
	 */
	public abstract long getAmountOfDataTransmitted();
}
