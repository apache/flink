/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.nephele.io.channels;

import java.io.IOException;

import eu.stratosphere.core.io.IOReadableWritable;
import eu.stratosphere.nephele.event.task.AbstractTaskEvent;
import eu.stratosphere.nephele.io.InputChannelResult;
import eu.stratosphere.nephele.io.InputGate;
import eu.stratosphere.nephele.jobgraph.JobID;

/**
 * InputChannel is an abstract base class to all different kinds of concrete
 * input channels that can be used. Input channels are always parameterized to
 * a specific type that can be transported through the channel.

 * @param <T> The Type of the record that can be transported through the channel.
 */
public abstract class AbstractInputChannel<T extends IOReadableWritable> extends AbstractChannel {

	private final InputGate<T> inputGate;

	/**
	 * Constructs an input channel with a given input gate associated.
	 * 
	 * @param inputGate
	 *        the input gate this channel is connected to
	 * @param channelIndex
	 *        the index of the channel in the input gate
	 * @param channelID
	 *        the ID of the channel
	 * @param connectedChannelID
	 *        the ID of the channel this channel is connected to
	 * @param compressionLevel
	 *        the level of compression to be used for this channel
	 */
	protected AbstractInputChannel(final InputGate<T> inputGate, final int channelIndex, final ChannelID channelID,
			final ChannelID connectedChannelID) {
		super(channelIndex, channelID, connectedChannelID);
		this.inputGate = inputGate;
	}

	/**
	 * Returns the input gate associated with the input channel.
	 * 
	 * @return the input gate associated with the input channel.
	 */
	public InputGate<T> getInputGate() {
		return this.inputGate;
	}

	/**
	 * Reads a record from the input channel. If currently no record is available the method
	 * returns <code>null</code>. If the channel is closed (i.e. no more records will be received), the method
	 * throws an {@link EOFException}.
	 * 
	 * @return a record that has been transported through the channel or <code>null</code> if currently no record is
	 *         available
	 * @throws IOException
	 *         thrown if the input channel is already closed {@link EOFException} or a transmission error has occurred
	 */
	public abstract InputChannelResult readRecord(T target) throws IOException;

	/**
	 * Immediately closes the input channel. The corresponding output channels are
	 * notified if necessary. Any remaining records in any buffers or queue is considered
	 * irrelevant and is discarded.
	 * 
	 * @throws InterruptedException
	 *         thrown if the thread is interrupted while waiting for the channel to close
	 * @throws IOException
	 *         thrown if an I/O error occurs while closing the channel
	 */
	public abstract void close() throws IOException, InterruptedException;



	@Override
	public boolean isInputChannel() {
		return true;
	}


	@Override
	public JobID getJobID() {
		return this.inputGate.getJobID();
	}
	
	public abstract AbstractTaskEvent getCurrentEvent();
}
