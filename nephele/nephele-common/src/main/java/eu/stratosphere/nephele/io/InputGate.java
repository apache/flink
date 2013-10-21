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

package eu.stratosphere.nephele.io;

import java.io.IOException;

import eu.stratosphere.nephele.event.task.AbstractTaskEvent;
import eu.stratosphere.nephele.io.channels.AbstractInputChannel;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.bytebuffered.InMemoryInputChannel;
import eu.stratosphere.nephele.io.channels.bytebuffered.NetworkInputChannel;
import eu.stratosphere.nephele.types.Record;

/**
 * @param <T> The type of record that can be transported through this gate.
 */
public interface InputGate<T extends Record> extends Gate<T> {

	/**
	 * Reads a record from one of the associated input channels. Channels are read such that one buffer from a channel is 
	 * consecutively consumed. The buffers in turn are consumed in the order in which they arrive.
	 * Note that this method is not guaranteed to return a record, because the currently available channel data may not always
	 * constitute an entire record, when events or partial records are part of the data.
	 * 
	 * When called even though no data is available, this call will block until data is available, so this method should be called
	 * when waiting is desired (such as when synchronously consuming a single gate) or only when it is known that data is available
	 * (such as when reading a union of multiple input gates).
	 * 
	 * @param target The record object into which to construct the complete record.
	 * @return The result indicating whether a complete record is available, a event is available, only incomplete data
	 *         is available (NONE), or the gate is exhausted.
	 * @throws IOException Thrown when an error occurred in the network stack relating to this channel.
	 * @throws InterruptedException Thrown, when the thread working on this channel is interrupted.
	 */
	InputChannelResult readRecord(T target) throws IOException, InterruptedException;

	/**
	 * Returns the number of input channels associated with this input gate.
	 * 
	 * @return the number of input channels associated with this input gate
	 */
	int getNumberOfInputChannels();

	/**
	 * Returns the input channel from position <code>pos</code> of the gate's internal channel list.
	 * 
	 * @param pos
	 *        the position to retrieve the channel from
	 * @return the channel from the given position or <code>null</code> if such position does not exist.
	 */
	AbstractInputChannel<T> getInputChannel(int pos);

	/**
	 * Notify the gate that the channel with the given index has
	 * at least one record available.
	 * 
	 * @param channelIndex
	 *        the index of the channel which has at least one record available
	 */
	void notifyRecordIsAvailable(int channelIndex);

	/**
	 * Notify the gate that is has consumed a data unit from the channel with the given index
	 * 
	 * @param channelIndex
	 *        the index of the channel from which a data unit has been consumed
	 */
	void notifyDataUnitConsumed(int channelIndex);

	/**
	 * Immediately closes the input gate and all its input channels. The corresponding
	 * output channels are notified. Any remaining records in any buffers or queue is considered
	 * irrelevant and is discarded.
	 * 
	 * @throws IOException
	 *         thrown if an I/O error occurs while closing the gate
	 * @throws InterruptedException
	 *         thrown if the thread is interrupted while waiting for the gate to be closed
	 */
	void close() throws IOException, InterruptedException;

	/**
	 * Creates a new network input channel and assigns it to the given input gate.
	 * 
	 * @param inputGate
	 *        the input gate the channel shall be assigned to
	 * @param channelID
	 *        the ID of the channel
	 * @param connectedChannelID
	 *        the ID of the channel this channel is connected to
	 * @param compressionLevel
	 *        the level of compression to be used for this channel
	 * @return the new network input channel
	 */
	NetworkInputChannel<T> createNetworkInputChannel(InputGate<T> inputGate, ChannelID channelID,
			ChannelID connectedChannelID);


	/**
	 * Creates a new in-memory input channel and assigns it to the given input gate.
	 * 
	 * @param inputGate
	 *        the input gate the channel shall be assigned to
	 * @param channelID
	 *        the ID of the channel
	 * @param connectedChannelID
	 *        the ID of the channel this channel is connected to
	 * @param compressionLevel
	 *        the level of compression to be used for this channel
	 * @return the new in-memory input channel
	 */
	InMemoryInputChannel<T> createInMemoryInputChannel(InputGate<T> inputGate, ChannelID channelID,
			ChannelID connectedChannelID);

	/**
	 * Registers a {@link RecordAvailabilityListener} with this input gate.
	 * 
	 * @param listener
	 *        the listener object to be registered
	 */
	void registerRecordAvailabilityListener(RecordAvailabilityListener<T> listener);
	
	
	AbstractTaskEvent getCurrentEvent();
}
