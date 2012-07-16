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

import eu.stratosphere.nephele.io.channels.AbstractInputChannel;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.bytebuffered.FileInputChannel;
import eu.stratosphere.nephele.io.channels.bytebuffered.InMemoryInputChannel;
import eu.stratosphere.nephele.io.channels.bytebuffered.NetworkInputChannel;
import eu.stratosphere.nephele.io.compression.CompressionException;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.types.Record;

/**
 * In Nephele input gates are a specialization of general gates and connect input channels and record readers. As
 * channels, input
 * gates are always parameterized to a specific type of record which they can transport. In contrast to output gates
 * input gates
 * can be associated with a {@link DistributionPattern} object which dictates the concrete wiring between two groups of
 * vertices.
 * 
 * @author warneke
 * @param <T>
 *        the type of record that can be transported through this gate
 */
public interface InputGate<T extends Record> extends Gate<T> {

	/**
	 * Reads a record from one of the associated input channels. The channels are
	 * chosen in a way so that channels with available records are preferred.
	 * The operation may block until at least one of the associated input channel
	 * is able to provide a record.
	 * 
	 * @return the record read from one of the input channels or <code>null</code> if all channels are already closed.
	 * @throws ExecutionFailureException
	 *         thrown if an error occurred while reading the channels
	 */

	T readRecord(T target) throws IOException, InterruptedException;

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
	 * Initializes the decompressor objects inside the input channels attached to this gate.
	 * 
	 * @throws CompressionException
	 *         thrown if an error occurs while loading the decompressor objects
	 */
	void initializeDecompressors() throws CompressionException;

	/**
	 * Activates all of the task's input channels.
	 * 
	 * @throws IOException
	 *         thrown if an I/O error occurs while transmitting one of the activation requests to the corresponding
	 *         output channels
	 * @throws InterruptedException
	 *         throws if the task is interrupted while waiting for the activation process to complete
	 */
	void activateInputChannels() throws IOException, InterruptedException;

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
			ChannelID connectedChannelID, CompressionLevel compressionLevel);

	/**
	 * Creates a new file input channel and assigns it to the given input gate.
	 * 
	 * @param inputGate
	 *        the input gate the channel shall be assigned to
	 * @param channelID
	 *        the ID of the channel
	 * @param connectedChannelID
	 *        the ID of the channel this channel is connected to
	 * @param compressionLevel
	 *        the level of compression to be used for this channel
	 * @return the new file input channel
	 */
	FileInputChannel<T> createFileInputChannel(InputGate<T> inputGate, ChannelID channelID,
			ChannelID connectedChannelID, CompressionLevel compressionLevel);

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
			ChannelID connectedChannelID, CompressionLevel compressionLevel);

	/**
	 * Removes all input channels from the input gate.
	 */
	void removeAllInputChannels();

	/**
	 * Registers a {@link RecordAvailabilityListener} with this input gate.
	 * 
	 * @param listener
	 *        the listener object to be registered
	 */
	void registerRecordAvailabilityListener(RecordAvailabilityListener<T> listener);

	/**
	 * Checks if the input gate has records available.
	 * 
	 * @return <code>true</code> if the gate has records available, <code>false</code> otherwise
	 * @throws IOException
	 * @throws InterruptedException
	 */
	boolean hasRecordAvailable() throws IOException, InterruptedException;
}
