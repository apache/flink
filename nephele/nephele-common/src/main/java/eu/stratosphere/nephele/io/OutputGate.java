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

import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.compression.CompressionException;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.types.Record;

/**
 * In Nephele output gates are a specialization of general gates and connect
 * record writers and output channels. As channels, output gates are always
 * parameterized to a specific type of record which they can transport.
 * 
 * @author warneke
 * @param <T>
 *        the type of record that can be transported through this gate
 */
public interface OutputGate<T extends Record> extends Gate<T> {

	/**
	 * Writes a record to one of the associated output channels. Currently, the
	 * channels are chosen in a simple round-robin fashion. This operation may
	 * block until the respective channel has received the data.
	 * 
	 * @param record
	 *        the record to be written
	 * @throws IOException
	 *         thrown if any error occurs during channel I/O
	 */
	void writeRecord(T record) throws IOException, InterruptedException;

	/**
	 * Flushes all connected output channels.
	 * 
	 * @throws IOException
	 *         thrown if an error occurs while flushing an output channel
	 * @throws InterruptedException
	 *         thrown if the thread is interrupted while waiting for the data to be flushed
	 */
	void flush() throws IOException, InterruptedException;

	/**
	 * Checks if this output gate operates in broadcast mode, i.e. all records passed to it are transferred through all
	 * connected output channels.
	 * 
	 * @return <code>true</code> if this output gate operates in broadcast mode, <code>false</code> otherwise
	 */
	boolean isBroadcast();

	/**
	 * Returns the output gate's channel selector.
	 * 
	 * @return the output gate's channel selector or <code>null</code> if the gate operates in broadcast mode
	 */
	ChannelSelector<T> getChannelSelector();

	/**
	 * Requests the output gate to closed. This means the application will send
	 * no records through this gate anymore.
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 */
	void requestClose() throws IOException, InterruptedException;

	/**
	 * Initializes the compression objects inside the input channels attached to this gate.
	 * 
	 * @throws CompressionException
	 *         thrown if an error occurs while loading the compression objects
	 */
	void initializeCompressors() throws CompressionException;

	/**
	 * Removes all output channels from the output gate.
	 */
	void removeAllOutputChannels();

	/**
	 * Creates a new network output channel and assigns it to the given output gate.
	 * 
	 * @param outputGate
	 *        the output gate the channel shall be assigned to
	 * @param channelID
	 *        the ID of the channel
	 * @param connectedChannelID
	 *        the ID of the channel this channel is connected to
	 * @param compressionLevel
	 *        the level of compression to be used for this channel
	 * @return the new network output channel
	 */
	void createNetworkOutputChannel(OutputGate<T> outputGate, ChannelID channelID,
			ChannelID connectedChannelID, CompressionLevel compressionLevel);

	/**
	 * Creates a new file output channel and assigns it to the given output gate.
	 * 
	 * @param outputGate
	 *        the output gate the channel shall be assigned to
	 * @param channelID
	 *        the ID of the channel
	 * @param connectedChannelID
	 *        the ID of the channel this channel is connected to
	 * @param compressionLevel
	 *        the level of compression to be used for this channel
	 * @return the new file output channel
	 */
	void createFileOutputChannel(OutputGate<T> outputGate, ChannelID channelID,
			ChannelID connectedChannelID, CompressionLevel compressionLevel);

	/**
	 * Creates a new in-memory output channel and assigns it to the given output gate.
	 * 
	 * @param outputGate
	 *        the output gate the channel shall be assigned to
	 * @param channelID
	 *        the ID of the channel
	 * @param connectedChannelID
	 *        the ID of the channel this channel is connected to
	 * @param compressionLevel
	 *        the level of compression to be used for this channel
	 * @return the new in-memory output channel
	 */
	void createInMemoryOutputChannel(OutputGate<T> outputGate, ChannelID channelID, ChannelID connectedChannelID,
			CompressionLevel compressionLevel);
}
