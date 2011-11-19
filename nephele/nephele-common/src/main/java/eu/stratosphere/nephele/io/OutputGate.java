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
import java.util.List;

import eu.stratosphere.nephele.io.channels.AbstractOutputChannel;
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
	 * Returns the type of record that can be transported through this gate.
	 * 
	 * @return the type of record that can be transported through this gate
	 */
	Class<T> getType();

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
	 * Returns all the OutputChannels connected to this gate
	 * 
	 * @return the list of OutputChannels connected to this RecordWriter
	 */
	List<AbstractOutputChannel<T>> getOutputChannels();

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
	 * This method is called by one of the attached output channel when its
	 * capacity is currently exhausted and no more data can be written to the channel.
	 * 
	 * @param channelIndex
	 *        the index of the exhausted output channel.
	 */
	void channelCapacityExhausted(int channelIndex);

	/**
	 * Checks if this output gate operates in broadcast mode, i.e. all records passed to it are transferred through all
	 * connected output channels.
	 * 
	 * @return <code>true</code> if this output gate operates in broadcast mode, <code>false</code> otherwise
	 */
	boolean isBroadcast();

	/**
	 * Returns the number of output channels associated with this output gate.
	 * 
	 * @return the number of output channels associated with this output gate
	 */
	int getNumberOfOutputChannels();
}
