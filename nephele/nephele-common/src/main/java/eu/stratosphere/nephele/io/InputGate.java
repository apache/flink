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
	 */
	void notifyRecordIsAvailable(int channelIndex);
}
