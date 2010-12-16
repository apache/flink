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

package eu.stratosphere.nephele.io.channels.direct;

import eu.stratosphere.nephele.io.OutputGate;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.types.Record;

/**
 * In-memory output channels are a concrete implementation of output channels. In-memory output channels directly write
 * records to corresponding
 * in-memory input channels without serialization. Two vertices connected by in-memory channels must run in the same
 * stage (i.e. at the same time)
 * and on the same instance.
 * 
 * @author warneke
 * @param <T>
 *        the type of record that can be transported through this channel
 */
public final class InMemoryOutputChannel<T extends Record> extends AbstractDirectOutputChannel<T> {

	/**
	 * Creates a new in-memory output channel.
	 * 
	 * @param outputGate
	 *        the output gate the new channel is wired to.
	 * @param channelIndex
	 *        the channel's index at the associated output gate
	 * @param channelID
	 *        the channel ID to assign to the new channel, <code>null</code> to generate a new ID
	 * @param compressionLevel
	 *        the level of compression to be used for this channel
	 */
	public InMemoryOutputChannel(OutputGate<T> outputGate, int channelIndex, ChannelID channelID,
			CompressionLevel compressionLevel) {
		super(outputGate, channelIndex, channelID, compressionLevel);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public ChannelType getType() {

		return ChannelType.INMEMORY;
	}
}
