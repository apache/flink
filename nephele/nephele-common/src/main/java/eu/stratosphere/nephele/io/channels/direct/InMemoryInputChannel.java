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

import eu.stratosphere.nephele.io.InputGate;
import eu.stratosphere.nephele.io.RecordDeserializer;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.types.Record;

/**
 * In-memory input channels are a concrete implementation of input channels. In-memory input channels directly read
 * records from corresponding
 * in-memory output channels without deserialization. Two vertices connected by in-memory channels must run in the same
 * stage (i.e. at the same time)
 * and on the same instance.
 * 
 * @author warneke
 * @param <T>
 *        the type of record that can be transported through this channel
 */
public final class InMemoryInputChannel<T extends Record> extends AbstractDirectInputChannel<T> {

	public InMemoryInputChannel(InputGate<T> inputGate, int channelIndex, RecordDeserializer<T> deserializer,
			ChannelID channelID, CompressionLevel compressionLevel) {
		super(inputGate, channelIndex, deserializer, channelID, compressionLevel);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public ChannelType getType() {

		return ChannelType.INMEMORY;
	}
}