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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.types.Record;

/**
 * This is the default implementation of the {@link ChannelSelector} interface. It represents a simple round-robin
 * strategy, i.e. regardless of the record every attached exactly one output channel is selected at a time.
 * 
 * @author warneke
 * @param <T>
 *        the type of record which is sent through the attached output gate
 */
public class DefaultChannelSelector<T extends Record> implements ChannelSelector<T> {

	private int nextChannelToSendTo = 0;

	/**
	 * {@inheritDoc}
	 */
	@Override

	public void selectChannels(T record, boolean[] channelFlags) {
		if(channelFlags.length == 0){
			return;
		}
		this.nextChannelToSendTo = (this.nextChannelToSendTo + 1) % channelFlags.length;
		channelFlags[this.nextChannelToSendTo] = true;

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(final DataInput in) throws IOException {

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput out) throws IOException {

	}
}
