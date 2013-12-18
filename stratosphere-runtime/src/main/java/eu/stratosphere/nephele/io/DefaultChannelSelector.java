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

import eu.stratosphere.core.io.IOReadableWritable;

/**
 * This is the default implementation of the {@link ChannelSelector} interface. It represents a simple round-robin
 * strategy, i.e. regardless of the record every attached exactly one output channel is selected at a time.

 * @param <T>
 *        the type of record which is sent through the attached output gate
 */
public class DefaultChannelSelector<T extends IOReadableWritable> implements ChannelSelector<T> {

	/**
	 * Stores the index of the channel to send the next record to.
	 */
	private final int[] nextChannelToSendTo = new int[1];

	/**
	 * Constructs a new default channel selector.
	 */
	public DefaultChannelSelector() {
		this.nextChannelToSendTo[0] = 0;
	}


	@Override
	public int[] selectChannels(final T record, final int numberOfOutpuChannels) {

		this.nextChannelToSendTo[0] = (this.nextChannelToSendTo[0] + 1) % numberOfOutpuChannels;

		return this.nextChannelToSendTo;
	}
}
