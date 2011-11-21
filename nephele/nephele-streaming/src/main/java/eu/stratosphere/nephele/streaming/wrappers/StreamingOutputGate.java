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

package eu.stratosphere.nephele.streaming.wrappers;

import java.io.IOException;

import eu.stratosphere.nephele.io.OutputGate;
import eu.stratosphere.nephele.plugins.wrapper.AbstractOutputGateWrapper;
import eu.stratosphere.nephele.streaming.listeners.StreamListener;
import eu.stratosphere.nephele.types.Record;

public final class StreamingOutputGate<T extends Record> extends AbstractOutputGateWrapper<T> {

	private final StreamListener streamListener;

	private long lastTimestamp = -1L;

	private long[] lastSentBytes = null;

	StreamingOutputGate(final OutputGate<T> wrappedOutputGate, final StreamListener streamListener) {
		super(wrappedOutputGate);

		if (streamListener == null) {
			throw new IllegalArgumentException("Argument streamListener must not be null");
		}

		this.streamListener = streamListener;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void writeRecord(final T record) throws IOException, InterruptedException {

		final long timestamp = this.streamListener.recordEmitted(record);
		if (timestamp >= 0) {

			final int numberOfOutputChannels = getNumberOfOutputChannels();

			if (this.lastTimestamp < 0) {
				// Initialize array and fill it
				this.lastSentBytes = new long[numberOfOutputChannels];
				for (int i = 0; i < numberOfOutputChannels; ++i) {
					this.lastSentBytes[i] = getOutputChannel(i).getAmountOfDataTransmitted();
				}
			} else {
				for (int i = 0; i < numberOfOutputChannels; ++i) {
					final long amountOfDataTransmitted = getOutputChannel(i).getAmountOfDataTransmitted(); 
					final long dataDiff = amountOfDataTransmitted - this.lastSentBytes[i];
					this.lastSentBytes[i] = amountOfDataTransmitted;
					System.out.println("Data diff " + dataDiff);
				}
			}

			this.lastTimestamp = timestamp;
		}

		getWrappedOutputGate().writeRecord(record);
	}
}
