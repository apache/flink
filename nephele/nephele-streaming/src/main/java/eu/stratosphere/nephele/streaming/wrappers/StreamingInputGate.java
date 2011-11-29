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

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayDeque;

import eu.stratosphere.nephele.io.InputGate;
import eu.stratosphere.nephele.plugins.wrapper.AbstractInputGateWrapper;
import eu.stratosphere.nephele.streaming.listeners.StreamListener;
import eu.stratosphere.nephele.types.Record;

public final class StreamingInputGate<T extends Record> extends AbstractInputGateWrapper<T> {

	private final StreamListener streamListener;

	/**
	 * Queue with indices of channels that store at least one available record.
	 */
	private final ArrayDeque<Integer> availableChannels = new ArrayDeque<Integer>();

	/**
	 * The channel to read from next.
	 */
	private int channelToReadFrom = -1;

	/**
	 * The value returned by the last call of waitForAnyChannelToBecomeAvailable
	 */
	private int availableChannelRetVal = -1;

	/**
	 * The thread which executes the task connected to the input gate.
	 */
	private Thread executingThread = null;

	StreamingInputGate(final InputGate<T> wrappedInputGate, final StreamListener streamListener) {
		super(wrappedInputGate);

		if (streamListener == null) {
			throw new IllegalArgumentException("Argument streamListener must not be null");
		}

		this.streamListener = streamListener;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public T readRecord(final T target) throws IOException, InterruptedException {

		T record = null;

		if (this.executingThread == null) {
			this.executingThread = Thread.currentThread();
		}

		if (this.executingThread.isInterrupted()) {
			throw new InterruptedException();
		}

		final int numberOfInputChannels = getNumberOfInputChannels();

		while (true) {

			if (this.channelToReadFrom == -1) {
				this.availableChannelRetVal = waitForAnyChannelToBecomeAvailable();
				this.channelToReadFrom = this.availableChannelRetVal;
			}
			try {
				record = this.getInputChannel(this.channelToReadFrom).readRecord(target);
			} catch (EOFException e) {
				// System.out.println("### Caught EOF exception at channel " + channelToReadFrom + "(" +
				// this.getInputChannel(channelToReadFrom).getType().toString() + ")");
				if (this.isClosed()) {
					return null;
				}
			}

			if (record == null && this.channelToReadFrom == this.availableChannelRetVal) {
				this.channelToReadFrom = -1;
				continue;
			}

			if (++this.channelToReadFrom == numberOfInputChannels) {
				this.channelToReadFrom = 0;
			}

			if (record != null) {
				break;
			}
		}

		reportRecordReceived(record);

		return record;
	}

	public void reportRecordReceived(final Record record) {
		
		this.streamListener.recordReceived(record);
	}
	
	/**
	 * This method returns the index of a channel which has at least
	 * one record available. The method may block until at least one
	 * channel has become ready.
	 * 
	 * @return the index of the channel which has at least one record available
	 */
	public int waitForAnyChannelToBecomeAvailable() throws InterruptedException {

		synchronized (this.availableChannels) {

			while (this.availableChannels.isEmpty()) {
				this.availableChannels.wait();
			}

			return this.availableChannels.removeFirst().intValue();
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void notifyRecordIsAvailable(final int channelIndex) {

		synchronized (this.availableChannels) {

			this.availableChannels.add(Integer.valueOf(channelIndex));
			this.availableChannels.notify();
		}
	}
}
