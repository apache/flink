/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.nephele.taskmanager.runtime;

import java.io.IOException;

import eu.stratosphere.core.io.IOReadableWritable;
import eu.stratosphere.nephele.io.GateID;
import eu.stratosphere.nephele.io.InputGate;
import eu.stratosphere.nephele.io.channels.AbstractInputChannel;
import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.bytebuffered.AbstractByteBufferedInputChannel;
import eu.stratosphere.nephele.taskmanager.bufferprovider.BufferAvailabilityListener;
import eu.stratosphere.nephele.taskmanager.bufferprovider.BufferProvider;
import eu.stratosphere.nephele.taskmanager.bufferprovider.LocalBufferPool;
import eu.stratosphere.nephele.taskmanager.bufferprovider.LocalBufferPoolOwner;
import eu.stratosphere.nephele.taskmanager.bytebuffered.InputChannelContext;
import eu.stratosphere.nephele.taskmanager.bytebuffered.InputGateContext;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelopeDispatcher;

final class RuntimeInputGateContext implements BufferProvider, InputGateContext, LocalBufferPoolOwner {

	private final String taskName;

	private final LocalBufferPool localBufferPool;

	private final TransferEnvelopeDispatcher transferEnvelopeDispatcher;

	private final InputGate<? extends IOReadableWritable> inputGate;

	RuntimeInputGateContext(final String taskName, final TransferEnvelopeDispatcher transferEnvelopeDispatcher,
			final InputGate<? extends IOReadableWritable> inputGate) {

		this.taskName = taskName;
		this.localBufferPool = new LocalBufferPool(1, false);

		this.transferEnvelopeDispatcher = transferEnvelopeDispatcher;
		this.inputGate = inputGate;
	}

	@Override
	public Buffer requestEmptyBuffer(final int minimumSizeOfBuffer) throws IOException {

		return this.localBufferPool.requestEmptyBuffer(minimumSizeOfBuffer);
	}


	@Override
	public Buffer requestEmptyBufferBlocking(final int minimumSizeOfBuffer) throws IOException, InterruptedException {

		final Buffer buffer = this.localBufferPool.requestEmptyBuffer(minimumSizeOfBuffer);
		if (buffer != null) {
			return buffer;
		}

		return this.localBufferPool.requestEmptyBufferBlocking(minimumSizeOfBuffer);
	}


	@Override
	public int getMaximumBufferSize() {

		return this.localBufferPool.getMaximumBufferSize();
	}


	@Override
	public boolean isShared() {

		return this.localBufferPool.isShared();
	}


	@Override
	public void reportAsynchronousEvent() {

		this.localBufferPool.reportAsynchronousEvent();
	}

	@Override
	public int getNumberOfChannels() {

		return this.inputGate.getNumberOfInputChannels();
	}


	@Override
	public void setDesignatedNumberOfBuffers(int numberOfBuffers) {

		this.localBufferPool.setDesignatedNumberOfBuffers(numberOfBuffers);
	}


	@Override
	public void clearLocalBufferPool() {

		this.localBufferPool.destroy();
	}


	@Override
	public void logBufferUtilization() {

		final int ava = this.localBufferPool.getNumberOfAvailableBuffers();
		final int req = this.localBufferPool.getRequestedNumberOfBuffers();
		final int des = this.localBufferPool.getDesignatedNumberOfBuffers();

		System.out
			.println("\t\tInput gate " + this.inputGate.getIndex() + " of " + this.taskName + ": " + ava
				+ " available, " + req + " requested, " + des + " designated");
	}


	@Override
	public GateID getGateID() {

		return this.inputGate.getGateID();
	}


	@Override
	public InputChannelContext createInputChannelContext(final ChannelID channelID,
			final InputChannelContext previousContext) {

		AbstractInputChannel<? extends IOReadableWritable> channel = null;
		for (int i = 0; i < this.inputGate.getNumberOfInputChannels(); ++i) {
			AbstractInputChannel<? extends IOReadableWritable> candidateChannel = this.inputGate.getInputChannel(i);
			if (candidateChannel.getID().equals(channelID)) {
				channel = candidateChannel;
				break;
			}
		}

		if (channel == null) {
			throw new IllegalArgumentException("Cannot find input channel with ID " + channelID);
		}

		if (!(channel instanceof AbstractByteBufferedInputChannel)) {
			throw new IllegalStateException("Channel with ID" + channelID
				+ " is not of type AbstractByteBufferedInputChannel");
		}

		return new RuntimeInputChannelContext(this, this.transferEnvelopeDispatcher,
			(AbstractByteBufferedInputChannel<? extends IOReadableWritable>) channel);
	}


	@Override
	public LocalBufferPoolOwner getLocalBufferPoolOwner() {

		return this;
	}

	/**
	 * Returns the name of the task this gate belongs to.
	 * 
	 * @return the name of the task this gate belongs to
	 */
	String getTaskName() {

		return this.taskName;
	}


	@Override
	public boolean registerBufferAvailabilityListener(final BufferAvailabilityListener bufferAvailabilityListener) {

		return this.localBufferPool.registerBufferAvailabilityListener(bufferAvailabilityListener);
	}

}
