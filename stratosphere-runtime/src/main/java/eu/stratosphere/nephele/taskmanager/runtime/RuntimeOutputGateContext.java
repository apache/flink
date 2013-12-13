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

package eu.stratosphere.nephele.taskmanager.runtime;

import java.io.IOException;

import eu.stratosphere.nephele.io.AbstractID;
import eu.stratosphere.nephele.io.GateID;
import eu.stratosphere.nephele.io.OutputGate;
import eu.stratosphere.nephele.io.channels.AbstractOutputChannel;
import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.bytebuffered.AbstractByteBufferedOutputChannel;
import eu.stratosphere.nephele.taskmanager.bufferprovider.BufferAvailabilityListener;
import eu.stratosphere.nephele.taskmanager.bufferprovider.BufferProvider;
import eu.stratosphere.nephele.taskmanager.bytebuffered.AbstractOutputChannelForwarder;
import eu.stratosphere.nephele.taskmanager.bytebuffered.OutputChannelContext;
import eu.stratosphere.nephele.taskmanager.bytebuffered.OutputChannelForwardingChain;
import eu.stratosphere.nephele.taskmanager.bytebuffered.OutputGateContext;
import eu.stratosphere.nephele.types.Record;

final class RuntimeOutputGateContext implements BufferProvider, OutputGateContext {

	private final RuntimeTaskContext taskContext;

	private final OutputGate<? extends Record> outputGate;

	RuntimeOutputGateContext(final RuntimeTaskContext taskContext, final OutputGate<? extends Record> outputGate) {

		this.taskContext = taskContext;
		this.outputGate = outputGate;
	}

	AbstractID getFileOwnerID() {

		return this.taskContext.getFileOwnerID();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getMaximumBufferSize() {

		return this.taskContext.getMaximumBufferSize();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Buffer requestEmptyBuffer(int minimumSizeOfBuffer) throws IOException {

		return this.taskContext.requestEmptyBuffer(minimumSizeOfBuffer);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Buffer requestEmptyBufferBlocking(int minimumSizeOfBuffer) throws IOException, InterruptedException {

		Buffer buffer = this.taskContext.requestEmptyBuffer(minimumSizeOfBuffer);

		// No memory-based buffer available
		if (buffer == null) {
			// Wait until a memory-based buffer is available
			buffer = this.taskContext.requestEmptyBufferBlocking(minimumSizeOfBuffer);
		}

		return buffer;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isShared() {

		return this.taskContext.isShared();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void reportAsynchronousEvent() {

		this.taskContext.reportAsynchronousEvent();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public GateID getGateID() {

		return this.outputGate.getGateID();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public OutputChannelContext createOutputChannelContext(ChannelID channelID, OutputChannelContext previousContext,
			boolean isReceiverRunning, boolean mergeSpillBuffers) {

		if (previousContext != null) {
			throw new IllegalStateException("Found previous output context for channel " + channelID);
		}

		AbstractOutputChannel<? extends Record> channel = null;
		for (int i = 0; i < this.outputGate.getNumberOfOutputChannels(); ++i) {
			AbstractOutputChannel<? extends Record> candidateChannel = this.outputGate.getOutputChannel(i);
			if (candidateChannel.getID().equals(channelID)) {
				channel = candidateChannel;
				break;
			}
		}

		if (channel == null) {
			throw new IllegalArgumentException("Cannot find output channel with ID " + channelID);
		}

		if (!(channel instanceof AbstractByteBufferedOutputChannel)) {
			throw new IllegalStateException("Channel with ID" + channelID
				+ " is not of type AbstractByteBufferedOutputChannel");
		}

		// The output channel for this context
		final AbstractByteBufferedOutputChannel<? extends Record> outputChannel = (AbstractByteBufferedOutputChannel<? extends Record>) channel;

		// Construct the forwarding chain
		RuntimeOutputChannelBroker outputChannelBroker;
		AbstractOutputChannelForwarder last;
		// Construction for in-memory and network channels
		final RuntimeDispatcher runtimeDispatcher = new RuntimeDispatcher(
			this.taskContext.getTransferEnvelopeDispatcher());
		/*
		 * final SpillingBarrier spillingBarrier = new SpillingBarrier(isReceiverRunning, mergeSpillBuffers,
		 * runtimeDispatcher);
		 * final ForwardingBarrier forwardingBarrier = new ForwardingBarrier(channelID, spillingBarrier);
		 */
		final ForwardingBarrier forwardingBarrier = new ForwardingBarrier(channelID, runtimeDispatcher);
		outputChannelBroker = new RuntimeOutputChannelBroker(this, outputChannel, forwardingBarrier);
		last = runtimeDispatcher;

		final OutputChannelForwardingChain forwardingChain = new OutputChannelForwardingChain(outputChannelBroker, last);

		// Set forwarding chain for broker
		outputChannelBroker.setForwardingChain(forwardingChain);

		return new RuntimeOutputChannelContext(outputChannel, forwardingChain);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean registerBufferAvailabilityListener(final BufferAvailabilityListener bufferAvailabilityListener) {

		return this.taskContext.registerBufferAvailabilityListener(bufferAvailabilityListener);
	}
}
