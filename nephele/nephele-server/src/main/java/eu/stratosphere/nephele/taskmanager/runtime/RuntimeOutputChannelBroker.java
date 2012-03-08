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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.event.task.AbstractEvent;
import eu.stratosphere.nephele.event.task.AbstractTaskEvent;
import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.channels.bytebuffered.AbstractByteBufferedOutputChannel;
import eu.stratosphere.nephele.io.channels.bytebuffered.BufferPairResponse;
import eu.stratosphere.nephele.io.channels.bytebuffered.ByteBufferedChannelCloseEvent;
import eu.stratosphere.nephele.io.channels.bytebuffered.ByteBufferedOutputChannelBroker;
import eu.stratosphere.nephele.taskmanager.bufferprovider.BufferProvider;
import eu.stratosphere.nephele.taskmanager.bytebuffered.AbstractOutputChannelForwarder;
import eu.stratosphere.nephele.taskmanager.bytebuffered.OutputChannelForwardingChain;
import eu.stratosphere.nephele.taskmanager.bytebuffered.ReceiverNotFoundEvent;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelope;

final class RuntimeOutputChannelBroker extends AbstractOutputChannelForwarder implements
		ByteBufferedOutputChannelBroker {

	/**
	 * The static object used for logging.
	 */
	private static final Log LOG = LogFactory.getLog(RuntimeOutputChannelBroker.class);

	/**
	 * The byte buffered output channel this context belongs to.
	 */
	private final AbstractByteBufferedOutputChannel<?> byteBufferedOutputChannel;

	/**
	 * The buffer provider this channel broker to obtain buffers from.
	 */
	private final BufferProvider bufferProvider;

	/**
	 * The forwarding chain along which the created transfer envelopes will be pushed.
	 */
	private OutputChannelForwardingChain forwardingChain;

	/**
	 * Points to the {@link TransferEnvelope} object that will be passed to the framework upon
	 * the next <code>releaseWriteBuffers</code> call.
	 */
	private TransferEnvelope outgoingTransferEnvelope = null;

	/**
	 * Stores whether the receiver has acknowledged the close request from this channel.
	 */
	private boolean closeAcknowledgmentReceived = false;

	/**
	 * Stores the last sequence number of the transfer envelope for which the receiver could not be found.
	 */
	private int lastSequenceNumberWithReceiverNotFound = -1;

	/**
	 * The sequence number for the next {@link TransferEnvelope} to be created.
	 */
	private int sequenceNumber = 0;

	RuntimeOutputChannelBroker(final BufferProvider bufferProvider,
			final AbstractByteBufferedOutputChannel<?> byteBufferedOutputChannel,
			final AbstractOutputChannelForwarder next) {

		super(next);

		if (next == null) {
			throw new IllegalArgumentException("Argument next must not be null");
		}

		this.bufferProvider = bufferProvider;
		this.byteBufferedOutputChannel = byteBufferedOutputChannel;
		this.byteBufferedOutputChannel.setByteBufferedOutputChannelBroker(this);
	}

	public void setForwardingChain(final OutputChannelForwardingChain forwardingChain) {
		this.forwardingChain = forwardingChain;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean hasDataLeft() throws IOException, InterruptedException {

		// Don't wait for an acknowledgment in case of a file channel, receiver is not running anyway
		if (this.byteBufferedOutputChannel.getType() == ChannelType.FILE) {
			return getNext().hasDataLeft();
		}

		if (this.closeAcknowledgmentReceived) {
			return getNext().hasDataLeft();
		}

		if ((this.lastSequenceNumberWithReceiverNotFound + 1) == this.sequenceNumber) {
			return getNext().hasDataLeft();
		}

		return true;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void processEvent(final AbstractEvent event) {

		if (event instanceof ByteBufferedChannelCloseEvent) {
			this.closeAcknowledgmentReceived = true;
		} else if (event instanceof ReceiverNotFoundEvent) {
			this.lastSequenceNumberWithReceiverNotFound = ((ReceiverNotFoundEvent) event).getSequenceNumber();
		} else if (event instanceof AbstractTaskEvent) {
			this.byteBufferedOutputChannel.processEvent(event);
		}

		getNext().processEvent(event);
	}

	@Override
	public BufferPairResponse requestEmptyWriteBuffers() throws InterruptedException, IOException {

		if (this.outgoingTransferEnvelope == null) {
			this.outgoingTransferEnvelope = createNewOutgoingTransferEnvelope();
		} else {
			if (this.outgoingTransferEnvelope.getBuffer() != null) {
				LOG.error("Channel " + this.byteBufferedOutputChannel.getID()
					+ "'s transfer envelope already has a buffer attached");
				return null;
			}
		}

		final int uncompressedBufferSize = calculateBufferSize();

		// TODO: This implementation breaks compression, we have to fix it later
		final Buffer buffer = this.bufferProvider.requestEmptyBufferBlocking(uncompressedBufferSize);
		final BufferPairResponse bufferResponse = new BufferPairResponse(null, buffer);

		// Put the buffer into the transfer envelope
		this.outgoingTransferEnvelope.setBuffer(bufferResponse.getUncompressedDataBuffer());

		return bufferResponse;
	}

	/**
	 * Creates a new {@link TransferEnvelope} object. The method assigns
	 * and increases the sequence number. Moreover, it will look up the list of receivers for this transfer envelope.
	 * This method will block until the lookup is completed.
	 * 
	 * @return a new {@link TransferEnvelope} object containing the correct sequence number and receiver list
	 */
	private TransferEnvelope createNewOutgoingTransferEnvelope() {

		final TransferEnvelope transferEnvelope = new TransferEnvelope(this.sequenceNumber++,
			this.byteBufferedOutputChannel.getJobID(),
			this.byteBufferedOutputChannel.getID());

		return transferEnvelope;
	}

	/**
	 * Calculates the recommended size of the next buffer to be
	 * handed to the attached channel object in bytes.
	 * 
	 * @return the recommended size of the next buffer in bytes
	 */
	private int calculateBufferSize() {

		// TODO: Include latency considerations
		return this.bufferProvider.getMaximumBufferSize();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void releaseWriteBuffers() throws IOException, InterruptedException {

		// Check for events
		this.forwardingChain.processQueuedEvents();

		if (this.outgoingTransferEnvelope == null) {
			LOG.error("Cannot find transfer envelope for channel with ID " + this.byteBufferedOutputChannel.getID());
			return;
		}

		// Consistency check
		if (this.outgoingTransferEnvelope.getBuffer() == null) {
			LOG.error("Channel " + this.byteBufferedOutputChannel.getID() + " has no buffer attached");
			return;
		}

		// Finish the write phase of the buffer
		final Buffer buffer = this.outgoingTransferEnvelope.getBuffer();
		buffer.finishWritePhase();

		this.forwardingChain.pushEnvelope(this.outgoingTransferEnvelope);
		this.outgoingTransferEnvelope = null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean hasDataLeftToTransmit() throws IOException, InterruptedException {

		// Check for events
		this.forwardingChain.processQueuedEvents();

		return this.forwardingChain.anyForwarderHasDataLeft();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void transferEventToInputChannel(final AbstractEvent event) throws IOException, InterruptedException {

		if (this.outgoingTransferEnvelope != null) {
			this.outgoingTransferEnvelope.addEvent(event);
		} else {

			final TransferEnvelope ephemeralTransferEnvelope = createNewOutgoingTransferEnvelope();
			ephemeralTransferEnvelope.addEvent(event);

			this.forwardingChain.pushEnvelope(ephemeralTransferEnvelope);
		}
	}
}
