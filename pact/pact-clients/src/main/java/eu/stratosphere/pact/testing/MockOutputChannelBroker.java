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
package eu.stratosphere.pact.testing;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

import eu.stratosphere.nephele.event.task.AbstractEvent;
import eu.stratosphere.nephele.event.task.AbstractTaskEvent;
import eu.stratosphere.nephele.io.channels.AbstractByteBufferedOutputChannel;
import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.io.channels.ByteBufferedOutputChannelBroker;
import eu.stratosphere.nephele.io.compression.CompressionException;
import eu.stratosphere.nephele.io.compression.Compressor;
import eu.stratosphere.nephele.taskmanager.bufferprovider.LocalBufferPool;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelope;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelopeDispatcher;

/**
 * @author Arvid Heise
 */
public class MockOutputChannelBroker implements ByteBufferedOutputChannelBroker, MockChannelBroker {

	private int sequenceNumber;

	/**
	 * The byte buffered output channel this context belongs to.
	 */
	private final AbstractByteBufferedOutputChannel<?> byteBufferedOutputChannel;

	private LocalBufferPool transitBufferPool;

	private Queue<TransferEnvelope> queuedOutgoingEnvelopes = new LinkedList<TransferEnvelope>();

	private TransferEnvelopeDispatcher transferEnvelopeDispatcher;

	private TransferEnvelope outgoingTransferEnvelope;

	public MockOutputChannelBroker(AbstractByteBufferedOutputChannel<?> byteBufferedOutputChannel,
			LocalBufferPool transitBufferPool, TransferEnvelopeDispatcher transferEnvelopeDispatcher) {
		this.byteBufferedOutputChannel = byteBufferedOutputChannel;
		this.transitBufferPool = transitBufferPool;
		this.transferEnvelopeDispatcher = transferEnvelopeDispatcher;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.channels.bytebuffered.ByteBufferedOutputChannelBroker#requestEmptyWriteBuffers()
	 */
	@Override
	public Buffer requestEmptyWriteBuffer() throws InterruptedException, IOException {

		this.outgoingTransferEnvelope = this.newEnvelope();
		final int uncompressedBufferSize = this.transitBufferPool.getMaximumBufferSize();

		return this.transitBufferPool.requestEmptyBuffer(uncompressedBufferSize);
	}

	protected TransferEnvelope newEnvelope() {
		TransferEnvelope transferEnvelope = new TransferEnvelope(this.sequenceNumber++,
			this.byteBufferedOutputChannel.getJobID(),
			this.byteBufferedOutputChannel.getID());

		return transferEnvelope;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.channels.bytebuffered.ByteBufferedOutputChannelBroker#releaseWriteBuffers()
	 */
	@Override
	public void releaseWriteBuffer(final Buffer buffer) throws IOException, InterruptedException {

		// Finish the write phase of the buffer
		buffer.finishWritePhase();

		this.outgoingTransferEnvelope.setBuffer(buffer);

		if (this.queuedOutgoingEnvelopes.isEmpty())
			this.transferEnvelopeDispatcher.processEnvelopeFromOutputChannel(this.outgoingTransferEnvelope);
		else {
			this.queuedOutgoingEnvelopes.add(this.outgoingTransferEnvelope);
			this.flushQueuedOutgoingEnvelopes();
		}

		this.outgoingTransferEnvelope = null;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.channels.bytebuffered.ByteBufferedOutputChannelBroker#hasDataLeftToTransmit()
	 */
	@Override
	public boolean hasDataLeftToTransmit() throws IOException, InterruptedException {
		this.flushQueuedOutgoingEnvelopes();

		return !this.queuedOutgoingEnvelopes.isEmpty();
	}

	protected void flushQueuedOutgoingEnvelopes() throws IOException, InterruptedException {
		while (!this.queuedOutgoingEnvelopes.isEmpty())
			this.transferEnvelopeDispatcher.processEnvelopeFromOutputChannel(this.queuedOutgoingEnvelopes.poll());
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.testing.MockChannelBroker#queueTransferEnvelope(eu.stratosphere.nephele.taskmanager.
	 * transferenvelope.TransferEnvelope)
	 */
	@Override
	public void queueTransferEnvelope(TransferEnvelope transferEnvelope) {
		final Iterator<AbstractEvent> it = transferEnvelope.getEventList().iterator();
		while (it.hasNext()) {

			final AbstractEvent event = it.next();

			if (event instanceof AbstractTaskEvent) {
				this.byteBufferedOutputChannel.processEvent(event);
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.nephele.io.channels.bytebuffered.ByteBufferedOutputChannelBroker#transferEventToInputChannel(
	 * eu.stratosphere.nephele.event.task.AbstractEvent)
	 */
	@Override
	public void transferEventToInputChannel(AbstractEvent event) throws IOException, InterruptedException {

		if (this.outgoingTransferEnvelope != null)
			this.outgoingTransferEnvelope.addEvent(event);
		else {

			final TransferEnvelope ephemeralTransferEnvelope = this.newEnvelope();
			ephemeralTransferEnvelope.addEvent(event);

			if (this.queuedOutgoingEnvelopes.isEmpty())
				this.transferEnvelopeDispatcher.processEnvelopeFromOutputChannel(ephemeralTransferEnvelope);
			else {
				this.queuedOutgoingEnvelopes.add(ephemeralTransferEnvelope);
				this.flushQueuedOutgoingEnvelopes();
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.testing.MockChannelBroker#getChannel()
	 */
	@Override
	public AbstractByteBufferedOutputChannel<?> getChannel() {
		return this.byteBufferedOutputChannel;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.channels.bytebuffered.ByteBufferedOutputChannelBroker#getCompressor()
	 */
	@Override
	public Compressor getCompressor() throws CompressionException {

		return null;
	}
}
