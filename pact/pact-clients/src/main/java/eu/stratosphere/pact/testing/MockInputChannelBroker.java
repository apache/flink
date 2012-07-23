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
import eu.stratosphere.nephele.event.task.EventList;
import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.io.channels.bytebuffered.AbstractByteBufferedInputChannel;
import eu.stratosphere.nephele.io.channels.bytebuffered.ByteBufferedInputChannelBroker;
import eu.stratosphere.nephele.io.compression.CompressionException;
import eu.stratosphere.nephele.io.compression.Decompressor;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelope;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelopeDispatcher;

/**
 * @author Arvid Heise
 */
public class MockInputChannelBroker implements ByteBufferedInputChannelBroker, MockChannelBroker {
	private final AbstractByteBufferedInputChannel<?> bbic;

	private TransferEnvelopeDispatcher transferEnvelopeDispatcher;

	private Queue<TransferEnvelope> queuedEnvelopes = new LinkedList<TransferEnvelope>();

	/**
	 * Initializes MockInputChannelBroker.
	 * 
	 * @param bbic
	 * @param transitBufferPool
	 */
	public MockInputChannelBroker(AbstractByteBufferedInputChannel<?> bbic,
			TransferEnvelopeDispatcher transferEnvelopeDispatcher) {
		this.bbic = bbic;
		this.transferEnvelopeDispatcher = transferEnvelopeDispatcher;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.channels.bytebuffered.ByteBufferedInputChannelBroker#releaseConsumedReadBuffer()
	 */
	@Override
	public void releaseConsumedReadBuffer(final Buffer buffer) {
		TransferEnvelope transferEnvelope = null;
		synchronized (this.queuedEnvelopes) {

			if (this.queuedEnvelopes.isEmpty())
				return;

			transferEnvelope = this.queuedEnvelopes.poll();
		}

		final Buffer consumedBuffer = transferEnvelope.getBuffer();
		if (consumedBuffer == null)
			return;

		// Recycle consumed read buffer
		buffer.recycleBuffer();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.channels.bytebuffered.ByteBufferedInputChannelBroker#getReadBufferToConsume()
	 */
	@Override
	public Buffer getReadBufferToConsume() {
		TransferEnvelope transferEnvelope = null;

		synchronized (this.queuedEnvelopes) {

			if (this.queuedEnvelopes.isEmpty())
				return null;

			transferEnvelope = this.queuedEnvelopes.peek();

			// If envelope does not have a buffer, remove it immediately
			if (transferEnvelope.getBuffer() == null)
				this.queuedEnvelopes.poll();
		}

		// Make sure we have all necessary buffers before we go on
		if (transferEnvelope.getBuffer() == null) {

			// No buffers necessary
			final EventList eventList = transferEnvelope.getEventList();
			if (eventList != null)
				if (!eventList.isEmpty()) {
					final Iterator<AbstractEvent> it = eventList.iterator();
					while (it.hasNext())
						this.bbic.processEvent(it.next());
				}

			return null;
		}

		final Buffer buffer = transferEnvelope.getBuffer(); // No need to copy anything

		// Process events
		final EventList eventList = transferEnvelope.getEventList();
		if (eventList != null)
			if (!eventList.isEmpty()) {
				final Iterator<AbstractEvent> it = eventList.iterator();
				while (it.hasNext())
					this.bbic.processEvent(it.next());
			}

		return buffer;
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.nephele.io.channels.bytebuffered.ByteBufferedInputChannelBroker#transferEventToOutputChannel(
	 * eu.stratosphere.nephele.event.task.AbstractEvent)
	 */
	@Override
	public void transferEventToOutputChannel(AbstractEvent event) throws IOException, InterruptedException {
		final TransferEnvelope ephemeralTransferEnvelope = new TransferEnvelope(0, this.bbic.getJobID(),
			this.bbic.getID());
		ephemeralTransferEnvelope.addEvent(event);
		this.transferEnvelopeDispatcher.processEnvelopeFromInputChannel(ephemeralTransferEnvelope);
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.pact.testing.MockChannelBroker#queueTransferEnvelope(eu.stratosphere.nephele.event.task.AbstractEvent
	 * )
	 */
	@Override
	public void queueTransferEnvelope(TransferEnvelope transferEnvelope) {
		synchronized (this.queuedEnvelopes) {
			this.queuedEnvelopes.add(transferEnvelope);
		}

		// Notify the channel about the new data
		this.bbic.checkForNetworkEvents();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.testing.MockChannelBroker#getChannel()
	 */
	@Override
	public AbstractByteBufferedInputChannel<?> getChannel() {
		return this.bbic;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.channels.bytebuffered.ByteBufferedInputChannelBroker#getDecompressor()
	 */
	@Override
	public Decompressor getDecompressor() throws CompressionException {

		return null;
	}
}
