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

package eu.stratosphere.nephele.taskmanager.bytebuffered;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Queue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.event.task.AbstractEvent;
import eu.stratosphere.nephele.event.task.EventList;
import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.bytebuffered.AbstractByteBufferedInputChannel;
import eu.stratosphere.nephele.io.channels.bytebuffered.BufferPairResponse;
import eu.stratosphere.nephele.io.channels.bytebuffered.ByteBufferedInputChannelBroker;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.taskmanager.bufferprovider.BufferProvider;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelope;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelopeDispatcher;

final class InputChannelContext implements ChannelContext, ByteBufferedInputChannelBroker, BufferProvider {

	private static final Log LOG = LogFactory.getLog(InputChannelContext.class);

	private final InputGateContext inputGateContext;

	private final AbstractByteBufferedInputChannel<?> byteBufferedInputChannel;

	private final TransferEnvelopeDispatcher transferEnvelopeDispatcher;

	private final Queue<TransferEnvelope> queuedEnvelopes = new ArrayDeque<TransferEnvelope>();

	private int lastReceivedEnvelope = -1;

	InputChannelContext(final InputGateContext inputGateContext,
			final TransferEnvelopeDispatcher transferEnvelopeDispatcher,
			final AbstractByteBufferedInputChannel<?> byteBufferedInputChannel) {

		this.inputGateContext = inputGateContext;
		this.transferEnvelopeDispatcher = transferEnvelopeDispatcher;
		this.byteBufferedInputChannel = byteBufferedInputChannel;
		this.byteBufferedInputChannel.setInputChannelBroker(this);
	}

	@Override
	public BufferPairResponse getReadBufferToConsume() {

		TransferEnvelope transferEnvelope = null;

		synchronized (this.queuedEnvelopes) {

			if (this.queuedEnvelopes.isEmpty()) {
				return null;
			}

			transferEnvelope = this.queuedEnvelopes.peek();

			// If envelope does not have a buffer, remove it immediately
			if (transferEnvelope.getBuffer() == null) {
				this.queuedEnvelopes.poll();
			}
		}

		// Make sure we have all necessary buffers before we go on
		if (transferEnvelope.getBuffer() == null) {

			// No buffers necessary
			final EventList eventList = transferEnvelope.getEventList();
			if (eventList != null) {
				if (!eventList.isEmpty()) {
					final Iterator<AbstractEvent> it = eventList.iterator();
					while (it.hasNext()) {
						this.byteBufferedInputChannel.processEvent(it.next());
					}
				}
			}

			return null;
		}

		// TODO: Fix implementation breaks compression, fix it later on
		final BufferPairResponse response = new BufferPairResponse(null, transferEnvelope.getBuffer()); // No need to

		// Moved event processing to releaseConsumedReadBuffer method // copy anything

		return response;
	}

	@Override
	public void releaseConsumedReadBuffer() {

		TransferEnvelope transferEnvelope = null;
		synchronized (this.queuedEnvelopes) {

			if (this.queuedEnvelopes.isEmpty()) {
				LOG.error("Inconsistency: releaseConsumedReadBuffer called on empty queue!");
				return;
			}

			transferEnvelope = this.queuedEnvelopes.poll();
		}

		// Process events
		final EventList eventList = transferEnvelope.getEventList();
		if (eventList != null) {
			if (!eventList.isEmpty()) {
				final Iterator<AbstractEvent> it = eventList.iterator();
				while (it.hasNext()) {
					this.byteBufferedInputChannel.processEvent(it.next());
				}
			}
		}

		final Buffer consumedBuffer = transferEnvelope.getBuffer();
		if (consumedBuffer == null) {
			LOG.error("Inconsistency: consumed read buffer is null!");
			return;
		}

		if (consumedBuffer.remaining() > 0) {
			LOG.error("consumedReadBuffer has " + consumedBuffer.remaining() + " unconsumed bytes left!!");
		}

		// Recycle consumed read buffer
		consumedBuffer.recycleBuffer();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void transferEventToOutputChannel(AbstractEvent event) throws IOException, InterruptedException {

		final TransferEnvelope ephemeralTransferEnvelope = new TransferEnvelope(0, getJobID(), getChannelID());

		ephemeralTransferEnvelope.addEvent(event);
		this.transferEnvelopeDispatcher.processEnvelopeFromInputChannel(ephemeralTransferEnvelope);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void queueTransferEnvelope(final TransferEnvelope transferEnvelope) {

		// The sequence number of the envelope to be queued
		final int sequenceNumber = transferEnvelope.getSequenceNumber();

		synchronized (this.queuedEnvelopes) {

			final int expectedSequenceNumber = this.lastReceivedEnvelope + 1;
			if (sequenceNumber != expectedSequenceNumber) {

				if (sequenceNumber > expectedSequenceNumber) {
					// This is a problem, now we are actually missing some data
					this.byteBufferedInputChannel.reportIOException(new IOException("Expected data packet "
						+ expectedSequenceNumber + " but received " + sequenceNumber));
					this.byteBufferedInputChannel.checkForNetworkEvents();
				}

				if (LOG.isDebugEnabled()) {
					LOG.info("Input channel " + getChannelID() + " expected envelope " + expectedSequenceNumber
						+ " but received " + sequenceNumber);
				}

				final Buffer buffer = transferEnvelope.getBuffer();
				if (buffer != null) {
					buffer.recycleBuffer();
				}

				return;
			}

			this.queuedEnvelopes.add(transferEnvelope);

			this.lastReceivedEnvelope = sequenceNumber;
		}

		// Notify the channel about the new data
		this.byteBufferedInputChannel.checkForNetworkEvents();
	}

	@Override
	public ChannelID getChannelID() {

		return this.byteBufferedInputChannel.getID();
	}

	@Override
	public ChannelID getConnectedChannelID() {

		return this.byteBufferedInputChannel.getConnectedChannelID();
	}

	@Override
	public JobID getJobID() {

		return this.byteBufferedInputChannel.getJobID();
	}

	@Override
	public boolean isInputChannel() {

		return this.byteBufferedInputChannel.isInputChannel();
	}

	public void releaseAllResources() {

		final Queue<Buffer> buffersToRecycle = new ArrayDeque<Buffer>();

		synchronized (this.queuedEnvelopes) {

			while (!this.queuedEnvelopes.isEmpty()) {
				final TransferEnvelope envelope = this.queuedEnvelopes.poll();
				if (envelope.getBuffer() != null) {
					buffersToRecycle.add(envelope.getBuffer());
				}
			}
		}

		while (!buffersToRecycle.isEmpty()) {
			buffersToRecycle.poll().recycleBuffer();
		}
	}

	public int getNumberOfQueuedEnvelopes() {

		synchronized (this.queuedEnvelopes) {

			return this.queuedEnvelopes.size();
		}
	}

	public int getNumberOfQueuedMemoryBuffers() {

		synchronized (this.queuedEnvelopes) {

			int count = 0;

			final Iterator<TransferEnvelope> it = this.queuedEnvelopes.iterator();
			while (it.hasNext()) {

				final TransferEnvelope envelope = it.next();
				if (envelope.getBuffer() != null) {
					if (envelope.getBuffer().isBackedByMemory()) {
						++count;
					}
				}
			}

			return count;
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Buffer requestEmptyBuffer(final int minimumSizeOfBuffer) throws IOException {

		throw new IllegalStateException("requestEmptyBuffer called on InputChannelContext");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Buffer requestEmptyBufferBlocking(final int minimumSizeOfBuffer) throws IOException, InterruptedException {

		return this.inputGateContext.requestEmptyBufferBlocking(minimumSizeOfBuffer);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getMaximumBufferSize() {

		return this.inputGateContext.getMaximumBufferSize();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isShared() {

		return this.inputGateContext.isShared();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void reportAsynchronousEvent() {

		this.inputGateContext.reportAsynchronousEvent();
	}
}
