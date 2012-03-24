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
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Queue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.event.task.AbstractEvent;
import eu.stratosphere.nephele.event.task.EventList;
import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.channels.bytebuffered.AbstractByteBufferedInputChannel;
import eu.stratosphere.nephele.io.channels.bytebuffered.BufferPairResponse;
import eu.stratosphere.nephele.io.channels.bytebuffered.ByteBufferedChannelCloseEvent;
import eu.stratosphere.nephele.io.channels.bytebuffered.ByteBufferedInputChannelBroker;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.taskmanager.bytebuffered.InputChannelContext;
import eu.stratosphere.nephele.taskmanager.bytebuffered.ReceiverNotFoundEvent;
import eu.stratosphere.nephele.taskmanager.bytebuffered.UnexpectedEnvelopeEvent;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelope;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelopeDispatcher;
import eu.stratosphere.nephele.util.StringUtils;

final class RuntimeInputChannelContext implements InputChannelContext, ByteBufferedInputChannelBroker {

	private static final Log LOG = LogFactory.getLog(RuntimeInputChannelContext.class);

	private final RuntimeInputGateContext inputGateContext;

	private final AbstractByteBufferedInputChannel<?> byteBufferedInputChannel;

	private final TransferEnvelopeDispatcher transferEnvelopeDispatcher;

	private final Queue<TransferEnvelope> queuedEnvelopes = new ArrayDeque<TransferEnvelope>();

	private final EnvelopeConsumptionLog envelopeConsumptionLog;

	private final boolean isReexecuted;

	private int lastReceivedEnvelope = -1;

	private boolean destroyCalled = false;

	RuntimeInputChannelContext(final RuntimeInputGateContext inputGateContext,
			final TransferEnvelopeDispatcher transferEnvelopeDispatcher,
			final AbstractByteBufferedInputChannel<?> byteBufferedInputChannel,
			final EnvelopeConsumptionLog envelopeConsumptionLog) {

		this.inputGateContext = inputGateContext;
		this.transferEnvelopeDispatcher = transferEnvelopeDispatcher;
		this.byteBufferedInputChannel = byteBufferedInputChannel;
		this.byteBufferedInputChannel.setInputChannelBroker(this);
		this.envelopeConsumptionLog = envelopeConsumptionLog;
		this.isReexecuted = (envelopeConsumptionLog.getNumberOfInitialLogEntries() > 0L);
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

			// Notify the channel that an envelope has been consumed
			this.envelopeConsumptionLog.reportEnvelopeConsumed(this.byteBufferedInputChannel);

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

		// Notify the channel that an envelope has been consumed
		this.envelopeConsumptionLog.reportEnvelopeConsumed(this.byteBufferedInputChannel);

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

		AbstractEvent eventToSend = null;

		if (ReceiverNotFoundEvent.isReceiverNotFoundEvent(transferEnvelope)) {
			return;
		}

		synchronized (this.queuedEnvelopes) {

			if (this.destroyCalled) {
				final Buffer buffer = transferEnvelope.getBuffer();
				if (buffer != null) {
					buffer.recycleBuffer();
				}
				return;
			}

			final int expectedSequenceNumber = this.lastReceivedEnvelope + 1;
			if (sequenceNumber != expectedSequenceNumber) {

				// We received an envelope with higher sequence number than expected
				if (sequenceNumber > expectedSequenceNumber) {

					/**
					 * In case the task is reexecuted, we might receive envelopes from the original run that have been
					 * stuck in some network queues. As a result, we will simply ignore those envelopes. If the task is
					 * not restarted, we are actually missing data.
					 */
					if (!this.isReexecuted) {

						// This is a problem, now we are actually missing some data
						this.byteBufferedInputChannel.reportIOException(new IOException("Expected data packet "
							+ expectedSequenceNumber + " but received " + sequenceNumber));
						this.byteBufferedInputChannel.checkForNetworkEvents();
					}

				} else {

					eventToSend = lookForCloseEvent(transferEnvelope);
					if (eventToSend == null) {

						// Tell the sender to skip all envelopes until the next envelope that could potentially include
						// the close event
						eventToSend = new UnexpectedEnvelopeEvent(expectedSequenceNumber - 1);
					}
				}

				if (!this.isReexecuted || sequenceNumber > expectedSequenceNumber) {
					if (LOG.isWarnEnabled()) {
						LOG.warn("Input channel " + getChannelName() + " expected envelope " + expectedSequenceNumber
							+ " but received " + sequenceNumber);
					}
				}

				final Buffer buffer = transferEnvelope.getBuffer();
				if (buffer != null) {
					buffer.recycleBuffer();
				}
			} else {

				this.queuedEnvelopes.add(transferEnvelope);
				this.lastReceivedEnvelope = sequenceNumber;

				// Notify the channel about the new data
				this.envelopeConsumptionLog.reportEnvelopeAvailability(this.byteBufferedInputChannel);
			}
		}

		if (eventToSend != null) {
			try {
				transferEventToOutputChannel(eventToSend);
			} catch (Exception e) {
				LOG.error(StringUtils.stringifyException(e));
			}
		}
	}

	/**
	 * Looks for a {@link ByteBufferedChannelCloseEvent} in the given envelope returns it if it is found.
	 * 
	 * @param envelope
	 *        the envelope to be inspected
	 * @return the found {@link ByteBufferedChannelCloseEvent} or <code>null</code> if no such event was stored inside
	 *         the given envelope
	 */
	private AbstractEvent lookForCloseEvent(final TransferEnvelope envelope) {

		final EventList eventList = envelope.getEventList();
		if (eventList == null) {
			return null;
		}

		final Iterator<AbstractEvent> it = eventList.iterator();
		while (it.hasNext()) {

			final AbstractEvent event = it.next();

			if (event instanceof ByteBufferedChannelCloseEvent) {
				return event;
			}
		}

		return null;
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

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void destroy() {

		final Queue<Buffer> buffersToRecycle = new ArrayDeque<Buffer>();

		synchronized (this.queuedEnvelopes) {

			this.destroyCalled = true;

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

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void logQueuedEnvelopes() {

		int numberOfQueuedEnvelopes = 0;
		int numberOfQueuedEnvelopesWithMemoryBuffers = 0;
		int numberOfQueuedEnvelopesWithFileBuffers = 0;

		synchronized (this.queuedEnvelopes) {

			final Iterator<TransferEnvelope> it = this.queuedEnvelopes.iterator();
			while (it.hasNext()) {

				final TransferEnvelope envelope = it.next();
				++numberOfQueuedEnvelopes;
				final Buffer buffer = envelope.getBuffer();
				if (buffer == null) {
					continue;
				}

				if (buffer.isBackedByMemory()) {
					++numberOfQueuedEnvelopesWithMemoryBuffers;
				} else {
					++numberOfQueuedEnvelopesWithFileBuffers;
				}
			}
		}

		System.out.println("\t\t" + getChannelName() + ": " + numberOfQueuedEnvelopes + " ("
			+ numberOfQueuedEnvelopesWithMemoryBuffers + ", " + numberOfQueuedEnvelopesWithFileBuffers + ")");

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

	/**
	 * {@inheritDoc}
	 */
	@Override
	public ChannelType getType() {

		return this.byteBufferedInputChannel.getType();
	}

	/**
	 * Constructs and returns a human-readable name of this channel used for debugging.
	 * 
	 * @return a human-readable name of this channel used for debugging
	 */
	private String getChannelName() {

		final StringBuilder sb = new StringBuilder(this.inputGateContext.getTaskName());

		sb.append(' ');
		sb.append('(');
		sb.append(this.byteBufferedInputChannel.getChannelIndex());
		sb.append(',');
		sb.append(' ');
		sb.append(this.byteBufferedInputChannel.getID());
		sb.append(')');

		return sb.toString();
	}
}
