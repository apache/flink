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
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Queue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.event.task.AbstractEvent;
import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.channels.bytebuffered.AbstractByteBufferedInputChannel;
import eu.stratosphere.nephele.io.channels.bytebuffered.BufferOrEvent;
import eu.stratosphere.nephele.io.channels.bytebuffered.ByteBufferedInputChannelBroker;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.taskmanager.bufferprovider.BufferAvailabilityListener;
import eu.stratosphere.nephele.taskmanager.bytebuffered.InputChannelContext;
import eu.stratosphere.nephele.taskmanager.bytebuffered.ReceiverNotFoundEvent;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelope;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelopeDispatcher;


final class RuntimeInputChannelContext implements InputChannelContext, ByteBufferedInputChannelBroker {

	private static final Log LOG = LogFactory.getLog(RuntimeInputChannelContext.class);

	private final RuntimeInputGateContext inputGateContext;

	private final AbstractByteBufferedInputChannel<?> byteBufferedInputChannel;

	private final TransferEnvelopeDispatcher transferEnvelopeDispatcher;

	private final Queue<TransferEnvelope> queuedEnvelopes = new ArrayDeque<TransferEnvelope>();
	
	private Iterator<AbstractEvent> pendingEvents;

	private int lastReceivedEnvelope = -1;

	private boolean destroyCalled = false;

	RuntimeInputChannelContext(final RuntimeInputGateContext inputGateContext,
			final TransferEnvelopeDispatcher transferEnvelopeDispatcher,
			final AbstractByteBufferedInputChannel<?> byteBufferedInputChannel) {

		this.inputGateContext = inputGateContext;
		this.transferEnvelopeDispatcher = transferEnvelopeDispatcher;
		this.byteBufferedInputChannel = byteBufferedInputChannel;
		this.byteBufferedInputChannel.setInputChannelBroker(this);
	}


	@Override
	public BufferOrEvent getNextBufferOrEvent() throws IOException {
		// return pending events first
		if (this.pendingEvents != null) {
			// if the field is not null, it must always have a next value!
			BufferOrEvent next = new BufferOrEvent(this.pendingEvents.next());
			if (!this.pendingEvents.hasNext()) {
				this.pendingEvents = null;
			}
			return next;
		}

		// if no events are pending, get the next buffer
		TransferEnvelope nextEnvelope;
		synchronized (this.queuedEnvelopes) {
			if (this.queuedEnvelopes.isEmpty()) {
				return null;
			}
			nextEnvelope = this.queuedEnvelopes.poll();
		}

		// schedule events as pending, because events come always after the buffer!
		if (nextEnvelope.getEventList() != null) {
			Iterator<AbstractEvent> events = nextEnvelope.getEventList().iterator();
			if (events.hasNext()) {
				this.pendingEvents = events;
			}
		}
		
		// get the buffer, if there is one
		if (nextEnvelope.getBuffer() != null) {
			return new BufferOrEvent(nextEnvelope.getBuffer());
		}
		else if (this.pendingEvents != null) {
			// if the field is not null, it must always have a next value!
			BufferOrEvent next = new BufferOrEvent(this.pendingEvents.next());
			if (!this.pendingEvents.hasNext()) {
				this.pendingEvents = null;
			}
			
			return next;
		}
		else {
			// no buffer and no events, this should be an error
			throw new IOException("Received an envelope with neither data nor events.");
		}
	}

	@Override
	public void transferEventToOutputChannel(AbstractEvent event) throws IOException, InterruptedException {
		TransferEnvelope ephemeralTransferEnvelope = new TransferEnvelope(0, getJobID(), getChannelID());
		ephemeralTransferEnvelope.addEvent(event);
		
		this.transferEnvelopeDispatcher.processEnvelopeFromInputChannel(ephemeralTransferEnvelope);
	}

	@Override
	public void queueTransferEnvelope(TransferEnvelope transferEnvelope) {

		if (ReceiverNotFoundEvent.isReceiverNotFoundEvent(transferEnvelope)) {
			return;
		}
		
		// The sequence number of the envelope to be queued
		final int sequenceNumber = transferEnvelope.getSequenceNumber();

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
				// This is a problem, now we are actually missing some data
				this.byteBufferedInputChannel.reportIOException(new IOException("Expected data packet "
						+ expectedSequenceNumber + " but received " + sequenceNumber));
				
				// notify that something (an exception) is available
				this.byteBufferedInputChannel.notifyGateThatInputIsAvailable();

				if (LOG.isDebugEnabled()) {
					LOG.debug("Input channel " + getChannelName() + " expected envelope " + expectedSequenceNumber
						+ " but received " + sequenceNumber);
				}

				// rescue the buffer
				final Buffer buffer = transferEnvelope.getBuffer();
				if (buffer != null) {
					buffer.recycleBuffer();
				}
			} else {

				this.queuedEnvelopes.add(transferEnvelope);
				this.lastReceivedEnvelope = sequenceNumber;

				// Notify the channel about the new data. notify as much as there is (buffer plus once per event)
				if (transferEnvelope.getBuffer() != null) {
					this.byteBufferedInputChannel.notifyGateThatInputIsAvailable();
				}
				if (transferEnvelope.getEventList() != null) {
					for (int i = 0; i < transferEnvelope.getEventList().size(); i++) {
						this.byteBufferedInputChannel.notifyGateThatInputIsAvailable();
					}
				}
			}
		}
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

	@Override
	public Buffer requestEmptyBuffer(int minimumSizeOfBuffer) throws IOException {
		return this.inputGateContext.requestEmptyBuffer(minimumSizeOfBuffer);
	}

	@Override
	public Buffer requestEmptyBufferBlocking(int minimumSizeOfBuffer) throws IOException, InterruptedException {
		return this.inputGateContext.requestEmptyBufferBlocking(minimumSizeOfBuffer);
	}

	@Override
	public int getMaximumBufferSize() {
		return this.inputGateContext.getMaximumBufferSize();
	}

	@Override
	public boolean isShared() {
		return this.inputGateContext.isShared();
	}

	@Override
	public void reportAsynchronousEvent() {
		this.inputGateContext.reportAsynchronousEvent();
	}

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
		StringBuilder sb = new StringBuilder(this.inputGateContext.getTaskName());
		sb.append(' ');
		sb.append('(');
		sb.append(this.byteBufferedInputChannel.getChannelIndex());
		sb.append(',');
		sb.append(' ');
		sb.append(this.byteBufferedInputChannel.getID());
		sb.append(')');
		return sb.toString();
	}

	@Override
	public boolean registerBufferAvailabilityListener(final BufferAvailabilityListener bufferAvailabilityListener) {
		return this.inputGateContext.registerBufferAvailabilityListener(bufferAvailabilityListener);
	}
}
