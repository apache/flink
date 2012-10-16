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

package eu.stratosphere.nephele.checkpointing;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.event.task.AbstractEvent;
import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.io.channels.ByteBufferedChannelCloseEvent;
import eu.stratosphere.nephele.taskmanager.bufferprovider.BufferAvailabilityListener;
import eu.stratosphere.nephele.taskmanager.bufferprovider.BufferProvider;
import eu.stratosphere.nephele.taskmanager.bytebuffered.AbstractOutputChannelForwarder;
import eu.stratosphere.nephele.taskmanager.bytebuffered.OutputChannelForwardingChain;
import eu.stratosphere.nephele.taskmanager.bytebuffered.ReceiverNotFoundEvent;
import eu.stratosphere.nephele.taskmanager.bytebuffered.UnexpectedEnvelopeEvent;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelope;

final class ReplayOutputChannelBroker extends AbstractOutputChannelForwarder implements BufferProvider {

	/**
	 * The logger to report information and problems.
	 */
	private static final Log LOG = LogFactory.getLog(ReplayOutputChannelBroker.class);

	private final BufferProvider bufferProvider;

	private OutputChannelForwardingChain forwardingChain;

	private int nextEnvelopeToSend = 0;

	ReplayOutputChannelBroker(final BufferProvider bufferProvider, final AbstractOutputChannelForwarder next) {
		super(next);

		this.bufferProvider = bufferProvider;
	}

	public void setForwardingChain(final OutputChannelForwardingChain forwardingChain) {
		this.forwardingChain = forwardingChain;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void processEvent(final AbstractEvent event) {

		if (event instanceof ByteBufferedChannelCloseEvent) {
			LOG.info("Replay output broker received event to close channel");
		} else if (event instanceof UnexpectedEnvelopeEvent) {
			final UnexpectedEnvelopeEvent uee = (UnexpectedEnvelopeEvent) event;
			if (uee.getExpectedSequenceNumber() > this.nextEnvelopeToSend) {
				this.nextEnvelopeToSend = uee.getExpectedSequenceNumber();
			}
		} else if (event instanceof ReceiverNotFoundEvent) {
			if (LOG.isDebugEnabled()) {
				final ReceiverNotFoundEvent rnfe = (ReceiverNotFoundEvent) event;
				LOG.debug("Cannot find receiver " + rnfe.getReceiverID() + " for envelope " + rnfe.getSequenceNumber()
					+ ", next envelope to send is " + this.nextEnvelopeToSend);
			}
		} else {
			LOG.warn("Received unknown event: " + event);
		}

		getNext().processEvent(event);
	}

	void outputEnvelope(final TransferEnvelope transferEnvelope) throws IOException, InterruptedException {

		this.forwardingChain.processQueuedEvents();

		if (transferEnvelope.getSequenceNumber() == this.nextEnvelopeToSend) {
			++this.nextEnvelopeToSend;
		}

		this.forwardingChain.pushEnvelope(transferEnvelope);
	}

	void reset() {
		this.nextEnvelopeToSend = 0;
	}

	int getNextEnvelopeToSend() {

		return this.nextEnvelopeToSend;
	}

	boolean hasFinished() throws IOException, InterruptedException {

		// Check for events
		this.forwardingChain.processQueuedEvents();

		return (!this.forwardingChain.anyForwarderHasDataLeft());
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Buffer requestEmptyBuffer(final int minimumSizeOfBuffer) throws IOException {

		return this.bufferProvider.requestEmptyBuffer(minimumSizeOfBuffer);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Buffer requestEmptyBufferBlocking(final int minimumSizeOfBuffer) throws IOException, InterruptedException {

		return this.bufferProvider.requestEmptyBufferBlocking(minimumSizeOfBuffer);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getMaximumBufferSize() {

		return this.bufferProvider.getMaximumBufferSize();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isShared() {

		return this.bufferProvider.isShared();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void reportAsynchronousEvent() {

		this.bufferProvider.reportAsynchronousEvent();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean registerBufferAvailabilityListener(final BufferAvailabilityListener bufferAvailabilityListener) {

		return this.bufferProvider.registerBufferAvailabilityListener(bufferAvailabilityListener);
	}
}
