package eu.stratosphere.nephele.checkpointing;

import java.io.IOException;

import eu.stratosphere.nephele.event.task.AbstractEvent;
import eu.stratosphere.nephele.io.channels.bytebuffered.ByteBufferedChannelCloseEvent;
import eu.stratosphere.nephele.taskmanager.bytebuffered.IncomingEventQueue;
import eu.stratosphere.nephele.taskmanager.bytebuffered.OutputChannelForwarder;
import eu.stratosphere.nephele.taskmanager.bytebuffered.OutputChannelForwardingChain;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelope;

final class ReplayOutputBroker implements OutputChannelForwarder {

	private final OutputChannelForwardingChain forwardingChain;

	private final IncomingEventQueue incomingEventQueue;

	/**
	 * Stores whether the receiver has acknowledged the close request from this channel.
	 */
	private boolean closeAcknowledgementReceived = false;

	ReplayOutputBroker(final OutputChannelForwardingChain forwardingChain, final IncomingEventQueue incomingEventQueue) {
		this.forwardingChain = forwardingChain;
		this.incomingEventQueue = incomingEventQueue;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean forward(final TransferEnvelope transferEnvelope) throws IOException, InterruptedException {

		// Nothing to do here

		return true;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean hasDataLeft() {

		return (!this.closeAcknowledgementReceived);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void processEvent(final AbstractEvent event) {

		if (event instanceof ByteBufferedChannelCloseEvent) {
			this.closeAcknowledgementReceived = true;
		} else {
			System.out.println("Received unknown event: " + event);
		}
	}

	void outputEnvelope(final TransferEnvelope transferEnvelope) throws IOException, InterruptedException {

		this.incomingEventQueue.processQueuedEvents();

		this.forwardingChain.forwardEnvelope(transferEnvelope);
	}

	boolean hasFinished() {

		return (!this.forwardingChain.anyForwarderHasDataLeft());
	}
}
