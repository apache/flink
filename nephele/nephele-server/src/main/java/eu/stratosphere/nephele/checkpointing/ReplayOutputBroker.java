package eu.stratosphere.nephele.checkpointing;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.event.task.AbstractEvent;
import eu.stratosphere.nephele.io.channels.bytebuffered.ByteBufferedChannelCloseEvent;
import eu.stratosphere.nephele.taskmanager.bytebuffered.IncomingEventQueue;
import eu.stratosphere.nephele.taskmanager.bytebuffered.OutputChannelForwarder;
import eu.stratosphere.nephele.taskmanager.bytebuffered.OutputChannelForwardingChain;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelope;

final class ReplayOutputBroker implements OutputChannelForwarder {

	/**
	 * The logger to report information and problems.
	 */
	private static final Log LOG = LogFactory.getLog(ReplayOutputBroker.class);

	private final OutputChannelForwardingChain forwardingChain;

	private final IncomingEventQueue incomingEventQueue;

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

		// A replay task will not wait for a close acknowledgement as it may have been sent to the corresponding runtime
		// task before.

		return false;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void processEvent(final AbstractEvent event) {

		if (event instanceof ByteBufferedChannelCloseEvent) {
			LOG.info("Replay output broker received event to close channel");
		} else {
			LOG.warn("Received unknown event: " + event);
		}
	}

	void outputEnvelope(final TransferEnvelope transferEnvelope) throws IOException, InterruptedException {

		this.incomingEventQueue.processQueuedEvents();

		this.forwardingChain.forwardEnvelope(transferEnvelope);
	}

	boolean hasFinished() {

		// Check for events
		this.incomingEventQueue.processQueuedEvents();

		return (!this.forwardingChain.anyForwarderHasDataLeft());
	}
}
