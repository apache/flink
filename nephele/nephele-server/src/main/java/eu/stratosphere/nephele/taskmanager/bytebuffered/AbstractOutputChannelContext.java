package eu.stratosphere.nephele.taskmanager.bytebuffered;

import java.util.Iterator;

import eu.stratosphere.nephele.event.task.AbstractEvent;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelope;

public abstract class AbstractOutputChannelContext implements OutputChannelContext {

	/**
	 * Stores incoming events for this output channel.
	 */
	private final IncomingEventQueue incomingEventQueue;

	public AbstractOutputChannelContext(final IncomingEventQueue incomingEventQueue) {

		this.incomingEventQueue = incomingEventQueue;
	}

	public static IncomingEventQueue createIncomingEventQueue(final OutputChannelForwardingChain forwardingChain) {

		return new IncomingEventQueue(forwardingChain);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void queueTransferEnvelope(final TransferEnvelope transferEnvelope) {

		if (transferEnvelope.getBuffer() != null) {
			throw new IllegalStateException("Transfer envelope for output channel has buffer attached");
		}

		final Iterator<AbstractEvent> it = transferEnvelope.getEventList().iterator();
		while (it.hasNext()) {
			this.incomingEventQueue.offer(it.next());
		}
	}
}
