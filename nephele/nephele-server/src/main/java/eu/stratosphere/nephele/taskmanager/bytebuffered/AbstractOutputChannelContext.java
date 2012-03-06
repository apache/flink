package eu.stratosphere.nephele.taskmanager.bytebuffered;

import java.util.Iterator;

import eu.stratosphere.nephele.event.task.AbstractEvent;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelope;

public abstract class AbstractOutputChannelContext implements OutputChannelContext {

	/**
	 * The forwarding chain used by this output channel context.
	 */
	private final OutputChannelForwardingChain forwardingChain;

	public AbstractOutputChannelContext(final OutputChannelForwardingChain forwardingChain) {

		this.forwardingChain = forwardingChain;
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
			this.forwardingChain.offerEvent(it.next());
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void destroy() {

		this.forwardingChain.destroy();
	}
}
