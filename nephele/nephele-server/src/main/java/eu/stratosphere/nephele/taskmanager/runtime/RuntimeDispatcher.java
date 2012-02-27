package eu.stratosphere.nephele.taskmanager.runtime;

import java.io.IOException;

import eu.stratosphere.nephele.event.task.AbstractEvent;
import eu.stratosphere.nephele.taskmanager.bytebuffered.OutputChannelForwarder;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelope;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelopeDispatcher;

public final class RuntimeDispatcher implements OutputChannelForwarder {

	private final TransferEnvelopeDispatcher dispatcher;

	public RuntimeDispatcher(final TransferEnvelopeDispatcher dispatcher) {

		this.dispatcher = dispatcher;
	}

	@Override
	public boolean forward(final TransferEnvelope transferEnvelope) throws IOException, InterruptedException {

		dispatcher.processEnvelopeFromOutputChannel(transferEnvelope);

		return true;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean hasDataLeft() {

		return false;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void processEvent(final AbstractEvent event) {

		// Nothing to do here
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void destroy() {

		// Nothing to do here
	}

}
