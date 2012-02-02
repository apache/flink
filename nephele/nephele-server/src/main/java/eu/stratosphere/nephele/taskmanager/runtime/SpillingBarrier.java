package eu.stratosphere.nephele.taskmanager.runtime;

import java.io.IOException;

import eu.stratosphere.nephele.event.task.AbstractEvent;
import eu.stratosphere.nephele.io.channels.bytebuffered.ByteBufferedChannelActivateEvent;
import eu.stratosphere.nephele.taskmanager.bytebuffered.OutputChannelForwarder;
import eu.stratosphere.nephele.taskmanager.transferenvelope.SpillingQueue;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelope;

public final class SpillingBarrier implements OutputChannelForwarder {

	/**
	 * Queue to store outgoing transfer envelope in case the receiver of the envelopes is not yet running.
	 */
	private final SpillingQueue queuedOutgoingEnvelopes;

	/**
	 * Indicates whether the receiver of an envelope is currently running.
	 */
	private boolean isReceiverRunning = false;

	SpillingBarrier(final boolean isReceiverRunning) {
		this.isReceiverRunning = isReceiverRunning;
		this.queuedOutgoingEnvelopes = null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean forward(final TransferEnvelope transferEnvelope) throws IOException, InterruptedException {

		if (!this.isReceiverRunning) {

			// TODO: Add this to the spilling queue

			return false;
		}

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

		if (event instanceof ByteBufferedChannelActivateEvent) {
			this.isReceiverRunning = true;
		}
	}

}
