package eu.stratosphere.nephele.checkpointing;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.event.task.AbstractEvent;
import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.io.channels.bytebuffered.ByteBufferedChannelCloseEvent;
import eu.stratosphere.nephele.taskmanager.bufferprovider.BufferProvider;
import eu.stratosphere.nephele.taskmanager.bytebuffered.IncomingEventQueue;
import eu.stratosphere.nephele.taskmanager.bytebuffered.OutputChannelForwarder;
import eu.stratosphere.nephele.taskmanager.bytebuffered.OutputChannelForwardingChain;
import eu.stratosphere.nephele.taskmanager.bytebuffered.UnexpectedEnvelopeEvent;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelope;

final class ReplayOutputBroker implements OutputChannelForwarder, BufferProvider {

	/**
	 * The logger to report information and problems.
	 */
	private static final Log LOG = LogFactory.getLog(ReplayOutputBroker.class);

	private final BufferProvider bufferProvider;

	private final OutputChannelForwardingChain forwardingChain;

	private final IncomingEventQueue incomingEventQueue;

	private int nextEnvelopeToSend = 0;

	ReplayOutputBroker(final BufferProvider bufferProvider, final OutputChannelForwardingChain forwardingChain,
			final IncomingEventQueue incomingEventQueue) {

		this.bufferProvider = bufferProvider;
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
		} else if (event instanceof UnexpectedEnvelopeEvent) {
			final UnexpectedEnvelopeEvent uee = (UnexpectedEnvelopeEvent) event;
			if (uee.getExpectedSequenceNumber() > this.nextEnvelopeToSend) {
				this.nextEnvelopeToSend = uee.getExpectedSequenceNumber();
			}
		} else {
			LOG.warn("Received unknown event: " + event);
		}
	}

	void outputEnvelope(final TransferEnvelope transferEnvelope) throws IOException, InterruptedException {

		this.incomingEventQueue.processQueuedEvents();

		if (transferEnvelope.getSequenceNumber() == this.nextEnvelopeToSend) {
			++this.nextEnvelopeToSend;
		}

		this.forwardingChain.forwardEnvelope(transferEnvelope);
	}

	int getNextEnvelopeToSend() {

		return this.nextEnvelopeToSend;
	}

	boolean hasFinished() {

		// Check for events
		this.incomingEventQueue.processQueuedEvents();

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
	public void destroy() {

		// Nothing to do here
	}
}
