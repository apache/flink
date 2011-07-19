package eu.stratosphere.nephele.taskmanager.bytebuffered;

import java.io.IOException;

import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.taskmanager.bufferprovider.BufferProvider;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelope;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelopeDispatcher;

final class OutputGateContext implements BufferProvider {

	private TaskContext taskContext;

	/**
	 * The dispatcher for received transfer envelopes.
	 */
	private final TransferEnvelopeDispatcher transferEnvelopeDispatcher;

	OutputGateContext(final TaskContext taskContext, final TransferEnvelopeDispatcher transferEnvelopeDispatcher) {

		this.taskContext = taskContext;
		this.transferEnvelopeDispatcher = transferEnvelopeDispatcher;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Buffer requestEmptyBuffer(final int minimumSizeOfBuffer) throws IOException {

		return this.taskContext.requestEmptyBuffer(minimumSizeOfBuffer);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Buffer requestEmptyBufferBlocking(final int minimumSizeOfBuffer) throws IOException, InterruptedException {

		return this.taskContext.requestEmptyBufferBlocking(minimumSizeOfBuffer);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getMaximumBufferSize() {

		return this.taskContext.getMaximumBufferSize();
	}

	/**
	 * Called by the attached output channel wrapper to forward a {@link TransferEnvelope} object
	 * to its final destination. Within this method the provided transfer envelope is possibly also
	 * forwarded to the assigned ephemeral checkpoint.
	 * 
	 * @param outgoingTransferEnvelope
	 *        the transfer envelope to be forwarded
	 * @throws IOException
	 *         thrown if an I/O error occurs while processing the envelope
	 * @throws InterruptedException
	 *         thrown if the thread is interrupted while waiting for the envelope to be processed
	 */
	public void processEnvelope(final TransferEnvelope outgoingTransferEnvelope) throws IOException, InterruptedException {

		// TODO: Adapt code to work with checkpointing again
		/*
		 * final TransferEnvelopeReceiverList processingLog = outgoingTransferEnvelope.getProcessingLog();
		 * // Check if the provided envelope must be written to the checkpoint
		 * if (this.ephemeralCheckpoint != null && processingLog.mustBeWrittenToCheckpoint()) {
		 * this.ephemeralCheckpoint.addTransferEnvelope(outgoingTransferEnvelope);
		 * // Look for a close event
		 * final EventList eventList = outgoingTransferEnvelope.getEventList();
		 * if (!eventList.isEmpty() && this.commonChannelType == ChannelType.FILE) {
		 * final Iterator<AbstractEvent> it = eventList.iterator();
		 * while (it.hasNext()) {
		 * if (it.next() instanceof ByteBufferedChannelCloseEvent) {
		 * // Mark corresponding channel as closed
		 * this.ephemeralCheckpoint.markChannelAsFinished(outgoingTransferEnvelope.getSource());
		 * // If checkpoint is persistent it is save to acknowledge the close event
		 * if (this.ephemeralCheckpoint.isPersistent()) {
		 * channelWrapper.processEvent(new ByteBufferedChannelCloseEvent());
		 * }
		 * break;
		 * }
		 * }
		 * }
		 * }
		 */

		this.transferEnvelopeDispatcher.processEnvelopeFromOutputChannel(outgoingTransferEnvelope);
	}
}
