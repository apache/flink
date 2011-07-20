package eu.stratosphere.nephele.taskmanager.bytebuffered;

import java.io.IOException;

import eu.stratosphere.nephele.io.OutputGate;
import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.io.channels.BufferFactory;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.channels.FileBufferManager;
import eu.stratosphere.nephele.taskmanager.bufferprovider.BufferProvider;
import eu.stratosphere.nephele.taskmanager.checkpointing.EphemeralCheckpoint;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelope;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelopeDispatcher;

final class OutputGateContext implements BufferProvider {

	private final TaskContext taskContext;

	private final OutputGate<?> outputGate;

	private final FileBufferManager fileBufferManager;

	private final EphemeralCheckpoint ephemeralCheckpoint;

	/**
	 * The dispatcher for received transfer envelopes.
	 */
	private final TransferEnvelopeDispatcher transferEnvelopeDispatcher;

	OutputGateContext(final TaskContext taskContext, final OutputGate<?> outputGate,
			final TransferEnvelopeDispatcher transferEnvelopeDispatcher, final FileBufferManager fileBufferManager) {

		this.taskContext = taskContext;
		this.outputGate = outputGate;

		this.transferEnvelopeDispatcher = transferEnvelopeDispatcher;
		this.fileBufferManager = fileBufferManager;

		this.ephemeralCheckpoint = new EphemeralCheckpoint(this.outputGate.getGateID(),
			(outputGate.getChannelType() == ChannelType.FILE) ? false : true, this.fileBufferManager);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Buffer requestEmptyBuffer(final int minimumSizeOfBuffer) throws IOException {

		throw new IllegalStateException("requestEmpty Buffer called on OutputGateContext");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Buffer requestEmptyBufferBlocking(final int minimumSizeOfBuffer) throws IOException, InterruptedException {

		final Buffer buffer = this.taskContext.requestEmptyBuffer(minimumSizeOfBuffer);
		if (buffer != null) {
			return buffer;
		} else {
			if (!this.ephemeralCheckpoint.isDecided()) {
				this.ephemeralCheckpoint.destroy();
				//this.ephemeralCheckpoint.write();
			}
		}

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
	 * @param forwardToReceiver
	 *        states whether the transfer envelope shall be forwarded to the receiver
	 * @throws IOException
	 *         thrown if an I/O error occurs while processing the envelope
	 * @throws InterruptedException
	 *         thrown if the thread is interrupted while waiting for the envelope to be processed
	 */
	public void processEnvelope(final TransferEnvelope outgoingTransferEnvelope, final boolean forwardToReceiver)
			throws IOException,
			InterruptedException {

		if (!this.ephemeralCheckpoint.isDiscarded()) {

			final TransferEnvelope dup = outgoingTransferEnvelope.duplicate();
			this.ephemeralCheckpoint.addTransferEnvelope(dup);
		}

		if (forwardToReceiver) {
			this.transferEnvelopeDispatcher.processEnvelopeFromOutputChannel(outgoingTransferEnvelope);
		}
	}

	public Buffer getFileBuffer(final int bufferSize) throws IOException {

		return BufferFactory.createFromFile(bufferSize, this.outputGate.getGateID(), this.fileBufferManager);
	}
}
