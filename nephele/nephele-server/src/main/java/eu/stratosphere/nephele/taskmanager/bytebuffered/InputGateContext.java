package eu.stratosphere.nephele.taskmanager.bytebuffered;

import java.io.IOException;
import java.util.Iterator;

import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.taskmanager.bufferprovider.BufferProvider;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelope;

final class InputGateContext implements BufferProvider {

	private TaskContext taskContext;

	InputGateContext(final TaskContext taskContext) {
		this.taskContext = taskContext;
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
}
