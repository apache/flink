package eu.stratosphere.nephele.taskmanager.bytebuffered;

import java.io.IOException;

import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.taskmanager.bufferprovider.BufferProvider;
import eu.stratosphere.nephele.taskmanager.bufferprovider.LocalBufferCache;

final class TaskContext implements BufferProvider {

	private final LocalBufferCache localBufferCache;

	public TaskContext() {

		this.localBufferCache = new LocalBufferCache(1, false);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Buffer requestEmptyBuffer(final int minimumSizeOfBuffer, final int minimumReserve) throws IOException {

		return this.localBufferCache.requestEmptyBuffer(minimumSizeOfBuffer, minimumReserve);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Buffer requestEmptyBufferBlocking(int minimumSizeOfBuffer, final int minimumReserve) throws IOException,
			InterruptedException {

		return this.localBufferCache.requestEmptyBufferBlocking(minimumSizeOfBuffer, minimumReserve);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getMaximumBufferSize() {

		return this.localBufferCache.getMaximumBufferSize();
	}

	public void releaseAllResources() {

		// Clear the buffer cache
		this.localBufferCache.clear();
	}

	public void setBufferLimit(int bufferLimit) {

		this.localBufferCache.setDesignatedNumberOfBuffers(bufferLimit);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isShared() {

		return false;
	}
}
