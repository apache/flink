/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.io.channels;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.atomic.AtomicBoolean;

public final class MemoryBuffer extends Buffer {

	private final MemoryBufferRecycler bufferRecycler;

	private final ByteBuffer byteBuffer;

	private final AtomicBoolean writeMode = new AtomicBoolean(true);

	MemoryBuffer(final int bufferSize, final ByteBuffer byteBuffer, final MemoryBufferPoolConnector bufferPoolConnector) {

		if (bufferSize > byteBuffer.capacity()) {
			throw new IllegalArgumentException("Requested buffer size is " + bufferSize
				+ ", but provided byte buffer only has a capacity of " + byteBuffer.capacity());
		}

		this.bufferRecycler = new MemoryBufferRecycler(byteBuffer, bufferPoolConnector);

		this.byteBuffer = byteBuffer;
		this.byteBuffer.position(0);
		this.byteBuffer.limit(bufferSize);
	}

	private MemoryBuffer(final int bufferSize, final ByteBuffer byteBuffer, final MemoryBufferRecycler bufferRecycler) {

		this.bufferRecycler = bufferRecycler;
		this.byteBuffer = byteBuffer;
		this.byteBuffer.position(0);
		this.byteBuffer.limit(bufferSize);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int read(final ByteBuffer dst) throws IOException {

		if (this.writeMode.get()) {
			throw new IOException("Buffer is still in write mode!");
		}

		if (!this.byteBuffer.hasRemaining()) {
			return -1;
		}
		if (!dst.hasRemaining()) {
			return 0;
		}

		final int oldPosition = this.byteBuffer.position();

		if (dst.remaining() < this.byteBuffer.remaining()) {
			final int excess = this.byteBuffer.remaining() - dst.remaining();
			this.byteBuffer.limit(this.byteBuffer.limit() - excess);
			dst.put(this.byteBuffer);
			this.byteBuffer.limit(this.byteBuffer.limit() + excess);
		} else {
			dst.put(this.byteBuffer);
		}

		return (this.byteBuffer.position() - oldPosition);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int read(final WritableByteChannel writableByteChannel) throws IOException {

		if (this.writeMode.get()) {
			throw new IOException("Buffer is still in write mode!");
		}

		if (!this.byteBuffer.hasRemaining()) {
			return -1;
		}

		return writableByteChannel.write(this.byteBuffer);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void close() throws IOException {

		this.byteBuffer.position(this.byteBuffer.limit());
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isOpen() {

		return this.byteBuffer.hasRemaining();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int write(final ByteBuffer src) throws IOException {

		if (!this.writeMode.get()) {
			throw new IOException("Cannot write to buffer, buffer already switched to read mode");
		}

		final int sourceRemaining = src.remaining();
		final int thisRemaining = this.byteBuffer.remaining();
		final int excess = sourceRemaining - thisRemaining;

		if (excess <= 0) {
			// there is enough space here for all the source data
			this.byteBuffer.put(src);
			return sourceRemaining;
		} else {
			// not enough space here, we need to limit the source
			final int oldLimit = src.limit();
			src.limit(src.position() + thisRemaining);
			this.byteBuffer.put(src);
			src.limit(oldLimit);
			return thisRemaining;
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int write(final ReadableByteChannel readableByteChannel) throws IOException {

		if (!this.writeMode.get()) {
			throw new IOException("Cannot write to buffer, buffer already switched to read mode");
		}

		if (!this.byteBuffer.hasRemaining()) {
			return 0;
		}

		return readableByteChannel.read(this.byteBuffer);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int remaining() {
		return this.byteBuffer.remaining();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int size() {
		return this.byteBuffer.limit();
	}

	public ByteBuffer getByteBuffer() {
		return this.byteBuffer;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void recycle() {

		this.bufferRecycler.decreaseReferenceCounter();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void finishWritePhase() {

		if (!this.writeMode.compareAndSet(true, false)) {
			throw new IllegalStateException("MemoryBuffer is already in read mode!");
		}

		this.byteBuffer.flip();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isBackedByMemory() {

		return true;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public MemoryBuffer duplicate() {

		if (this.writeMode.get()) {
			throw new IllegalStateException("Cannot duplicate buffer that is still in write mode");
		}

		final MemoryBuffer duplicatedMemoryBuffer = new MemoryBuffer(this.byteBuffer.limit(),
			this.byteBuffer.duplicate(), this.bufferRecycler);

		this.bufferRecycler.increaseReferenceCounter();
		duplicatedMemoryBuffer.writeMode.set(this.writeMode.get());
		return duplicatedMemoryBuffer;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void copyToBuffer(final Buffer destinationBuffer) throws IOException {

		if (this.writeMode.get()) {
			throw new IllegalStateException("Cannot copy buffer that is still in write mode");
		}
		if (size() > destinationBuffer.size()) {
			throw new IllegalArgumentException("Destination buffer is too small to store content of source buffer: "
				+ size() + " vs. " + destinationBuffer.size());
		}

		final int oldPos = this.byteBuffer.position();
		this.byteBuffer.position(0);

		while (remaining() > 0) {
			destinationBuffer.write(this.byteBuffer);
		}

		this.byteBuffer.position(oldPos);

		destinationBuffer.finishWritePhase();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isInWriteMode() {

		return this.writeMode.get();
	}
}
