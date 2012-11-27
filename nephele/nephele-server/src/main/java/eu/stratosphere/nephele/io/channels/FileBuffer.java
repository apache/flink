/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.atomic.AtomicBoolean;

import eu.stratosphere.nephele.io.AbstractID;

public final class FileBuffer extends Buffer {

	private final AbstractID ownerID;

	private final FileBufferManager fileBufferManager;

	private final FileChannel fileChannel;

	private long offset;

	private int bufferSize;

	private int totalBytesWritten;

	private int totalBytesRead;

	private final AtomicBoolean writeMode = new AtomicBoolean(true);

	/**
	 * Creates a file buffer for a chunk of data that is already in a file.
	 * 
	 * @param bufferSize
	 * @param offset
	 * @param ownerID
	 */
	FileBuffer(final int bufferSize, final long offset, final AbstractID ownerID,
			final FileBufferManager fileBufferManager, final boolean distributed, final boolean deleteOnClose)
			throws IOException {

		this.ownerID = ownerID;
		this.fileBufferManager = fileBufferManager;
		this.offset = offset;
		this.bufferSize = bufferSize;
		this.writeMode.set(false);

		this.fileChannel = fileBufferManager.getChannelAndIncrementReferences(ownerID, distributed, deleteOnClose);
	}

	/**
	 * Creates a file Buffer that will have space reserved in the file for its data.
	 * 
	 * @param bufferSize
	 * @param ownerID
	 * @param fileBufferManager
	 * @throws IOException
	 */
	FileBuffer(final int bufferSize, final AbstractID ownerID, final FileBufferManager fileBufferManager,
			final boolean distributed, final boolean deleteOnClose) throws IOException {

		this.fileBufferManager = fileBufferManager;
		this.ownerID = ownerID;
		this.bufferSize = bufferSize;
		this.writeMode.set(true);

		final ChannelWithPosition cwp = fileBufferManager.getChannelForWriteAndIncrementReferences(ownerID, bufferSize,
			distributed, deleteOnClose);

		this.fileChannel = cwp.getChannel();
		this.offset = cwp.getOffset();
	}

	private FileBuffer(final FileBuffer toCopy) {

		this.ownerID = toCopy.ownerID;
		this.fileBufferManager = toCopy.fileBufferManager;
		this.fileChannel = toCopy.fileChannel;
		this.offset = toCopy.offset;
		this.bufferSize = toCopy.bufferSize;
		this.totalBytesWritten = toCopy.totalBytesWritten;
		this.totalBytesRead = toCopy.totalBytesRead;
		this.writeMode.set(toCopy.writeMode.get());
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int read(final WritableByteChannel writableByteChannel) throws IOException {

		if (this.writeMode.get()) {
			throw new IOException("FileBuffer is still in write mode!");
		}

		if (this.totalBytesRead >= this.bufferSize) {
			return -1;
		}

		final long bytesRead = this.fileChannel.transferTo(this.offset + this.totalBytesRead,
			this.bufferSize - this.totalBytesRead, writableByteChannel);
		this.totalBytesRead += bytesRead;

		return (int) bytesRead;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int read(final ByteBuffer dst) throws IOException {

		if (this.writeMode.get()) {
			throw new IOException("FileBuffer is still in write mode!");
		}

		if (this.totalBytesRead >= this.bufferSize) {
			return -1;
		}

		final int bytesRead = readInternal(dst, this.offset + this.totalBytesRead, remaining());
		if (bytesRead < 0) {
			return -1;
		}

		this.totalBytesRead += bytesRead;
		return bytesRead;
	}

	private final int readInternal(final ByteBuffer dst, final long position, final int numBytes) throws IOException {

		if (dst.remaining() > numBytes) {
			final int excess = dst.remaining() - numBytes;
			dst.limit(dst.limit() - excess);
			final int bytesRead = this.fileChannel.read(dst, position);
			dst.limit(dst.limit() + excess);
			return bytesRead >= 0 ? bytesRead : -1;

		} else {
			return this.fileChannel.read(dst, position);
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

		if (this.totalBytesWritten >= this.bufferSize) {
			return 0;
		}

		final long bytesWritten = this.fileChannel.transferFrom(readableByteChannel,
			(this.offset + this.totalBytesWritten), (this.bufferSize - this.totalBytesWritten));
		this.totalBytesWritten += bytesWritten;

		return (int) bytesWritten;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int write(final ByteBuffer src) throws IOException {

		if (!this.writeMode.get()) {
			throw new IOException("Cannot write to buffer, buffer already switched to read mode");
		}

		if (this.totalBytesWritten >= this.bufferSize) {
			return 0;
		}

		// Make sure we do not exceed the buffer limit
		int bytesWritten;
		final int rem = (int) (this.bufferSize - this.totalBytesWritten);
		if (src.remaining() > rem) {
			final int excess = src.remaining() - rem;
			src.limit(src.limit() - excess);
			bytesWritten = this.fileChannel.write(src, this.offset + this.totalBytesWritten);
			src.limit(src.limit() + excess);
		} else {
			bytesWritten = this.fileChannel.write(src, this.offset + this.totalBytesWritten);
		}

		this.totalBytesWritten += bytesWritten;

		return bytesWritten;
	}

	@Override
	public void close() throws IOException {

		this.fileChannel.close();
	}

	@Override
	public boolean isOpen() {

		return this.fileChannel.isOpen();
	}

	@Override
	public int remaining() {

		if (this.writeMode.get()) {
			return this.bufferSize - this.totalBytesWritten;
		} else {
			return this.bufferSize - this.totalBytesRead;
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int size() {

		return (int) this.bufferSize;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void recycle() {

		this.fileBufferManager.decrementReferences(this.ownerID);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void finishWritePhase() throws IOException {

		if (this.writeMode.compareAndSet(true, false)) {
			this.bufferSize = this.totalBytesWritten;
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isBackedByMemory() {
		return false;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public FileBuffer duplicate() throws IOException, InterruptedException {

		if (this.writeMode.get()) {
			throw new IllegalStateException("Cannot duplicate buffer that is still in write mode");
		}

		this.fileBufferManager.incrementReferences(this.ownerID);
		return new FileBuffer(this);
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

		if (destinationBuffer.isBackedByMemory()) {
			final ByteBuffer bb = ((MemoryBuffer) destinationBuffer).getByteBuffer();
			final int tbr = this.totalBytesRead;
			this.totalBytesRead = 0;

			int rem = 0;
			while ((rem = remaining()) > 0) {
				this.totalBytesRead += readInternal(bb, this.offset + this.totalBytesRead, rem);
			}

			destinationBuffer.finishWritePhase();
			this.totalBytesRead = tbr;
			return;
		}

		throw new UnsupportedOperationException("FileBuffer-to-FileBuffer copy is not yet implemented");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isInWriteMode() {

		return this.writeMode.get();
	}

	/**
	 * Returns the offset in bytes which marks the begin of the buffer's data in the underlying file.
	 * 
	 * @return the buffer's offset in bytes
	 */
	public long getOffset() {

		return this.offset;
	}

	/**
	 * Gets this file buffer's owner id.
	 * 
	 * @return This file buffer's owner id.
	 */
	public AbstractID getOwnerID() {

		return this.ownerID;
	}
}
