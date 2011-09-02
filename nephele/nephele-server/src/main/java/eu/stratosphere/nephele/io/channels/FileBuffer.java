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
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import eu.stratosphere.nephele.io.AbstractID;
import eu.stratosphere.nephele.io.channels.InternalBuffer;

public class FileBuffer implements InternalBuffer {

	private long bufferSize;

	private final FileBufferManager fileBufferManager;

	private final AbstractID ownerID;

	private FileID fileID;

	private FileChannel fileChannel;

	private volatile boolean writeMode = true;

	private long totalBytesWritten = 0;

	private long totalBytesRead = 0;

	private long offset;

	FileBuffer(final int bufferSize, final FileID fileID, final long offset, final AbstractID ownerID, final FileBufferManager fileBufferManager) {
		this.bufferSize = bufferSize;
		this.fileID = fileID;
		this.offset = offset;
		this.ownerID = ownerID;
		this.fileBufferManager = fileBufferManager;
	}
	
	FileBuffer(final int bufferSize, final AbstractID ownerID, final FileBufferManager fileBufferManager) {
		this(bufferSize, null, 0, ownerID, fileBufferManager);
	}

	@Override
	public int read(WritableByteChannel writableByteChannel) throws IOException {

		if (this.writeMode) {
			throw new IOException("FileBuffer is still in write mode!");
		}

		if (this.fileChannel == null) {
			try {
				this.fileChannel = this.fileBufferManager.getFileChannelForReading(this.ownerID, this.fileID);
				if (this.fileChannel == null) {
					return 0;
				}
			} catch (InterruptedException e) {
				return -1;
			}
			if (this.fileChannel.position() != (this.offset + this.totalBytesRead)) {
				this.fileChannel.position(this.offset + this.totalBytesRead);
			}
		}

		if (this.totalBytesRead >= this.bufferSize) {
			return -1;
		}

		final long bytesRead = this.fileChannel.transferTo(this.offset + this.totalBytesRead, this.bufferSize
			- this.totalBytesRead, writableByteChannel);
		this.totalBytesRead += bytesRead;

		return (int) bytesRead;
	}

	@Override
	public int read(ByteBuffer dst) throws IOException {

		if (this.writeMode) {
			throw new IOException("FileBuffer is still in write mode!");
		}

		if (this.fileChannel == null) {
			try {
				this.fileChannel = this.fileBufferManager.getFileChannelForReading(this.ownerID, this.fileID);
				if (this.fileChannel == null) {
					return 0;
				}
			} catch (InterruptedException e) {
				return -1;
			}
			if (this.fileChannel.position() != (this.offset + this.totalBytesRead)) {
				this.fileChannel.position(this.offset + this.totalBytesRead);
			}
		}

		if (this.totalBytesRead >= this.bufferSize) {
			return -1;
		}

		final int rem = remaining();
		int bytesRead;
		if (dst.remaining() > rem) {
			final int excess = dst.remaining() - rem;
			dst.limit(dst.limit() - excess);
			bytesRead = this.fileChannel.read(dst);
			dst.limit(dst.limit() + excess);
		} else {
			bytesRead = this.fileChannel.read(dst);
		}

		if (bytesRead < 0) {
			return -1;
		}

		this.totalBytesRead += bytesRead;

		return bytesRead;
	}

	@Override
	public int write(final ReadableByteChannel readableByteChannel) throws IOException {

		if (!this.writeMode) {
			throw new IOException("Cannot write to buffer, buffer already switched to read mode");
		}

		if (this.fileChannel == null) {
			this.fileChannel = this.fileBufferManager.getFileChannelForWriting(this.ownerID);
			if (this.fileChannel == null) {
				return 0;
			}
			this.offset = this.fileChannel.position();
		}

		if (this.totalBytesWritten >= this.bufferSize) {
			return 0;
		}

		final long bytesWritten = this.fileChannel.transferFrom(readableByteChannel,
			(this.offset + this.totalBytesWritten), (this.bufferSize - this.totalBytesWritten));
		this.totalBytesWritten += bytesWritten;

		return (int) bytesWritten;
	}

	@Override
	public int write(final ByteBuffer src) throws IOException {

		if (!this.writeMode) {
			throw new IOException("Cannot write to buffer, buffer already switched to read mode");
		}

		if (this.fileChannel == null) {
			this.fileChannel = this.fileBufferManager.getFileChannelForWriting(this.ownerID);
			if (this.fileChannel == null) {
				return 0;
			}
		}

		if (this.totalBytesWritten >= this.bufferSize) {
			return 0;
		}

		// Make sure we do not exceed the buffer limit
		long bytesWritten;
		final int rem = (int) (this.bufferSize - this.totalBytesWritten);
		if (src.remaining() > rem) {
			final int excess = src.remaining() - rem;
			src.limit(src.limit() - excess);
			bytesWritten = this.fileChannel.write(src);
			src.limit(src.limit() + excess);
		} else {
			bytesWritten = this.fileChannel.write(src);
		}

		this.totalBytesWritten += bytesWritten;

		return (int) bytesWritten;
	}

	@Override
	public void close() throws IOException {

		System.out.println("Close");
		this.fileChannel.close();
	}

	@Override
	public boolean isOpen() {

		return this.fileChannel.isOpen();
	}

	@Override
	public int remaining() {

		if (this.writeMode) {
			return (int) (this.bufferSize - this.totalBytesWritten);
		} else {
			return (int) (this.bufferSize - this.totalBytesRead);
		}
	}

	@Override
	public int size() {
		return (int) this.bufferSize;
	}

	@Override
	public void recycleBuffer() {

		try {
			if (this.fileChannel != null) {
				this.fileBufferManager.releaseFileChannelForReading(this.ownerID, this.fileID);
				this.fileChannel = null;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		this.fileBufferManager.decreaseBufferCounter(this.ownerID, this.fileID);
	}

	@Override
	public void finishWritePhase() throws IOException {

		if (this.writeMode) {

			final long currentFileSize = this.offset + this.totalBytesWritten;
			// If the input channel this buffer belongs to is already canceled, fileChannel may be null
			if (this.fileChannel != null) {
				this.fileChannel.position(currentFileSize);
			}
			this.fileChannel = null;
			this.bufferSize = this.totalBytesWritten;
			// System.out.println("Buffer size: " + this.bufferSize);
			// TODO: Check synchronization
			this.writeMode = false;
			this.fileID = this.fileBufferManager.reportEndOfWritePhase(this.ownerID, currentFileSize);
		}

	}

	@Override
	public boolean isBackedByMemory() {

		return false;
	}

	@Override
	public InternalBuffer duplicate() throws IOException, InterruptedException {

		this.fileBufferManager.increaseBufferCounter(this.ownerID, this.fileID);

		final FileBuffer dup = new FileBuffer((int) this.bufferSize, this.ownerID, this.fileBufferManager);
		dup.writeMode = this.writeMode;
		dup.fileID = this.fileID;
		dup.offset = this.offset;

		return dup;
	}

	@Override
	public void copyToBuffer(final Buffer destinationBuffer) throws IOException {

		if (destinationBuffer.isBackedByMemory()) {

			final long tbr = this.totalBytesRead;
			if (this.fileChannel != null) {
				this.fileBufferManager.releaseFileChannelForReading(this.ownerID, this.fileID);
			}
			this.totalBytesRead = 0;
			while (remaining() > 0) {
				destinationBuffer.write(this);
			}
			destinationBuffer.finishWritePhase();
			if (this.fileChannel != null) {
				this.fileBufferManager.releaseFileChannelForReading(this.ownerID, this.fileID);
			}
			this.fileChannel = null;
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

		return this.writeMode;
	}

	/**
	 * Returns the ID of the file which backs this buffers.
	 * 
	 * @return the ID of the file which backs this buffer
	 */
	public FileID getFileID() {

		return this.fileID;
	}

	/**
	 * Returns the offset in bytes which marks the begin of the buffer's data in the underlying file.
	 * 
	 * @return the buffer's offset in bytes
	 */
	public long getOffset() {

		return this.offset;
	}
}
