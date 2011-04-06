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

import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.InternalBuffer;

public class FileBuffer implements InternalBuffer {

	private long bufferSize;

	private final FileBufferManager fileBufferManager;

	private final ChannelID channelID;

	private FileChannel fileChannel;

	private volatile boolean writeMode = true;

	private long totalBytesWritten = 0;

	private long totalBytesRead = 0;

	private long offset = 0;

	FileBuffer(int bufferSize, ChannelID channelID, FileBufferManager fileBufferManager) {
		this.bufferSize = bufferSize;
		this.channelID = channelID;
		this.fileBufferManager = fileBufferManager;
	}

	@Override
	public int read(WritableByteChannel writableByteChannel) throws IOException {

		if (this.writeMode) {
			throw new IOException("FileBuffer is still in write mode!");
		}

		if (this.fileChannel == null) {
			try {
				this.fileChannel = this.fileBufferManager.getFileChannelForReading(this.channelID);
			} catch (InterruptedException e) {
				return -1;
			}
			if (this.fileChannel.position() != this.offset) {
				System.out.println("Expected offset " + this.offset + ", but channel position is "
					+ this.fileChannel.position());
				this.fileChannel.position(this.offset);
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
				this.fileChannel = this.fileBufferManager.getFileChannelForReading(this.channelID);
			} catch (InterruptedException e) {
				return -1;
			}
			if (this.fileChannel.position() != this.offset) {
				System.out.println("Expected offset " + this.offset + ", but channel position is "
					+ this.fileChannel.position());
				this.fileChannel.position(this.offset);
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
	public int write(ReadableByteChannel readableByteChannel) throws IOException {

		if (!this.writeMode) {
			throw new IOException("Cannot write to buffer, buffer already switched to read mode");
		}

		if (this.fileChannel == null) {
			try {
				this.fileChannel = this.fileBufferManager.getFileChannelForWriting(this.channelID);
			} catch (ChannelCanceledException cce) {
				return writeContentForCanceledChannel(readableByteChannel);
			}
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

	private int writeContentForCanceledChannel(final ReadableByteChannel readableByteChannel) throws IOException {

		final ByteBuffer tmpBuffer = ByteBuffer.allocate(128);
		long bytesWritten = 0;

		long diff = this.bufferSize - this.totalBytesWritten;
		if (diff <= 0) {
			return 0;
		}

		while (diff > 0) {

			// Make sure we don't read too much data from the stream
			if (diff < tmpBuffer.remaining()) {
				tmpBuffer.limit(tmpBuffer.position() + (int) diff);
			}

			final long b = readableByteChannel.read(tmpBuffer);
			if (b == 0) {
				break;
			}
			if (b == -1) {
				throw new IOException("Read unexception -1 from stream");
			}

			if (!tmpBuffer.hasRemaining()) {
				tmpBuffer.clear();
			}

			bytesWritten += b;
			this.totalBytesWritten += bytesWritten;
			diff = this.bufferSize - this.totalBytesWritten;
		}

		return (int) bytesWritten;
	}

	@Override
	public int write(ByteBuffer src) throws IOException {

		if (!this.writeMode) {
			throw new IOException("Cannot write to buffer, buffer already switched to read mode");
		}

		if (this.fileChannel == null) {
			try {
				this.fileChannel = this.fileBufferManager.getFileChannelForWriting(this.channelID);
			} catch (ChannelCanceledException e) {
				throw new IOException("Received unexpected ChannelCanceledException");
			}
			if (this.fileChannel == null) {
				return 0;
			}
		}

		if (this.totalBytesWritten >= this.bufferSize) {
			return 0;
		}

		final long bytesWritten = this.fileChannel.write(src);
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

		this.fileBufferManager.reportFileBufferAsConsumed(this.channelID);
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
			this.fileBufferManager.reportEndOfWritePhase(this.channelID, currentFileSize);
		}

	}

	@Override
	public boolean isBackedByMemory() {

		return false;
	}

	@Override
	public InternalBuffer duplicate() {

		throw new RuntimeException("Not yet implemented");
	}

}
