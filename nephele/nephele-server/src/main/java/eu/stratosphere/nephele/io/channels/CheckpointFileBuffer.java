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

public class CheckpointFileBuffer implements InternalBuffer {

	private final int bufferSize;

	private final ChannelID sourceChannelID;

	private final long offset;

	private final FileBufferManager fileBufferManager;

	private FileChannel fileChannel = null;

	private int totalBytesRead = 0;

	CheckpointFileBuffer(int bufferSize, ChannelID sourceChannelID, long offset, FileBufferManager fileBufferManager) {
		// System.out.println("Buffer created with size" + bufferSize + " and offset " + offset);
		this.bufferSize = bufferSize;
		this.sourceChannelID = sourceChannelID;
		this.offset = offset;
		this.fileBufferManager = fileBufferManager;
	}

	@Override
	public void finishWritePhase() throws IOException {

		throw new IllegalStateException("finishWritePhase called on CheckpointFileBuffer");
	}

	@Override
	public int read(WritableByteChannel writableByteChannel) throws IOException {

		throw new IllegalStateException("Read called with WritableByteChannel");
	}

	@Override
	public int read(ByteBuffer dst) throws IOException {

		if (this.fileChannel == null) {
			try {
				this.fileChannel = this.fileBufferManager.getFileChannelForReading(this.sourceChannelID);
			} catch (InterruptedException e) {
				return -1;
			}
			if (this.fileChannel.position() > this.offset) {
				throw new IOException("Offset of CheckpointFileBuffer is smaller than current file pointer!");
			}
			this.fileChannel.position(this.offset);
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

		this.totalBytesRead += bytesRead;

		return bytesRead;
	}

	@Override
	public void recycleBuffer() {

		// System.out.println("Recycle called on checkpoint file buffer");
		this.fileBufferManager.reportFileBufferAsConsumed(this.sourceChannelID);
	}

	@Override
	public int remaining() {

		return (this.bufferSize - this.totalBytesRead);
	}

	@Override
	public int size() {

		return this.bufferSize;
	}

	@Override
	public int write(ReadableByteChannel readableByteChannel) throws IOException {

		throw new IllegalStateException("write called on CheckpointFileBuffer");
	}

	@Override
	public void close() throws IOException {

		throw new IllegalStateException("close called on CheckpointFileBuffer");
	}

	@Override
	public boolean isOpen() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int write(ByteBuffer src) throws IOException {

		throw new IllegalStateException("write called on CheckpointFileBuffer");
	}

	@Override
	public boolean isBackedByMemory() {

		return false;
	}

	@Override
	public InternalBuffer duplicate() {

		throw new IllegalStateException("duplicate called on CheckpointFileBuffer");
	}

	@Override
	public boolean isReadBuffer() {
		
		return true;
	}

	@Override
	public void copyToBuffer(Buffer destinationBuffer) throws IOException {
		// TODO Auto-generated method stub
		
	}
}
