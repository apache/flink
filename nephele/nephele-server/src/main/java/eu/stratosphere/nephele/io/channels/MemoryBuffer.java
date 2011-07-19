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
import java.util.Queue;

import eu.stratosphere.nephele.io.channels.InternalBuffer;

public class MemoryBuffer implements InternalBuffer {

	private final MemoryBufferRecycler bufferRecycler;
	
	private final ByteBuffer byteBuffer;
	
	private volatile boolean writeMode = true;

	MemoryBuffer(int bufferSize, ByteBuffer byteBuffer, Queue<ByteBuffer> queueForRecycledBuffers) {

		if (bufferSize > byteBuffer.capacity()) {
			throw new IllegalArgumentException("Requested buffer size is " + bufferSize
				+ ", but provided byte buffer only has a capacity of " + byteBuffer.capacity());
		}
		
		this.bufferRecycler = new MemoryBufferRecycler(byteBuffer, queueForRecycledBuffers);
		
		this.byteBuffer = byteBuffer;
		this.byteBuffer.position(0);
		this.byteBuffer.limit(bufferSize);
	}
	
	private MemoryBuffer(int bufferSize, ByteBuffer byteBuffer, MemoryBufferRecycler bufferRecycler) {
		
		this.bufferRecycler = bufferRecycler;

		
		this.byteBuffer = byteBuffer;
		this.byteBuffer.position(0);
		this.byteBuffer.limit(bufferSize);
	}

	@Override
	public int read(ByteBuffer dst) throws IOException {

		if (this.writeMode) {
			this.writeMode = false;
			this.byteBuffer.flip();
			// System.out.println("Switching to read mode: " + this.byteBuffer);
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

		// System.out.println("Position is " + this.byteBuffer);
		return (this.byteBuffer.position() - oldPosition);
	}

	@Override
	public int read(WritableByteChannel writableByteChannel) throws IOException {

		if (this.writeMode) {
			throw new IOException("Buffer is still in write mode!");
		}

		if (!this.byteBuffer.hasRemaining()) {
			return -1;
		}

		return writableByteChannel.write(this.byteBuffer);
	}

	@Override
	public void close() throws IOException {

		this.byteBuffer.position(this.byteBuffer.limit());
	}

	@Override
	public boolean isOpen() {

		return this.byteBuffer.hasRemaining();
	}

	@Override
	public int write(ByteBuffer src) throws IOException {

		if (!this.writeMode) {
			throw new IOException("Cannot write to buffer, buffer already switched to read mode");
		}

		final int bytesToCopy = Math.min(src.remaining(), this.byteBuffer.remaining());

		if (bytesToCopy == 0) {
			return 0;
		}

		this.byteBuffer.put(src.array(), src.position(), bytesToCopy);
		src.position(src.position() + bytesToCopy);
		// this.byteBuffer.position(this.byteBuffer.position() + bytesToCopy);

		return bytesToCopy;
	}

	@Override
	public int write(ReadableByteChannel readableByteChannel) throws IOException {

		if (!this.writeMode) {
			throw new IOException("Cannot write to buffer, buffer already switched to read mode");
		}

		if (!this.byteBuffer.hasRemaining()) {
			return 0;
		}

		return readableByteChannel.read(this.byteBuffer);
	}

	@Override
	public int remaining() {

		return this.byteBuffer.remaining();
	}

	@Override
	public int size() {
		return this.byteBuffer.limit();
	}

	public ByteBuffer getByteBuffer() {
		return this.byteBuffer;
	}

	@Override
	public void recycleBuffer() {

		this.bufferRecycler.decreaseReferenceCounter();
	}

	@Override
	public void finishWritePhase() {

		if (!this.writeMode) {
			throw new IllegalStateException("MemoryBuffer is already in write mode!");
		}

		this.byteBuffer.flip();
		this.writeMode = false;
	}

	@Override
	public boolean isBackedByMemory() {

		return true;
	}

	@Override
	public InternalBuffer duplicate() {

		final MemoryBuffer duplicatedMemoryBuffer = new MemoryBuffer(this.byteBuffer.limit(), this.byteBuffer
			.duplicate(), this.bufferRecycler);
		
		this.bufferRecycler.increaseReferenceCounter();

		duplicatedMemoryBuffer.byteBuffer.position(this.byteBuffer.position());
		duplicatedMemoryBuffer.byteBuffer.limit(this.byteBuffer.limit());
		duplicatedMemoryBuffer.writeMode = this.writeMode;

		return duplicatedMemoryBuffer;
	}

	@Override
	public void copyToBuffer(Buffer destinationBuffer) throws IOException {
		
		final int oldPos = this.byteBuffer.position();
		
		while(remaining() > 0) {
			destinationBuffer.write(this);
		}
		
		this.byteBuffer.position(oldPos);
		
		if(!this.writeMode) {
			destinationBuffer.finishWritePhase();
		}
	}
}
