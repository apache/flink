/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.nephele.io.channels;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;


import eu.stratosphere.core.memory.MemorySegment;

public final class MemoryBuffer extends Buffer {

	private final MemoryBufferRecycler bufferRecycler;

	private final MemorySegment internalMemorySegment;
	
	/**
	 * Internal index that points to the next byte to write
	 */
	private int index = 0;
	
	/**
	 * Internal limit to simulate ByteBuffer behavior of MemorySegment. index > limit is not allowed.
	 */
	private int limit = 0;

	MemoryBuffer(final int bufferSize, final MemorySegment memory, final MemoryBufferPoolConnector bufferPoolConnector) {
		if (bufferSize > memory.size()) {
			throw new IllegalArgumentException("Requested segment size is " + bufferSize
				+ ", but provided MemorySegment only has a capacity of " + memory.size());
		}

		this.bufferRecycler = new MemoryBufferRecycler(memory, bufferPoolConnector);
		this.internalMemorySegment = memory;
		this.position(0);
		this.limit(bufferSize);
	}

	private MemoryBuffer(final int bufferSize, final int pos, final MemorySegment memory, final MemoryBufferRecycler bufferRecycler) {
		this.bufferRecycler = bufferRecycler;
		this.internalMemorySegment = memory;
		this.position(pos);
		this.limit(bufferSize);
	}

	@Override
	public int read(ByteBuffer dst) throws IOException {

		if (!this.hasRemaining()) {
			return -1;
		}
		int numBytes = dst.remaining();
		final int remaining = this.remaining();
		if (numBytes == 0) {
			return 0;
		}
		if(numBytes > remaining) {
			numBytes = remaining;
		}
		internalMemorySegment.get(position(), dst, numBytes);
		index += numBytes;
		return numBytes;
	}
	

	@Override
	public int writeTo(WritableByteChannel writableByteChannel) throws IOException {
		if (!this.hasRemaining()) {
			return -1;
		}
		
		final ByteBuffer wrapped = this.internalMemorySegment.wrap(index, limit-index);
		final int written = writableByteChannel.write(wrapped);
		position(wrapped.position());
		return written;
	}


	@Override
	public void close() throws IOException {

		this.position(this.limit());
	}


	@Override
	public boolean isOpen() {

		return this.hasRemaining();
	}

	/**
	 * Resets the memory buffer.
	 * 
	 * @param bufferSize
	 *        the size of buffer in bytes after the reset
	 */
	public final void reset(final int bufferSize) {
		if(bufferSize > this.internalMemorySegment.size()) {
			throw new RuntimeException("Given buffer size exceeds underlying buffer size");
		}
		this.position(0);
		this.limit(bufferSize);
	}

	public final void position(final int i) {
		if(i > limit) {
			throw new IndexOutOfBoundsException("new position is larger than the limit");
		}
		index = i;
	}
	
	@Override
	public final int position() {
		return index;
	}
	
	public final void limit(final int l) {
		if(limit > internalMemorySegment.size()) {
			throw new RuntimeException("Limit is larger than MemoryBuffer size");
		}
		if (index > limit) {
			index = limit;
		}
		limit = l;
	}
	
	public final int limit() {
		return limit;
	}
	
	public final boolean hasRemaining() {
		return index < limit;
    }
	
	public final int remaining() {
		return limit - index;
	}

	/**
	 * Put MemoryBuffer into read mode
	 */
	public final void flip() {
		limit = position();
		position(0);
	}

	public void clear() {
		this.limit = getTotalSize();
		this.position(0);
	}

	/**
	 * 
	 * @return Returns the size of the underlying MemorySegment
	 */
	public int getTotalSize() {
		return this.internalMemorySegment.size();
	}
	
	@Override
	public final int size() {
		return this.limit();
	}

	public MemorySegment getMemorySegment() {
		return this.internalMemorySegment;
	}


	@Override
	protected void recycle() {
		this.bufferRecycler.decreaseReferenceCounter();
		if(bufferRecycler.referenceCounter.get() == 0) {
			clear();
		}
	}



	@Override
	public boolean isBackedByMemory() {
		return true;
	}


	@Override
	public MemoryBuffer duplicate() {
		final MemoryBuffer duplicatedMemoryBuffer = new MemoryBuffer(this.limit(), this.position(), this.internalMemorySegment, this.bufferRecycler);
		this.bufferRecycler.increaseReferenceCounter();
		return duplicatedMemoryBuffer;
	}


	@Override
	public void copyToBuffer(final Buffer destinationBuffer) throws IOException {
		if (size() > destinationBuffer.size()) {
			throw new IllegalArgumentException("Destination buffer is too small to store content of source buffer: "
				+ size() + " vs. " + destinationBuffer.size());
		}
		final MemoryBuffer target = (MemoryBuffer) destinationBuffer;
		this.internalMemorySegment.copyTo(this.position(), target.getMemorySegment(), destinationBuffer.position(), limit()-position());
		target.position(limit()-position()); // even if we do not change the source (this), we change the destination!!
		destinationBuffer.flip();
	}

	 

	@Override
	public int write(final ByteBuffer src) throws IOException {
		int numBytes = src.remaining();
		final int thisRemaining = this.remaining();
		if(thisRemaining == 0) {
			return 0;
		}
		if(numBytes > thisRemaining) {
			numBytes = thisRemaining;
		}
		this.internalMemorySegment.put(position(), src, numBytes);
		this.index += numBytes;
		return numBytes;
	}


	@Override
	public int write(final ReadableByteChannel readableByteChannel) throws IOException {

		if (!this.hasRemaining()) {
			return 0;
		}
		ByteBuffer wrapper = this.internalMemorySegment.wrap(index, limit-index);
		final int written = readableByteChannel.read(wrapper);
		this.position(wrapper.position());
		this.limit(wrapper.limit());
		return written;
	}
}
