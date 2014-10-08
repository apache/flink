/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.io.network.buffer;

import org.apache.flink.core.memory.MemorySegment;

import java.nio.ByteBuffer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Wrapper for pooled {@link MemorySegment} instances.
 */
public class Buffer {

	private final Object monitor = new Object();

	/** Size of the backing {@link MemorySegment} instance */
	private final MemorySegment memorySegment;

	/** The recycler for the backing {@link MemorySegment} */
	private final BufferRecycler recycler;

	/** The current number of references to this buffer */
	private int referenceCount = 1;

	/**
	 * The current size of the buffer in the range from 0 (inclusive) to the
	 * size of the backing {@link MemorySegment} (inclusive).
	 */
	private int currentSize;

	public Buffer(MemorySegment memorySegment, int initialSize, BufferRecycler recycler) {
		this.memorySegment = checkNotNull(memorySegment);
		this.currentSize = checkSizeWithinBounds(initialSize);
		this.recycler = checkNotNull(recycler);
	}

	public MemorySegment getMemorySegment() {
		synchronized (monitor) {
			ensureNotRecycled();

			return memorySegment;
		}
	}

	public ByteBuffer getNioBuffer() {
		synchronized (monitor) {
			ensureNotRecycled();

			return memorySegment.wrap(0, currentSize);
		}
	}

	public int getSize() {
		synchronized (monitor) {
			ensureNotRecycled();

			return currentSize;
		}
	}

	public void setSize(int newSize) {
		synchronized (monitor) {
			ensureNotRecycled();

			currentSize = checkSizeWithinBounds(newSize);
		}
	}

	public void recycle() {
		synchronized (monitor) {
			ensureNotRecycled();

			referenceCount--;

			if (referenceCount == 0) {
				recycler.recycle(memorySegment);
			}
		}
	}

	public Buffer retain() {
		synchronized (monitor) {
			ensureNotRecycled();

			referenceCount++;

			return this;
		}
	}

	public boolean isRecycled() {
		synchronized (monitor) {
			return referenceCount > 0;
		}
	}

	public void copyTo(Buffer targetBuffer) {
		synchronized (monitor) {
			ensureNotRecycled();

			checkArgument(currentSize <= targetBuffer.getMemorySegment().size(), "Target buffer is too small to store content of source buffer.");

			memorySegment.copyTo(0, targetBuffer.memorySegment, 0, currentSize);
			targetBuffer.setSize(currentSize);
		}
	}

	private void ensureNotRecycled() {
		synchronized (monitor) {
			checkState(referenceCount > 0, "Buffer has already been recycled.");
		}
	}

	private int checkSizeWithinBounds(int size) {
		checkArgument(size >= 0 && size <= memorySegment.size(), "Size of buffer must be >= 0 and <= " + memorySegment.size() + ", but was " + size + ".");

		return size;
	}

	@Override
	public String toString() {
		synchronized (monitor) {
			return String.format("Buffer %s [size: %d, reference count: %d]",
					hashCode(), currentSize, referenceCount);
		}
	}
}
