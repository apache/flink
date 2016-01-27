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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Wrapper for pooled {@link MemorySegment} instances.
 */
public class Buffer {

	/** The backing {@link MemorySegment} instance */
	private final MemorySegment memorySegment;

	private final Object recycleLock = new Object();

	/** The recycler for the backing {@link MemorySegment} */
	private final BufferRecycler recycler;

	private boolean isBuffer;

	/** The current number of references to this buffer */
	private int referenceCount = 1;

	/**
	 * The current size of the buffer in the range from 0 (inclusive) to the
	 * size of the backing {@link MemorySegment} (inclusive).
	 */
	private int currentSize;

	public Buffer(MemorySegment memorySegment, BufferRecycler recycler) {
		this(memorySegment, recycler, true);
	}

	public Buffer(MemorySegment memorySegment, BufferRecycler recycler, boolean isBuffer) {
		this.memorySegment = checkNotNull(memorySegment);
		this.recycler = checkNotNull(recycler);
		this.isBuffer = isBuffer;

		this.currentSize = memorySegment.size();
	}

	public boolean isBuffer() {
		return isBuffer;
	}

	public void tagAsEvent() {
		synchronized (recycleLock) {
			ensureNotRecycled();
		}

		isBuffer = false;
	}

	public MemorySegment getMemorySegment() {
		synchronized (recycleLock) {
			ensureNotRecycled();

			return memorySegment;
		}
	}

	public ByteBuffer getNioBuffer() {
		synchronized (recycleLock) {
			ensureNotRecycled();
			// the memory segment returns a distinct buffer every time
			return memorySegment.wrap(0, currentSize);
		}
	}
	
	public BufferRecycler getRecycler(){
		return recycler;
	}

	public int getSize() {
		synchronized (recycleLock) {
			return currentSize;
		}
	}

	public void setSize(int newSize) {
		synchronized (recycleLock) {
			ensureNotRecycled();

			if (newSize < 0 || newSize > memorySegment.size()) {
				throw new IllegalArgumentException("Size of buffer must be >= 0 and <= " +
													memorySegment.size() + ", but was " + newSize + ".");
			}

			currentSize = newSize;
		}
	}

	public void recycle() {
		synchronized (recycleLock) {
			if (--referenceCount == 0) {
				recycler.recycle(memorySegment);
			}
		}
	}

	public Buffer retain() {
		synchronized (recycleLock) {
			ensureNotRecycled();

			referenceCount++;

			return this;
		}
	}

	public boolean isRecycled() {
		synchronized (recycleLock) {
			return referenceCount == 0;
		}
	}

	// Must be called from synchronized scope
	private void ensureNotRecycled() {
		checkState(referenceCount > 0, "Buffer has already been recycled.");
	}

	@Override
	public String toString() {
		synchronized (recycleLock) {
			return String.format("Buffer %s [size: %d, reference count: %d]", hashCode(), currentSize, referenceCount);
		}
	}
}
