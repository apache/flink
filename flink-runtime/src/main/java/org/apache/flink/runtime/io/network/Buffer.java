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

package org.apache.flink.runtime.io.network;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.flink.core.memory.MemorySegment;

public class Buffer {

	private final MemorySegment memorySegment;

	private final BufferRecycler recycler;

	// -----------------------------------------------------------------------------------------------------------------

	private final AtomicInteger referenceCounter;

	private int size;

	// -----------------------------------------------------------------------------------------------------------------

	public Buffer(MemorySegment memorySegment, int size, BufferRecycler recycler) {
		this.memorySegment = memorySegment;
		this.size = size;
		this.recycler = recycler;

		// we are the first, so we start with reference count of one
		this.referenceCounter = new AtomicInteger(1);
	}

	/**
	 * @param toDuplicate Buffer instance to duplicate
	 */
	private Buffer(Buffer toDuplicate) {
		if (toDuplicate.referenceCounter.getAndIncrement() == 0) {
			throw new IllegalStateException("Buffer was released before duplication.");
		}
		
		this.memorySegment = toDuplicate.memorySegment;
		this.size = toDuplicate.size;
		this.recycler = toDuplicate.recycler;
		this.referenceCounter = toDuplicate.referenceCounter;
	}

	// -----------------------------------------------------------------------------------------------------------------

	public MemorySegment getMemorySegment() {
		return this.memorySegment;
	}

	public int size() {
		return this.size;
	}

	public void limitSize(int size) {
		if (size >= 0 && size <= this.memorySegment.size()) {
			this.size = size;
		} else {
			throw new IllegalArgumentException();
		}
	}

	public void recycleBuffer() {
		int refCount = this.referenceCounter.decrementAndGet();
		if (refCount == 0) {
			this.recycler.recycle(this.memorySegment);
		}
	}

	public Buffer duplicate() {
		return new Buffer(this);
	}

	public void copyToBuffer(Buffer destinationBuffer) {
		if (size() > destinationBuffer.size()) {
			throw new IllegalArgumentException("Destination buffer is too small to store content of source buffer.");
		}

		this.memorySegment.copyTo(0, destinationBuffer.memorySegment, 0, size);
	}
}
