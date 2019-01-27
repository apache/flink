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

package org.apache.flink.runtime.io.network.api.serialization;

import org.apache.flink.core.memory.HeapMemorySegment;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;

/**
 * Helper class to reuse an resizable buffer.
 */
final class ResizableBuffer {

	static class EmptyBufferRecycler implements BufferRecycler {
		@Override
		public void recycle(MemorySegment segment) {
			// Do nothing.
		}
	}

	private static EmptyBufferRecycler emptyBufferRecycler = new EmptyBufferRecycler();

	private HeapMemorySegment segment;
	private NetworkBuffer buffer;
	private volatile boolean isRecycled;

	public ResizableBuffer() {
		isRecycled = true;
	}

	public void recycle() {
		if (segment != null && !isRecycled) {
			if (this.segment == segment) {
				isRecycled = true;
				return;
			}
			throw new IllegalStateException("MemorySegment to be recycled doesn't match.");
		}
	}

	public Buffer getBuffer() {
		if (!isRecycled) {
			return buffer;
		}
		throw new IllegalStateException("The buffer has been recycled or uninitialized.");
	}

	public byte[] getHeapMemory() {
		return segment.getArray();
	}

	public void resetCapacity(int length) {
		// Ensure previous buffer has been recycled.
		if (!isRecycled) {
			throw new IllegalStateException("ResizableBuffer needs to be recycled before reset.");
		}

		// Ensure memory segment has enough capacity.
		if (segment != null && segment.size() >= length) {
			buffer.clear();
		} else {
			if (segment == null) {
				segment = HeapMemorySegment.FACTORY.wrapPooledHeapMemory(new byte[length], this);
			} else if (segment.size() < length) {
				segment.pointTo(new byte[length], this);
			}
			buffer = new NetworkBuffer(segment, emptyBufferRecycler);
		}

		isRecycled = false;
	}

	public void clear() {
		isRecycled = true;
		buffer = null;
		if (segment != null) {
			segment.free();
			segment = null;
		}
	}
}
