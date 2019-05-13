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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Putting and getting of a sequence of buffers to/from a ByteBuffer.
 * This class handles the headers, length encoding, memory slicing.
 */
final class BufferToByteBuffer {

	// all fields and methods below here have package-private access to avoid bridge
	// methods when accessing them from the nested classes

	static final int HEADER_LENGTH = 8;

	static final int HEADER_VALUE_IS_BUFFER = 0;

	static final int HEADER_VALUE_IS_EVENT = 1;

	static ByteBuffer checkAndConfigureByteBuffer(ByteBuffer buffer) {
		checkArgument(buffer.position() == 0);
		checkArgument(buffer.capacity() > 8);
		checkArgument(buffer.limit() == buffer.capacity());

		return buffer.order(ByteOrder.nativeOrder());
	}

	// ------------------------------------------------------------------------

	static final class Writer {

		private final ByteBuffer memory;

		Writer(ByteBuffer memory) {
			this.memory = checkAndConfigureByteBuffer(memory);
		}

		public boolean writeBuffer(Buffer buffer) {
			final ByteBuffer memory = this.memory;
			final int bufferSize = buffer.getSize();

			if (memory.remaining() < bufferSize + HEADER_LENGTH) {
				return false;
			}

			memory.putInt(buffer.isBuffer() ? HEADER_VALUE_IS_BUFFER : HEADER_VALUE_IS_EVENT);
			memory.putInt(bufferSize);
			memory.put(buffer.getNioBufferReadable());
			return true;
		}

		public ByteBuffer complete() {
			memory.flip();
			return memory;
		}

		public int getNumBytes() {
			return memory.position();
		}
	}

	static final class Reader {

		private final ByteBuffer memory;

		Reader(ByteBuffer memory) {
			this.memory = checkAndConfigureByteBuffer(memory);
		}

		@Nullable
		public Buffer sliceNextBuffer() {
			final ByteBuffer memory = this.memory;
			final int remaining = memory.remaining();

			// we only check the correct case where data is exhausted
			// all other cases can only occur if our write logic is wrong and will already throw
			// buffer underflow exceptions which will cause the read to fail.
			if (remaining == 0) {
				return null;
			}

			final int header = memory.getInt();
			final int size = memory.getInt();

			memory.limit(memory.position() + size);
			ByteBuffer buf = memory.slice();
			memory.position(memory.limit());
			memory.limit(memory.capacity());

			MemorySegment memorySegment = MemorySegmentFactory.wrapOffHeapMemory(buf);
			Buffer buffer = new NetworkBuffer(memorySegment, FreeingBufferRecycler.INSTANCE);
			buffer.setSize(size);

			if (header == HEADER_VALUE_IS_EVENT) {
				buffer.tagAsEvent();
			}

			return buffer;
		}
	}
}
