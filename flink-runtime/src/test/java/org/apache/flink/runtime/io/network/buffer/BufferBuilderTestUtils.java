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
import org.apache.flink.core.memory.MemorySegmentFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.junit.Assert.assertEquals;

/**
 * Utility class for create not-pooled {@link BufferBuilder}.
 */
public class BufferBuilderTestUtils {
	public static final int BUFFER_SIZE = 32 * 1024;

	public static BufferBuilder createBufferBuilder() {
		return createBufferBuilder(BUFFER_SIZE);
	}

	public static BufferBuilder createBufferBuilder(int size) {
		return createFilledBufferBuilder(size, 0);
	}

	public static BufferBuilder createFilledBufferBuilder(int size, int dataSize) {
		checkArgument(size >= dataSize);
		BufferBuilder bufferBuilder = new BufferBuilder(
			MemorySegmentFactory.allocateUnpooledSegment(size),
			FreeingBufferRecycler.INSTANCE);
		return fillBufferBuilder(bufferBuilder, dataSize);
	}

	public static BufferBuilder fillBufferBuilder(BufferBuilder bufferBuilder, int dataSize) {
		bufferBuilder.appendAndCommit(ByteBuffer.allocate(dataSize));
		return bufferBuilder;
	}

	public static Buffer buildSingleBuffer(BufferBuilder bufferBuilder) {
		try (BufferConsumer bufferConsumer = bufferBuilder.createBufferConsumer()) {
			return bufferConsumer.build();
		}
	}

	public static Buffer buildSingleBuffer(BufferConsumer bufferConsumer) {
		Buffer buffer = bufferConsumer.build();
		bufferConsumer.close();
		return buffer;
	}

	public static BufferConsumer createFilledFinishedBufferConsumer(int dataSize) {
		return createFilledBufferConsumer(dataSize, dataSize, true);
	}

	public static BufferConsumer createFilledUnfinishedBufferConsumer(int dataSize) {
		return createFilledBufferConsumer(dataSize, dataSize, false);
	}

	public static BufferConsumer createFilledBufferConsumer(int size, int dataSize, boolean isFinished) {
		checkArgument(size >= dataSize);

		BufferBuilder bufferBuilder = createBufferBuilder(size);
		BufferConsumer bufferConsumer = bufferBuilder.createBufferConsumer();
		fillBufferBuilder(bufferBuilder, dataSize);

		if (isFinished) {
			bufferBuilder.finish();
		}

		return bufferConsumer;
	}

	public static BufferConsumer createEventBufferConsumer(int size, Buffer.DataType dataType) {
		return new BufferConsumer(
			MemorySegmentFactory.allocateUnpooledSegment(size),
			FreeingBufferRecycler.INSTANCE,
			dataType);
	}

	public static Buffer buildBufferWithAscendingInts(int bufferSize, int numInts, int nextValue) {
		final MemorySegment seg = MemorySegmentFactory.allocateUnpooledSegment(bufferSize);
		for (int i = 0; i < numInts; i++) {
			seg.putIntLittleEndian(4 * i, nextValue++);
		}

		return new NetworkBuffer(seg, MemorySegment::free, Buffer.DataType.DATA_BUFFER, 4 * numInts);
	}

	public static void validateBufferWithAscendingInts(Buffer buffer, int numInts, int nextValue) {
		final ByteBuffer bb = buffer.getNioBufferReadable().order(ByteOrder.LITTLE_ENDIAN);

		for (int i = 0; i < numInts; i++) {
			assertEquals(nextValue++, bb.getInt());
		}
	}

	public static Buffer buildSomeBuffer() {
		return buildSomeBuffer(1024);
	}

	public static Buffer buildSomeBuffer(int size) {
		final MemorySegment seg = MemorySegmentFactory.allocateUnpooledSegment(size);
		return new NetworkBuffer(seg, MemorySegment::free, Buffer.DataType.DATA_BUFFER, size);
	}

	public static BufferBuilder createEmptyBufferBuilder(int bufferSize) {
		return new BufferBuilder(MemorySegmentFactory.allocateUnpooledSegment(bufferSize), FreeingBufferRecycler.INSTANCE);
	}
}
