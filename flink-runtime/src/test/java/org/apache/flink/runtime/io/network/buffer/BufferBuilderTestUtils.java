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

import org.apache.flink.core.memory.MemorySegmentFactory;

import java.nio.ByteBuffer;

import static org.apache.flink.util.Preconditions.checkArgument;

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

	public static BufferBuilder createFilledBufferBuilder(int dataSize) {
		return createFilledBufferBuilder(BUFFER_SIZE, dataSize);
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

	public static BufferConsumer createFilledBufferConsumer(int size, int dataSize) {
		BufferBuilder bufferBuilder = createFilledBufferBuilder(size, dataSize);
		bufferBuilder.finish();
		return bufferBuilder.createBufferConsumer();
	}

	public static BufferConsumer createFilledBufferConsumer(int dataSize) {
		return createFilledBufferConsumer(BUFFER_SIZE, dataSize);
	}

	public static BufferConsumer createEventBufferConsumer(int size) {
		return new BufferConsumer(
			MemorySegmentFactory.allocateUnpooledSegment(size),
			FreeingBufferRecycler.INSTANCE,
			false);
	}
}
