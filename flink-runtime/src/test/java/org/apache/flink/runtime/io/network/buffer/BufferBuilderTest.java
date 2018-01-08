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
import org.apache.flink.runtime.testutils.DiscardingRecycler;

import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link BufferBuilder}.
 */
public class BufferBuilderTest {
	private static final int BUFFER_SIZE = 10 * Integer.BYTES;

	@Test
	public void append() {
		BufferBuilder bufferBuilder = createBufferBuilder();
		int[] intsToWrite = new int[] {0, 1, 2, 3, 42};
		ByteBuffer bytesToWrite = toByteBuffer(intsToWrite);

		assertEquals(bytesToWrite.limit(), bufferBuilder.append(bytesToWrite));

		assertEquals(bytesToWrite.limit(), bytesToWrite.position());
		assertFalse(bufferBuilder.isFull());
		Buffer buffer = bufferBuilder.build();
		assertBufferContent(buffer, intsToWrite);
		assertEquals(5 * Integer.BYTES, buffer.getSize());
		assertEquals(DiscardingRecycler.INSTANCE, buffer.getRecycler());
	}

	@Test
	public void multipleAppends() {
		BufferBuilder bufferBuilder = createBufferBuilder();

		bufferBuilder.append(toByteBuffer(0, 1));
		bufferBuilder.append(toByteBuffer(2));
		bufferBuilder.append(toByteBuffer(3, 42));

		Buffer buffer = bufferBuilder.build();
		assertBufferContent(buffer, 0, 1, 2, 3, 42);
		assertEquals(5 * Integer.BYTES, buffer.getSize());
	}

	@Test
	public void appendOverSize() {
		BufferBuilder bufferBuilder = createBufferBuilder();
		ByteBuffer bytesToWrite = toByteBuffer(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 42);

		assertEquals(BUFFER_SIZE, bufferBuilder.append(bytesToWrite));

		assertTrue(bufferBuilder.isFull());
		Buffer buffer = bufferBuilder.build();
		assertBufferContent(buffer, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
		assertEquals(BUFFER_SIZE, buffer.getSize());

		bufferBuilder = createBufferBuilder();
		assertEquals(Integer.BYTES, bufferBuilder.append(bytesToWrite));

		assertFalse(bufferBuilder.isFull());
		buffer = bufferBuilder.build();
		assertBufferContent(buffer, 42);
		assertEquals(Integer.BYTES, buffer.getSize());
	}

	@Test
	public void buildEmptyBuffer() {
		Buffer buffer = createBufferBuilder().build();
		assertEquals(0, buffer.getSize());
		assertBufferContent(buffer);
	}

	@Test(expected = IllegalStateException.class)
	public void buildingBufferTwice() {
		BufferBuilder bufferBuilder = createBufferBuilder();
		bufferBuilder.build();
		bufferBuilder.build();
	}

	private static ByteBuffer toByteBuffer(int... data) {
		ByteBuffer byteBuffer = ByteBuffer.allocate(data.length * Integer.BYTES);
		byteBuffer.asIntBuffer().put(data);
		return byteBuffer;
	}

	private static void assertBufferContent(Buffer actualBuffer, int... expected) {
		assertEquals(toByteBuffer(expected), actualBuffer.getNioBuffer());
	}

	private static BufferBuilder createBufferBuilder() {
		return new BufferBuilder(MemorySegmentFactory.allocateUnpooledSegment(BUFFER_SIZE), DiscardingRecycler.INSTANCE);
	}
}
