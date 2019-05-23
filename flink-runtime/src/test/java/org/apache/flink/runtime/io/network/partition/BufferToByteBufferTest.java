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

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils;
import org.apache.flink.runtime.io.network.partition.BufferToByteBuffer.Reader;
import org.apache.flink.runtime.io.network.partition.BufferToByteBuffer.Writer;

import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

/**
 * Tests for the {@link BufferToByteBuffer}.
 */
public class BufferToByteBufferTest {

	@Test
	public void testCompleteIsSameBufferAsOriginal() {
		final ByteBuffer bb = ByteBuffer.allocateDirect(128);
		final BufferToByteBuffer.Writer writer = new BufferToByteBuffer.Writer(bb);

		final ByteBuffer result = writer.complete();

		assertSame(bb, result);
	}

	@Test
	public void testWriteReadMatchesCapacity() {
		final ByteBuffer bb = ByteBuffer.allocateDirect(1200);
		testWriteAndReadMultipleBuffers(bb, 100);
	}

	@Test
	public void testWriteReadWithLeftoverCapacity() {
		final ByteBuffer bb = ByteBuffer.allocateDirect(1177);
		testWriteAndReadMultipleBuffers(bb, 100);
	}

	private void testWriteAndReadMultipleBuffers(ByteBuffer buffer, int numIntsPerBuffer) {
		final Writer writer = new Writer(buffer);

		int numBuffers = 0;
		while (writer.writeBuffer(BufferBuilderTestUtils.buildBufferWithAscendingInts(1024, numIntsPerBuffer, 0))) {
			numBuffers++;
		}

		final ByteBuffer bb = writer.complete().slice();

		final Reader reader = new Reader(bb);
		Buffer buf;
		while ((buf = reader.sliceNextBuffer()) != null) {
			BufferBuilderTestUtils.validateBufferWithAscendingInts(buf, numIntsPerBuffer, 0);
			numBuffers--;
		}

		assertEquals(0, numBuffers);
	}

}
