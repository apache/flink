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
import org.apache.flink.runtime.io.network.partition.MemoryMappedBuffers.BufferSlicer;

import org.hamcrest.Matchers;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Tests that read the BoundedBlockingSubpartition with multiple threads in parallel.
 */
public class MemoryMappedBuffersTest {

	@ClassRule
	public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

	@Test
	public void testWriteAndReadData() throws Exception {
		testWriteAndReadData(10_000_000, Integer.MAX_VALUE);
	}

	@Test
	public void testWriteAndReadDataAcrossRegions() throws Exception {
		testWriteAndReadData(10_000_000, 1_276_347);
	}

	private static void testWriteAndReadData(int numInts, int regionSize) throws Exception {
		try (MemoryMappedBuffers memory = MemoryMappedBuffers.createWithRegionSize(createTempPath(), regionSize)) {
			final int numBuffers = writeInts(memory, numInts);
			memory.finishWrite();

			readInts(memory.getFullBuffers(), numBuffers, numInts);
		}
	}

	@Test
	public void returnNullAfterEmpty() throws Exception {
		try (MemoryMappedBuffers memory = MemoryMappedBuffers.create(createTempPath())) {
			memory.writeBuffer(BufferBuilderTestUtils.buildSomeBuffer());
			memory.finishWrite();

			final BufferSlicer reader = memory.getFullBuffers();
			assertNotNull(reader.sliceNextBuffer());

			// check that multiple calls now return empty buffers
			assertNull(reader.sliceNextBuffer());
			assertNull(reader.sliceNextBuffer());
			assertNull(reader.sliceNextBuffer());
		}
	}

	@Test
	public void testDeleteFileOnClose() throws Exception {
		final Path path = createTempPath();
		final MemoryMappedBuffers mmb = MemoryMappedBuffers.create(path);
		assertTrue(Files.exists(path));

		mmb.close();

		assertFalse(Files.exists(path));
	}

	@Test
	public void testGetSizeSingleRegion() throws Exception {
		testGetSize(Integer.MAX_VALUE);
	}

	@Test
	public void testGetSizeMultipleRegions() throws Exception {
		testGetSize(100_000);
	}

	private static void testGetSize(int regionSize) throws Exception {
		final int bufferSize1 = 60_787;
		final int bufferSize2 = 76_687;
		final int expectedSize1 = bufferSize1 + BufferToByteBuffer.HEADER_LENGTH;
		final int expectedSizeFinal = bufferSize1 + bufferSize2 + 2 * BufferToByteBuffer.HEADER_LENGTH;

		try (MemoryMappedBuffers memory = MemoryMappedBuffers.createWithRegionSize(createTempPath(), regionSize)) {

			memory.writeBuffer(BufferBuilderTestUtils.buildSomeBuffer(bufferSize1));
			assertEquals(expectedSize1, memory.getSize());

			memory.writeBuffer(BufferBuilderTestUtils.buildSomeBuffer(bufferSize2));
			assertEquals(expectedSizeFinal, memory.getSize());

			memory.finishWrite();
			assertEquals(expectedSizeFinal, memory.getSize());
		}
	}

	// ------------------------------------------------------------------------
	//  utils
	// ------------------------------------------------------------------------

	private static int writeInts(MemoryMappedBuffers memory, int numInts) throws IOException {
		final int bufferSize = 1024 * 1024; // 1 MiByte
		final int numIntsInBuffer = bufferSize / 4;
		int numBuffers = 0;

		for (int nextValue = 0; nextValue < numInts; nextValue += numIntsInBuffer) {
			Buffer buffer = BufferBuilderTestUtils.buildBufferWithAscendingInts(bufferSize, numIntsInBuffer, nextValue);
			memory.writeBuffer(buffer);
			numBuffers++;
		}

		return numBuffers;
	}

	private static void readInts(MemoryMappedBuffers.BufferSlicer memory, int numBuffersExpected, int numInts) throws IOException {
		Buffer b;
		int nextValue = 0;
		int numBuffers = 0;

		while ((b = memory.sliceNextBuffer()) != null) {
			final int numIntsInBuffer = b.getSize() / 4;
			BufferBuilderTestUtils.validateBufferWithAscendingInts(b, numIntsInBuffer, nextValue);
			nextValue += numIntsInBuffer;
			numBuffers++;
		}

		assertEquals(numBuffersExpected, numBuffers);
		assertThat(nextValue, Matchers.greaterThanOrEqualTo(numInts));
	}

	private static Path createTempPath() throws IOException {
		return new File(TMP_FOLDER.newFolder(), "subpartitiondata").toPath();
	}
}
