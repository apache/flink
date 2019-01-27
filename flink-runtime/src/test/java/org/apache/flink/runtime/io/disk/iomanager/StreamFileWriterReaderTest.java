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

package org.apache.flink.runtime.io.disk.iomanager;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class StreamFileWriterReaderTest {

	private BufferFileWriter writer;

	private static IOManager writerIoManager = new IOManagerAsync();

	private BufferFileReader reader;

	private static IOManager readerIoManager = new IOManagerAsync();

	private LinkedBlockingQueue<Buffer> returnedBuffers = new LinkedBlockingQueue<>();

	private static final BufferRecycler BUFFER_RECYCLER = FreeingBufferRecycler.INSTANCE;

	private static final Random random = new Random();

	@BeforeClass
	public static void setUpClass() {
		writerIoManager = new IOManagerAsync();
		readerIoManager = new IOManagerAsync();
	}

	@AfterClass
	public static void shutdown() {
		writerIoManager.shutdown();
		readerIoManager.shutdown();
	}

	@Before
	public void setUpWriterAndReader() {
		final FileIOChannel.ID channel = writerIoManager.createChannel();

		try {
			writer = writerIoManager.createStreamFileWriter(channel);
			reader = readerIoManager.createStreamFileReader(channel, new QueuingCallback<>(returnedBuffers));
		}
		catch (IOException e) {
			if (writer != null) {
				writer.deleteChannel();
			}

			if (reader != null) {
				reader.deleteChannel();
			}

			fail("Failed to setup writer and reader.");
		}
	}

	@After
	public void tearDownWriterAndReader() {
		if (writer != null) {
			writer.deleteChannel();
		}

		if (reader != null) {
			reader.deleteChannel();
		}

		returnedBuffers.clear();
	}

	@Test
	public void testWriteRead() throws IOException {
		final int bytesToWrite = 32 << 20;

		final int writeBufferSize = 32 << 10;
		final int minWriteBytes = writeBufferSize >> 2;

		final int minReaderBufferSize = writeBufferSize >> 2;
		final int maxReaderBufferSize = writeBufferSize << 2;

		// Write buffers filled with ascending numbers...
		int bytesWritten = 0;
		int currentNumber = 0;
		while (bytesWritten < bytesToWrite) {
			final Buffer buffer = createBuffer(writeBufferSize);
			int currentWriteBytes = Math.min(bytesToWrite - bytesWritten,
				getNextMultipleOf(getRandomNumberInRange(minWriteBytes, writeBufferSize), 4));
			currentNumber = fillBufferWithAscendingNumbers(buffer, currentNumber, currentWriteBytes);
			writer.writeBlock(buffer);
			bytesWritten += currentWriteBytes;
		}
		final int lastNumber = currentNumber;

		// Make sure that the writes are finished
		writer.close();

		// Read buffers back in...
		int bytesRead = 0;
		while (bytesRead < bytesToWrite) {
			int currentReadBytes = Math.min(bytesToWrite - bytesRead,
				getNextMultipleOf(getRandomNumberInRange(minReaderBufferSize, maxReaderBufferSize), 4));
			final Buffer buffer = createBuffer(currentReadBytes);
			assertFalse(reader.hasReachedEndOfFile());
			reader.readInto(buffer, currentReadBytes);
			bytesRead += currentReadBytes;
		}

		reader.close();

		assertTrue(reader.hasReachedEndOfFile());

		currentNumber = 0;
		Buffer buffer;

		while ((buffer = returnedBuffers.poll()) != null) {
			currentNumber = verifyBufferFilledWithAscendingNumbers(buffer, currentNumber);
		}

		assertEquals(lastNumber, currentNumber);
	}

	private int getRandomNumberInRange(int min, int max) {
		return random.nextInt((max - min) + 1) + min;
	}

	private int getNextMultipleOf(int number, int multiple) {
		final int mod = number % multiple;

		if (mod == 0) {
			return number;
		}

		return number + multiple - mod;
	}

	private Buffer createBuffer(int bufferSize) {
		return new NetworkBuffer(MemorySegmentFactory.allocateUnpooledSegment(bufferSize), BUFFER_RECYCLER);
	}

	private static int fillBufferWithAscendingNumbers(Buffer buffer, int currentNumber, int size) {
		checkArgument(size % 4 == 0);

		MemorySegment segment = buffer.getMemorySegment();

		for (int i = 0; i < size; i += 4) {
			segment.putInt(i, currentNumber++);
		}
		buffer.setSize(size);

		return currentNumber;
	}

	private static int verifyBufferFilledWithAscendingNumbers(Buffer buffer, int currentNumber) {
		MemorySegment segment = buffer.getMemorySegment();

		int size = buffer.getSize();

		for (int i = 0; i < size; i += 4) {
			if (segment.getInt(i) != currentNumber++) {
				throw new IllegalStateException("Read unexpected number from buffer.");
			}
		}

		return currentNumber;
	}
}
