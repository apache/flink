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
import org.apache.flink.runtime.io.disk.iomanager.BufferFileWriter;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.testutils.DiscardingRecycler;
import org.apache.flink.runtime.util.event.EventListener;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.Random;

import static com.google.common.base.Preconditions.checkArgument;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class SpilledPartitionViewTest {

	private static final int BUFFER_SIZE = 32 * 1024;

	private static final BufferRecycler BUFFER_RECYCLER = new DiscardingRecycler();

	private static final Random random = new Random();

	private static final IOManager ioManager = new IOManagerAsync();

	private FileIOChannel.ID channel;

	private BufferFileWriter writer;

	@Before
	public void setUpWriterAndReader() {
		channel = ioManager.createChannel();

		try {
			writer = ioManager.createBufferFileWriter(channel);
		}
		catch (IOException e) {
			if (writer != null) {
				writer.deleteChannel();
			}

			fail("Failed to setup writer and reader.");
		}
	}

	@After
	public void tearDownWriterAndReader() {
		if (writer != null) {
			writer.deleteChannel();
		}
	}

	@Test
	public void testWriteConsume() throws IOException, InterruptedException {
		final int numBuffers = 1024;

		int currentNumber = 0;

		final int minBufferSize = BUFFER_SIZE / 4;

		// Write buffers filled with ascending numbers...
		for (int i = 0; i < numBuffers; i++) {
			final Buffer buffer = createBuffer();

			int size = getNextMultipleOf(getRandomNumberInRange(minBufferSize, BUFFER_SIZE), 4);

			buffer.setSize(size);

			currentNumber = fillBufferWithAscendingNumbers(buffer, currentNumber);

			writer.writeBlock(buffer);
		}

		// Make sure that the writes are finished
		writer.close();

		// - CONSUME ----------------------------------------------------------

		MockBufferProvider bufferProvider = new MockBufferProvider(1, BUFFER_SIZE);

		SpilledSubpartitionView iterator = new SpilledSubpartitionView(mock(ResultSubpartition.class), ioManager, channel, bufferProvider);

		MockNotificationListener listener = new MockNotificationListener();

		currentNumber = 0;
		int numReadBuffers = 0;

		// Consume
		while (true) {
			Buffer buffer = iterator.getNextBuffer();

			if (buffer == null) {

				int current = listener.getNumberOfNotifications();

				if (iterator.subscribe(listener)) {
					listener.waitForNotification(current);
				}
				else if (iterator.isConsumed()) {
					break;
				}
			}
			else {
				numReadBuffers++;

				try {
					if (buffer.isBuffer()) {
						currentNumber = verifyBufferFilledWithAscendingNumbers(buffer, currentNumber);
					}
				}
				finally {
					buffer.recycle();
				}
			}
		}

		assertEquals(numBuffers, numReadBuffers);
	}

	// ------------------------------------------------------------------------

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

	private Buffer createBuffer() {
		return new Buffer(new MemorySegment(new byte[BUFFER_SIZE]), BUFFER_RECYCLER);
	}

	public static int fillBufferWithAscendingNumbers(Buffer buffer, int currentNumber) {
		MemorySegment segment = buffer.getMemorySegment();

		final int size = buffer.getSize();

		for (int i = 0; i < size; i += 4) {
			segment.putInt(i, currentNumber++);
		}

		return currentNumber;
	}

	private int verifyBufferFilledWithAscendingNumbers(Buffer buffer, int currentNumber) {
		MemorySegment segment = buffer.getMemorySegment();

		final int size = buffer.getSize();

		for (int i = 0; i < size; i += 4) {
			if (segment.getInt(i) != currentNumber++) {
				throw new IllegalStateException("Read unexpected number from buffer: was " + segment.getInt(i) + ", expected " + (currentNumber-1));
			}
		}

		return currentNumber;
	}

	private static class MockBufferProvider implements BufferProvider, BufferRecycler {

		private final int numBuffers;

		private final int bufferSize;

		private final Queue<Buffer> buffers;

		public MockBufferProvider(int numBuffers, int bufferSize) {
			checkArgument(numBuffers > 0);
			checkArgument(bufferSize > 0);

			this.numBuffers = numBuffers;
			this.bufferSize = bufferSize;

			this.buffers = new ArrayDeque<Buffer>(numBuffers);
			for (int i = 0; i < numBuffers; i++) {
				buffers.add(new Buffer(new MemorySegment(new byte[bufferSize]), this));
			}
		}

		@Override
		public Buffer requestBuffer() throws IOException {
			return buffers.poll();
		}

		@Override
		public Buffer requestBufferBlocking() throws IOException, InterruptedException {
			throw new UnsupportedOperationException("requestBufferBlocking() not supported by mock buffer provider.");
		}

		@Override
		public boolean addListener(EventListener<Buffer> listener) {
			throw new UnsupportedOperationException("addListener() not supported by mock buffer provider.");
		}

		@Override
		public void recycle(MemorySegment memorySegment) {
			buffers.add(new Buffer(memorySegment, this));
		}

		// --------------------------------------------------------------------

		public boolean verifyAllBuffersAvailable() {
			return numBuffers == buffers.size();
		}
	}

}
