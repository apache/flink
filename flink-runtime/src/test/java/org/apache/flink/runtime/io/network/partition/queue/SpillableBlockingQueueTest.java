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

package org.apache.flink.runtime.io.network.partition.queue;

import org.apache.flink.runtime.io.disk.iomanager.IntermediateResultWriterReaderTest.TestEvent;
import org.apache.flink.runtime.io.disk.iomanager.Channel.Enumerator;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferPoolOwner;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.IntermediateResultPartitionQueueIterator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class SpillableBlockingQueueTest {

	private static final long RANDOM_SEED = 0xBADC0FFE;

	private IOManager ioManager;

	private Enumerator enumerator;

	private NetworkBufferPool buffers;

	@Before
	public void setupIoManager() {
		ioManager = new IOManager("/Users/uce/Desktop/writer-test/");
		enumerator = ioManager.createChannelEnumerator();
		buffers = new NetworkBufferPool(1024, 16 * 1024);
	}

	@After
	public void verifyAllBuffersReturned() {
		assertEquals(buffers.getNumMemorySegments(), buffers.getNumAvailableMemorySegments());
	}

	@Test(expected = IllegalStateException.class)
	public void testConsumeBeforeFinishException() {
		final SpillableBlockingQueue queue = new SpillableBlockingQueue(ioManager, enumerator.next());

		queue.getLocalIterator(mock(BufferProvider.class));
	}

	@Test
	public void testSerialWriteReadInMemory() {
		try {
			final SpillableBlockingQueue queue = new SpillableBlockingQueue(ioManager, enumerator.next());

			final Random random = new Random(RANDOM_SEED);

			final BufferPool bufferPool = buffers.createBufferPool(mock(BufferPoolOwner.class), 1, false);

			for (int i = 0; i < buffers.getNumMemorySegments(); i++) {
				queue.add(random.nextDouble() < 0.9
						? new BufferOrEvent(bufferPool.requestBuffer().getBuffer())
						: new BufferOrEvent(new TestEvent(0L)));
			}

			queue.finish();

			assertEquals(0, queue.getNumConsumed());
			assertEquals(0, queue.getNumActiveConsumers());

			IntermediateResultPartitionQueueIterator it = queue.getLocalIterator(mock(BufferProvider.class));

			assertEquals(0, queue.getNumConsumed());
			assertEquals(1, queue.getNumActiveConsumers());

			random.setSeed(RANDOM_SEED); // reset seed

			while (it.hasNext()) {
				BufferOrEvent boe = it.getNextBufferOrEvent();

				if (random.nextDouble() < 0.9) {
					assertTrue(boe.isBuffer());
					boe.getBuffer().recycle();
				}
				else {
					assertTrue(boe.isEvent());
				}
			}

			assertEquals(1, queue.getNumConsumed());
			assertEquals(0, queue.getNumActiveConsumers());

			queue.discard();

			bufferPool.destroy();
		}
		catch (Throwable t) {
			t.printStackTrace();
			Assert.fail(t.getMessage());
		}
	}

	@Test
	public void testSerialWriteReadConcurrentSpill() {
		try {
			final SpillableBlockingQueue queue = new SpillableBlockingQueue(ioManager, enumerator.next());

			final Random random = new Random(RANDOM_SEED);

			final BufferPool bufferPool = buffers.createBufferPool(mock(BufferPoolOwner.class), 1, false);

			int numRequests = 1024;

			for (int i = 0; i < numRequests; i++) {
				queue.add(random.nextDouble() < 0.9
						? new BufferOrEvent(bufferPool.requestBuffer().getBuffer())
						: new BufferOrEvent(new TestEvent(0L)));
			}

			queue.finish();

			assertEquals(0, queue.getNumConsumed());
			assertEquals(0, queue.getNumActiveConsumers());

			final BufferPool inputBufferPool = buffers.createBufferPool(mock(BufferPoolOwner.class), 1, false);

			IntermediateResultPartitionQueueIterator it = queue.getLocalIterator(inputBufferPool);

			assertEquals(0, queue.getNumConsumed());
			assertEquals(1, queue.getNumActiveConsumers());

			random.setSeed(RANDOM_SEED); // reset seed

			while (it.hasNext()) {
				BufferOrEvent boe = it.getNextBufferOrEvent();

				if (boe != null) {
					if (random.nextDouble() < 0.9) {
						assertTrue(boe.isBuffer());
						boe.getBuffer().recycle();
					}
					else {
						assertTrue(boe.isEvent());
					}

					queue.recycleBuffers();

					numRequests--;
				}
			}

			assertEquals(0, numRequests);

			assertEquals(1, queue.getNumConsumed());
			assertEquals(0, queue.getNumActiveConsumers());

			queue.discard();

			inputBufferPool.destroy();

			bufferPool.destroy();
		}
		catch (Throwable t) {
			t.printStackTrace();
			Assert.fail(t.getMessage());
		}
	}
}
