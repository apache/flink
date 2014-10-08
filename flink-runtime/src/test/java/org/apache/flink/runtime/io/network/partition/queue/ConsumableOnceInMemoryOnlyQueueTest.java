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

import com.google.common.base.Optional;
import org.apache.flink.runtime.event.task.TaskEvent;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.IntermediateResultPartitionQueueIterator;
import org.apache.flink.runtime.io.network.partition.IntermediateResultPartitionQueue;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class ConsumableOnceInMemoryOnlyQueueTest {

	private static final long RANDOM_SEED = 0xBADC0FFE;

	private static final int SLOW_SLEEP_MS = 5;

	@Test(expected = IllegalStateException.class)
	public void testMultipleConsumersException() {
		final ConsumableOnceInMemoryOnlyQueue queue = new ConsumableOnceInMemoryOnlyQueue();

		assertEquals(0, queue.getNumConsumed());
		assertEquals(0, queue.getNumActiveConsumers());

		queue.getQueueIterator(Optional.of(mock(BufferProvider.class)));

		assertEquals(0, queue.getNumConsumed());
		assertEquals(1, queue.getNumActiveConsumers());

		queue.getQueueIterator(Optional.of(mock(BufferProvider.class)));
	}

	@Test
	public void testSerialProduceConsume() {
		try {
			final ConsumableOnceInMemoryOnlyQueue queue = new ConsumableOnceInMemoryOnlyQueue();

			final Random random = new Random(RANDOM_SEED);

			for (int i = 0; i < 1024; i++) {
				queue.add(random.nextDouble() < 0.9
						? new BufferOrEvent(mock(Buffer.class))
						: new BufferOrEvent(mock(TaskEvent.class)));
			}

			queue.finish();

			assertEquals(0, queue.getNumConsumed());
			assertEquals(0, queue.getNumActiveConsumers());

			IntermediateResultPartitionQueueIterator it = queue.getQueueIterator(Optional.of(mock(BufferProvider.class)));

			assertEquals(0, queue.getNumConsumed());
			assertEquals(1, queue.getNumActiveConsumers());

			random.setSeed(RANDOM_SEED); // reset seed

			while (it.hasNext()) {
				BufferOrEvent boe = it.getNextBufferOrEvent();

				assertNotNull(boe); // should not happen with serial produce-consume

				if (random.nextDouble() < 0.9) {
					assertTrue(boe.isBuffer());
				}
				else {
					assertTrue(boe.isEvent());
				}
			}

			assertEquals(1, queue.getNumConsumed());
			assertEquals(0, queue.getNumActiveConsumers());
		} catch (Throwable t) {
			t.printStackTrace();
			Assert.fail(t.getMessage());
		}
	}

	@Test
	public void testConcurrentProduceConsume() {
		try {
			final ExecutorService executorService = Executors.newCachedThreadPool();

			final NetworkBufferPool buffers = new NetworkBufferPool(512, 8 * 1024);
			final BufferPool bufferPool = buffers.createBufferPool(1, false);

			{
				final ConsumableOnceInMemoryOnlyQueue queue = new ConsumableOnceInMemoryOnlyQueue();
				final IntermediateResultPartitionQueueIterator iterator = queue.getQueueIterator(Optional.of(mock(BufferProvider.class)));

				Future<Void> producer = executorService.submit(new DummyProducer(queue, 512, bufferPool, false));
				Future<Integer> consumer = executorService.submit(new DummyConsumer(iterator, false));

				producer.get();

				assertEquals(512, consumer.get().intValue());
			}

			{
				final ConsumableOnceInMemoryOnlyQueue queue = new ConsumableOnceInMemoryOnlyQueue();
				final IntermediateResultPartitionQueueIterator iterator = queue.getQueueIterator(Optional.of(mock(BufferProvider.class)));


				Future<Void> producer = executorService.submit(new DummyProducer(queue, 512, bufferPool, true));
				Future<Integer> consumer = executorService.submit(new DummyConsumer(iterator, false));

				producer.get();
				assertEquals(512, consumer.get().intValue());
			}

			{
				final ConsumableOnceInMemoryOnlyQueue queue = new ConsumableOnceInMemoryOnlyQueue();
				final IntermediateResultPartitionQueueIterator iterator = queue.getQueueIterator(Optional.of(mock(BufferProvider.class)));


				Future<Void> producer = executorService.submit(new DummyProducer(queue, 512, bufferPool, false));
				Future<Integer> consumer = executorService.submit(new DummyConsumer(iterator, true));

				producer.get();
				assertEquals(512, consumer.get().intValue());
			}

			bufferPool.destroy();

			assertEquals(buffers.getNumMemorySegments(), buffers.getNumAvailableMemorySegments());
		}
		catch (Throwable t) {
			t.printStackTrace();
			Assert.fail(t.getMessage());
		}
	}

	@Test
	public void testConcurrentConsumeDiscard() {
		try {
			final ExecutorService executorService = Executors.newCachedThreadPool();

			final NetworkBufferPool buffers = new NetworkBufferPool(512, 8 * 1024);
			final BufferPool bufferPool = buffers.createBufferPool(1, false);


			final ConsumableOnceInMemoryOnlyQueue queue = new ConsumableOnceInMemoryOnlyQueue();
			final IntermediateResultPartitionQueueIterator iterator = queue.getQueueIterator(Optional.of(mock(BufferProvider.class)));

			Future<Void> producer = executorService.submit(new DummyProducer(queue, 512, bufferPool, false));

			Future<Integer> consumer = executorService.submit(new DummyConsumer(iterator, true));

			producer.get();

			queue.discard();

			assertTrue(consumer.get() < 512); // verify not everything consumed because of discard

			bufferPool.destroy();

			assertEquals(buffers.getNumMemorySegments(), buffers.getNumAvailableMemorySegments());
		}
		catch (Throwable t) {
			t.printStackTrace();
			Assert.fail(t.getMessage());
		}
	}

	// ------------------------------------------------------------------------

	private static class DummyProducer implements Callable<Void> {

		private final Random random = new Random(RANDOM_SEED);

		private final IntermediateResultPartitionQueue queue;

		private final int numToProduce;

		private final BufferProvider bufferProvider;

		private final boolean isSlow;

		private DummyProducer(IntermediateResultPartitionQueue queue, int numToProduce, BufferProvider bufferProvider, boolean isSlow) {
			this.queue = queue;
			this.numToProduce = numToProduce;
			this.bufferProvider = bufferProvider;
			this.isSlow = isSlow;
		}

		@Override
		public Void call() throws Exception {
			for (int i = 0; i < numToProduce; i++) {
				if (random.nextDouble() < 0.9) {
					Buffer buffer = bufferProvider.requestBuffer().waitForBuffer().getBuffer();

					queue.add(new BufferOrEvent(buffer));
				}
				else {
					queue.add(new BufferOrEvent(mock(TaskEvent.class)));
				}

				if (isSlow) {
					Thread.sleep(SLOW_SLEEP_MS);
				}
			}

			queue.finish();

			return null;
		}
	}

	private static class DummyConsumer implements Callable<Integer> {

		private final Random random = new Random(RANDOM_SEED);

		private final IntermediateResultPartitionQueueIterator iterator;

		private final boolean isSlow;

		private DummyConsumer(IntermediateResultPartitionQueueIterator iterator, boolean isSlow) {
			this.iterator = iterator;
			this.isSlow = isSlow;
		}

		@Override
		public Integer call() throws Exception {

			int numRead = 0;

			while (iterator.hasNext()) {
				BufferOrEvent boe = iterator.getNextBufferOrEvent();
				if (boe != null) {
					if (random.nextDouble() < 0.9) {
						assertTrue(boe.isBuffer());
						boe.getBuffer().recycle();
					}
					else {
						assertTrue(boe.isEvent());
					}

					numRead++;
				}

				if (isSlow) {
					Thread.sleep(SLOW_SLEEP_MS);
				}
			}

			return numRead;
		}
	}
}
