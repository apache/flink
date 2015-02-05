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
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.MockConsumer;
import org.apache.flink.runtime.io.network.partition.MockNotificationListener;
import org.apache.flink.runtime.io.network.partition.MockProducer;
import org.apache.flink.runtime.io.network.partition.queue.IntermediateResultPartitionQueueIterator.AlreadySubscribedException;
import org.apache.flink.runtime.util.event.NotificationListener;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class PipelinedPartitionQueueTest {

	private static final int NUM_BUFFERS = 1024;

	private static final int BUFFER_SIZE = 32 * 1024;

	private static final NetworkBufferPool networkBuffers = new NetworkBufferPool(NUM_BUFFERS, BUFFER_SIZE);

	private PipelinedPartitionQueue queue;

	@Before
	public void setup() {
		this.queue = new PipelinedPartitionQueue();
	}

	@Test(expected = IllegalQueueIteratorRequestException.class)
	public void testExceptionWhenMultipleConsumers() throws IOException {
		queue.getQueueIterator(Optional.<BufferProvider>absent());

		// This queue is only consumable once, so this should throw an Exception
		queue.getQueueIterator(Optional.<BufferProvider>absent());
	}

	@Test(expected = AlreadySubscribedException.class)
	public void testExceptionWhenMultipleSubscribers() throws IOException {
		IntermediateResultPartitionQueueIterator iterator = queue.getQueueIterator(Optional.<BufferProvider>absent());

		NotificationListener listener = mock(NotificationListener.class);

		// First subscribe should be fine
		assertTrue(iterator.subscribe(listener));

		// This should throw an already subscribed exception
		iterator.subscribe(listener);
	}

	@Test
	public void testProduceConsume() throws Exception {
		Buffer boe = mock(Buffer.class);

		MockNotificationListener listener = new MockNotificationListener();

		IntermediateResultPartitionQueueIterator iterator = queue.getQueueIterator(Optional.<BufferProvider>absent());

		// Empty queue => should return null
		assertNull(iterator.getNextBuffer());

		// But iterator should not be consumed yet...
		assertFalse(iterator.isConsumed());

		// Subscribe for notifications
		assertTrue(iterator.subscribe(listener));

		assertEquals(0, listener.getNumberOfNotifications());

		// Add data to the queue...
		queue.add(boe);

		// ...should result in a notification
		assertEquals(1, listener.getNumberOfNotifications());

		// ...and one available result
		assertNotNull(iterator.getNextBuffer());
		assertNull(iterator.getNextBuffer());
		assertFalse(iterator.isConsumed());

		// Add data to the queue...
		queue.add(boe);
		// ...don't allow to subscribe, if data is available
		assertFalse(iterator.subscribe(listener));

		assertEquals(1, listener.getNumberOfNotifications());
	}

	@Test
	public void testDiscardingProduceWhileSubscribedConsumer() throws IOException {
		IntermediateResultPartitionQueueIterator iterator = queue.getQueueIterator(Optional.<BufferProvider>absent());

		NotificationListener listener = mock(NotificationListener.class);

		assertTrue(iterator.subscribe(listener));

		queue.discard();

		verify(listener, times(1)).onNotification();

		assertTrue(iterator.isConsumed());
	}

	@Test
	public void testConcurrentProduceConsume() throws Exception {
		doTestConcurrentProduceConsume(false, false);
	}

	@Test
	public void testConcurrentSlowProduceConsume() throws Exception {
		doTestConcurrentProduceConsume(true, false);
	}

	@Test
	public void testConcurrentProduceSlowConsume() throws Exception {
		doTestConcurrentProduceConsume(true, false);
	}

	@Test
	public void testConcurrentDiscardingProduceConsume() throws Exception {
		doTestConcurrentProduceConsume(false, false, true);
	}

	@Test
	public void testConcurrentDiscardingSlowProduceConsume() throws Exception {
		doTestConcurrentProduceConsume(true, false, true);
	}

	@Test
	public void testConcurrentDiscardingProduceSlowConsume() throws Exception {
		doTestConcurrentProduceConsume(false, true, true);
	}

	private void doTestConcurrentProduceConsume(boolean slowProducer, boolean slowConsumer) throws Exception {
		doTestConcurrentProduceConsume(slowProducer, slowConsumer, false);
	}

	private void doTestConcurrentProduceConsume(boolean slowProducer, boolean slowConsumer, boolean discardProduce) throws Exception {

		final int bufferPoolSize = 8;

		final int numBuffersToProduce = 64;

		BufferPool producerBufferPool = networkBuffers.createBufferPool(bufferPoolSize, true);

		MockProducer producer = new MockProducer(queue, producerBufferPool, numBuffersToProduce, slowProducer);

		if (discardProduce) {
			producer.discardAfter(new Random().nextInt(numBuffersToProduce));
		}

		MockConsumer consumer = new MockConsumer(queue.getQueueIterator(Optional.<BufferProvider>absent()), slowConsumer);

		ExecutorService executorService = Executors.newCachedThreadPool();

		try {
			Future<Boolean> producerSuccess = executorService.submit(producer);
			Future<Boolean> consumerSuccess = executorService.submit(consumer);

			boolean success = false;
			try {
				success = producerSuccess.get(30, TimeUnit.SECONDS);
				success &= consumerSuccess.get(30, TimeUnit.SECONDS);
			}
			catch (Throwable t) {
				t.printStackTrace();

				if (producer.getError() != null) {
					System.err.println("Producer error:");
					producer.getError().printStackTrace();
				}

				if (consumer.getError() != null) {
					System.err.println("Consumer error:");
					consumer.getError().printStackTrace();
				}

				fail("Unexpected failure during test: " + t.getMessage() + ". Producer error: " + producer.getError() + ", consumer error: " + consumer.getError());
			}

			producerBufferPool.destroy();

			assertTrue(success);
		} finally {
			executorService.shutdownNow();
		}
	}

}
