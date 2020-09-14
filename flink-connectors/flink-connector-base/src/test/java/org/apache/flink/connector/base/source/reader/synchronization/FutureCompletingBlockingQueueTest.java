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

package org.apache.flink.connector.base.source.reader.synchronization;

import org.junit.Test;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The unit test for {@link FutureCompletingBlockingQueue}.
 */
public class FutureCompletingBlockingQueueTest {
	private static final Integer DEFAULT_CAPACITY = 1;
	private static final Integer SPECIFIED_CAPACITY = 20000;

	@Test
	public void testBasics() throws InterruptedException {
		FutureNotifier futureNotifier = new FutureNotifier();
		FutureCompletingBlockingQueue<Integer> queue = new FutureCompletingBlockingQueue<>(futureNotifier, 5);

		CompletableFuture<Void> future = futureNotifier.future();
		assertTrue(queue.isEmpty());
		assertEquals(0, queue.size());

		queue.put(0, 1234);

		assertTrue(future.isDone());
		assertEquals(1, queue.size());
		assertFalse(queue.isEmpty());
		assertEquals(4, queue.remainingCapacity());
		assertNotNull(queue.peek());
		assertEquals(1234, (int) queue.peek());
		assertEquals(1234, (int) queue.poll());

		assertEquals(0, queue.size());
		assertTrue(queue.isEmpty());
		assertEquals(5, queue.remainingCapacity());
	}

	@Test
	public void testPoll() throws InterruptedException {
		FutureNotifier futureNotifier = new FutureNotifier();
		FutureCompletingBlockingQueue<Integer> queue = new FutureCompletingBlockingQueue<>(futureNotifier);
		queue.put(0, 1234);
		Integer value = queue.poll();
		assertNotNull(value);
		assertEquals(1234, (int) value);
	}

	@Test
	public void testWakeUpPut() throws InterruptedException {
		FutureNotifier futureNotifier = new FutureNotifier();
		FutureCompletingBlockingQueue<Integer> queue = new FutureCompletingBlockingQueue<>(futureNotifier, 1);

		CountDownLatch latch = new CountDownLatch(1);
		new Thread(() -> {
			try {
				assertTrue(queue.put(0, 1234));
				assertFalse(queue.put(0, 1234));
				latch.countDown();
			} catch (InterruptedException e) {
				fail("Interrupted unexpectedly.");
			}
		}).start();

		queue.wakeUpPuttingThread(0);
		latch.await();
		assertEquals(0, latch.getCount());
	}

	@Test
	public void testConcurrency() throws InterruptedException {
		FutureNotifier futureNotifier = new FutureNotifier();
		FutureCompletingBlockingQueue<Integer> queue = new FutureCompletingBlockingQueue<>(futureNotifier, 5);
		final int numValuesPerThread = 10000;
		final int numPuttingThreads = 5;
		List<Thread> threads = new ArrayList<>();

		for (int i = 0; i < numPuttingThreads; i++) {
			final int index = i;
			Thread t = new Thread(() -> {
				for (int j = 0; j < numValuesPerThread; j++) {
					int base = index * numValuesPerThread;
					try {
						queue.put(index, base + j);
					} catch (InterruptedException e) {
						fail("putting thread interrupted.");
					}
				}
			});
			t.start();
			threads.add(t);
		}

		BitSet bitSet = new BitSet();
		AtomicInteger count = new AtomicInteger(0);
		for (int i = 0; i < 5; i++) {
			Thread t = new Thread(() -> {
				while (count.get() < numPuttingThreads * numValuesPerThread) {
					Integer value = queue.poll();
					if (value == null) {
						continue;
					}
					count.incrementAndGet();
					if (bitSet.get(value)) {
						fail("Value " + value + " has been consumed before");
					}
					synchronized (bitSet) {
						bitSet.set(value);
					}
				}});
			t.start();
			threads.add(t);
		}
		for (Thread t : threads) {
			t.join();
		}
	}

	@Test
	public void testFutureCompletingBlockingQueueConstructor() {
		FutureNotifier notifier = new FutureNotifier();
		FutureCompletingBlockingQueue<Object> defaultCapacityFutureCompletingBlockingQueue = new FutureCompletingBlockingQueue<>(notifier);
		FutureCompletingBlockingQueue<Object> specifiedCapacityFutureCompletingBlockingQueue = new FutureCompletingBlockingQueue<>(notifier, SPECIFIED_CAPACITY);
		// The capacity of the queue needs to be equal to 10000
		assertEquals(defaultCapacityFutureCompletingBlockingQueue.remainingCapacity(), (int) DEFAULT_CAPACITY);
		// The capacity of the queue needs to be equal to SPECIFIED_CAPACITY
		assertEquals(specifiedCapacityFutureCompletingBlockingQueue.remainingCapacity(), (int) SPECIFIED_CAPACITY);
	}
}
