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

import org.apache.flink.connector.base.source.reader.SourceReaderOptions;
import org.apache.flink.runtime.io.AvailabilityProvider;

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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** The unit test for {@link FutureCompletingBlockingQueue}. */
public class FutureCompletingBlockingQueueTest {

    private static final int DEFAULT_CAPACITY = 2;

    @Test
    public void testBasics() throws InterruptedException {
        FutureCompletingBlockingQueue<Integer> queue = new FutureCompletingBlockingQueue<>(5);

        CompletableFuture<Void> future = queue.getAvailabilityFuture();
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
        FutureCompletingBlockingQueue<Integer> queue = new FutureCompletingBlockingQueue<>();
        queue.put(0, 1234);
        Integer value = queue.poll();
        assertNotNull(value);
        assertEquals(1234, (int) value);
    }

    @Test
    public void testPollEmptyQueue() throws InterruptedException {
        FutureCompletingBlockingQueue<Integer> queue = new FutureCompletingBlockingQueue<>();
        queue.put(0, 1234);

        assertNotNull(queue.poll());
        assertNull(queue.poll());
        assertNull(queue.poll());
    }

    @Test
    public void testWakeUpPut() throws InterruptedException {
        FutureCompletingBlockingQueue<Integer> queue = new FutureCompletingBlockingQueue<>(1);

        CountDownLatch latch = new CountDownLatch(1);
        new Thread(
                        () -> {
                            try {
                                assertTrue(queue.put(0, 1234));
                                assertFalse(queue.put(0, 1234));
                                latch.countDown();
                            } catch (InterruptedException e) {
                                fail("Interrupted unexpectedly.");
                            }
                        })
                .start();

        queue.wakeUpPuttingThread(0);
        latch.await();
        assertEquals(0, latch.getCount());
    }

    @Test
    public void testConcurrency() throws InterruptedException {
        FutureCompletingBlockingQueue<Integer> queue = new FutureCompletingBlockingQueue<>(5);
        final int numValuesPerThread = 10000;
        final int numPuttingThreads = 5;
        List<Thread> threads = new ArrayList<>();

        for (int i = 0; i < numPuttingThreads; i++) {
            final int index = i;
            Thread t =
                    new Thread(
                            () -> {
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
            Thread t =
                    new Thread(
                            () -> {
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
                                }
                            });
            t.start();
            threads.add(t);
        }
        for (Thread t : threads) {
            t.join();
        }
    }

    @Test
    public void testSpecifiedQueueCapacity() {
        final int capacity = 8_000;
        final FutureCompletingBlockingQueue<Object> queue =
                new FutureCompletingBlockingQueue<>(capacity);
        assertEquals(capacity, queue.remainingCapacity());
    }

    @Test
    public void testQueueDefaultCapacity() {
        final FutureCompletingBlockingQueue<Object> queue = new FutureCompletingBlockingQueue<>();
        assertEquals(DEFAULT_CAPACITY, queue.remainingCapacity());
        assertEquals(
                DEFAULT_CAPACITY,
                SourceReaderOptions.ELEMENT_QUEUE_CAPACITY.defaultValue().intValue());
    }

    @Test
    public void testUnavailableWhenEmpty() {
        final FutureCompletingBlockingQueue<Object> queue = new FutureCompletingBlockingQueue<>();
        assertFalse(queue.getAvailabilityFuture().isDone());
    }

    @Test
    public void testImmediatelyAvailableAfterPut() throws InterruptedException {
        final FutureCompletingBlockingQueue<Object> queue = new FutureCompletingBlockingQueue<>();
        queue.put(0, new Object());
        assertTrue(queue.getAvailabilityFuture().isDone());
    }

    @Test
    public void testFutureBecomesAvailableAfterPut() throws InterruptedException {
        final FutureCompletingBlockingQueue<Object> queue = new FutureCompletingBlockingQueue<>();
        final CompletableFuture<?> future = queue.getAvailabilityFuture();
        queue.put(0, new Object());
        assertTrue(future.isDone());
    }

    @Test
    public void testUnavailableWhenBecomesEmpty() throws InterruptedException {
        final FutureCompletingBlockingQueue<Object> queue = new FutureCompletingBlockingQueue<>();
        queue.put(0, new Object());
        queue.poll();
        assertFalse(queue.getAvailabilityFuture().isDone());
    }

    @Test
    public void testAvailableAfterNotifyAvailable() throws InterruptedException {
        final FutureCompletingBlockingQueue<Object> queue = new FutureCompletingBlockingQueue<>();
        queue.notifyAvailable();
        assertTrue(queue.getAvailabilityFuture().isDone());
    }

    @Test
    public void testFutureBecomesAvailableAfterNotifyAvailable() throws InterruptedException {
        final FutureCompletingBlockingQueue<Object> queue = new FutureCompletingBlockingQueue<>();
        final CompletableFuture<?> future = queue.getAvailabilityFuture();
        queue.notifyAvailable();
        assertTrue(future.isDone());
    }

    @Test
    public void testPollResetsAvailability() throws InterruptedException {
        final FutureCompletingBlockingQueue<Object> queue = new FutureCompletingBlockingQueue<>();
        queue.notifyAvailable();

        final CompletableFuture<?> beforePoll = queue.getAvailabilityFuture();
        queue.poll();
        final CompletableFuture<?> afterPoll = queue.getAvailabilityFuture();

        assertTrue(beforePoll.isDone());
        assertFalse(afterPoll.isDone());
    }

    /**
     * This test is to guard that our reflection is not broken and we do not lose the performance
     * advantage. This is possible, because the tests depend on the runtime modules while the main
     * scope does not.
     */
    @Test
    public void testQueueUsesShortCircuitFuture() {
        assertSame(AvailabilityProvider.AVAILABLE, FutureCompletingBlockingQueue.AVAILABLE);
    }
}
