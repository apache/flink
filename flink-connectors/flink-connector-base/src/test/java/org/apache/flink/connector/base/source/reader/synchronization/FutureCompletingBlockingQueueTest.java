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
import org.apache.flink.runtime.testutils.CommonTestUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** The unit test for {@link FutureCompletingBlockingQueue}. */
class FutureCompletingBlockingQueueTest {

    private static final int DEFAULT_CAPACITY = 2;

    @Test
    void testBasics() throws InterruptedException {
        FutureCompletingBlockingQueue<Integer> queue = new FutureCompletingBlockingQueue<>(5);

        CompletableFuture<Void> future = queue.getAvailabilityFuture();
        assertThat(queue.isEmpty()).isTrue();
        assertThat(queue.size()).isEqualTo(0);

        queue.put(0, 1234);

        assertThat(future.isDone()).isTrue();
        assertThat(queue.size()).isEqualTo(1);
        assertThat(queue.isEmpty()).isFalse();
        assertThat(queue.remainingCapacity()).isEqualTo(4);
        assertThat(queue.peek()).isNotNull();
        assertThat((int) queue.peek()).isEqualTo(1234);
        assertThat((int) queue.poll()).isEqualTo(1234);

        assertThat(queue.size()).isEqualTo(0);
        assertThat(queue.isEmpty()).isTrue();
        assertThat(queue.remainingCapacity()).isEqualTo(5);
    }

    @Test
    void testPoll() throws InterruptedException {
        FutureCompletingBlockingQueue<Integer> queue = new FutureCompletingBlockingQueue<>();
        queue.put(0, 1234);
        Integer value = queue.poll();
        assertThat(value).isNotNull();
        assertThat((int) value).isEqualTo(1234);
    }

    @Test
    void testPollEmptyQueue() throws InterruptedException {
        FutureCompletingBlockingQueue<Integer> queue = new FutureCompletingBlockingQueue<>();
        queue.put(0, 1234);

        assertThat(queue.poll()).isNotNull();
        assertThat(queue.poll()).isNull();
        assertThat(queue.poll()).isNull();
    }

    @Test
    void testWakeUpPut() throws InterruptedException {
        final FutureCompletingBlockingQueue<Integer> queue = new FutureCompletingBlockingQueue<>(1);

        queue.wakeUpPuttingThread(0);
        assertThat(queue.put(1, 1234)).isTrue();
        assertThat(queue.put(0, 1234)).isFalse();
    }

    /**
     * A putter gracefully woken via {@link FutureCompletingBlockingQueue#wakeUpPuttingThread(int)}
     * must not prevent another, genuinely waiting putter from being signalled once a slot frees up.
     * See FLINK-37663.
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testWakeUpDoesNotStrandAnotherPutter() throws Exception {
        final FutureCompletingBlockingQueue<Integer> queue = new FutureCompletingBlockingQueue<>(1);

        queue.put(2, 0);

        final AtomicBoolean wokenPutterResult = new AtomicBoolean(true);
        final Thread wokenPutter =
                new Thread(() -> wokenPutterResult.set(putUnchecked(queue, 0, 1)), "wokenPutter");
        final CountDownLatch genuinePutterDone = new CountDownLatch(1);
        final Thread genuinePutter =
                new Thread(
                        () -> {
                            putUnchecked(queue, 1, 2);
                            genuinePutterDone.countDown();
                        },
                        "genuinePutter");

        // Block putter 0 before putter 1 so putter 0 is at the head of the wait queue.
        wokenPutter.start();
        CommonTestUtils.waitUntilCondition(() -> queue.getNumberOfQueuedPutters() == 1);
        genuinePutter.start();
        CommonTestUtils.waitUntilCondition(() -> queue.getNumberOfQueuedPutters() == 2);

        queue.wakeUpPuttingThread(0);
        wokenPutter.join();
        assertThat(wokenPutterResult).isFalse();

        queue.poll();

        assertThat(genuinePutterDone.await(10, TimeUnit.SECONDS))
                .as("A still-waiting putter must be signalled when a slot frees up")
                .isTrue();
    }

    /**
     * Without {@link FutureCompletingBlockingQueue#releaseProducer(int)} the queue keeps one
     * condition per producer index it has ever seen. Because {@code SplitFetcherManager} allocates
     * a fresh, never-recycled index per {@code SplitFetcher}, a source with short-lived fetchers
     * accumulates them for the lifetime of the JVM.
     */
    @Test
    void testReleaseProducerBoundsTheWakeupState() throws InterruptedException {
        final FutureCompletingBlockingQueue<Integer> queue = new FutureCompletingBlockingQueue<>(1);

        for (int producer = 0; producer < 3; producer++) {
            // Fill the single slot, so the next put takes the full-queue path that registers
            // wakeup state for this producer.
            assertThat(queue.put(producer, producer)).isTrue();
            queue.wakeUpPuttingThread(producer);
            assertThat(queue.put(producer, producer)).isFalse();
            queue.poll();

            assertThat(QueueProbe.liveProducerStates(queue)).isOne();
            assertThat(QueueProbe.producerStateStorageSize(queue)).isOne();

            queue.releaseProducer(producer);

            assertThat(QueueProbe.liveProducerStates(queue)).isZero();
            assertThat(QueueProbe.producerStateStorageSize(queue)).isZero();
        }

        assertThat(queue.getNumberOfQueuedPutters()).isZero();
    }

    /**
     * The storage assertions are what separate releasing the state from merely clearing it. An
     * implementation that kept the {@code ConditionAndFlag[]} and only nulled the released slot
     * would satisfy every live-count assertion above while still growing its backing array to the
     * largest index ever seen, so this pins the storage down as well.
     */
    @Test
    void testSparseProducerIndexDoesNotExpandStorage() throws InterruptedException {
        final FutureCompletingBlockingQueue<Integer> queue = new FutureCompletingBlockingQueue<>(1);

        assertThat(queue.put(100_000, 1)).isTrue();
        assertThat(QueueProbe.liveProducerStates(queue)).isZero();
        assertThat(QueueProbe.producerStateStorageSize(queue)).isZero();
        assertThat(queue.poll()).isOne();

        queue.wakeUpPuttingThread(100_000);

        assertThat(QueueProbe.liveProducerStates(queue)).isOne();
        assertThat(QueueProbe.producerStateStorageSize(queue)).isOne();

        queue.releaseProducer(100_000);

        assertThat(QueueProbe.liveProducerStates(queue)).isZero();
        assertThat(QueueProbe.producerStateStorageSize(queue)).isZero();
    }

    /** Release is idempotent, tolerates unknown ids, and only removes the target state. */
    @Test
    void testReleaseProducerOnlyRemovesTheTargetState() {
        final FutureCompletingBlockingQueue<Integer> queue = new FutureCompletingBlockingQueue<>(1);

        queue.wakeUpPuttingThread(3);
        queue.wakeUpPuttingThread(4);

        queue.releaseProducer(7);
        queue.releaseProducer(3);
        queue.releaseProducer(3);

        assertThat(QueueProbe.containsProducerState(queue, 3)).isFalse();
        assertThat(QueueProbe.containsProducerState(queue, 4)).isTrue();
        assertThat(QueueProbe.liveProducerStates(queue)).isOne();

        queue.releaseProducer(4);
        assertThat(QueueProbe.liveProducerStates(queue)).isZero();
    }

    /**
     * Releasing a producer that is currently parked in {@code waitOnPut} would discard the wakeUp
     * flag it is about to read and could leave it parked for good, so the release must be refused
     * while it is still waiting.
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testReleaseProducerIsRefusedWhileTheProducerIsParked() throws Exception {
        final FutureCompletingBlockingQueue<Integer> queue = new FutureCompletingBlockingQueue<>(1);

        queue.put(0, 0);

        final AtomicBoolean parkedPutterResult = new AtomicBoolean(true);
        final Thread parkedPutter =
                new Thread(() -> parkedPutterResult.set(putUnchecked(queue, 1, 1)), "parkedPutter");
        parkedPutter.start();
        try {
            CommonTestUtils.waitUntilCondition(() -> queue.getNumberOfQueuedPutters() == 1);

            queue.releaseProducer(1);
            assertThat(QueueProbe.liveProducerStates(queue))
                    .as("must not drop wakeup state for a producer currently inside put()")
                    .isOne();

            // The graceful wakeup still reaches it, which is what the refusal protects.
            queue.wakeUpPuttingThread(1);
            joinWithinTimeout(parkedPutter);
            assertThat(parkedPutterResult).isFalse();

            // Once it has left put(), the release goes through.
            queue.releaseProducer(1);
            assertThat(QueueProbe.liveProducerStates(queue)).isZero();
        } finally {
            if (parkedPutter.isAlive()) {
                queue.wakeUpPuttingThread(1);
                parkedPutter.interrupt();
                queue.poll();
                parkedPutter.join(TimeUnit.SECONDS.toMillis(10));
            }
            queue.releaseProducer(1);
        }
    }

    /** An interrupted wait must not leave the producer permanently marked as waiting. */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void testInterruptedPutterDoesNotPreventProducerRelease() throws Exception {
        final FutureCompletingBlockingQueue<Integer> queue = new FutureCompletingBlockingQueue<>(1);
        assertThat(queue.put(0, 0)).isTrue();

        final CompletableFuture<Boolean> putInterrupted = new CompletableFuture<>();
        final Thread putter =
                new Thread(
                        () -> {
                            try {
                                queue.put(1, 1);
                                putInterrupted.complete(false);
                            } catch (InterruptedException expected) {
                                putInterrupted.complete(true);
                            } catch (Throwable failure) {
                                putInterrupted.completeExceptionally(failure);
                            }
                        },
                        "interruptedPutter");
        putter.start();
        try {
            CommonTestUtils.waitUntilCondition(() -> queue.getNumberOfQueuedPutters() == 1);

            putter.interrupt();
            assertThat(putInterrupted.get(10, TimeUnit.SECONDS)).isTrue();
            joinWithinTimeout(putter);
            assertThat(queue.getNumberOfQueuedPutters()).isZero();
            assertThat(QueueProbe.containsProducerState(queue, 1)).isTrue();

            queue.releaseProducer(1);
            assertThat(QueueProbe.containsProducerState(queue, 1)).isFalse();
        } finally {
            if (putter.isAlive()) {
                putter.interrupt();
                queue.poll();
                putter.join(TimeUnit.SECONDS.toMillis(10));
            }
            queue.releaseProducer(1);
        }
    }

    /**
     * The companion to the test above, for the window that makes membership of {@code notFull} an
     * unreliable answer to "is this producer still inside {@code put()}?".
     *
     * <p>{@code signalNextPutter()} removes a condition from {@code notFull} at signal time, not
     * when its producer resumes, so between the signal and that producer reacquiring the lock it is
     * still inside {@code put()} while absent from {@code notFull}. A release in that window would
     * drop the state together with a wakeUp flag the producer has not read yet, and the producer
     * would then build fresh state with no flag and park again. The {@code waitingPutters} counter
     * spans the whole of {@code cond.await()} and therefore covers it.
     *
     * <p>The window is driven deterministically rather than raced for: the test thread takes the
     * queue's own lock, so the signalled producer cannot reacquire it and cannot leave {@code
     * put()} until the test releases it.
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void testReleaseProducerIsRefusedAfterSignalBeforePutterReacquiresLock() throws Exception {
        final FutureCompletingBlockingQueue<Integer> queue = new FutureCompletingBlockingQueue<>(1);
        assertThat(queue.put(0, 0)).isTrue();

        final AtomicBoolean putterResult = new AtomicBoolean(true);
        final Thread putter =
                new Thread(() -> putterResult.set(putUnchecked(queue, 1, 1)), "signalledPutter");
        putter.start();
        try {
            CommonTestUtils.waitUntilCondition(() -> queue.getNumberOfQueuedPutters() == 1);

            final ReentrantLock queueLock = QueueProbe.queueLock(queue);
            queueLock.lock();
            try {
                assertThat(queue.poll()).isZero();
                assertThat(QueueProbe.queuedPutters(queue)).isZero();

                queue.wakeUpPuttingThread(1);
                queue.releaseProducer(1);
                assertThat(QueueProbe.containsProducerState(queue, 1)).isTrue();

                assertThat(queue.put(2, 2)).isTrue();
            } finally {
                queueLock.unlock();
            }

            joinWithinTimeout(putter);
            assertThat(putterResult).isFalse();

            queue.releaseProducer(1);
            assertThat(QueueProbe.liveProducerStates(queue)).isZero();
        } finally {
            if (putter.isAlive()) {
                queue.wakeUpPuttingThread(1);
                putter.interrupt();
                queue.poll();
                putter.join(TimeUnit.SECONDS.toMillis(10));
            }
            queue.releaseProducer(1);
            queue.poll();
        }
    }

    private static void joinWithinTimeout(Thread thread) throws InterruptedException {
        thread.join(TimeUnit.SECONDS.toMillis(10));
        assertThat(thread.isAlive()).as("The putting thread should have terminated").isFalse();
    }

    private static boolean putUnchecked(
            FutureCompletingBlockingQueue<Integer> queue, int threadIndex, int value) {
        try {
            return queue.put(threadIndex, value);
        } catch (InterruptedException e) {
            return fail("Putting thread interrupted unexpectedly.");
        }
    }

    @Test
    void testConcurrency() throws InterruptedException {
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
    void testSpecifiedQueueCapacity() {
        final int capacity = 8_000;
        final FutureCompletingBlockingQueue<Object> queue =
                new FutureCompletingBlockingQueue<>(capacity);
        assertThat(queue.remainingCapacity()).isEqualTo(capacity);
    }

    @Test
    void testQueueDefaultCapacity() {
        final FutureCompletingBlockingQueue<Object> queue = new FutureCompletingBlockingQueue<>();
        assertThat(queue.remainingCapacity()).isEqualTo(DEFAULT_CAPACITY);
        assertThat(SourceReaderOptions.ELEMENT_QUEUE_CAPACITY.defaultValue().intValue())
                .isEqualTo(DEFAULT_CAPACITY);
    }

    @Test
    void testUnavailableWhenEmpty() {
        final FutureCompletingBlockingQueue<Object> queue = new FutureCompletingBlockingQueue<>();
        assertThat(queue.getAvailabilityFuture().isDone()).isFalse();
    }

    @Test
    void testImmediatelyAvailableAfterPut() throws InterruptedException {
        final FutureCompletingBlockingQueue<Object> queue = new FutureCompletingBlockingQueue<>();
        queue.put(0, new Object());
        assertThat(queue.getAvailabilityFuture().isDone()).isTrue();
    }

    @Test
    void testFutureBecomesAvailableAfterPut() throws InterruptedException {
        final FutureCompletingBlockingQueue<Object> queue = new FutureCompletingBlockingQueue<>();
        final CompletableFuture<?> future = queue.getAvailabilityFuture();
        queue.put(0, new Object());
        assertThat(future.isDone()).isTrue();
    }

    @Test
    void testUnavailableWhenBecomesEmpty() throws InterruptedException {
        final FutureCompletingBlockingQueue<Object> queue = new FutureCompletingBlockingQueue<>();
        queue.put(0, new Object());
        queue.poll();
        assertThat(queue.getAvailabilityFuture().isDone()).isFalse();
    }

    @Test
    void testAvailableAfterNotifyAvailable() throws InterruptedException {
        final FutureCompletingBlockingQueue<Object> queue = new FutureCompletingBlockingQueue<>();
        queue.notifyAvailable();
        assertThat(queue.getAvailabilityFuture().isDone()).isTrue();
    }

    @Test
    void testFutureBecomesAvailableAfterNotifyAvailable() throws InterruptedException {
        final FutureCompletingBlockingQueue<Object> queue = new FutureCompletingBlockingQueue<>();
        final CompletableFuture<?> future = queue.getAvailabilityFuture();
        queue.notifyAvailable();
        assertThat(future.isDone()).isTrue();
    }

    @Test
    void testPollResetsAvailability() throws InterruptedException {
        final FutureCompletingBlockingQueue<Object> queue = new FutureCompletingBlockingQueue<>();
        queue.notifyAvailable();

        final CompletableFuture<?> beforePoll = queue.getAvailabilityFuture();
        queue.poll();
        final CompletableFuture<?> afterPoll = queue.getAvailabilityFuture();

        assertThat(beforePoll.isDone()).isTrue();
        assertThat(afterPoll.isDone()).isFalse();
    }

    /**
     * This test is to guard that our reflection is not broken and we do not lose the performance
     * advantage. This is possible, because the tests depend on the runtime modules while the main
     * scope does not.
     */
    @Test
    void testQueueUsesShortCircuitFuture() {
        assertThat(FutureCompletingBlockingQueue.AVAILABLE)
                .isSameAs(AvailabilityProvider.AVAILABLE);
    }
}
