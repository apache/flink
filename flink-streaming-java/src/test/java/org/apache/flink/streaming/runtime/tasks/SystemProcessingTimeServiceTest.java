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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.util.Preconditions;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

/** Tests for {@link SystemProcessingTimeService}. */
class SystemProcessingTimeServiceTest {

    /**
     * Tests that SystemProcessingTimeService#scheduleAtFixedRate is actually triggered multiple
     * times.
     */
    @Timeout(value = 10000, unit = TimeUnit.MILLISECONDS)
    @Test
    void testScheduleAtFixedRate() throws Exception {
        final AtomicReference<Throwable> errorRef = new AtomicReference<>();
        final long period = 10L;
        final int countDown = 3;

        final SystemProcessingTimeService timer = createSystemProcessingTimeService(errorRef);

        final CountDownLatch countDownLatch = new CountDownLatch(countDown);

        try {
            timer.scheduleAtFixedRate(timestamp -> countDownLatch.countDown(), 0L, period);

            countDownLatch.await();

            assertThat(errorRef.get()).isNull();
        } finally {
            timer.shutdownService();
        }
    }

    /**
     * Tests that shutting down the SystemProcessingTimeService will also cancel the scheduled at
     * fix rate future.
     */
    @Test
    void testQuiesceAndAwaitingCancelsScheduledAtFixRateFuture() throws Exception {
        final AtomicReference<Throwable> errorRef = new AtomicReference<>();
        final long period = 10L;

        final SystemProcessingTimeService timer = createSystemProcessingTimeService(errorRef);

        try {
            Future<?> scheduledFuture = timer.scheduleAtFixedRate(timestamp -> {}, 0L, period);

            assertThat(scheduledFuture).isNotDone();

            // this should cancel our future
            timer.quiesce().get();

            // it may be that the cancelled status is not immediately visible after the
            // termination (not necessary a volatile update), so we need to "get()" the cancellation
            // exception to be on the safe side
            assertThatThrownBy(scheduledFuture::get)
                    .as("scheduled future is not cancelled")
                    .isInstanceOf(CancellationException.class);

            scheduledFuture =
                    timer.scheduleAtFixedRate(
                            timestamp -> {
                                throw new Exception("Test exception.");
                            },
                            0L,
                            100L);

            assertThat(scheduledFuture).isNotNull();

            assertThat(timer.getNumTasksScheduled()).isZero();

            assertThat(errorRef.get()).isNull();
        } finally {
            timer.shutdownService();
        }
    }

    @Test
    void testImmediateShutdown() throws Exception {
        final CompletableFuture<Throwable> errorFuture = new CompletableFuture<>();

        final SystemProcessingTimeService timer = createSystemProcessingTimeService(errorFuture);

        try {
            assertThat(timer.isTerminated()).isFalse();

            final OneShotLatch latch = new OneShotLatch();

            // the task should trigger immediately and sleep until terminated with interruption
            timer.registerTimer(
                    System.currentTimeMillis(),
                    timestamp -> {
                        latch.trigger();
                        Thread.sleep(100000000);
                    });

            latch.await();
            timer.shutdownService();

            assertThat(timer.isTerminated()).isTrue();
            assertThat(timer.getNumTasksScheduled()).isZero();

            assertThatThrownBy(
                            () ->
                                    timer.registerTimer(
                                            System.currentTimeMillis() + 1000,
                                            timestamp -> fail("should not be called")))
                    .isInstanceOf(IllegalStateException.class);

            assertThatThrownBy(
                            () ->
                                    timer.scheduleAtFixedRate(
                                            timestamp -> fail("should not be called"), 0L, 100L))
                    .isInstanceOf(IllegalStateException.class);

            // check that the task eventually responded to interruption
            assertThat(errorFuture.get(30L, TimeUnit.SECONDS))
                    .isInstanceOf(InterruptedException.class);
        } finally {
            timer.shutdownService();
        }
    }

    @Test
    void testQuiescing() throws Exception {

        final AtomicReference<Throwable> errorRef = new AtomicReference<>();

        final SystemProcessingTimeService timer = createSystemProcessingTimeService(errorRef);

        try {
            final OneShotLatch latch = new OneShotLatch();

            final ReentrantLock scopeLock = new ReentrantLock();

            timer.registerTimer(
                    timer.getCurrentProcessingTime() + 20L,
                    timestamp -> {
                        scopeLock.lock();
                        try {
                            latch.trigger();
                            // delay a bit before leaving the method
                            Thread.sleep(5);
                        } finally {
                            scopeLock.unlock();
                        }
                    });

            // after the task triggered, shut the timer down cleanly, waiting for the task to finish
            latch.await();
            timer.quiesce().get();

            // should be able to immediately acquire the lock, since the task must have exited by
            // now
            assertThat(scopeLock.tryLock()).isTrue();

            // should be able to schedule more tasks (that never get executed)
            Future<?> future =
                    timer.registerTimer(
                            timer.getCurrentProcessingTime() - 5L,
                            timestamp -> {
                                throw new Exception("test");
                            });
            assertThat(future).isNotNull();

            // nothing should be scheduled right now
            assertThat(timer.getNumTasksScheduled()).isZero();

            // check that no asynchronous error was reported - that ensures that the newly scheduled
            // triggerable did, in fact, not trigger
            assertThat(errorRef.get()).isNull();
        } finally {
            timer.shutdownService();
        }
    }

    @Test
    void testFutureCancellation() {

        final AtomicReference<Throwable> errorRef = new AtomicReference<>();

        final SystemProcessingTimeService timer = createSystemProcessingTimeService(errorRef);

        try {
            assertThat(timer.getNumTasksScheduled()).isZero();

            // schedule something
            ScheduledFuture<?> future =
                    timer.registerTimer(System.currentTimeMillis() + 100000000, timestamp -> {});
            assertThat(timer.getNumTasksScheduled()).isOne();

            future.cancel(false);

            assertThat(timer.getNumTasksScheduled()).isZero();

            future = timer.scheduleAtFixedRate(timestamp -> {}, 10000000000L, 50L);

            assertThat(timer.getNumTasksScheduled()).isOne();

            future.cancel(false);

            assertThat(timer.getNumTasksScheduled()).isZero();

            // check that no asynchronous error was reported
            assertThat(errorRef.get()).isNull();
        } finally {
            timer.shutdownService();
        }
    }

    @Test
    void testShutdownAndWaitPending() throws Exception {

        final OneShotLatch blockUntilTriggered = new OneShotLatch();
        final AtomicBoolean timerExecutionFinished = new AtomicBoolean(false);

        final SystemProcessingTimeService timeService =
                createBlockingSystemProcessingTimeService(
                        blockUntilTriggered, timerExecutionFinished);

        assertThat(timeService.isTerminated()).isFalse();

        // Check that we wait for the timer to terminate. As the timer blocks on the second latch,
        // this should time out.
        assertThat(timeService.shutdownAndAwaitPending(1, TimeUnit.SECONDS)).isFalse();

        // Let the timer proceed.
        blockUntilTriggered.trigger();

        // Now we should succeed in terminating the timer.
        assertThat(timeService.shutdownAndAwaitPending(60, TimeUnit.SECONDS)).isTrue();

        assertThat(timerExecutionFinished).isTrue();
        assertThat(timeService.isTerminated()).isTrue();
    }

    @Test
    void testShutdownServiceUninterruptible() {
        final OneShotLatch blockUntilTriggered = new OneShotLatch();
        final AtomicBoolean timerFinished = new AtomicBoolean(false);

        final SystemProcessingTimeService timeService =
                createBlockingSystemProcessingTimeService(blockUntilTriggered, timerFinished);

        assertThat(timeService.isTerminated()).isFalse();

        final Thread interruptTarget = Thread.currentThread();
        final AtomicBoolean runInterrupts = new AtomicBoolean(true);
        final Thread interruptCallerThread =
                new Thread(
                        () -> {
                            while (runInterrupts.get()) {
                                interruptTarget.interrupt();
                                try {
                                    Thread.sleep(1);
                                } catch (InterruptedException ignore) {
                                }
                            }
                        });

        interruptCallerThread.start();

        final long timeoutMs = 50L;
        final long startTime = System.nanoTime();
        assertThat(timeService.isTerminated()).isFalse();
        // check that termination did not succeed (because of blocking timer execution)
        assertThat(timeService.shutdownServiceUninterruptible(timeoutMs)).isFalse();
        // check that termination flag was set.
        assertThat(timeService.isTerminated()).isTrue();
        // check that the blocked timer is still in flight.
        assertThat(timerFinished).isFalse();
        // check that we waited until timeout
        assertThat(System.nanoTime() - startTime).isGreaterThanOrEqualTo(1_000_000L * timeoutMs);

        runInterrupts.set(false);

        do {
            try {
                interruptCallerThread.join();
            } catch (InterruptedException ignore) {
            }
        } while (interruptCallerThread.isAlive());

        // clear the interrupted flag in case join didn't do it
        final boolean ignored = Thread.interrupted();

        blockUntilTriggered.trigger();
        assertThat(timeService.shutdownServiceUninterruptible(timeoutMs)).isTrue();
        assertThat(timerFinished).isTrue();
    }

    private static SystemProcessingTimeService createSystemProcessingTimeService(
            CompletableFuture<Throwable> errorFuture) {
        Preconditions.checkArgument(!errorFuture.isDone());

        return new SystemProcessingTimeService(errorFuture::complete);
    }

    private static SystemProcessingTimeService createSystemProcessingTimeService(
            AtomicReference<Throwable> errorRef) {
        Preconditions.checkArgument(errorRef.get() == null);

        return new SystemProcessingTimeService(ex -> errorRef.compareAndSet(null, ex));
    }

    private static SystemProcessingTimeService createBlockingSystemProcessingTimeService(
            final OneShotLatch blockUntilTriggered, final AtomicBoolean check) {

        final OneShotLatch waitUntilTimerStarted = new OneShotLatch();

        Preconditions.checkState(!check.get());

        final SystemProcessingTimeService timeService =
                new SystemProcessingTimeService(exception -> {});

        timeService.scheduleAtFixedRate(
                timestamp -> {
                    waitUntilTimerStarted.trigger();

                    boolean unblocked = false;

                    while (!unblocked) {
                        try {
                            blockUntilTriggered.await();
                            unblocked = true;
                        } catch (InterruptedException ignore) {
                        }
                    }

                    check.set(true);
                },
                0L,
                10L);

        try {
            waitUntilTimerStarted.await();
        } catch (InterruptedException e) {
            fail("Problem while starting up service.");
        }

        return timeService;
    }
}
