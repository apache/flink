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
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for {@link SystemProcessingTimeService}.
 */
public class SystemProcessingTimeServiceTest extends TestLogger {

	/**
	 * Tests that SystemProcessingTimeService#scheduleAtFixedRate is actually triggered multiple
	 * times.
	 */
	@Test(timeout = 10000)
	public void testScheduleAtFixedRate() throws Exception {
		final AtomicReference<Throwable> errorRef = new AtomicReference<>();
		final long period = 10L;
		final int countDown = 3;

		final SystemProcessingTimeService timer = createSystemProcessingTimeService(errorRef);

		final CountDownLatch countDownLatch = new CountDownLatch(countDown);

		try {
			timer.scheduleAtFixedRate(new ProcessingTimeCallback() {
				@Override
				public void onProcessingTime(long timestamp) throws Exception {
					countDownLatch.countDown();
				}
			}, 0L, period);

			countDownLatch.await();

			if (errorRef.get() != null) {
				throw new Exception(errorRef.get());
			}

		} finally {
			timer.shutdownService();
		}
	}

	/**
	 * Tests that shutting down the SystemProcessingTimeService will also cancel the scheduled at
	 * fix rate future.
	 */
	@Test
	public void testQuiesceAndAwaitingCancelsScheduledAtFixRateFuture() throws Exception {
		final AtomicReference<Throwable> errorRef = new AtomicReference<>();
		final long period = 10L;

		final SystemProcessingTimeService timer = createSystemProcessingTimeService(errorRef);

		try {
			ScheduledFuture<?> scheduledFuture = timer.scheduleAtFixedRate(new ProcessingTimeCallback() {
				@Override
				public void onProcessingTime(long timestamp) throws Exception {
				}
			}, 0L, period);

			assertFalse(scheduledFuture.isDone());

			// this should cancel our future
			timer.quiesce();
			timer.awaitPendingAfterQuiesce();

			// it may be that the cancelled status is not immediately visible after the
			// termination (not necessary a volatile update), so we need to "get()" the cancellation
			// exception to be on the safe side
			try {
				scheduledFuture.get();
				fail("scheduled future is not cancelled");
			}
			catch (CancellationException ignored) {
				// expected
			}

			scheduledFuture = timer.scheduleAtFixedRate(new ProcessingTimeCallback() {
				@Override
				public void onProcessingTime(long timestamp) throws Exception {
					throw new Exception("Test exception.");
				}
			}, 0L, 100L);

			assertNotNull(scheduledFuture);

			assertEquals(0, timer.getNumTasksScheduled());

			if (errorRef.get() != null) {
				throw new Exception(errorRef.get());
			}

		} finally {
			timer.shutdownService();
		}
	}

	@Test
	public void testImmediateShutdown() throws Exception {

		final Object lock = new Object();
		final AtomicReference<Throwable> errorRef = new AtomicReference<>();

		final SystemProcessingTimeService timer = createSystemProcessingTimeService(errorRef);

		try {
			assertFalse(timer.isTerminated());

			final OneShotLatch latch = new OneShotLatch();

			// the task should trigger immediately and should block until terminated with interruption
			timer.registerTimer(System.currentTimeMillis(), timestamp -> {
				synchronized (lock) {
					latch.trigger();
					Thread.sleep(100000000);
				}
			});

			latch.await();
			timer.shutdownService();
			synchronized (lock) {
				assertTrue(timer.isTerminated());

				// The shutdownService() may not necessary wait for active tasks to finish properly.
				// From the ScheduledThreadPoolExecutor Java docs:
				// 	There are no guarantees beyond best-effort attempts to stop processing actively executing tasks.
				// 	This implementation cancels tasks via {@link Thread#interrupt}, so any task that
				// 	fails to respond to interrupts may never terminate.
				assertThat(errorRef.get(), is(anyOf(nullValue(), instanceOf(InterruptedException.class))));
				assertEquals(0, timer.getNumTasksScheduled());
			}

			errorRef.set(null);

			try {
				timer.registerTimer(System.currentTimeMillis() + 1000, timestamp -> fail("should not be called"));

				fail("should result in an exception");
			}
			catch (IllegalStateException e) {
				// expected
			}
			assertThat(errorRef.get(), is(nullValue()));

			try {
				timer.scheduleAtFixedRate(timestamp -> fail("should not be called"), 0L, 100L);

				fail("should result in an exception");
			}
			catch (IllegalStateException e) {
				// expected
			}
			assertThat(errorRef.get(), is(nullValue()));
		}
		finally {
			timer.shutdownService();
		}
	}

	@Test
	public void testQuiescing() throws Exception {

		final AtomicReference<Throwable> errorRef = new AtomicReference<>();

		final SystemProcessingTimeService timer = createSystemProcessingTimeService(errorRef);

		try {
			final OneShotLatch latch = new OneShotLatch();

			final ReentrantLock scopeLock = new ReentrantLock();

			timer.registerTimer(timer.getCurrentProcessingTime() + 20L, new ProcessingTimeCallback() {
				@Override
				public void onProcessingTime(long timestamp) throws Exception {
					scopeLock.lock();
					try {
						latch.trigger();
						// delay a bit before leaving the method
						Thread.sleep(5);
					} finally {
						scopeLock.unlock();
					}
				}
			});

			// after the task triggered, shut the timer down cleanly, waiting for the task to finish
			latch.await();
			timer.quiesce();
			timer.awaitPendingAfterQuiesce();

			// should be able to immediately acquire the lock, since the task must have exited by now
			assertTrue(scopeLock.tryLock());

			// should be able to schedule more tasks (that never get executed)
			ScheduledFuture<?> future = timer.registerTimer(timer.getCurrentProcessingTime() - 5L, new ProcessingTimeCallback() {
				@Override
				public void onProcessingTime(long timestamp) throws Exception {
					throw new Exception("test");
				}
			});
			assertNotNull(future);

			// nothing should be scheduled right now
			assertEquals(0L, timer.getNumTasksScheduled());

			// check that no asynchronous error was reported - that ensures that the newly scheduled
			// triggerable did, in fact, not trigger
			if (errorRef.get() != null) {
				throw new Exception(errorRef.get());
			}
		}
		finally {
			timer.shutdownService();
		}
	}

	@Test
	public void testFutureCancellation() throws Exception {

		final AtomicReference<Throwable> errorRef = new AtomicReference<>();

		final SystemProcessingTimeService timer = createSystemProcessingTimeService(errorRef);

		try {
			assertEquals(0, timer.getNumTasksScheduled());

			// schedule something
			ScheduledFuture<?> future = timer.registerTimer(System.currentTimeMillis() + 100000000, timestamp -> {});
			assertEquals(1, timer.getNumTasksScheduled());

			future.cancel(false);

			assertEquals(0, timer.getNumTasksScheduled());

			future = timer.scheduleAtFixedRate(timestamp -> {}, 10000000000L, 50L);

			assertEquals(1, timer.getNumTasksScheduled());

			future.cancel(false);

			assertEquals(0, timer.getNumTasksScheduled());

			// check that no asynchronous error was reported
			if (errorRef.get() != null) {
				throw new Exception(errorRef.get());
			}
		}
		finally {
			timer.shutdownService();
		}
	}

	@Test
	public void testShutdownAndWaitPending() {

		final OneShotLatch blockUntilTriggered = new OneShotLatch();
		final AtomicBoolean timerExecutionFinished = new AtomicBoolean(false);

		final SystemProcessingTimeService timeService =
			createBlockingSystemProcessingTimeService(blockUntilTriggered, timerExecutionFinished);

		Assert.assertFalse(timeService.isTerminated());

		// Check that we wait for the timer to terminate. As the timer blocks on the second latch, this should time out.
		try {
			Assert.assertFalse(timeService.shutdownAndAwaitPending(1, TimeUnit.SECONDS));
		} catch (InterruptedException e) {
			Assert.fail("Unexpected interruption.");
		}

		// Let the timer proceed.
		blockUntilTriggered.trigger();

		// Now we should succeed in terminating the timer.
		try {
			Assert.assertTrue(timeService.shutdownAndAwaitPending(60, TimeUnit.SECONDS));
		} catch (InterruptedException e) {
			Assert.fail("Unexpected interruption.");
		}

		Assert.assertTrue(timerExecutionFinished.get());
		Assert.assertTrue(timeService.isTerminated());
	}

	@Test
	public void testShutdownServiceUninterruptible() {
		final OneShotLatch blockUntilTriggered = new OneShotLatch();
		final AtomicBoolean timerFinished = new AtomicBoolean(false);

		final SystemProcessingTimeService timeService =
			createBlockingSystemProcessingTimeService(blockUntilTriggered, timerFinished);

		Assert.assertFalse(timeService.isTerminated());

		final Thread interruptTarget = Thread.currentThread();
		final AtomicBoolean runInterrupts = new AtomicBoolean(true);
		final Thread interruptCallerThread = new Thread(() -> {
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
		Assert.assertFalse(timeService.isTerminated());
		// check that termination did not succeed (because of blocking timer execution)
		Assert.assertFalse(timeService.shutdownServiceUninterruptible(timeoutMs));
		// check that termination flag was set.
		Assert.assertTrue(timeService.isTerminated());
		// check that the blocked timer is still in flight.
		Assert.assertFalse(timerFinished.get());
		// check that we waited until timeout
		Assert.assertTrue((System.nanoTime() - startTime) >= (1_000_000L * timeoutMs));

		runInterrupts.set(false);

		do {
			try {
				interruptCallerThread.join();
			} catch (InterruptedException ignore) {
			}
		} while (interruptCallerThread.isAlive());

		blockUntilTriggered.trigger();
		Assert.assertTrue(timeService.shutdownServiceUninterruptible(timeoutMs));
		Assert.assertTrue(timerFinished.get());
	}

	private static SystemProcessingTimeService createSystemProcessingTimeService(AtomicReference<Throwable> errorRef) {

		Preconditions.checkArgument(errorRef.get() == null);

		return new SystemProcessingTimeService(
			new TestOnTimerCallbackContext(ex -> errorRef.compareAndSet(null, ex)));
	}

	private static SystemProcessingTimeService createBlockingSystemProcessingTimeService(
			final OneShotLatch blockUntilTriggered,
			final AtomicBoolean check) {

		final OneShotLatch waitUntilTimerStarted = new OneShotLatch();

		Preconditions.checkState(!check.get());

		final SystemProcessingTimeService timeService = new SystemProcessingTimeService(
			new TestOnTimerCallbackContext(exception -> {}));

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
			Assert.fail("Problem while starting up service.");
		}

		return timeService;
	}

	private static class TestOnTimerCallbackContext implements SystemProcessingTimeService.ScheduledCallbackExecutionContext {
		private final Consumer<Throwable> exceptionHandler;

		TestOnTimerCallbackContext(Consumer<Throwable> exceptionHandler) {
			this.exceptionHandler = exceptionHandler;
		}

		@Override
		public void invoke(ProcessingTimeCallback callback, long timestamp) {
			try {
				callback.onProcessingTime(timestamp);
			} catch (Throwable t) {
				exceptionHandler.accept(t);
			}
		}
	}
}
