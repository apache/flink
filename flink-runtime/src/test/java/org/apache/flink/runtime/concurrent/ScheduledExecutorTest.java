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

package org.apache.flink.runtime.concurrent;

import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Helper class for {@link ScheduledExecutor} implementation.
 */
public abstract class ScheduledExecutorTest extends TestLogger {

	private ScheduledExecutor scheduledExecutor = null;

	/**
	 * A function used to schedule periodically (scheduleAtFixedRate/scheduleWithFixedDelay).
	 */
	protected interface PeriodicallyScheduleFunction {
		ScheduledFuture<?> periodicallySchedule(
			Runnable runnable, long delay, long period, TimeUnit timeUnit);
	}

	private final PeriodicallyScheduleFunction scheduleAtFixedRateFunction =
		(runnable, delay, period, timeUnit) ->
			scheduledExecutor.scheduleWithFixedDelay(runnable, delay, period, timeUnit);

	private final PeriodicallyScheduleFunction scheduleWithFixedDelayFunction =
		(runnable, delay, period, timeUnit) ->
			scheduledExecutor.scheduleWithFixedDelay(runnable, delay, period, timeUnit);

	@Before
	public void setup() throws Exception {
		scheduledExecutor = createScheduledExecutor();
	}

	@After
	public void teardown() throws Exception {
		destroyScheduleExecutor(scheduledExecutor);
		scheduledExecutor = null;
	}

	public abstract ScheduledExecutor createScheduledExecutor() throws Exception;

	public abstract void destroyScheduleExecutor(ScheduledExecutor scheduledExecutor)
		throws Exception;

	@Test
	public void testScheduleRunnable() throws Exception {
		final CountDownLatch latch = new CountDownLatch(1);

		final long start = System.nanoTime();
		scheduledExecutor.schedule(latch::countDown, 10L, TimeUnit.MILLISECONDS);
		latch.await();
		final long duration = System.nanoTime() - start;
		assertTrue(duration > TimeUnit.MILLISECONDS.toNanos(10L));
	}

	@Test
	public void testScheduleCallable() throws Exception {
		final long start = System.nanoTime();
		boolean returned = scheduledExecutor
			.schedule(() -> true, 10L, TimeUnit.MILLISECONDS)
			.get();
		final long duration = System.nanoTime() - start;
		assertTrue(duration >= TimeUnit.MILLISECONDS.toNanos(10L));
		assertTrue(returned);
	}

	@Test
	public void testScheduleAtFixedRate() throws Exception {
		final CountDownLatch latch = new CountDownLatch(2);
		final long start = System.nanoTime();
		final ScheduledFuture future = scheduledExecutor.scheduleAtFixedRate(
			latch::countDown, 10L, 10L, TimeUnit.MILLISECONDS);
		latch.await();
		final long duration = System.nanoTime() - start;
		assertTrue(duration > TimeUnit.MILLISECONDS.toNanos(20L));
		assertTrue(future.cancel(false));
	}

	@Test
	public void testScheduleWithFixedDelay() throws Exception {
		final CountDownLatch latch = new CountDownLatch(2);

		final long start = System.nanoTime();
		final ScheduledFuture future =
			scheduledExecutor.scheduleWithFixedDelay(() -> {
				latch.countDown();
				if (latch.getCount() > 0) {
					try {
						// the next task should be scheduled after waiting for a while
						// Sleeps for a bit long to reduce affection of inaccurate overhead
						Thread.sleep(100L);
					} catch (InterruptedException ignored) {
					}
				}
			}, 0L, 1L, TimeUnit.MILLISECONDS);
		latch.await();
		final long duration = System.nanoTime() - start;
		assertTrue(duration > TimeUnit.MILLISECONDS.toNanos(101L));
		assertTrue(future.cancel(false));
	}

	@Test
	public void testScheduleAtFixedRateWithCancellation() throws Exception {
		testSchedulePeriodicallyWithCancellation(scheduleAtFixedRateFunction);
	}

	@Test
	public void testScheduleWithFixedDelayWithCancellation() throws Exception {
		testSchedulePeriodicallyWithCancellation(scheduleWithFixedDelayFunction);
	}

	@Test(expected = ExecutionException.class)
	public void testScheduleAtFixedRateWithException() throws Exception {
		testSchedulePeriodicallyWithException(scheduleAtFixedRateFunction);
	}

	@Test(expected = ExecutionException.class)
	public void testScheduleWithFixedDelayWithException() throws Exception {
		testSchedulePeriodicallyWithException(scheduleWithFixedDelayFunction);
	}

	/**
	 * Tests if an exception is thrown when task is executed periodically.
	 * The task would not be executed anymore after the exception thrown.
	 *
	 * @param function the periodically schedule function
	 * @throws Exception {@link ExecutionException} is expected
	 */
	private void testSchedulePeriodicallyWithException(PeriodicallyScheduleFunction function)
		throws Exception {

		final CountDownLatch latch = new CountDownLatch(2);
		final ScheduledFuture future = function.periodicallySchedule(() -> {
			latch.countDown();
			if (latch.getCount() == 1) {
				throw new RuntimeException("By designed");
			}
		}, 0L, 1L, TimeUnit.MILLISECONDS);
		assertFalse(latch.await(10L, TimeUnit.MILLISECONDS));
		future.get();
	}

	/**
	 * This case is designed to check cancellation behavior upon periodic scheduling.
	 *
	 * <p>
	 *     1. Schedules a task periodically.
	 *     2. Waits for the task being executed at the first time. Blocks in this invocation.
	 *     3. Cancels the periodic scheduling.
	 *     4. Verifies that the task only be scheduled once.
	 * </p>
	 *
	 * @param function the periodically schedule function
	 * @throws Exception exception
	 */
	private void testSchedulePeriodicallyWithCancellation(PeriodicallyScheduleFunction function)
		throws Exception {

		final CountDownLatch waitForCancellationLatch = new CountDownLatch(1);
		final CountDownLatch executedLatch = new CountDownLatch(1);
		final AtomicBoolean interrupted = new AtomicBoolean(false);
		final AtomicInteger executedTimes = new AtomicInteger(0);
		final ScheduledFuture future = function.periodicallySchedule(() -> {
				executedTimes.incrementAndGet();
				executedLatch.countDown();
				try {
					waitForCancellationLatch.await();
				} catch (InterruptedException ignored) {
					interrupted.set(true);
				}
			}, 0L, 1L, TimeUnit.MILLISECONDS);
		executedLatch.await();
		// now task is being executed, do the cancellation
		assertTrue(future.cancel(false));
		waitForCancellationLatch.countDown();
		// wait for a while to check if the cancellation is successful
		Thread.sleep(10L);
		// the task should be cancelled and never be interrupted
		assertEquals(1, executedTimes.get());
		assertFalse(interrupted.get());
	}
}
