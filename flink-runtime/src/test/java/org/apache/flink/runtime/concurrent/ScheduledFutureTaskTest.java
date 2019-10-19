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

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.util.clock.Clock;
import org.apache.flink.runtime.util.clock.ManualClock;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import javax.annotation.Nonnull;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link ScheduledFutureTask}.
 */
public class ScheduledFutureTaskTest extends TestLogger {

	@Test
	public void testScheduledRun() {
		final AtomicBoolean executed = new AtomicBoolean(false);

		final ScheduledFutureTask task = new ScheduledFutureTask(() -> {
			executed.compareAndSet(false, true);
		}, 1024L, TimeUnit.MILLISECONDS);

		task.run();

		assertTrue(executed.get());
	}

	@Test
	public void testPeriodicScheduledRun() {
		final AtomicBoolean executed = new AtomicBoolean(false);

		final ScheduledFutureTask task = new ScheduledFutureTask(() -> {
			executed.compareAndSet(false, true);
		}, 0L, 1024L, TimeUnit.SECONDS);

		assertTrue(task.getDelay(TimeUnit.SECONDS) <= 0L);

		task.run();

		assertTrue(executed.get());
		assertTrue(task.getDelay(TimeUnit.SECONDS) > 0L);
	}

	@Test
	public void testPeriodicReScheduledRun() {
		final AtomicBoolean executed = new AtomicBoolean(false);
		final Tuple3<Runnable, Long, TimeUnit> reScheduledResult = new Tuple3<>();
		final AtomicBoolean cancelled = new AtomicBoolean(false);

		final ScheduledFutureTask task = new ScheduledFutureTask(() -> {
			executed.compareAndSet(false, true);
		},
		0L,
		1024L,
		TimeUnit.SECONDS,
		(runnable, delay, timeUnit) -> {
			reScheduledResult.f0 = runnable;
			reScheduledResult.f1 = delay;
			reScheduledResult.f2 = timeUnit;
			return new TestingCancellable(cancelled);
		});

		task.run();

		assertTrue(executed.get());
		assertTrue(task.getDelay(TimeUnit.NANOSECONDS) > 0L);
		assertEquals(task, reScheduledResult.f0);
		assertEquals(1024L, reScheduledResult.f2.toSeconds(reScheduledResult.f1));

		task.cancel(false);

		assertTrue(cancelled.get());
	}

	@Test
	public void testCompareTo() {
		final Clock clock = new ManualClock();
		final ScheduledFutureTask<Long> task = new ScheduledFutureTask<>(
			() -> 0L, 1024L, 0L, null, clock);

		//noinspection EqualsWithItself
		assertEquals(0, task.compareTo(task));
		assertTrue(task.compareTo(new Delayed() {
			@Override
			public long getDelay(@Nonnull TimeUnit unit) {
				return TimeUnit.NANOSECONDS.convert(1023L, unit);
			}

			@Override
			public int compareTo(@Nonnull Delayed o) {
				return Long.compare(1023L, o.getDelay(TimeUnit.NANOSECONDS));
			}
		}) > 0);

		final ScheduledFutureTask<Long> laterTask = new ScheduledFutureTask<>(
			() -> 0L, 1024L, 0L, null, clock);
		assertTrue(task.compareTo(laterTask) < 0);
	}

	private static class TestingCancellable implements ScheduledFutureTask.Cancellable {

		private final AtomicBoolean cancelled;

		TestingCancellable(AtomicBoolean cancelled) {
			this.cancelled = cancelled;
		}

		@Override
		public boolean cancel(boolean mayInterruptIfRunning) {
			cancelled.set(true);
			return true;
		}

		@Override
		public boolean isCancelled() {
			return cancelled.get();
		}
	}
}
