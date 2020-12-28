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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.util.clock.Clock;
import org.apache.flink.runtime.util.clock.SystemClock;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A future task which supports one-shot, periodic scheduling and re-scheduling.
 *
 * @param <V> the result type of returned by {@code get} method
 */
@Internal
public class ScheduledFutureTask<V> extends FutureTask<V> implements RunnableScheduledFuture<V> {

	/** The uid sequence generator. */
	private static final AtomicLong SEQUENCE_GEN = new AtomicLong();

	private final long period;

	@Nullable
	private final Scheduler scheduler;

	/** Tie-breaker for {@link #compareTo(Delayed)}. */
	private final long tieBreakerUid;

	private final Clock clock;

	/** The time the task is enabled to execute in nanoTime units. */
	private long time;

	private volatile Cancellable cancellable;

	/**
	 * Instantiates a scheduled future task with runnable task.
	 *
	 * @param runnable the runnable task
	 * @param delay    the delay
	 * @param timeUnit the time unit
	 */
	public ScheduledFutureTask(Runnable runnable, long delay, TimeUnit timeUnit) {
		this(runnable, timeUnit.toNanos(delay), 0, (Scheduler) null);
	}

	/**
	 * Instantiates a scheduled future task with callable task.
	 *
	 * @param callable the callable task
	 * @param delay    the delay
	 * @param timeUnit the time unit
	 */
	public ScheduledFutureTask(Callable<V> callable, long delay, TimeUnit timeUnit) {
		this(callable, timeUnit.toNanos(delay), 0, null);
	}

	/**
	 * Instantiates a periodic scheduled future task with runnable task.
	 *
	 * @param runnable the runnable task
	 * @param delay    the delay
	 * @param period   the period
	 * @param timeUnit the time unit
	 */
	public ScheduledFutureTask(Runnable runnable, long delay, long period, TimeUnit timeUnit) {
		this(runnable, timeUnit.toNanos(delay), timeUnit.toNanos(period), (Scheduler) null);
	}

	/**
	 * Creates a periodic re-scheduled future task with runnable task.
	 *
	 * @param runnable  the runnable task
	 * @param delay     the delay
	 * @param period    the period
	 * @param timeUnit  the time unit
	 * @param scheduler the scheduler used to re-schedule task
	 */
	public ScheduledFutureTask(
		Runnable runnable,
		long delay,
		long period,
		TimeUnit timeUnit,
		Scheduler scheduler) {

		this(runnable, timeUnit.toNanos(delay), timeUnit.toNanos(-period), scheduler);
	}

	/**
	 * Instantiates a new Scheduled future task with runnable task.
	 *
	 * @param runnable  the runnable task
	 * @param delay     the delay (in nanoseconds)
	 * @param period    the period (in nanoseconds)
	 *                     zero means non-periodic
	 *                     positive value means periodic scheduled
	 *                     negative value means periodic re-scheduled
	 * @param scheduler the scheduler used to re-schedule task
	 */
	private ScheduledFutureTask(
		Runnable runnable,
		long delay,
		long period,
		@Nullable Scheduler scheduler) {

		this(
			java.util.concurrent.Executors.callable(runnable, null),
			delay,
			period,
			scheduler);
	}

	/**
	 * Instantiates a new Scheduled future task with callable task.
	 *
	 * @param callable  the callable task
	 * @param delay     the delay (in nanoseconds)
	 * @param period    the period (in nanoseconds)
	 *                     zero means non-periodic
	 *                     positive value means periodic scheduled
	 *                     negative value means periodic re-scheduled
	 * @param scheduler the scheduler used to re-schedule task
	 */
	private ScheduledFutureTask(
		Callable<V> callable,
		long delay,
		long period,
		@Nullable Scheduler scheduler) {

		this(callable, delay, period, scheduler, SystemClock.getInstance());
	}

	/**
	 * Instantiates a new Scheduled future task with callable task.
	 *
	 * @param callable  the callable task
	 * @param delay     the delay (in nanoseconds)
	 * @param period    the period (in nanoseconds)
	 *                     zero means non-periodic
	 *                     positive value means periodic scheduled
	 *                     negative value means periodic re-scheduled
	 * @param scheduler the scheduler used to re-schedule task
	 */
	@VisibleForTesting
	ScheduledFutureTask(
		Callable<V> callable,
		long delay,
		long period,
		@Nullable Scheduler scheduler,
		Clock clock) {

		super(callable);

		checkArgument(delay >= 0);
		if (period < 0) {
			checkNotNull(scheduler);
		}

		this.period = period;
		this.scheduler = scheduler;
		this.clock = checkNotNull(clock);
		this.tieBreakerUid = SEQUENCE_GEN.incrementAndGet();
		this.time = triggerTime(delay);
	}

	/**
	 * Sets cancellable.
	 *
	 * @param newCancellable the new cancellable
	 */
	public void setCancellable(Cancellable newCancellable) {
		this.cancellable = checkNotNull(newCancellable);
	}

	@Override
	public void run() {
		if (!isPeriodic()) {
			super.run();
		} else if (runAndReset()){
			if (period > 0L) {
				time += period;
			} else {
				checkNotNull(scheduler);
				cancellable = scheduler.schedule(this, -period, TimeUnit.NANOSECONDS);

				// check whether we have been cancelled concurrently
				if (isCancelled()) {
					if (cancellable != null) {
						cancellable.cancel(false);
					}
				} else {
					time = triggerTime(-period);
				}
			}
		}
	}

	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		boolean result = super.cancel(mayInterruptIfRunning);
		if (cancellable != null) {
			boolean internalCancelled = cancellable.cancel(mayInterruptIfRunning);
			if (scheduler == null) {
				return result && internalCancelled;
			} else {
				// swallows internal cancellation result under re-scheduling mode
				// because the internal cancellation might be failed due to a race condition that
				// task has been completed at the same time
				// it should not return false under this scenario
				return result;
			}
		} else {
			return result;
		}
	}

	@Override
	public long getDelay(@Nonnull TimeUnit unit) {
		return unit.convert(time - now(), TimeUnit.NANOSECONDS);
	}

	@Override
	public int compareTo(@Nonnull Delayed o) {
		if (o == this) {
			return 0;
		}

		int cmp = Long.compare(getDelay(TimeUnit.NANOSECONDS), o.getDelay(TimeUnit.NANOSECONDS));
		if (cmp == 0 && o instanceof ScheduledFutureTask) {
			ScheduledFutureTask<?> typedOther = (ScheduledFutureTask<?>) o;
			return Long.compare(tieBreakerUid, typedOther.tieBreakerUid);
		} else {
			return cmp;
		}
	}

	@Override
	public boolean isPeriodic() {
		return period != 0L;
	}

	private long now() {
		return clock.relativeTimeNanos();
	}

	private long triggerTime(long delay) {
		return now() + delay;
	}

	/**
	 * The interface Cancellable.
	 */
	public interface Cancellable {
		/**
		 * Cancels the task.
		 *
		 * @param mayInterruptIfRunning {@code true} if the thread executing this
		 * task should be interrupted; otherwise, in-progress tasks are allowed
		 * to complete
		 * @return the true if success, false otherwise
		 */
		boolean cancel(boolean mayInterruptIfRunning);

		/**
		 * Checks the task is cancelled or not.
		 *
		 * @return the true if cancelled, false otherwise
		 */
		boolean isCancelled();
	}

	/**
	 * The interface Scheduler.
	 */
	public interface Scheduler {
		/**
		 * Schedules the runnable task.
		 *
		 * @param runnable the runnable task
		 * @param delay    the delay
		 * @param timeUnit the time unit
		 * @return the cancellable handler
		 */
		Cancellable schedule(Runnable runnable, long delay, TimeUnit timeUnit);
	}
}
