/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.concurrent;

import org.apache.flink.annotation.VisibleForTesting;

import javax.annotation.Nonnull;

import java.util.Objects;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Adapter from {@link Future} to {@link ScheduledFuture}. This enriches the basic future with scheduling information.
 * @param <V> value type of the future.
 */
public class ScheduledFutureAdapter<V> implements ScheduledFuture<V> {

	/** The uid sequence generator. */
	private static final AtomicLong SEQUENCE_GEN = new AtomicLong();

	/** The encapsulated basic future to which execution is delegated. */
	@Nonnull
	private final Future<V> delegate;

	/** Tie-breaker for {@link #compareTo(Delayed)}. */
	private final long tieBreakerUid;

	/** The time when this is scheduled for execution in nanoseconds.*/
	private final long scheduleTimeNanos;

	public ScheduledFutureAdapter(
		@Nonnull Future<V> delegate,
		long delay,
		@Nonnull TimeUnit timeUnit) {
		this(
			delegate,
			System.nanoTime() + TimeUnit.NANOSECONDS.convert(delay, timeUnit),
			SEQUENCE_GEN.incrementAndGet());
	}

	@VisibleForTesting
	ScheduledFutureAdapter(
		@Nonnull Future<V> delegate,
		long scheduleTimeNanos,
		long tieBreakerUid) {

		this.delegate = delegate;
		this.scheduleTimeNanos = scheduleTimeNanos;
		this.tieBreakerUid = tieBreakerUid;
	}

	@Override
	public long getDelay(@Nonnull TimeUnit unit) {
		return unit.convert(scheduleTimeNanos - System.nanoTime(), TimeUnit.NANOSECONDS);
	}

	@Override
	public int compareTo(@Nonnull Delayed o) {

		if (o == this) {
			return 0;
		}

		// tie breaking for ScheduledFutureAdapter objects
		if (o instanceof ScheduledFutureAdapter) {
			ScheduledFutureAdapter<?> typedOther = (ScheduledFutureAdapter<?>) o;
			int cmp = Long.compare(scheduleTimeNanos, typedOther.scheduleTimeNanos);
			return cmp != 0 ? cmp : Long.compare(tieBreakerUid, typedOther.tieBreakerUid);
		}

		return Long.compare(getDelay(NANOSECONDS), o.getDelay(NANOSECONDS));
	}

	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		return delegate.cancel(mayInterruptIfRunning);
	}

	@Override
	public boolean isCancelled() {
		return delegate.isCancelled();
	}

	@Override
	public boolean isDone() {
		return delegate.isDone();
	}

	@Override
	public V get() throws InterruptedException, ExecutionException {
		return delegate.get();
	}

	@Override
	public V get(long timeout, @Nonnull TimeUnit unit)
		throws InterruptedException, ExecutionException, TimeoutException {
		return delegate.get(timeout, unit);
	}

	@VisibleForTesting
	long getTieBreakerUid() {
		return tieBreakerUid;
	}

	@VisibleForTesting
	long getScheduleTimeNanos() {
		return scheduleTimeNanos;
	}

	@Override
	public boolean equals(Object o) {

		if (this == o) {
			return true;
		}

		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		ScheduledFutureAdapter<?> that = (ScheduledFutureAdapter<?>) o;
		return tieBreakerUid == that.tieBreakerUid && scheduleTimeNanos == that.scheduleTimeNanos;
	}

	@Override
	public int hashCode() {
		return Objects.hash(tieBreakerUid, scheduleTimeNanos);
	}
}
