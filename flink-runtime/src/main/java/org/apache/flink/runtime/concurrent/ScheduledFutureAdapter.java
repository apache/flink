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

import javax.annotation.Nonnull;

import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Adapter from {@link Future} to {@link ScheduledFuture}.
 * @param <V> value type of the future.
 */
public class ScheduledFutureAdapter<V> implements ScheduledFuture<V> {

	@Nonnull
	private final Future<V> delegate;
	@Nonnull
	private final TimeUnit timeUnit;

	private final long delay;

	public ScheduledFutureAdapter(
		@Nonnull Future<V> delegate,
		long delay,
		@Nonnull TimeUnit timeUnit) {

		this.delegate = delegate;
		this.delay = delay;
		this.timeUnit = timeUnit;
	}

	@Override
	public long getDelay(@Nonnull TimeUnit unit) {
		return unit.convert(delay, timeUnit);
	}

	@Override
	public int compareTo(@Nonnull Delayed o) {
		return Long.compare(delay, o.getDelay(timeUnit));
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
}
