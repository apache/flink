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

import org.apache.flink.core.testutils.ManuallyTriggeredDirectExecutor;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Simple {@link ScheduledExecutor} implementation for testing purposes.
 */
public class ManuallyTriggeredScheduledExecutor extends ManuallyTriggeredDirectExecutor implements ScheduledExecutor {

	private final ConcurrentLinkedQueue<ScheduledTask<?>> scheduledTasks = new ConcurrentLinkedQueue<>();

	@Override
	public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
		return insertRunnable(command, false);
	}

	@Override
	public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
		final ScheduledTask<V> scheduledTask = new ScheduledTask<>(callable, false);

		scheduledTasks.offer(scheduledTask);

		return scheduledTask;
	}

	@Override
	public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
		return insertRunnable(command, true);
	}

	@Override
	public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
		return insertRunnable(command, true);
	}

	Collection<ScheduledFuture<?>> getScheduledTasks() {
		return new ArrayList<>(scheduledTasks);
	}

	/**
	 * Triggers all registered tasks.
	 */
	public void triggerScheduledTasks() {
		final Iterator<ScheduledTask<?>> iterator = scheduledTasks.iterator();

		while (iterator.hasNext()) {
			final ScheduledTask<?> scheduledTask = iterator.next();

			scheduledTask.execute();

			if (!scheduledTask.isPeriodic) {
				iterator.remove();
			}
		}
	}

	private ScheduledFuture<?> insertRunnable(Runnable command, boolean isPeriodic) {
		final ScheduledTask<?> scheduledTask = new ScheduledTask<>(
			() -> {
				command.run();
				return null;
			},
			isPeriodic);

		scheduledTasks.offer(scheduledTask);

		return scheduledTask;
	}

	private static final class ScheduledTask<T> implements ScheduledFuture<T> {

		private final Callable<T> callable;

		private final boolean isPeriodic;

		private final CompletableFuture<T> result;

		private ScheduledTask(Callable<T> callable, boolean isPeriodic) {
			this.callable = Preconditions.checkNotNull(callable);
			this.isPeriodic = isPeriodic;

			this.result = new CompletableFuture<>();
		}

		public void execute() {
			if (!result.isDone()) {
				if (!isPeriodic) {
					try {
						result.complete(callable.call());
					} catch (Exception e) {
						result.completeExceptionally(e);
					}
				} else {
					try {
						callable.call();
					} catch (Exception e) {
						result.completeExceptionally(e);
					}
				}
			}
		}

		@Override
		public long getDelay(TimeUnit unit) {
			return 0;
		}

		@Override
		public int compareTo(Delayed o) {
			return 0;
		}

		@Override
		public boolean cancel(boolean mayInterruptIfRunning) {
			return result.cancel(mayInterruptIfRunning);
		}

		@Override
		public boolean isCancelled() {
			return result.isCancelled();
		}

		@Override
		public boolean isDone() {
			return result.isDone();
		}

		@Override
		public T get() throws InterruptedException, ExecutionException {
			return result.get();
		}

		@Override
		public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
			return result.get(timeout, unit);
		}
	}
}
