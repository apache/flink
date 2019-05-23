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

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** The direct executor service directly executes the runnables and the callables in the calling thread. */
class DirectExecutorService implements ExecutorService {
	static final DirectExecutorService INSTANCE = new DirectExecutorService();

	private boolean isShutdown = false;

	@Override
	public void shutdown() {
		isShutdown = true;
	}

	@Override
	@Nonnull
	public List<Runnable> shutdownNow() {
		isShutdown = true;
		return Collections.emptyList();
	}

	@Override
	public boolean isShutdown() {
		return isShutdown;
	}

	@Override
	public boolean isTerminated() {
		return isShutdown;
	}

	@Override
	public boolean awaitTermination(long timeout, @Nonnull TimeUnit unit) {
		return isShutdown;
	}

	@Override
	@Nonnull
	public <T> Future<T> submit(@Nonnull Callable<T> task) {
		try {
			T result = task.call();

			return new CompletedFuture<>(result, null);
		} catch (Exception e) {
			return new CompletedFuture<>(null, e);
		}
	}

	@Override
	@Nonnull
	public <T> Future<T> submit(@Nonnull Runnable task, T result) {
		task.run();

		return new CompletedFuture<>(result, null);
	}

	@Override
	@Nonnull
	public Future<?> submit(@Nonnull Runnable task) {
		task.run();
		return new CompletedFuture<>(null, null);
	}

	@Override
	@Nonnull
	public <T> List<Future<T>> invokeAll(@Nonnull Collection<? extends Callable<T>> tasks) {
		ArrayList<Future<T>> result = new ArrayList<>();

		for (Callable<T> task : tasks) {
			try {
				result.add(new CompletedFuture<>(task.call(), null));
			} catch (Exception e) {
				result.add(new CompletedFuture<>(null, e));
			}
		}
		return result;
	}

	@Override
	@Nonnull
	public <T> List<Future<T>> invokeAll(
		@Nonnull Collection<? extends Callable<T>> tasks, long timeout, @Nonnull TimeUnit unit) {

		long end = System.currentTimeMillis() + unit.toMillis(timeout);
		Iterator<? extends Callable<T>> iterator = tasks.iterator();
		ArrayList<Future<T>> result = new ArrayList<>();

		while (end > System.currentTimeMillis() && iterator.hasNext()) {
			Callable<T> callable = iterator.next();

			try {
				result.add(new CompletedFuture<>(callable.call(), null));
			} catch (Exception e) {
				result.add(new CompletedFuture<>(null, e));
			}
		}

		while (iterator.hasNext()) {
			iterator.next();
			result.add(new Future<T>() {
				@Override
				public boolean cancel(boolean mayInterruptIfRunning) {
					return false;
				}

				@Override
				public boolean isCancelled() {
					return true;
				}

				@Override
				public boolean isDone() {
					return false;
				}

				@Override
				public T get() {
					throw new CancellationException("Task has been cancelled.");
				}

				@Override
				public T get(long timeout, @Nonnull TimeUnit unit) {
					throw new CancellationException("Task has been cancelled.");
				}
			});
		}

		return result;
	}

	@Override
	@Nonnull
	public <T> T invokeAny(@Nonnull Collection<? extends Callable<T>> tasks) throws ExecutionException {
		Exception exception = null;

		for (Callable<T> task : tasks) {
			try {
				return task.call();
			} catch (Exception e) {
				// try next task
				exception = e;
			}
		}

		throw new ExecutionException("No tasks finished successfully.", exception);
	}

	@Override
	public <T> T invokeAny(
		@Nonnull Collection<? extends Callable<T>> tasks,
		long timeout,
		@Nonnull TimeUnit unit) throws ExecutionException, TimeoutException {

		long end = System.currentTimeMillis() + unit.toMillis(timeout);
		Exception exception = null;

		Iterator<? extends Callable<T>> iterator = tasks.iterator();

		while (end > System.currentTimeMillis() && iterator.hasNext()) {
			Callable<T> callable = iterator.next();

			try {
				return callable.call();
			} catch (Exception e) {
				// ignore exception and try next
				exception = e;
			}
		}

		if (iterator.hasNext()) {
			throw new TimeoutException("Could not finish execution of tasks within time.");
		} else {
			throw new ExecutionException("No tasks finished successfully.", exception);
		}
	}

	@Override
	public void execute(@Nonnull Runnable command) {
		command.run();
	}

	static class CompletedFuture<V> implements Future<V> {
		private final V value;
		private final Exception exception;

		CompletedFuture(V value, Exception exception) {
			this.value = value;
			this.exception = exception;
		}

		@Override
		public boolean cancel(boolean mayInterruptIfRunning) {
			return false;
		}

		@Override
		public boolean isCancelled() {
			return false;
		}

		@Override
		public boolean isDone() {
			return true;
		}

		@Override
		public V get() throws ExecutionException {
			if (exception != null) {
				throw new ExecutionException(exception);
			} else {
				return value;
			}
		}

		@Override
		public V get(long timeout, @Nonnull TimeUnit unit) throws ExecutionException {
			return get();
		}
	}
}
