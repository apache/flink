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

package org.apache.flink.runtime.util;

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

public class DirectExecutorService implements ExecutorService {
	private boolean _shutdown = false;

	@Override
	public void shutdown() {
		_shutdown = true;
	}

	@Override
	public List<Runnable> shutdownNow() {
		_shutdown = true;
		return Collections.emptyList();
	}

	@Override
	public boolean isShutdown() {
		return _shutdown;
	}

	@Override
	public boolean isTerminated() {
		return _shutdown;
	}

	@Override
	public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
		return _shutdown;
	}

	@Override
	public <T> Future<T> submit(Callable<T> task) {
		try {
			T result = task.call();

			return new CompletedFuture<>(result, null);
		} catch (Exception e) {
			return new CompletedFuture<>(null, e);
		}
	}

	@Override
	public <T> Future<T> submit(Runnable task, T result) {
		task.run();

		return new CompletedFuture<>(result, null);
	}

	@Override
	public Future<?> submit(Runnable task) {
		task.run();
		return new CompletedFuture<>(null, null);
	}

	@Override
	public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
		ArrayList<Future<T>> result = new ArrayList<>();

		for (Callable<T> task : tasks) {
			try {
				result.add(new CompletedFuture<T>(task.call(), null));
			} catch (Exception e) {
				result.add(new CompletedFuture<T>(null, e));
			}
		}
		return result;
	}

	@Override
	public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
		long end = System.currentTimeMillis() + unit.toMillis(timeout);
		Iterator<? extends Callable<T>> iterator = tasks.iterator();
		ArrayList<Future<T>> result = new ArrayList<>();

		while (end > System.currentTimeMillis() && iterator.hasNext()) {
			Callable<T> callable = iterator.next();

			try {
				result.add(new CompletedFuture<T>(callable.call(), null));
			} catch (Exception e) {
				result.add(new CompletedFuture<T>(null, e));
			}
		}

		while(iterator.hasNext()) {
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
				public T get() throws InterruptedException, ExecutionException {
					throw new CancellationException("Task has been cancelled.");
				}

				@Override
				public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
					throw new CancellationException("Task has been cancelled.");
				}
			});
		}

		return result;
	}

	@Override
	public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
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
	public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
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
	public void execute(Runnable command) {
		command.run();
	}

	public static class CompletedFuture<V> implements Future<V> {
		private final V value;
		private final Exception exception;

		public CompletedFuture(V value, Exception exception) {
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
		public V get() throws InterruptedException, ExecutionException {
			if (exception != null) {
				throw new ExecutionException(exception);
			} else {
				return value;
			}
		}

		@Override
		public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
			return get();
		}
	}
}
