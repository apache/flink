/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package org.apache.flink.runtime.source.coordinator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

/**
 * This class is used to coordinate between two components, where one component has an
 * executor following the mailbox model and the other component notifies it when needed.
 */
public class ExecutorNotifier implements AutoCloseable {
	private static final Logger LOG = LoggerFactory.getLogger(ExecutorNotifier.class);
	private final ScheduledExecutorService workerExecutor;
	private final Executor executorToNotify;
	private final AtomicBoolean closed;

	public ExecutorNotifier(ScheduledExecutorService workerExecutor,
							Executor executorToNotify) {
		this.executorToNotify = executorToNotify;
		this.workerExecutor = workerExecutor;
		this.closed = new AtomicBoolean(false);
	}

	/**
	 * Call the given callable once. Notify the {@link #executorToNotify} to execute
	 * the handler.
	 *
	 * <p>Note that when this method is invoked multiple times, it is possible that
	 * multiple callables are executed concurrently, so do the handlers. For example,
	 * assuming both the workerExecutor and executorToNotify are single threaded.
	 * The following code may still throw a <code>ConcurrentModificationException</code>.
	 *
	 * <pre>{@code
	 *  final List<Integer> list = new ArrayList<>();
	 *
	 *  // The callable adds an integer 1 to the list, while it works at the first glance,
	 *  // A ConcurrentModificationException may be thrown because the caller and
	 *  // handler may modify the list at the same time.
	 *  notifier.notifyReadyAsync(
	 *  	() -> list.add(1),
	 *  	(ignoredValue, ignoredThrowable) -> list.add(2));
	 * }</pre>
	 *
	 * <p>Instead, the above logic should be implemented in as:
	 * <pre>{@code
	 *  // Modify the state in the handler.
	 *  notifier.notifyReadyAsync(() -> 1, (v, ignoredThrowable) -> {
	 *  	list.add(v));
	 *  	list.add(2);
	 *  });
	 * }</pre>
	 *
	 * @param callable the callable to invoke before notifying the executor.
	 * @param handler the handler to handle the result of the callable.
	 */
	public <T> void notifyReadyAsync(Callable<T> callable, BiConsumer<T, Throwable> handler) {
		workerExecutor.execute(() -> {
			try {
				T result = callable.call();
				executorToNotify.execute(() -> handler.accept(result, null));
			} catch (Throwable t) {
				executorToNotify.execute(() -> handler.accept(null, t));
			}
		});
	}

	/**
	 * Call the given callable once. Notify the {@link #executorToNotify} to execute
	 * the handler.
	 *
	 * <p>Note that when this method is invoked multiple times, it is possible that
	 * multiple callables are executed concurrently, so do the handlers. For example,
	 * assuming both the workerExecutor and executorToNotify are single threaded.
	 * The following code may still throw a <code>ConcurrentModificationException</code>.
	 *
	 * <pre>{@code
	 *  final List<Integer> list = new ArrayList<>();
	 *
	 *  // The callable adds an integer 1 to the list, while it works at the first glance,
	 *  // A ConcurrentModificationException may be thrown because the caller and
	 *  // handler may modify the list at the same time.
	 *  notifier.notifyReadyAsync(
	 *  	() -> list.add(1),
	 *  	(ignoredValue, ignoredThrowable) -> list.add(2));
	 * }</pre>
	 *
	 * <p>Instead, the above logic should be implemented in as:
	 * <pre>{@code
	 *  // Modify the state in the handler.
	 *  notifier.notifyReadyAsync(() -> 1, (v, ignoredThrowable) -> {
	 *  	list.add(v));
	 *  	list.add(2);
	 *  });
	 * }</pre>
	 *
	 * @param callable the callable to execute before notifying the executor to notify.
	 * @param handler the handler that handles the result from the callable.
	 * @param initialDelayMs the initial delay in ms before invoking the given callable.
	 * @param periodMs the interval in ms to invoke the callable.
	 */
	public <T> void notifyReadyAsync(
			Callable<T> callable,
			BiConsumer<T, Throwable> handler,
			long initialDelayMs,
			long periodMs) {
		workerExecutor.scheduleAtFixedRate(() -> {
			try {
				T result = callable.call();
				executorToNotify.execute(() -> handler.accept(result, null));
			} catch (Throwable t) {
				executorToNotify.execute(() -> handler.accept(null, t));
			}
		}, initialDelayMs, periodMs, TimeUnit.MILLISECONDS);
	}

	/**
	 * Close the executor notifier. This is a blocking call which waits for all the
	 * async calls to finish before it returns.
	 *
	 * @throws InterruptedException when interrupted during closure.
	 */
	public void close() throws InterruptedException {
		if (!closed.compareAndSet(false, true)) {
			LOG.debug("The executor notifier has been closed.");
			return;
		}
		// Shutdown the worker executor, so no more worker tasks can run.
		workerExecutor.shutdownNow();
		workerExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
	}
}
