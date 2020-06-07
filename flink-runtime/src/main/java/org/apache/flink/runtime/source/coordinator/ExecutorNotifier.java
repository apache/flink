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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * This class is used to coordinate between two components, where one component has an
 * executor following the mailbox model and the other component notifies it when needed.
 */
public class ExecutorNotifier implements AutoCloseable {
	private static final Logger LOG = LoggerFactory.getLogger(ExecutorNotifier.class);
	private static final Future<Void> PENDING_FUTURE = new CompletableFuture<>();
	private final ScheduledExecutorService workerExecutor;
	private final ExecutorService executorToNotify;
	private final ConcurrentSkipListMap<Long, Future<?>> executionFutures;
	private final AtomicLong nextSubmissionId;
	private final Thread.UncaughtExceptionHandler executionErrorHandler;
	private final AtomicBoolean closed;

	public ExecutorNotifier(ScheduledExecutorService workerExecutor,
							ExecutorService executorToNotify,
							Thread.UncaughtExceptionHandler executionErrorHandler) {
		this.executorToNotify = executorToNotify;
		this.workerExecutor = workerExecutor;
		this.executionFutures = new ConcurrentSkipListMap<>();
		this.nextSubmissionId = new AtomicLong(0);
		this.executionErrorHandler = executionErrorHandler;
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
	public <T> boolean notifyReadyAsync(Callable<T> callable, BiConsumer<T, Throwable> handler) {
		if (closed.get()) {
			return false;
		}
		workerExecutor.submit(getRunnable(callable, handler));
		return true;
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
	public <T> boolean notifyReadyAsync(
			Callable<T> callable,
			BiConsumer<T, Throwable> handler,
			long initialDelayMs,
			long periodMs) {
		if (closed.get()) {
			return false;
		}
		workerExecutor.scheduleAtFixedRate(
				getRunnable(callable, handler),
				initialDelayMs,
				periodMs,
				TimeUnit.MILLISECONDS);
		return true;
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
		// First shutdown the worker executor, so no more worker tasks can run.
		workerExecutor.shutdown();
		workerExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
		// Wait for the handler tasks that have been submitted to the executorToNotify to finish.
		// These tasks have to be canceled in their submission order.
		List<Future<?>> futures = new ArrayList<>(executionFutures.values());
		for (int i = futures.size() - 1; i > 0; i--) {
			futures.get(i).cancel(true);
		}
		// Remove the submission future of the finished / canceled handler tasks.
		executionFutures.entrySet().removeIf(longFutureEntry -> longFutureEntry.getValue().isDone());
		waitForHandlerTasksToFinish();
	}

	// --------------- private methods -------------

	private <T> Runnable getRunnable(
			final Callable<T> callable,
			final BiConsumer<T, Throwable> handler) {
		return () -> {
			try {
				if (closed.get()) {
					LOG.debug("The executor epoch has been bumped, skip running this task.");
					return;
				}
				T result = callable.call();
				submitAndTrackExecution(handlerSubmissionId ->
						executorToNotify.submit(() -> {
							try {
								if (closed.get()) {
									LOG.debug("The executor epoch has been bumped, skip running this task.");
									return;
								}
								handler.accept(result, null);
							} catch (Throwable t) {
								// We need to catch throwable here because the executor.submit() does not
								// invoke uncaught exception handler set in the thread factory.
								executionErrorHandler.uncaughtException(Thread.currentThread(), t);
							} finally {
								finishExecution(handlerSubmissionId);
							}
						}));
			} catch (Throwable t) {
				LOG.error("Unexpected exception {}", t);
				handler.accept(null, t);
			}
		};
	}

	private void submitAndTrackExecution(Function<Long, Future> submitAction) {
		long submissionId = prepareSubmission();
		Future<?> submissionFuture = null;
		try {
			submissionFuture = submitAction.apply(submissionId);
		} finally {
			trackExecution(submissionId, submissionFuture);
		}
	}

	private long prepareSubmission() {
		long submissionId = nextSubmissionId.getAndIncrement();
		executionFutures.put(submissionId, PENDING_FUTURE);
		return submissionId;
	}

	private void trackExecution(long submissionId, Future<?> future) {
		// If the future is null, that means the submission has failed, we just remove the
		// submission id from the execution futures map.
		if (future == null) {
			executionFutures.remove(submissionId);
		}
		// If the id does not exist in the map anymore, that means the execution has finished,
		// so we don't put anything into the map. Note that in the runnable, the future is removed
		// from the map before it is completed. When the runnable has finished running but the thread
		// is still executing some finishing sequence, the submissionId would have been removed from
		// the execution futures map but the submission future may have not been completed. Therefore
		// we use the absence of the submissionId to decide whether the execution future needs to
		// be put into the map or not, instead of using the submission future state.
		executionFutures.compute(submissionId, (id, existing) -> existing == null ? null : future);
	}

	private void waitForHandlerTasksToFinish() throws InterruptedException {
		synchronized (executionFutures) {
			while (!executionFutures.isEmpty()) {
				LOG.debug("Waiting for {} handler tasks to finish.", executionFutures.size());
				executionFutures.wait();
			}
		}
	}

	private void finishExecution(long submissionId) {
		executionFutures.remove(submissionId);
		if (executionFutures.isEmpty()) {
			synchronized (executionFutures) {
				executionFutures.notifyAll();
			}
		}
	}
}
