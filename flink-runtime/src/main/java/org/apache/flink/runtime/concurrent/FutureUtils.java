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

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.runtime.concurrent.impl.FlinkCompletableFuture;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import scala.concurrent.duration.Deadline;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A collection of utilities that expand the usage of {@link Future} and {@link CompletableFuture}.
 */
public class FutureUtils {

	// ------------------------------------------------------------------------
	//  retrying operations
	// ------------------------------------------------------------------------

	/**
	 * Retry the given operation the given number of times in case of a failure.
	 *
	 * @param operation to executed
	 * @param retries if the operation failed
	 * @param executor to use to run the futures
	 * @param <T> type of the result
	 * @return Future containing either the result of the operation or a {@link RetryException}
	 */
	public static <T> Future<T> retry(
		final Callable<Future<T>> operation,
		final int retries,
		final Executor executor) {

		Future<T> operationResultFuture;

		try {
			operationResultFuture = operation.call();
		} catch (Exception e) {
			return FlinkCompletableFuture.completedExceptionally(
				new RetryException("Could not execute the provided operation.", e));
		}

		return operationResultFuture.handleAsync(new BiFunction<T, Throwable, Future<T>>() {
			@Override
			public Future<T> apply(T t, Throwable throwable) {
				if (throwable != null) {
					if (retries > 0) {
						return retry(operation, retries - 1, executor);
					} else {
						return FlinkCompletableFuture.completedExceptionally(
							new RetryException("Could not complete the operation. Number of retries " +
								"has been exhausted.", throwable));
					}
				} else {
					return FlinkCompletableFuture.completed(t);
				}
			}
		}, executor)
		.thenCompose(new ApplyFunction<Future<T>, Future<T>>() {
			@Override
			public Future<T> apply(Future<T> value) {
				return value;
			}
		});
	}

	/**
	 * Retry the given operation the given number of times in case of a failure.
	 *
	 * @param operation to executed
	 * @param successPredicate if the result is acceptable
	 * @param deadline how much time we have left
	 * @param executor to use to run the futures
	 * @param <T> type of the result
	 * @return Future containing either the result of the operation or a {@link RetryException}
	 */
	public static <T> Future<T> retrySuccessful(
		final Callable<Future<T>> operation,
		final FilterFunction<T> successPredicate,
		final Deadline deadline,
		final Executor executor) {

		Future<T> operationResultFuture;

		try {
			operationResultFuture = operation.call();
		} catch (Exception e) {
			return FlinkCompletableFuture.completedExceptionally(
				new RetryException("Could not execute the provided operation.", e));
		}

		return operationResultFuture.handleAsync(new BiFunction<T, Throwable, Future<T>>() {
			@Override
			public Future<T> apply(T t, Throwable throwable) {
				if (throwable != null) {
					if (deadline.hasTimeLeft()) {
						return retrySuccessful(operation, successPredicate, deadline, executor);
					} else {
						return FlinkCompletableFuture.completedExceptionally(
							new RetryException("Could not complete the operation. Number of retries " +
								"has been exhausted.", throwable));
					}
				} else {
					Boolean predicateResult;
					try {
						predicateResult = successPredicate.filter(t);
					} catch (Exception e) {
						return FlinkCompletableFuture.completedExceptionally(
							new RetryException("Predicate threw an exception.", e));

					}
					if (predicateResult) {
						return FlinkCompletableFuture.completed(t);
					} if (deadline.hasTimeLeft()) {
						return retrySuccessful(operation, successPredicate, deadline, executor);
					} else {
						return FlinkCompletableFuture.completedExceptionally(
							new RetryException("No time left and predicate returned false for " + t));

					}
				}
			}
		}, executor)
			.thenCompose(new ApplyFunction<Future<T>, Future<T>>() {
				@Override
				public Future<T> apply(Future<T> value) {
					return value;
				}
			});
	}


	public static class RetryException extends Exception {

		private static final long serialVersionUID = 3613470781274141862L;

		public RetryException(String message) {
			super(message);
		}

		public RetryException(String message, Throwable cause) {
			super(message, cause);
		}

		public RetryException(Throwable cause) {
			super(cause);
		}
	}

	// ------------------------------------------------------------------------
	//  composing futures
	// ------------------------------------------------------------------------

	/**
	 * Creates a future that is complete once multiple other futures completed.
	 * The future fails (completes exceptionally) once one of the futures in the
	 * conjunction fails. Upon successful completion, the future returns the
	 * collection of the futures' results.
	 *
	 * <p>The ConjunctFuture gives access to how many Futures in the conjunction have already
	 * completed successfully, via {@link ConjunctFuture#getNumFuturesCompleted()}.
	 *
	 * @param futures The futures that make up the conjunction. No null entries are allowed.
	 * @return The ConjunctFuture that completes once all given futures are complete (or one fails).
	 */
	public static <T> ConjunctFuture<Collection<T>> combineAll(Collection<? extends Future<? extends T>> futures) {
		checkNotNull(futures, "futures");

		final ResultConjunctFuture<T> conjunct = new ResultConjunctFuture<>(futures.size());

		if (futures.isEmpty()) {
			conjunct.complete(Collections.<T>emptyList());
		}
		else {
			for (Future<? extends T> future : futures) {
				future.handle(conjunct.completionHandler);
			}
		}

		return conjunct;
	}

	/**
	 * Creates a future that is complete once all of the given futures have completed.
	 * The future fails (completes exceptionally) once one of the given futures
	 * fails.
	 *
	 * <p>The ConjunctFuture gives access to how many Futures have already
	 * completed successfully, via {@link ConjunctFuture#getNumFuturesCompleted()}.
	 *
	 * @param futures The futures to wait on. No null entries are allowed.
	 * @return The WaitingFuture that completes once all given futures are complete (or one fails).
	 */
	public static ConjunctFuture<Void> waitForAll(Collection<? extends Future<?>> futures) {
		checkNotNull(futures, "futures");

		return new WaitingConjunctFuture(futures);
	}

	/**
	 * A future that is complete once multiple other futures completed. The futures are not
	 * necessarily of the same type. The ConjunctFuture fails (completes exceptionally) once
	 * one of the Futures in the conjunction fails.
	 *
	 * <p>The advantage of using the ConjunctFuture over chaining all the futures (such as via
	 * {@link Future#thenCombine(Future, BiFunction)}) is that ConjunctFuture also tracks how
	 * many of the Futures are already complete.
	 */
	public interface ConjunctFuture<T> extends CompletableFuture<T> {

		/**
		 * Gets the total number of Futures in the conjunction.
		 * @return The total number of Futures in the conjunction.
		 */
		int getNumFuturesTotal();

		/**
		 * Gets the number of Futures in the conjunction that are already complete.
		 * @return The number of Futures in the conjunction that are already complete
		 */
		int getNumFuturesCompleted();
	}

	/**
	 * The implementation of the {@link ConjunctFuture} which returns its Futures' result as a collection.
	 */
	private static class ResultConjunctFuture<T> extends FlinkCompletableFuture<Collection<T>> implements ConjunctFuture<Collection<T>> {

		/** The total number of futures in the conjunction */
		private final int numTotal;

		/** The next free index in the results arrays */
		private final AtomicInteger nextIndex = new AtomicInteger(0);

		/** The number of futures in the conjunction that are already complete */
		private final AtomicInteger numCompleted = new AtomicInteger(0);

		/** The set of collected results so far */
		private volatile T[] results;

		/** The function that is attached to all futures in the conjunction. Once a future
		 * is complete, this function tracks the completion or fails the conjunct.
		 */
		final BiFunction<T, Throwable, Void> completionHandler = new BiFunction<T, Throwable, Void>() {

			@Override
			public Void apply(T o, Throwable throwable) {
				if (throwable != null) {
					completeExceptionally(throwable);
				} else {
					int index = nextIndex.getAndIncrement();

					results[index] = o;

					if (numCompleted.incrementAndGet() == numTotal) {
						complete(Arrays.asList(results));
					}
				}

				return null;
			}
		};

		@SuppressWarnings("unchecked")
		ResultConjunctFuture(int numTotal) {
			this.numTotal = numTotal;
			results = (T[])new Object[numTotal];
		}

		@Override
		public int getNumFuturesTotal() {
			return numTotal;
		}

		@Override
		public int getNumFuturesCompleted() {
			return numCompleted.get();
		}
	}

	/**
	 * Implementation of the {@link ConjunctFuture} interface which waits only for the completion
	 * of its futures and does not return their values.
	 */
	private static final class WaitingConjunctFuture extends FlinkCompletableFuture<Void> implements ConjunctFuture<Void> {

		/** Number of completed futures */
		private final AtomicInteger numCompleted = new AtomicInteger(0);

		/** Total number of futures to wait on */
		private final int numTotal;

		/** Handler which increments the atomic completion counter and completes or fails the WaitingFutureImpl */
		private final BiFunction<Object, Throwable, Void> completionHandler = new BiFunction<Object, Throwable, Void>() {
			@Override
			public Void apply(Object o, Throwable throwable) {
				if (throwable == null) {
					if (numTotal == numCompleted.incrementAndGet()) {
						complete(null);
					}
				} else {
					completeExceptionally(throwable);
				}

				return null;
			}
		};

		private WaitingConjunctFuture(Collection<? extends Future<?>> futures) {
			Preconditions.checkNotNull(futures, "Futures must not be null.");

			this.numTotal = futures.size();

			if (futures.isEmpty()) {
				complete(null);
			} else {
				for (Future<?> future : futures) {
					future.handle(completionHandler);
				}
			}
		}

		@Override
		public int getNumFuturesTotal() {
			return numTotal;
		}

		@Override
		public int getNumFuturesCompleted() {
			return numCompleted.get();
		}
	}
}
