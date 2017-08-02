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

import org.apache.flink.runtime.concurrent.impl.FlinkCompletableFuture;
import org.apache.flink.util.Preconditions;

import akka.dispatch.OnComplete;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

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
	public static <T> java.util.concurrent.CompletableFuture<T> retry(
		final Callable<java.util.concurrent.CompletableFuture<T>> operation,
		final int retries,
		final Executor executor) {

		java.util.concurrent.CompletableFuture<T> operationResultFuture;

		try {
			operationResultFuture = operation.call();
		} catch (Exception e) {
			return FutureUtils.completedExceptionally(new RetryException("Could not execute the provided operation.", e));
		}

		return operationResultFuture.handleAsync(
			(t, throwable) -> {
				if (throwable != null) {
					if (retries > 0) {
						return retry(operation, retries - 1, executor);
					} else {
						return FutureUtils.<T>completedExceptionally(new RetryException("Could not complete the operation. Number of retries " +
							"has been exhausted.", throwable));
					}
				} else {
					return java.util.concurrent.CompletableFuture.completedFuture(t);
				}
			},
			executor)
		.thenCompose(value -> value);
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
	public static <T> ConjunctFuture<Collection<T>> combineAll(Collection<? extends java.util.concurrent.CompletableFuture<? extends T>> futures) {
		checkNotNull(futures, "futures");

		final ResultConjunctFuture<T> conjunct = new ResultConjunctFuture<>(futures.size());

		if (futures.isEmpty()) {
			conjunct.complete(Collections.emptyList());
		}
		else {
			for (java.util.concurrent.CompletableFuture<? extends T> future : futures) {
				future.whenComplete(conjunct::handleCompletedFuture);
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
	public static ConjunctFuture<Void> waitForAll(Collection<? extends java.util.concurrent.CompletableFuture<?>> futures) {
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
	public abstract static class ConjunctFuture<T> extends java.util.concurrent.CompletableFuture<T> {

		/**
		 * Gets the total number of Futures in the conjunction.
		 * @return The total number of Futures in the conjunction.
		 */
		public abstract int getNumFuturesTotal();

		/**
		 * Gets the number of Futures in the conjunction that are already complete.
		 * @return The number of Futures in the conjunction that are already complete
		 */
		public abstract int getNumFuturesCompleted();
	}

	/**
	 * The implementation of the {@link ConjunctFuture} which returns its Futures' result as a collection.
	 */
	private static class ResultConjunctFuture<T> extends ConjunctFuture<Collection<T>> {

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
		final void handleCompletedFuture(T value, Throwable throwable) {
			if (throwable != null) {
				completeExceptionally(throwable);
			} else {
				int index = nextIndex.getAndIncrement();

				results[index] = value;

				if (numCompleted.incrementAndGet() == numTotal) {
					complete(Arrays.asList(results));
				}
			}
		}

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
	private static final class WaitingConjunctFuture extends ConjunctFuture<Void> {

		/** Number of completed futures */
		private final AtomicInteger numCompleted = new AtomicInteger(0);

		/** Total number of futures to wait on */
		private final int numTotal;

		/** Method which increments the atomic completion counter and completes or fails the WaitingFutureImpl */
		private void handleCompletedFuture(Object ignored, Throwable throwable) {
			if (throwable == null) {
				if (numTotal == numCompleted.incrementAndGet()) {
					complete(null);
				}
			} else {
				completeExceptionally(throwable);
			}
		}

		private WaitingConjunctFuture(Collection<? extends java.util.concurrent.CompletableFuture<?>> futures) {
			Preconditions.checkNotNull(futures, "Futures must not be null.");

			this.numTotal = futures.size();

			if (futures.isEmpty()) {
				complete(null);
			} else {
				for (java.util.concurrent.CompletableFuture<?> future : futures) {
					future.whenComplete(this::handleCompletedFuture);
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

	// ------------------------------------------------------------------------
	//  Helper methods
	// ------------------------------------------------------------------------

	/**
	 * Returns an exceptionally completed {@link java.util.concurrent.CompletableFuture}.
	 *
	 * @param cause to complete the future with
	 * @param <T> type of the future
	 * @return An exceptionally completed CompletableFuture
	 */
	public static <T>java.util.concurrent.CompletableFuture<T> completedExceptionally(Throwable cause) {
		java.util.concurrent.CompletableFuture<T> result = new java.util.concurrent.CompletableFuture<>();
		result.completeExceptionally(cause);

		return result;
	}

	// ------------------------------------------------------------------------
	//  Converting futures
	// ------------------------------------------------------------------------

	/**
	 * Converts a Scala {@link scala.concurrent.Future} to a {@link java.util.concurrent.CompletableFuture}.
	 *
	 * @param scalaFuture to convert to a Java 8 CompletableFuture
	 * @param <T> type of the future value
	 * @return Java 8 CompletableFuture
	 */
	public static <T> java.util.concurrent.CompletableFuture<T> toJava(scala.concurrent.Future<T> scalaFuture) {
		final java.util.concurrent.CompletableFuture<T> result = new java.util.concurrent.CompletableFuture<>();

		scalaFuture.onComplete(new OnComplete<T>() {
			@Override
			public void onComplete(Throwable failure, T success) throws Throwable {
				if (failure != null) {
					result.completeExceptionally(failure);
				} else {
					result.complete(success);
				}
			}
		}, Executors.directExecutionContext());

		return result;
	}

	/**
	 * Converts a Flink {@link Future} into a {@link CompletableFuture}.
	 *
	 * @param flinkFuture to convert to a Java 8 CompletableFuture
	 * @param <T> type of the future value
	 * @return Java 8 CompletableFuture
	 *
	 * @deprecated Will be removed once we completely remove Flink's futures
	 */
	@Deprecated
	public static <T> java.util.concurrent.CompletableFuture<T> toJava(Future<T> flinkFuture) {
		final java.util.concurrent.CompletableFuture<T> result = new java.util.concurrent.CompletableFuture<>();

		flinkFuture.handle(
			(t, throwable) -> {
				if (throwable != null) {
					result.completeExceptionally(throwable);
				} else {
					result.complete(t);
				}

				return null;
			}
		);

		return result;
	}

	/**
	 * Converts a Java 8 {@link java.util.concurrent.CompletableFuture} into a Flink {@link Future}.
	 *
	 * @param javaFuture to convert to a Flink future
	 * @param <T> type of the future value
	 * @return Flink future
	 *
	 * @deprecated Will be removed once we completely remove Flink's futures
	 */
	@Deprecated
	public static <T> Future<T> toFlinkFuture(java.util.concurrent.CompletableFuture<T> javaFuture) {
		FlinkCompletableFuture<T> result = new FlinkCompletableFuture<>();

		javaFuture.whenComplete(
			(value, throwable) -> {
				if (throwable == null) {
					result.complete(value);
				} else {
					result.completeExceptionally(throwable);
				}
			});

		return result;
	}
}
