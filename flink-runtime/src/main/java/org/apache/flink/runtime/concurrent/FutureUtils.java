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

import java.util.Collection;
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
	 * The ConjunctFuture fails (completes exceptionally) once one of the Futures in the
	 * conjunction fails.
	 *
	 * <p>The ConjunctFuture gives access to how many Futures in the conjunction have already
	 * completed successfully, via {@link ConjunctFuture#getNumFuturesCompleted()}. 
	 * 
	 * @param futures The futures that make up the conjunction. No null entries are allowed.
	 * @return The ConjunctFuture that completes once all given futures are complete (or one fails).
	 */
	public static ConjunctFuture combineAll(Collection<? extends Future<?>> futures) {
		checkNotNull(futures, "futures");

		final ConjunctFutureImpl conjunct = new ConjunctFutureImpl(futures.size());

		if (futures.isEmpty()) {
			conjunct.complete(null);
		}
		else {
			for (Future<?> future : futures) {
				future.handle(conjunct.completionHandler);
			}
		}

		return conjunct;
	}

	/**
	 * A future that is complete once multiple other futures completed. The futures are not
	 * necessarily of the same type, which is why the type of this Future is {@code Void}.
	 * The ConjunctFuture fails (completes exceptionally) once one of the Futures in the
	 * conjunction fails.
	 * 
	 * <p>The advantage of using the ConjunctFuture over chaining all the futures (such as via
	 * {@link Future#thenCombine(Future, BiFunction)}) is that ConjunctFuture also tracks how
	 * many of the Futures are already complete.
	 */
	public interface ConjunctFuture extends CompletableFuture<Void> {

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
	 * The implementation of the {@link ConjunctFuture}.
	 * 
	 * <p>Implementation notice: The member fields all have package-private access, because they are
	 * either accessed by an inner subclass or by the enclosing class.
	 */
	private static class ConjunctFutureImpl extends FlinkCompletableFuture<Void> implements ConjunctFuture {

		/** The total number of futures in the conjunction */
		final int numTotal;

		/** The number of futures in the conjunction that are already complete */
		final AtomicInteger numCompleted = new AtomicInteger();

		/** The function that is attached to all futures in the conjunction. Once a future
		 * is complete, this function tracks the completion or fails the conjunct.  
		 */
		final BiFunction<Object, Throwable, Void> completionHandler = new BiFunction<Object, Throwable, Void>() {

			@Override
			public Void apply(Object o, Throwable throwable) {
				if (throwable != null) {
					completeExceptionally(throwable);
				}
				else if (numTotal == numCompleted.incrementAndGet()) {
					complete(null);
				}

				return null;
			}
		};

		ConjunctFutureImpl(int numTotal) {
			this.numTotal = numTotal;
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
