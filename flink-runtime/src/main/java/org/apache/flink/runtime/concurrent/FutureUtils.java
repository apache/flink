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

import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

public class FutureUtils {

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
}
