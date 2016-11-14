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

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Flink's basic future abstraction. A future represents an asynchronous operation whose result
 * will be contained in this instance upon completion.
 *
 * @param <T> type of the future's result
 */
public interface Future<T> {

	/**
	 * Checks if the future has been completed. A future is completed, if the result has been
	 * delivered.
	 *
	 * @return true if the future is completed; otherwise false
	 */
	boolean isDone();

	/**
	 * Tries to cancel the future's operation. Note that not all future operations can be canceled.
	 * The result of the cancelling will be returned.
	 *
	 * @param mayInterruptIfRunning true iff the future operation may be interrupted
	 * @return true if the cancelling was successful; otherwise false
	 */
	boolean cancel(boolean mayInterruptIfRunning);

	/**
	 * Gets the result value of the future. If the future has not been completed, then this
	 * operation will block indefinitely until the result has been delivered.
	 *
	 * @return the result value
	 * @throws CancellationException if the future has been cancelled
	 * @throws InterruptedException if the current thread was interrupted while waiting for the result
	 * @throws ExecutionException if the future has been completed with an exception
	 */
	T get() throws InterruptedException, ExecutionException;

	/**
	 * Gets the result value of the future. If the future has not been done, then this operation
	 * will block the given timeout value. If the result has not been delivered within the timeout,
	 * then the method throws an {@link TimeoutException}.
	 *
	 * @param timeout the time to wait for the future to be done
	 * @param unit time unit for the timeout argument
	 * @return the result value
	 * @throws CancellationException if the future has been cancelled
	 * @throws InterruptedException if the current thread was interrupted while waiting for the result
	 * @throws ExecutionException if the future has been completed with an exception
	 * @throws TimeoutException if the future has not been completed within the given timeout
	 */
	T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException;

	/**
	 * Gets the value of the future. If the future has not been completed when calling this
	 * function, the given value is returned.
	 *
	 * @param valueIfAbsent value which is returned if the future has not been completed
	 * @return value of the future or the given value if the future has not been completed
	 * @throws ExecutionException if the future has been completed with an exception
	 */
	T getNow(T valueIfAbsent) throws ExecutionException;

	/**
	 * Applies the given function to the value of the future. The result of the apply function is
	 * the value of the newly returned future.
	 * <p>
	 * The apply function is executed asynchronously by the given executor.
	 *
	 * @param applyFunction function to apply to the future's value
	 * @param executor used to execute the given apply function asynchronously
	 * @param <R> type of the apply function's return value
	 * @return future representing the return value of the given apply function
	 */
	<R> Future<R> thenApplyAsync(ApplyFunction<? super T, ? extends R> applyFunction, Executor executor);

	/**
	 * Applies the given function to the value of the future. The result of the apply function is
	 * the value of the newly returned future.
	 *
	 * @param applyFunction function to apply to the future's value
	 * @param <R> type of the apply function's return value
	 * @return future representing the return value of the given apply function
	 */
	<R> Future<R> thenApply(ApplyFunction<? super T, ? extends R> applyFunction);

	/**
	 * Applies the accept function to the value of the future. Unlike the {@link ApplyFunction}, the
	 * {@link AcceptFunction} does not return a value. The returned future, thus, represents only
	 * the completion of the accept callback.
	 * <p>
	 * The accept function is executed asynchronously by the given executor.
	 *
	 * @param acceptFunction function to apply to the future's value
	 * @param executor used to execute the given apply function asynchronously
	 * @return future representing the completion of the accept callback
	 */
	Future<Void> thenAcceptAsync(AcceptFunction<? super T> acceptFunction, Executor executor);

	/**
	 * Applies the accept function to the value of the future. Unlike the {@link ApplyFunction}, the
	 * {@link AcceptFunction} does not return a value. The returned future, thus, represents only
	 * the completion of the accept callback.
	 *
	 * @param acceptFunction function to apply to the future's value
	 * @return future representing the completion of the accept callback
	 */
	Future<Void> thenAccept(AcceptFunction<? super T> acceptFunction);

	/**
	 * Applies the given function to the value of the future if the future has been completed
	 * exceptionally. The completing exception is given to the apply function which can return a new
	 * value which is the value of the returned future.
	 * <p>
	 * The apply function is executed asynchronously by the given executor.
	 *
	 * @param exceptionallyFunction to apply to the future's value if it is an exception
	 * @param executor used to execute the given apply function asynchronously
	 * @param <R> type of the apply function's return value
	 * @return future representing the return value of the given apply function
	 */
	<R> Future<R> exceptionallyAsync(ApplyFunction<Throwable, ? extends R> exceptionallyFunction, Executor executor);

	/**
	 * Applies the given function to the value of the future if the future has been completed
	 * exceptionally. The completing exception is given to the apply function which can return a new
	 * value which is the value of the returned future.
	 *
	 * @param exceptionallyFunction to apply to the future's value if it is an exception
	 * @param <R> type of the apply function's return value
	 * @return future representing the return value of the given apply function
	 */
	<R> Future<R> exceptionally(ApplyFunction<Throwable, ? extends R> exceptionallyFunction);

	/**
	 * Applies the given function to the value of the future. The apply function returns a future
	 * result, which is flattened. This means that the resulting future of this method represents
	 * the future's value of the apply function.
	 * <p>
	 * The apply function is executed asynchronously by the given executor.
	 *
	 * @param composeFunction to apply to the future's value. The function returns a future which is
	 *                        flattened
	 * @param executor used to execute the given apply function asynchronously
	 * @param <R> type of the returned future's value
	 * @return future representing the flattened return value of the apply function
	 */
	<R> Future<R> thenComposeAsync(ApplyFunction<? super T, ? extends Future<R>> composeFunction, Executor executor);

	/**
	 * Applies the given function to the value of the future. The apply function returns a future
	 * result, which is flattened. This means that the resulting future of this method represents
	 * the future's value of the apply function.
	 *
	 * @param composeFunction to apply to the future's value. The function returns a future which is
	 *                        flattened
	 * @param <R> type of the returned future's value
	 * @return future representing the flattened return value of the apply function
	 */
	<R> Future<R> thenCompose(ApplyFunction<? super T, ? extends Future<R>> composeFunction);

	/**
	 * Applies the given handle function to the result of the future. The result can either be the
	 * future's value or the exception with which the future has been completed. The two cases are
	 * mutually exclusive. This means that either the left or right argument of the handle function
	 * are non null. The result of the handle function is the returned future's value.
	 * <p>
	 * The handle function is executed asynchronously by the given executor.
	 *
	 * @param biFunction applied to the result (normal and exceptional) of the future
	 * @param executor used to execute the handle function asynchronously
	 * @param <R> type of the handle function's return value
	 * @return future representing the handle function's return value
	 */
	<R> Future<R> handleAsync(BiFunction<? super T, Throwable, ? extends R> biFunction, Executor executor);

	/**
	 * Applies the given handle function to the result of the future. The result can either be the
	 * future's value or the exception with which the future has been completed. The two cases are
	 * mutually exclusive. This means that either the left or right argument of the handle function
	 * are non null. The result of the handle function is the returned future's value.
	 *
	 * @param biFunction applied to the result (normal and exceptional) of the future
	 * @param <R> type of the handle function's return value
	 * @return future representing the handle function's return value
	 */
	<R> Future<R> handle(BiFunction<? super T, Throwable, ? extends R> biFunction);

	/**
	 * Applies the given function to the result of this and the other future after both futures
	 * have completed. The result of the bi-function is the result of the returned future.
	 * <p>
	 * The bi-function is executed asynchronously by the given executor.
	 *
	 * @param other future whose result is the right input to the bi-function
	 * @param biFunction applied to the result of this and that future
	 * @param executor used to execute the bi-function asynchronously
	 * @param <U> type of that future's return value
	 * @param <R> type of the bi-function's return value
	 * @return future representing the bi-function's return value
	 */
	<U, R> Future<R> thenCombineAsync(Future<U> other, BiFunction<? super T, ? super U, ? extends R> biFunction, Executor executor);

	/**
	 * Applies the given function to the result of this and the other future after both futures
	 * have completed. The result of the bi-function is the result of the returned future.
	 *
	 * @param other future whose result is the right input to the bi-function
	 * @param biFunction applied to the result of this and that future
	 * @param <U> type of that future's return value
	 * @param <R> type of the bi-function's return value
	 * @return future representing the bi-function's return value
	 */
	<U, R> Future<R> thenCombine(Future<U> other, BiFunction<? super T, ? super U, ? extends R> biFunction);
}
