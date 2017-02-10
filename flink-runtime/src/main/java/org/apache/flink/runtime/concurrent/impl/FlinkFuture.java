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

package org.apache.flink.runtime.concurrent.impl;

import akka.dispatch.ExecutionContexts$;
import akka.dispatch.Futures;
import akka.dispatch.Mapper;
import akka.dispatch.OnComplete;
import akka.dispatch.Recover;
import akka.japi.Procedure;

import org.apache.flink.runtime.concurrent.AcceptFunction;
import org.apache.flink.runtime.concurrent.ApplyFunction;
import org.apache.flink.runtime.concurrent.CompletableFuture;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.concurrent.Future;
import org.apache.flink.runtime.concurrent.BiFunction;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Option;
import scala.Tuple2;
import scala.concurrent.Await;
import scala.concurrent.ExecutionContext;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;
import scala.util.Failure;
import scala.util.Success;
import scala.util.Try;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Implementation of {@link Future} which is backed by {@link scala.concurrent.Future}.
 *
 * @param <T> type of the future's value
 */
public class FlinkFuture<T> implements Future<T> {

	private static final Logger LOG = LoggerFactory.getLogger(FlinkFuture.class);

	private static final Executor DIRECT_EXECUTOR = Executors.directExecutor();

	private static final ExecutionContext DIRECT_EXECUTION_CONTEXT = executionContextFromExecutor(DIRECT_EXECUTOR);

	// ------------------------------------------------------------------------

	protected scala.concurrent.Future<T> scalaFuture;

	FlinkFuture() {
		scalaFuture = null;
	}

	public FlinkFuture(scala.concurrent.Future<T> scalaFuture) {
		this.scalaFuture = Preconditions.checkNotNull(scalaFuture);
	}

	public scala.concurrent.Future<T> getScalaFuture() {
		return scalaFuture;
	}

	//-----------------------------------------------------------------------------------
	// Future's methods
	//-----------------------------------------------------------------------------------

	@Override
	public boolean isDone() {
		return scalaFuture.isCompleted();
	}

	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		return false;
	}

	@Override
	public T get() throws InterruptedException, ExecutionException {
		Preconditions.checkNotNull(scalaFuture);

		try {
			return Await.result(scalaFuture, Duration.Inf());
		} catch (InterruptedException e) {
			throw e;
		} catch (FlinkFuture.ThrowableWrapperException e) {
			throw new ExecutionException(e.getCause());
		} catch (Exception e) {
			throw new ExecutionException(e);
		}
	}

	@Override
	public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
		Preconditions.checkNotNull(scalaFuture);
		Preconditions.checkArgument(timeout >= 0L, "The timeout value has to be larger or " +
			"equal than 0.");

		try {
			return Await.result(scalaFuture, new FiniteDuration(timeout, unit));
		} catch (InterruptedException | TimeoutException e) {
			throw e;
		} catch (Exception e) {
			throw new ExecutionException(e);
		}
	}

	@Override
	public T getNow(T valueIfAbsent) throws ExecutionException {
		Preconditions.checkNotNull(scalaFuture);

		Option<Try<T>> value = scalaFuture.value();

		if (value.isDefined()) {
			Try<T> tri = value.get();

			if (tri instanceof Success) {
				return ((Success<T>)tri).value();
			} else {
				throw new ExecutionException(((Failure<T>)tri).exception());
			}
		} else {
			return valueIfAbsent;
		}
	}

	@Override
	public <R> Future<R> thenApplyAsync(final ApplyFunction<? super T, ? extends R> applyFunction, Executor executor) {
		Preconditions.checkNotNull(scalaFuture);
		Preconditions.checkNotNull(applyFunction);
		Preconditions.checkNotNull(executor);

		scala.concurrent.Future<R> mappedFuture = scalaFuture.map(new Mapper<T, R>() {
			@Override
			public R apply(T value) {
				return applyFunction.apply(value);
			}
		}, createExecutionContext(executor));

		return new FlinkFuture<>(mappedFuture);
	}

	@Override
	public <R> Future<R> thenApply(final ApplyFunction<? super T, ? extends R> applyFunction) {
		return thenApplyAsync(applyFunction, Executors.directExecutor());
	}

	@Override
	public Future<Void> thenAcceptAsync(final AcceptFunction<? super T> acceptFunction, Executor executor) {
		Preconditions.checkNotNull(scalaFuture);
		Preconditions.checkNotNull(acceptFunction);
		Preconditions.checkNotNull(executor);

		scala.concurrent.Future<Void> acceptedFuture = scalaFuture.map(new Mapper<T, Void>() {
			@Override
			public Void apply(T value) {
				acceptFunction.accept(value);

				return null;
			}
		}, createExecutionContext(executor));

		return new FlinkFuture<>(acceptedFuture);
	}

	@Override
	public Future<Void> thenAccept(AcceptFunction<? super T> acceptFunction) {
		return thenAcceptAsync(acceptFunction, Executors.directExecutor());
	}

	@Override
	public <R> Future<R> exceptionallyAsync(final ApplyFunction<Throwable, ? extends R> exceptionallyFunction, Executor executor) {
		Preconditions.checkNotNull(scalaFuture);
		Preconditions.checkNotNull(exceptionallyFunction);
		Preconditions.checkNotNull(executor);

		scala.concurrent.Future<R> recoveredFuture = scalaFuture.recover(new Recover<R>() {
			@Override
			public R recover(Throwable failure) throws Throwable {
				return exceptionallyFunction.apply(failure);
			}
		}, createExecutionContext(executor));

		return new FlinkFuture<>(recoveredFuture);
	}

	@Override
	public <R> Future<R> exceptionally(ApplyFunction<Throwable, ? extends R> exceptionallyFunction) {
		return exceptionallyAsync(exceptionallyFunction, Executors.directExecutor());
	}

	@Override
	public <R> Future<R> thenComposeAsync(final ApplyFunction<? super T, ? extends Future<R>> applyFunction, Executor executor) {
		Preconditions.checkNotNull(scalaFuture);
		Preconditions.checkNotNull(applyFunction);
		Preconditions.checkNotNull(executor);

		final ExecutionContext executionContext = createExecutionContext(executor);

		scala.concurrent.Future<R> flatMappedFuture = scalaFuture.flatMap(new Mapper<T, scala.concurrent.Future<R>>() {
			@Override
			public scala.concurrent.Future<R> apply(T value) {
				final Future<? extends R> future = applyFunction.apply(value);

				if (future instanceof FlinkFuture) {
					@SuppressWarnings("unchecked")
					FlinkFuture<R> flinkFuture = (FlinkFuture<R>) future;

					return flinkFuture.scalaFuture;
				} else {
					return Futures.future(new Callable<R>() {
						@Override
						public R call() throws Exception {
							try {
								return future.get();
							} catch (ExecutionException e) {
								// unwrap the execution exception if it's not a throwable
								if (e.getCause() instanceof Exception) {
									throw (Exception) e.getCause();
								} else {
									throw new FlinkFuture.ThrowableWrapperException(e.getCause());
								}
							}
						}
					}, executionContext);
				}
			}
		}, executionContext);

		return new FlinkFuture<>(flatMappedFuture);
	}

	@Override
	public <R> Future<R> thenCompose(ApplyFunction<? super T, ? extends Future<R>> composeFunction) {
		return thenComposeAsync(composeFunction, Executors.directExecutor());
	}

	@Override
	public <R> Future<R> handleAsync(final BiFunction<? super T, Throwable, ? extends R> biFunction, Executor executor) {
		Preconditions.checkNotNull(scalaFuture);
		Preconditions.checkNotNull(biFunction);
		Preconditions.checkNotNull(executor);

		final ExecutionContext executionContext = createExecutionContext(executor);

		final CompletableFuture<R> resultFuture = new FlinkCompletableFuture<>();

		scalaFuture.onComplete(new OnComplete<T>() {
			@Override
			public void onComplete(Throwable failure, T success) throws Throwable {
				final R result = biFunction.apply(success, failure);

				resultFuture.complete(result);
			}
		}, executionContext);

		return resultFuture;
	}

	@Override
	public <R> Future<R> handle(BiFunction<? super T, Throwable, ? extends R> biFunction) {
		return handleAsync(biFunction, Executors.directExecutor());
	}

	@Override
	public <U, R> Future<R> thenCombineAsync(final Future<U> other, final BiFunction<? super T, ? super U, ? extends R> biFunction, final Executor executor) {
		Preconditions.checkNotNull(other);
		Preconditions.checkNotNull(biFunction);
		Preconditions.checkNotNull(executor);

		final ExecutionContext executionContext = createExecutionContext(executor);

		final scala.concurrent.Future<U> thatScalaFuture;

		if (other instanceof FlinkFuture) {
			thatScalaFuture = ((FlinkFuture<U>) other).scalaFuture;
		} else {
			thatScalaFuture = Futures.future(new Callable<U>() {
				@Override
				public U call() throws Exception {
					try {
						return other.get();
					} catch (ExecutionException e) {
						// unwrap the execution exception if the cause is an Exception
						if (e.getCause() instanceof Exception) {
							throw (Exception) e.getCause();
						} else {
							// it's an error or a throwable which we have to wrap for the moment
							throw new FlinkFuture.ThrowableWrapperException(e.getCause());
						}
					}
				}
			}, executionContext);
		}

		scala.concurrent.Future<R>  result = scalaFuture.zip(thatScalaFuture).map(new Mapper<Tuple2<T, U>, R>() {
			@Override
			public R apply(Tuple2<T, U> tuple2) {
				return biFunction.apply(tuple2._1, tuple2._2);
			}
		}, executionContext);

		return new FlinkFuture<>(result);
	}

	@Override
	public <U, R> Future<R> thenCombine(Future<U> other, BiFunction<? super T, ? super U, ? extends R> biFunction) {
		return thenCombineAsync(other, biFunction, Executors.directExecutor());
	}

	//-----------------------------------------------------------------------------------
	// Static factory methods
	//-----------------------------------------------------------------------------------

	/**
	 * Creates a future whose value is determined by the asynchronously executed callable.
	 *
	 * @param callable whose value is delivered by the future
	 * @param executor to be used to execute the callable
	 * @param <T> type of the future's value
	 * @return future which represents the value of the callable
	 */
	public static <T> Future<T> supplyAsync(Callable<T> callable, Executor executor) {
		Preconditions.checkNotNull(callable);
		Preconditions.checkNotNull(executor);

		scala.concurrent.Future<T> scalaFuture = Futures.future(callable, createExecutionContext(executor));

		return new FlinkFuture<>(scalaFuture);
	}

	//-----------------------------------------------------------------------------------
	// Helper functions and types
	//-----------------------------------------------------------------------------------

	private static ExecutionContext createExecutionContext(final Executor executor) {
		if (executor == DIRECT_EXECUTOR) {
			return DIRECT_EXECUTION_CONTEXT;
		} else {
			return executionContextFromExecutor(executor);
		}
	}

	private static ExecutionContext executionContextFromExecutor(final Executor executor) {
		return ExecutionContexts$.MODULE$.fromExecutor(executor, new Procedure<Throwable>() {
			@Override
			public void apply(Throwable throwable) throws Exception {
				if (executor instanceof ExecutorService) {
					ExecutorService executorService = (ExecutorService) executor;
					// only log the exception if the executor service is still running
					if (!executorService.isShutdown()) {
						logThrowable(throwable);
					}
				} else {
					logThrowable(throwable);
				}
			}

			private void logThrowable(Throwable throwable) {
				LOG.warn("Uncaught exception in execution context.", throwable);
			}
		});
	}

	/**
	 * Wrapper for {@link Throwable} which is used to emit the proper exception when calling
	 * {@link Future#get}.
	 */
	private static class ThrowableWrapperException extends Exception {

		private static final long serialVersionUID = 3855668690181179801L;

		ThrowableWrapperException(Throwable throwable) {
			super(Preconditions.checkNotNull(throwable));
		}
	}
}
