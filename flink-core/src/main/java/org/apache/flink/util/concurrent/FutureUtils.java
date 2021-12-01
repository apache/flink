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

package org.apache.flink.util.concurrent;

import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FatalExitExceptionHandler;
import org.apache.flink.util.function.RunnableWithException;
import org.apache.flink.util.function.SupplierWithException;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.apache.flink.util.Preconditions.checkCompletedNormally;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** A collection of utilities that expand the usage of {@link CompletableFuture}. */
public class FutureUtils {

    private static final CompletableFuture<Void> COMPLETED_VOID_FUTURE =
            CompletableFuture.completedFuture(null);

    /**
     * Returns a completed future of type {@link Void}.
     *
     * @return a completed future of type {@link Void}
     */
    public static CompletableFuture<Void> completedVoidFuture() {
        return COMPLETED_VOID_FUTURE;
    }

    private static final CompletableFuture<?> UNSUPPORTED_OPERATION_FUTURE =
            completedExceptionally(
                    new UnsupportedOperationException("This method is unsupported."));

    /**
     * Returns an exceptionally completed future with an {@link UnsupportedOperationException}.
     *
     * @param <T> type of the future
     * @return exceptionally completed future
     */
    public static <T> CompletableFuture<T> unsupportedOperationFuture() {
        return (CompletableFuture<T>) UNSUPPORTED_OPERATION_FUTURE;
    }

    /**
     * Fakes asynchronous execution by immediately executing the operation and completing the
     * supplied future either normally or exceptionally.
     *
     * @param operation to executed
     * @param <T> type of the result
     */
    public static <T> void completeFromCallable(
            CompletableFuture<T> future, Callable<T> operation) {
        try {
            future.complete(operation.call());
        } catch (Exception e) {
            future.completeExceptionally(e);
        }
    }

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
    public static <T> CompletableFuture<T> retry(
            final Supplier<CompletableFuture<T>> operation,
            final int retries,
            final Executor executor) {

        return retry(operation, retries, ignore -> true, executor);
    }

    /**
     * Retry the given operation the given number of times in case of a failure only when an
     * exception is retryable.
     *
     * @param operation to executed
     * @param retries if the operation failed
     * @param retryPredicate Predicate to test whether an exception is retryable
     * @param executor to use to run the futures
     * @param <T> type of the result
     * @return Future containing either the result of the operation or a {@link RetryException}
     */
    public static <T> CompletableFuture<T> retry(
            final Supplier<CompletableFuture<T>> operation,
            final int retries,
            final Predicate<Throwable> retryPredicate,
            final Executor executor) {

        final CompletableFuture<T> resultFuture = new CompletableFuture<>();

        retryOperation(resultFuture, operation, retries, retryPredicate, executor);

        return resultFuture;
    }

    /**
     * Helper method which retries the provided operation in case of a failure.
     *
     * @param resultFuture to complete
     * @param operation to retry
     * @param retries until giving up
     * @param retryPredicate Predicate to test whether an exception is retryable
     * @param executor to run the futures
     * @param <T> type of the future's result
     */
    private static <T> void retryOperation(
            final CompletableFuture<T> resultFuture,
            final Supplier<CompletableFuture<T>> operation,
            final int retries,
            final Predicate<Throwable> retryPredicate,
            final Executor executor) {

        if (!resultFuture.isDone()) {
            final CompletableFuture<T> operationFuture = operation.get();

            operationFuture.whenCompleteAsync(
                    (t, throwable) -> {
                        if (throwable != null) {
                            if (throwable instanceof CancellationException) {
                                resultFuture.completeExceptionally(
                                        new RetryException(
                                                "Operation future was cancelled.", throwable));
                            } else {
                                throwable = ExceptionUtils.stripExecutionException(throwable);
                                if (!retryPredicate.test(throwable)) {
                                    resultFuture.completeExceptionally(
                                            new RetryException(
                                                    "Stopped retrying the operation because the error is not "
                                                            + "retryable.",
                                                    throwable));
                                } else {
                                    if (retries > 0) {
                                        retryOperation(
                                                resultFuture,
                                                operation,
                                                retries - 1,
                                                retryPredicate,
                                                executor);
                                    } else {
                                        resultFuture.completeExceptionally(
                                                new RetryException(
                                                        "Could not complete the operation. Number of retries "
                                                                + "has been exhausted.",
                                                        throwable));
                                    }
                                }
                            }
                        } else {
                            resultFuture.complete(t);
                        }
                    },
                    executor);

            resultFuture.whenComplete((t, throwable) -> operationFuture.cancel(false));
        }
    }

    /**
     * Retry the given operation with the given delay in between failures.
     *
     * @param operation to retry
     * @param retries number of retries
     * @param retryDelay delay between retries
     * @param retryPredicate Predicate to test whether an exception is retryable
     * @param scheduledExecutor executor to be used for the retry operation
     * @param <T> type of the result
     * @return Future which retries the given operation a given amount of times and delays the retry
     *     in case of failures
     */
    public static <T> CompletableFuture<T> retryWithDelay(
            final Supplier<CompletableFuture<T>> operation,
            final int retries,
            final Time retryDelay,
            final Predicate<Throwable> retryPredicate,
            final ScheduledExecutor scheduledExecutor) {
        return retryWithDelay(
                operation,
                new FixedRetryStrategy(retries, Duration.ofMillis(retryDelay.toMilliseconds())),
                retryPredicate,
                scheduledExecutor);
    }

    /**
     * Retry the given operation with the given delay in between failures.
     *
     * @param operation to retry
     * @param retryStrategy the RetryStrategy
     * @param retryPredicate Predicate to test whether an exception is retryable
     * @param scheduledExecutor executor to be used for the retry operation
     * @param <T> type of the result
     * @return Future which retries the given operation a given amount of times and delays the retry
     *     in case of failures
     */
    public static <T> CompletableFuture<T> retryWithDelay(
            final Supplier<CompletableFuture<T>> operation,
            final RetryStrategy retryStrategy,
            final Predicate<Throwable> retryPredicate,
            final ScheduledExecutor scheduledExecutor) {

        final CompletableFuture<T> resultFuture = new CompletableFuture<>();

        retryOperationWithDelay(
                resultFuture, operation, retryStrategy, retryPredicate, scheduledExecutor);

        return resultFuture;
    }

    /**
     * Retry the given operation with the given delay in between failures.
     *
     * @param operation to retry
     * @param retries number of retries
     * @param retryDelay delay between retries
     * @param scheduledExecutor executor to be used for the retry operation
     * @param <T> type of the result
     * @return Future which retries the given operation a given amount of times and delays the retry
     *     in case of failures
     */
    public static <T> CompletableFuture<T> retryWithDelay(
            final Supplier<CompletableFuture<T>> operation,
            final int retries,
            final Time retryDelay,
            final ScheduledExecutor scheduledExecutor) {
        return retryWithDelay(
                operation,
                new FixedRetryStrategy(retries, Duration.ofMillis(retryDelay.toMilliseconds())),
                scheduledExecutor);
    }

    /**
     * Retry the given operation with the given delay in between failures.
     *
     * @param operation to retry
     * @param retryStrategy the RetryStrategy
     * @param scheduledExecutor executor to be used for the retry operation
     * @param <T> type of the result
     * @return Future which retries the given operation a given amount of times and delays the retry
     *     in case of failures
     */
    public static <T> CompletableFuture<T> retryWithDelay(
            final Supplier<CompletableFuture<T>> operation,
            final RetryStrategy retryStrategy,
            final ScheduledExecutor scheduledExecutor) {
        return retryWithDelay(operation, retryStrategy, (throwable) -> true, scheduledExecutor);
    }

    /**
     * Schedule the operation with the given delay.
     *
     * @param operation to schedule
     * @param delay delay to schedule
     * @param scheduledExecutor executor to be used for the operation
     * @return Future which schedules the given operation with given delay.
     */
    public static CompletableFuture<Void> scheduleWithDelay(
            final Runnable operation, final Time delay, final ScheduledExecutor scheduledExecutor) {
        Supplier<Void> operationSupplier =
                () -> {
                    operation.run();
                    return null;
                };
        return scheduleWithDelay(operationSupplier, delay, scheduledExecutor);
    }

    /**
     * Schedule the operation with the given delay.
     *
     * @param operation to schedule
     * @param delay delay to schedule
     * @param scheduledExecutor executor to be used for the operation
     * @param <T> type of the result
     * @return Future which schedules the given operation with given delay.
     */
    public static <T> CompletableFuture<T> scheduleWithDelay(
            final Supplier<T> operation,
            final Time delay,
            final ScheduledExecutor scheduledExecutor) {
        final CompletableFuture<T> resultFuture = new CompletableFuture<>();

        ScheduledFuture<?> scheduledFuture =
                scheduledExecutor.schedule(
                        () -> {
                            try {
                                resultFuture.complete(operation.get());
                            } catch (Throwable t) {
                                resultFuture.completeExceptionally(t);
                            }
                        },
                        delay.getSize(),
                        delay.getUnit());

        resultFuture.whenComplete(
                (t, throwable) -> {
                    if (!scheduledFuture.isDone()) {
                        scheduledFuture.cancel(false);
                    }
                });
        return resultFuture;
    }

    private static <T> void retryOperationWithDelay(
            final CompletableFuture<T> resultFuture,
            final Supplier<CompletableFuture<T>> operation,
            final RetryStrategy retryStrategy,
            final Predicate<Throwable> retryPredicate,
            final ScheduledExecutor scheduledExecutor) {

        if (!resultFuture.isDone()) {
            final CompletableFuture<T> operationResultFuture = operation.get();

            operationResultFuture.whenComplete(
                    (t, throwable) -> {
                        if (throwable != null) {
                            if (throwable instanceof CancellationException) {
                                resultFuture.completeExceptionally(
                                        new RetryException(
                                                "Operation future was cancelled.", throwable));
                            } else {
                                throwable = ExceptionUtils.stripExecutionException(throwable);
                                if (!retryPredicate.test(throwable)) {
                                    resultFuture.completeExceptionally(throwable);
                                } else if (retryStrategy.getNumRemainingRetries() > 0) {
                                    long retryDelayMillis =
                                            retryStrategy.getRetryDelay().toMillis();
                                    final ScheduledFuture<?> scheduledFuture =
                                            scheduledExecutor.schedule(
                                                    (Runnable)
                                                            () ->
                                                                    retryOperationWithDelay(
                                                                            resultFuture,
                                                                            operation,
                                                                            retryStrategy
                                                                                    .getNextRetryStrategy(),
                                                                            retryPredicate,
                                                                            scheduledExecutor),
                                                    retryDelayMillis,
                                                    TimeUnit.MILLISECONDS);

                                    resultFuture.whenComplete(
                                            (innerT, innerThrowable) ->
                                                    scheduledFuture.cancel(false));
                                } else {
                                    RetryException retryException =
                                            new RetryException(
                                                    "Could not complete the operation. Number of retries has been exhausted.",
                                                    throwable);
                                    resultFuture.completeExceptionally(retryException);
                                }
                            }
                        } else {
                            resultFuture.complete(t);
                        }
                    });

            resultFuture.whenComplete((t, throwable) -> operationResultFuture.cancel(false));
        }
    }

    /**
     * Retry the given operation with the given delay in between successful completions where the
     * result does not match a given predicate.
     *
     * @param operation to retry
     * @param retryDelay delay between retries
     * @param deadline A deadline that specifies at what point we should stop retrying
     * @param acceptancePredicate Predicate to test whether the result is acceptable
     * @param scheduledExecutor executor to be used for the retry operation
     * @param <T> type of the result
     * @return Future which retries the given operation a given amount of times and delays the retry
     *     in case the predicate isn't matched
     */
    public static <T> CompletableFuture<T> retrySuccessfulWithDelay(
            final Supplier<CompletableFuture<T>> operation,
            final Time retryDelay,
            final Deadline deadline,
            final Predicate<T> acceptancePredicate,
            final ScheduledExecutor scheduledExecutor) {

        final CompletableFuture<T> resultFuture = new CompletableFuture<>();

        retrySuccessfulOperationWithDelay(
                resultFuture,
                operation,
                retryDelay,
                deadline,
                acceptancePredicate,
                scheduledExecutor);

        return resultFuture;
    }

    private static <T> void retrySuccessfulOperationWithDelay(
            final CompletableFuture<T> resultFuture,
            final Supplier<CompletableFuture<T>> operation,
            final Time retryDelay,
            final Deadline deadline,
            final Predicate<T> acceptancePredicate,
            final ScheduledExecutor scheduledExecutor) {

        if (!resultFuture.isDone()) {
            final CompletableFuture<T> operationResultFuture = operation.get();

            operationResultFuture.whenComplete(
                    (t, throwable) -> {
                        if (throwable != null) {
                            if (throwable instanceof CancellationException) {
                                resultFuture.completeExceptionally(
                                        new RetryException(
                                                "Operation future was cancelled.", throwable));
                            } else {
                                resultFuture.completeExceptionally(throwable);
                            }
                        } else {
                            if (acceptancePredicate.test(t)) {
                                resultFuture.complete(t);
                            } else if (deadline.hasTimeLeft()) {
                                final ScheduledFuture<?> scheduledFuture =
                                        scheduledExecutor.schedule(
                                                (Runnable)
                                                        () ->
                                                                retrySuccessfulOperationWithDelay(
                                                                        resultFuture,
                                                                        operation,
                                                                        retryDelay,
                                                                        deadline,
                                                                        acceptancePredicate,
                                                                        scheduledExecutor),
                                                retryDelay.toMilliseconds(),
                                                TimeUnit.MILLISECONDS);

                                resultFuture.whenComplete(
                                        (innerT, innerThrowable) -> scheduledFuture.cancel(false));
                            } else {
                                resultFuture.completeExceptionally(
                                        new RetryException(
                                                "Could not satisfy the predicate within the allowed time."));
                            }
                        }
                    });

            resultFuture.whenComplete((t, throwable) -> operationResultFuture.cancel(false));
        }
    }

    /**
     * Exception with which the returned future is completed if the {@link #retry(Supplier, int,
     * Executor)} operation fails.
     */
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

    /**
     * Times the given future out after the timeout.
     *
     * @param future to time out
     * @param timeout after which the given future is timed out
     * @param timeUnit time unit of the timeout
     * @param <T> type of the given future
     * @return The timeout enriched future
     */
    public static <T> CompletableFuture<T> orTimeout(
            CompletableFuture<T> future, long timeout, TimeUnit timeUnit) {
        return orTimeout(future, timeout, timeUnit, Executors.directExecutor(), null);
    }

    /**
     * Times the given future out after the timeout.
     *
     * @param future to time out
     * @param timeout after which the given future is timed out
     * @param timeUnit time unit of the timeout
     * @param timeoutMsg timeout message for exception
     * @param <T> type of the given future
     * @return The timeout enriched future
     */
    public static <T> CompletableFuture<T> orTimeout(
            CompletableFuture<T> future,
            long timeout,
            TimeUnit timeUnit,
            @Nullable String timeoutMsg) {
        return orTimeout(future, timeout, timeUnit, Executors.directExecutor(), timeoutMsg);
    }

    /**
     * Times the given future out after the timeout.
     *
     * @param future to time out
     * @param timeout after which the given future is timed out
     * @param timeUnit time unit of the timeout
     * @param timeoutFailExecutor executor that will complete the future exceptionally after the
     *     timeout is reached
     * @param <T> type of the given future
     * @return The timeout enriched future
     */
    public static <T> CompletableFuture<T> orTimeout(
            CompletableFuture<T> future,
            long timeout,
            TimeUnit timeUnit,
            Executor timeoutFailExecutor) {
        return orTimeout(future, timeout, timeUnit, timeoutFailExecutor, null);
    }

    /**
     * Times the given future out after the timeout.
     *
     * @param future to time out
     * @param timeout after which the given future is timed out
     * @param timeUnit time unit of the timeout
     * @param timeoutFailExecutor executor that will complete the future exceptionally after the
     *     timeout is reached
     * @param timeoutMsg timeout message for exception
     * @param <T> type of the given future
     * @return The timeout enriched future
     */
    public static <T> CompletableFuture<T> orTimeout(
            CompletableFuture<T> future,
            long timeout,
            TimeUnit timeUnit,
            Executor timeoutFailExecutor,
            @Nullable String timeoutMsg) {

        if (!future.isDone()) {
            final ScheduledFuture<?> timeoutFuture =
                    Delayer.delay(
                            () -> timeoutFailExecutor.execute(new Timeout(future, timeoutMsg)),
                            timeout,
                            timeUnit);

            future.whenComplete(
                    (T value, Throwable throwable) -> {
                        if (!timeoutFuture.isDone()) {
                            timeoutFuture.cancel(false);
                        }
                    });
        }

        return future;
    }

    // ------------------------------------------------------------------------
    //  Delayed completion
    // ------------------------------------------------------------------------

    /**
     * Asynchronously completes the future after a certain delay.
     *
     * @param future The future to complete.
     * @param success The element to complete the future with.
     * @param delay The delay after which the future should be completed.
     */
    public static <T> void completeDelayed(CompletableFuture<T> future, T success, Duration delay) {
        Delayer.delay(() -> future.complete(success), delay.toMillis(), TimeUnit.MILLISECONDS);
    }

    // ------------------------------------------------------------------------
    //  Future actions
    // ------------------------------------------------------------------------

    /**
     * Run the given {@code RunnableFuture} if it is not done, and then retrieves its result.
     *
     * @param future to run if not done and get
     * @param <T> type of the result
     * @return the result after running the future
     * @throws ExecutionException if a problem occurred
     * @throws InterruptedException if the current thread has been interrupted
     */
    public static <T> T runIfNotDoneAndGet(RunnableFuture<T> future)
            throws ExecutionException, InterruptedException {

        if (null == future) {
            return null;
        }

        if (!future.isDone()) {
            future.run();
        }

        return future.get();
    }

    /**
     * Run the given action after the completion of the given future. The given future can be
     * completed normally or exceptionally. In case of an exceptional completion the, the action's
     * exception will be added to the initial exception.
     *
     * @param future to wait for its completion
     * @param runnable action which is triggered after the future's completion
     * @return Future which is completed after the action has completed. This future can contain an
     *     exception, if an error occurred in the given future or action.
     */
    public static CompletableFuture<Void> runAfterwards(
            CompletableFuture<?> future, RunnableWithException runnable) {
        return runAfterwardsAsync(future, runnable, Executors.directExecutor());
    }

    /**
     * Run the given action after the completion of the given future. The given future can be
     * completed normally or exceptionally. In case of an exceptional completion the, the action's
     * exception will be added to the initial exception.
     *
     * @param future to wait for its completion
     * @param runnable action which is triggered after the future's completion
     * @return Future which is completed after the action has completed. This future can contain an
     *     exception, if an error occurred in the given future or action.
     */
    public static CompletableFuture<Void> runAfterwardsAsync(
            CompletableFuture<?> future, RunnableWithException runnable) {
        return runAfterwardsAsync(future, runnable, ForkJoinPool.commonPool());
    }

    /**
     * Run the given action after the completion of the given future. The given future can be
     * completed normally or exceptionally. In case of an exceptional completion the action's
     * exception will be added to the initial exception.
     *
     * @param future to wait for its completion
     * @param runnable action which is triggered after the future's completion
     * @param executor to run the given action
     * @return Future which is completed after the action has completed. This future can contain an
     *     exception, if an error occurred in the given future or action.
     */
    public static CompletableFuture<Void> runAfterwardsAsync(
            CompletableFuture<?> future, RunnableWithException runnable, Executor executor) {
        final CompletableFuture<Void> resultFuture = new CompletableFuture<>();

        future.whenCompleteAsync(
                (Object ignored, Throwable throwable) -> {
                    try {
                        runnable.run();
                    } catch (Throwable e) {
                        throwable = ExceptionUtils.firstOrSuppressed(e, throwable);
                    }

                    if (throwable != null) {
                        resultFuture.completeExceptionally(throwable);
                    } else {
                        resultFuture.complete(null);
                    }
                },
                executor);

        return resultFuture;
    }

    /**
     * Run the given asynchronous action after the completion of the given future. The given future
     * can be completed normally or exceptionally. In case of an exceptional completion, the
     * asynchronous action's exception will be added to the initial exception.
     *
     * @param future to wait for its completion
     * @param composedAction asynchronous action which is triggered after the future's completion
     * @return Future which is completed after the asynchronous action has completed. This future
     *     can contain an exception if an error occurred in the given future or asynchronous action.
     */
    public static CompletableFuture<Void> composeAfterwards(
            CompletableFuture<?> future, Supplier<CompletableFuture<?>> composedAction) {
        final CompletableFuture<Void> resultFuture = new CompletableFuture<>();

        future.whenComplete(
                (Object outerIgnored, Throwable outerThrowable) -> {
                    final CompletableFuture<?> composedActionFuture = composedAction.get();

                    composedActionFuture.whenComplete(
                            (Object innerIgnored, Throwable innerThrowable) -> {
                                if (innerThrowable != null) {
                                    resultFuture.completeExceptionally(
                                            ExceptionUtils.firstOrSuppressed(
                                                    innerThrowable, outerThrowable));
                                } else if (outerThrowable != null) {
                                    resultFuture.completeExceptionally(outerThrowable);
                                } else {
                                    resultFuture.complete(null);
                                }
                            });
                });

        return resultFuture;
    }

    // ------------------------------------------------------------------------
    //  composing futures
    // ------------------------------------------------------------------------

    /**
     * Creates a future that is complete once multiple other futures completed. The future fails
     * (completes exceptionally) once one of the futures in the conjunction fails. Upon successful
     * completion, the future returns the collection of the futures' results.
     *
     * <p>The ConjunctFuture gives access to how many Futures in the conjunction have already
     * completed successfully, via {@link ConjunctFuture#getNumFuturesCompleted()}.
     *
     * @param futures The futures that make up the conjunction. No null entries are allowed.
     * @return The ConjunctFuture that completes once all given futures are complete (or one fails).
     */
    public static <T> ConjunctFuture<Collection<T>> combineAll(
            Collection<? extends CompletableFuture<? extends T>> futures) {
        checkNotNull(futures, "futures");

        return new ResultConjunctFuture<>(futures);
    }

    /**
     * Creates a future that is complete once all of the given futures have completed. The future
     * fails (completes exceptionally) once one of the given futures fails.
     *
     * <p>The ConjunctFuture gives access to how many Futures have already completed successfully,
     * via {@link ConjunctFuture#getNumFuturesCompleted()}.
     *
     * @param futures The futures to wait on. No null entries are allowed.
     * @return The WaitingFuture that completes once all given futures are complete (or one fails).
     */
    public static ConjunctFuture<Void> waitForAll(
            Collection<? extends CompletableFuture<?>> futures) {
        checkNotNull(futures, "futures");

        return new WaitingConjunctFuture(futures);
    }

    /**
     * A future that is complete once multiple other futures completed. The futures are not
     * necessarily of the same type. The ConjunctFuture fails (completes exceptionally) once one of
     * the Futures in the conjunction fails.
     *
     * <p>The advantage of using the ConjunctFuture over chaining all the futures (such as via
     * {@link CompletableFuture#thenCombine(CompletionStage, BiFunction)} )}) is that ConjunctFuture
     * also tracks how many of the Futures are already complete.
     */
    public abstract static class ConjunctFuture<T> extends CompletableFuture<T> {

        /**
         * Gets the total number of Futures in the conjunction.
         *
         * @return The total number of Futures in the conjunction.
         */
        public abstract int getNumFuturesTotal();

        /**
         * Gets the number of Futures in the conjunction that are already complete.
         *
         * @return The number of Futures in the conjunction that are already complete
         */
        public abstract int getNumFuturesCompleted();
    }

    /**
     * The implementation of the {@link ConjunctFuture} which returns its Futures' result as a
     * collection.
     */
    private static class ResultConjunctFuture<T> extends ConjunctFuture<Collection<T>> {

        /** The total number of futures in the conjunction. */
        private final int numTotal;

        /** The number of futures in the conjunction that are already complete. */
        private final AtomicInteger numCompleted = new AtomicInteger(0);

        /** The set of collected results so far. */
        private final T[] results;

        /**
         * The function that is attached to all futures in the conjunction. Once a future is
         * complete, this function tracks the completion or fails the conjunct.
         */
        private void handleCompletedFuture(int index, T value, Throwable throwable) {
            if (throwable != null) {
                completeExceptionally(throwable);
            } else {
                /**
                 * This {@link #results} update itself is not synchronised in any way and it's fine
                 * because:
                 *
                 * <ul>
                 *   <li>There is a happens-before relationship for each thread (that is completing
                 *       the future) between setting {@link #results} and incrementing {@link
                 *       #numCompleted}.
                 *   <li>Each thread is updating uniquely different field of the {@link #results}
                 *       array.
                 *   <li>There is a happens-before relationship between all of the writing threads
                 *       and the last one thread (thanks to the {@code
                 *       numCompleted.incrementAndGet() == numTotal} check.
                 *   <li>The last thread will be completing the future, so it has transitively
                 *       happens-before relationship with all of preceding updated/writes to {@link
                 *       #results}.
                 *   <li>{@link AtomicInteger#incrementAndGet} is an equivalent of both volatile
                 *       read & write
                 * </ul>
                 */
                results[index] = value;

                if (numCompleted.incrementAndGet() == numTotal) {
                    complete(Arrays.asList(results));
                }
            }
        }

        @SuppressWarnings("unchecked")
        ResultConjunctFuture(Collection<? extends CompletableFuture<? extends T>> resultFutures) {
            this.numTotal = resultFutures.size();
            results = (T[]) new Object[numTotal];

            if (resultFutures.isEmpty()) {
                complete(Collections.emptyList());
            } else {
                int counter = 0;
                for (CompletableFuture<? extends T> future : resultFutures) {
                    final int index = counter;
                    counter++;
                    future.whenComplete(
                            (value, throwable) -> handleCompletedFuture(index, value, throwable));
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

    /**
     * Implementation of the {@link ConjunctFuture} interface which waits only for the completion of
     * its futures and does not return their values.
     */
    private static final class WaitingConjunctFuture extends ConjunctFuture<Void> {

        /** Number of completed futures. */
        private final AtomicInteger numCompleted = new AtomicInteger(0);

        /** Total number of futures to wait on. */
        private final int numTotal;

        /**
         * Method which increments the atomic completion counter and completes or fails the
         * WaitingFutureImpl.
         */
        private void handleCompletedFuture(Object ignored, Throwable throwable) {
            if (throwable == null) {
                if (numTotal == numCompleted.incrementAndGet()) {
                    complete(null);
                }
            } else {
                completeExceptionally(throwable);
            }
        }

        private WaitingConjunctFuture(Collection<? extends CompletableFuture<?>> futures) {
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

    /**
     * Creates a {@link ConjunctFuture} which is only completed after all given futures have
     * completed. Unlike {@link FutureUtils#waitForAll(Collection)}, the resulting future won't be
     * completed directly if one of the given futures is completed exceptionally. Instead, all
     * occurring exception will be collected and combined to a single exception. If at least on
     * exception occurs, then the resulting future will be completed exceptionally.
     *
     * @param futuresToComplete futures to complete
     * @return Future which is completed after all given futures have been completed.
     */
    public static ConjunctFuture<Void> completeAll(
            Collection<? extends CompletableFuture<?>> futuresToComplete) {
        return new CompletionConjunctFuture(futuresToComplete);
    }

    /**
     * {@link ConjunctFuture} implementation which is completed after all the given futures have
     * been completed. Exceptional completions of the input futures will be recorded but it won't
     * trigger the early completion of this future.
     */
    private static final class CompletionConjunctFuture extends ConjunctFuture<Void> {

        private final Object lock = new Object();

        private final int numFuturesTotal;

        private int futuresCompleted;

        private Throwable globalThrowable;

        private CompletionConjunctFuture(
                Collection<? extends CompletableFuture<?>> futuresToComplete) {
            numFuturesTotal = futuresToComplete.size();

            futuresCompleted = 0;

            globalThrowable = null;

            if (futuresToComplete.isEmpty()) {
                complete(null);
            } else {
                for (CompletableFuture<?> completableFuture : futuresToComplete) {
                    completableFuture.whenComplete(this::completeFuture);
                }
            }
        }

        private void completeFuture(Object ignored, Throwable throwable) {
            synchronized (lock) {
                futuresCompleted++;

                if (throwable != null) {
                    globalThrowable = ExceptionUtils.firstOrSuppressed(throwable, globalThrowable);
                }

                if (futuresCompleted == numFuturesTotal) {
                    if (globalThrowable != null) {
                        completeExceptionally(globalThrowable);
                    } else {
                        complete(null);
                    }
                }
            }
        }

        @Override
        public int getNumFuturesTotal() {
            return numFuturesTotal;
        }

        @Override
        public int getNumFuturesCompleted() {
            synchronized (lock) {
                return futuresCompleted;
            }
        }
    }

    // ------------------------------------------------------------------------
    //  Helper methods
    // ------------------------------------------------------------------------

    /**
     * Returns an exceptionally completed {@link CompletableFuture}.
     *
     * @param cause to complete the future with
     * @param <T> type of the future
     * @return An exceptionally completed CompletableFuture
     */
    public static <T> CompletableFuture<T> completedExceptionally(Throwable cause) {
        CompletableFuture<T> result = new CompletableFuture<>();
        result.completeExceptionally(cause);

        return result;
    }

    /**
     * Returns a future which is completed with the result of the {@link SupplierWithException}.
     *
     * @param supplier to provide the future's value
     * @param executor to execute the supplier
     * @param <T> type of the result
     * @return Future which is completed with the value of the supplier
     */
    public static <T> CompletableFuture<T> supplyAsync(
            SupplierWithException<T, ?> supplier, Executor executor) {
        return CompletableFuture.supplyAsync(
                () -> {
                    try {
                        return supplier.get();
                    } catch (Throwable e) {
                        throw new CompletionException(e);
                    }
                },
                executor);
    }

    /**
     * Converts Flink time into a {@link Duration}.
     *
     * @param time to convert into a Duration
     * @return Duration with the length of the given time
     */
    public static Duration toDuration(Time time) {
        return Duration.ofMillis(time.toMilliseconds());
    }

    // ------------------------------------------------------------------------
    //  Converting futures
    // ------------------------------------------------------------------------

    /**
     * This function takes a {@link CompletableFuture} and a function to apply to this future. If
     * the input future is already done, this function returns {@link
     * CompletableFuture#thenApply(Function)}. Otherwise, the return value is {@link
     * CompletableFuture#thenApplyAsync(Function, Executor)} with the given executor.
     *
     * @param completableFuture the completable future for which we want to apply.
     * @param executor the executor to run the apply function if the future is not yet done.
     * @param applyFun the function to apply.
     * @param <IN> type of the input future.
     * @param <OUT> type of the output future.
     * @return a completable future that is applying the given function to the input future.
     */
    public static <IN, OUT> CompletableFuture<OUT> thenApplyAsyncIfNotDone(
            CompletableFuture<IN> completableFuture,
            Executor executor,
            Function<? super IN, ? extends OUT> applyFun) {
        return completableFuture.isDone()
                ? completableFuture.thenApply(applyFun)
                : completableFuture.thenApplyAsync(applyFun, executor);
    }

    /**
     * This function takes a {@link CompletableFuture} and a function to compose with this future.
     * If the input future is already done, this function returns {@link
     * CompletableFuture#thenCompose(Function)}. Otherwise, the return value is {@link
     * CompletableFuture#thenComposeAsync(Function, Executor)} with the given executor.
     *
     * @param completableFuture the completable future for which we want to compose.
     * @param executor the executor to run the compose function if the future is not yet done.
     * @param composeFun the function to compose.
     * @param <IN> type of the input future.
     * @param <OUT> type of the output future.
     * @return a completable future that is a composition of the input future and the function.
     */
    public static <IN, OUT> CompletableFuture<OUT> thenComposeAsyncIfNotDone(
            CompletableFuture<IN> completableFuture,
            Executor executor,
            Function<? super IN, ? extends CompletionStage<OUT>> composeFun) {
        return completableFuture.isDone()
                ? completableFuture.thenCompose(composeFun)
                : completableFuture.thenComposeAsync(composeFun, executor);
    }

    /**
     * This function takes a {@link CompletableFuture} and a bi-consumer to call on completion of
     * this future. If the input future is already done, this function returns {@link
     * CompletableFuture#whenComplete(BiConsumer)}. Otherwise, the return value is {@link
     * CompletableFuture#whenCompleteAsync(BiConsumer, Executor)} with the given executor.
     *
     * @param completableFuture the completable future for which we want to call #whenComplete.
     * @param executor the executor to run the whenComplete function if the future is not yet done.
     * @param whenCompleteFun the bi-consumer function to call when the future is completed.
     * @param <IN> type of the input future.
     * @return the new completion stage.
     */
    public static <IN> CompletableFuture<IN> whenCompleteAsyncIfNotDone(
            CompletableFuture<IN> completableFuture,
            Executor executor,
            BiConsumer<? super IN, ? super Throwable> whenCompleteFun) {
        return completableFuture.isDone()
                ? completableFuture.whenComplete(whenCompleteFun)
                : completableFuture.whenCompleteAsync(whenCompleteFun, executor);
    }

    /**
     * This function takes a {@link CompletableFuture} and a consumer to accept the result of this
     * future. If the input future is already done, this function returns {@link
     * CompletableFuture#thenAccept(Consumer)}. Otherwise, the return value is {@link
     * CompletableFuture#thenAcceptAsync(Consumer, Executor)} with the given executor.
     *
     * @param completableFuture the completable future for which we want to call #thenAccept.
     * @param executor the executor to run the thenAccept function if the future is not yet done.
     * @param consumer the consumer function to call when the future is completed.
     * @param <IN> type of the input future.
     * @return the new completion stage.
     */
    public static <IN> CompletableFuture<Void> thenAcceptAsyncIfNotDone(
            CompletableFuture<IN> completableFuture,
            Executor executor,
            Consumer<? super IN> consumer) {
        return completableFuture.isDone()
                ? completableFuture.thenAccept(consumer)
                : completableFuture.thenAcceptAsync(consumer, executor);
    }

    /**
     * This function takes a {@link CompletableFuture} and a handler function for the result of this
     * future. If the input future is already done, this function returns {@link
     * CompletableFuture#handle(BiFunction)}. Otherwise, the return value is {@link
     * CompletableFuture#handleAsync(BiFunction, Executor)} with the given executor.
     *
     * @param completableFuture the completable future for which we want to call #handle.
     * @param executor the executor to run the handle function if the future is not yet done.
     * @param handler the handler function to call when the future is completed.
     * @param <IN> type of the handler input argument.
     * @param <OUT> type of the handler return value.
     * @return the new completion stage.
     */
    public static <IN, OUT> CompletableFuture<OUT> handleAsyncIfNotDone(
            CompletableFuture<IN> completableFuture,
            Executor executor,
            BiFunction<? super IN, Throwable, ? extends OUT> handler) {
        return completableFuture.isDone()
                ? completableFuture.handle(handler)
                : completableFuture.handleAsync(handler, executor);
    }

    /** @return true if future has completed normally, false otherwise. */
    public static boolean isCompletedNormally(CompletableFuture<?> future) {
        return future.isDone() && !future.isCompletedExceptionally();
    }

    /**
     * Perform check state that future has completed normally and return the result.
     *
     * @return the result of completable future.
     * @throws IllegalStateException Thrown, if future has not completed or it has completed
     *     exceptionally.
     */
    public static <T> T checkStateAndGet(CompletableFuture<T> future) {
        checkCompletedNormally(future);
        return getWithoutException(future);
    }

    /**
     * Gets the result of a completable future without any exception thrown.
     *
     * @param future the completable future specified.
     * @param <T> the type of result
     * @return the result of completable future, or null if it's unfinished or finished
     *     exceptionally
     */
    @Nullable
    public static <T> T getWithoutException(CompletableFuture<T> future) {
        if (isCompletedNormally(future)) {
            try {
                return future.get();
            } catch (InterruptedException | ExecutionException ignored) {
            }
        }
        return null;
    }

    /**
     * @return the result of completable future, or the defaultValue if it has not yet completed.
     */
    public static <T> T getOrDefault(CompletableFuture<T> future, T defaultValue) {
        T value = getWithoutException(future);
        return value == null ? defaultValue : value;
    }

    /** Runnable to complete the given future with a {@link TimeoutException}. */
    private static final class Timeout implements Runnable {

        private final CompletableFuture<?> future;
        private final String timeoutMsg;

        private Timeout(CompletableFuture<?> future, @Nullable String timeoutMsg) {
            this.future = checkNotNull(future);
            this.timeoutMsg = timeoutMsg;
        }

        @Override
        public void run() {
            future.completeExceptionally(new TimeoutException(timeoutMsg));
        }
    }

    /**
     * Delay scheduler used to timeout futures.
     *
     * <p>This class creates a singleton scheduler used to run the provided actions.
     */
    private enum Delayer {
        ;
        static final ScheduledThreadPoolExecutor DELAYER =
                new ScheduledThreadPoolExecutor(
                        1, new ExecutorThreadFactory("FlinkCompletableFutureDelayScheduler"));

        /**
         * Delay the given action by the given delay.
         *
         * @param runnable to execute after the given delay
         * @param delay after which to execute the runnable
         * @param timeUnit time unit of the delay
         * @return Future of the scheduled action
         */
        private static ScheduledFuture<?> delay(Runnable runnable, long delay, TimeUnit timeUnit) {
            checkNotNull(runnable);
            checkNotNull(timeUnit);

            return DELAYER.schedule(runnable, delay, timeUnit);
        }
    }

    /**
     * Asserts that the given {@link CompletableFuture} is not completed exceptionally. If the
     * future is completed exceptionally, then it will call the {@link FatalExitExceptionHandler}.
     *
     * @param completableFuture to assert for no exceptions
     */
    public static void assertNoException(CompletableFuture<?> completableFuture) {
        handleUncaughtException(completableFuture, FatalExitExceptionHandler.INSTANCE);
    }

    /**
     * Checks that the given {@link CompletableFuture} is not completed exceptionally with the
     * specified class. If the future is completed exceptionally with the specific class, then try
     * to recover using a given exception handler. If the exception does not match the specified
     * class, just pass it through to later stages.
     *
     * @param completableFuture to assert for a given exception
     * @param exceptionClass exception class to assert for
     * @param exceptionHandler to call if the future is completed exceptionally with the specific
     *     exception
     * @return completable future, that can recover from a specified exception
     */
    public static <T, E extends Throwable> CompletableFuture<T> handleException(
            CompletableFuture<? extends T> completableFuture,
            Class<E> exceptionClass,
            Function<? super E, ? extends T> exceptionHandler) {
        final CompletableFuture<T> handledFuture = new CompletableFuture<>();
        checkNotNull(completableFuture)
                .whenComplete(
                        (result, throwable) -> {
                            if (throwable == null) {
                                handledFuture.complete(result);
                            } else if (exceptionClass.isAssignableFrom(throwable.getClass())) {
                                final E exception = exceptionClass.cast(throwable);
                                try {
                                    handledFuture.complete(exceptionHandler.apply(exception));
                                } catch (Throwable t) {
                                    handledFuture.completeExceptionally(t);
                                }
                            } else {
                                handledFuture.completeExceptionally(throwable);
                            }
                        });
        return handledFuture;
    }

    /**
     * Checks that the given {@link CompletableFuture} is not completed exceptionally. If the future
     * is completed exceptionally, then it will call the given uncaught exception handler.
     *
     * @param completableFuture to assert for no exceptions
     * @param uncaughtExceptionHandler to call if the future is completed exceptionally
     */
    public static void handleUncaughtException(
            CompletableFuture<?> completableFuture,
            Thread.UncaughtExceptionHandler uncaughtExceptionHandler) {
        checkNotNull(completableFuture)
                .whenComplete(
                        (ignored, throwable) -> {
                            if (throwable != null) {
                                uncaughtExceptionHandler.uncaughtException(
                                        Thread.currentThread(), throwable);
                            }
                        });
    }

    /**
     * Forwards the value from the source future to the target future.
     *
     * @param source future to forward the value from
     * @param target future to forward the value to
     * @param <T> type of the value
     */
    public static <T> void forward(CompletableFuture<T> source, CompletableFuture<T> target) {
        source.whenComplete(forwardTo(target));
    }

    /**
     * Forwards the value from the source future to the target future using the provided executor.
     *
     * @param source future to forward the value from
     * @param target future to forward the value to
     * @param executor executor to forward the source value to the target future
     * @param <T> type of the value
     */
    public static <T> void forwardAsync(
            CompletableFuture<T> source, CompletableFuture<T> target, Executor executor) {
        source.whenCompleteAsync(forwardTo(target), executor);
    }

    /**
     * Throws the causing exception if the given future is completed exceptionally, otherwise do
     * nothing.
     *
     * @param future the future to check.
     * @throws Exception when the future is completed exceptionally.
     */
    public static void throwIfCompletedExceptionally(CompletableFuture<?> future) throws Exception {
        if (future.isCompletedExceptionally()) {
            future.get();
        }
    }

    private static <T> BiConsumer<T, Throwable> forwardTo(CompletableFuture<T> target) {
        return (value, throwable) -> doForward(value, throwable, target);
    }

    /**
     * Completes the given future with either the given value or throwable, depending on which
     * parameter is not null.
     *
     * @param value value with which the future should be completed
     * @param throwable throwable with which the future should be completed exceptionally
     * @param target future to complete
     * @param <T> completed future
     */
    public static <T> void doForward(
            @Nullable T value, @Nullable Throwable throwable, CompletableFuture<T> target) {
        if (throwable != null) {
            target.completeExceptionally(throwable);
        } else {
            target.complete(value);
        }
    }

    /**
     * Switches the execution context of the given source future. This works for normally and
     * exceptionally completed futures.
     *
     * @param source source to switch the execution context for
     * @param executor executor representing the new execution context
     * @param <T> type of the source
     * @return future which is executed by the given executor
     */
    public static <T> CompletableFuture<T> switchExecutor(
            CompletableFuture<? extends T> source, Executor executor) {
        return source.handleAsync(
                (t, throwable) -> {
                    if (throwable != null) {
                        throw new CompletionException(throwable);
                    } else {
                        return t;
                    }
                },
                executor);
    }
}
