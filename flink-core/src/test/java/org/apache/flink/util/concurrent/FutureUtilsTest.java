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

import org.apache.flink.core.testutils.FlinkAssertions;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.FlinkRuntimeException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import static org.apache.flink.core.testutils.FlinkAssertions.assertThatFuture;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the utility methods in {@link FutureUtils}. */
class FutureUtilsTest {

    @RegisterExtension
    static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorExtension();

    /** Tests that we can retry an operation. */
    @Test
    void testRetrySuccess() {
        final int retries = 10;
        final AtomicInteger atomicInteger = new AtomicInteger(0);
        CompletableFuture<Boolean> retryFuture =
                FutureUtils.retry(
                        () ->
                                CompletableFuture.supplyAsync(
                                        () -> {
                                            if (atomicInteger.incrementAndGet() == retries) {
                                                return true;
                                            } else {
                                                throw new CompletionException(
                                                        new FlinkException("Test exception"));
                                            }
                                        },
                                        EXECUTOR_RESOURCE.getExecutor()),
                        retries,
                        EXECUTOR_RESOURCE.getExecutor());

        assertThatFuture(retryFuture).eventuallySucceeds().isEqualTo(true);
        assertThat(atomicInteger).hasValue(retries);
    }

    /** Tests that a retry future is failed after all retries have been consumed. */
    @Test
    void testRetryFailureFixedRetries() {
        final int retries = 3;

        CompletableFuture<?> retryFuture =
                FutureUtils.retry(
                        () ->
                                FutureUtils.completedExceptionally(
                                        new FlinkException("Test exception")),
                        retries,
                        EXECUTOR_RESOURCE.getExecutor());

        assertThatFuture(retryFuture)
                .eventuallyFailsWith(ExecutionException.class)
                .withCauseInstanceOf(FutureUtils.RetryException.class);
    }

    /** Tests that we can cancel a retry future. */
    @Test
    void testRetryCancellation() throws InterruptedException {
        final int retries = 10;
        final AtomicInteger atomicInteger = new AtomicInteger(0);
        final OneShotLatch notificationLatch = new OneShotLatch();
        final OneShotLatch waitLatch = new OneShotLatch();
        final AtomicReference<Throwable> atomicThrowable = new AtomicReference<>(null);

        CompletableFuture<?> retryFuture =
                FutureUtils.retry(
                        () ->
                                CompletableFuture.supplyAsync(
                                        () -> {
                                            if (atomicInteger.incrementAndGet() == 2) {
                                                notificationLatch.trigger();
                                                try {
                                                    waitLatch.await();
                                                } catch (InterruptedException e) {
                                                    atomicThrowable.compareAndSet(null, e);
                                                }
                                            }

                                            throw new CompletionException(
                                                    new FlinkException("Test exception"));
                                        },
                                        EXECUTOR_RESOURCE.getExecutor()),
                        retries,
                        EXECUTOR_RESOURCE.getExecutor());

        // await that we have failed once
        notificationLatch.await();

        assertThat(retryFuture).isNotDone();

        // cancel the retry future
        retryFuture.cancel(false);

        // let the retry operation continue
        waitLatch.trigger();

        assertThat(retryFuture).isCancelled();
        assertThat(atomicInteger).hasValue(2);

        assertThat(atomicThrowable.get()).isNull();
    }

    /** Test that {@link FutureUtils#retry} should stop at non-retryable exception. */
    @Test
    void testStopAtNonRetryableException() {
        final int retries = 10;
        final int notRetry = 3;
        final AtomicInteger atomicInteger = new AtomicInteger(0);
        final FlinkRuntimeException nonRetryableException =
                new FlinkRuntimeException("Non-retryable exception");
        CompletableFuture<Boolean> retryFuture =
                FutureUtils.retry(
                        () ->
                                CompletableFuture.supplyAsync(
                                        () -> {
                                            if (atomicInteger.incrementAndGet() == notRetry) {
                                                // throw non-retryable exception
                                                throw new CompletionException(
                                                        nonRetryableException);
                                            } else {
                                                throw new CompletionException(
                                                        new FlinkException("Test exception"));
                                            }
                                        },
                                        EXECUTOR_RESOURCE.getExecutor()),
                        retries,
                        throwable ->
                                ExceptionUtils.findThrowable(throwable, FlinkException.class)
                                        .isPresent(),
                        EXECUTOR_RESOURCE.getExecutor());

        assertThatFuture(retryFuture)
                .eventuallyFailsWith(ExecutionException.class)
                .extracting(FlinkAssertions::chainOfCauses, FlinkAssertions.STREAM_THROWABLE)
                .last()
                .isEqualTo(nonRetryableException);
        assertThat(atomicInteger).hasValue(notRetry);
    }

    /** Tests that retry with delay fails after having exceeded all retries. */
    @Test
    void testRetryWithDelayRetryStrategyFailure() {
        CompletableFuture<?> retryFuture =
                FutureUtils.retryWithDelay(
                        () ->
                                FutureUtils.completedExceptionally(
                                        new FlinkException("Test exception")),
                        new FixedRetryStrategy(3, Duration.ofMillis(1L)),
                        new ScheduledExecutorServiceAdapter(EXECUTOR_RESOURCE.getExecutor()));

        assertThatFuture(retryFuture)
                .eventuallyFailsWith(ExecutionException.class)
                .withCauseInstanceOf(FutureUtils.RetryException.class);
    }

    /**
     * Tests that the delay is respected between subsequent retries of a retry future with retry
     * delay.
     */
    @Test
    void testRetryWithDelayRetryStrategy() {
        final int retries = 4;
        final AtomicInteger countDown = new AtomicInteger(retries);

        long start = System.currentTimeMillis();

        CompletableFuture<Boolean> retryFuture =
                FutureUtils.retryWithDelay(
                        () -> {
                            if (countDown.getAndDecrement() == 0) {
                                return CompletableFuture.completedFuture(true);
                            } else {
                                return FutureUtils.completedExceptionally(
                                        new FlinkException("Test exception."));
                            }
                        },
                        new ExponentialBackoffRetryStrategy(
                                retries, Duration.ofMillis(2L), Duration.ofMillis(5L)),
                        new ScheduledExecutorServiceAdapter(EXECUTOR_RESOURCE.getExecutor()));

        assertThatFuture(retryFuture).eventuallySucceeds().isEqualTo(true);

        long completionTime = System.currentTimeMillis() - start;

        assertThat(completionTime)
                .as("The completion time should be at least retries times delay between retries.")
                .isGreaterThanOrEqualTo(2 + 4 + 5 + 5);
    }

    /** Tests that all scheduled tasks are canceled if the retry future is being cancelled. */
    @Test
    void testRetryWithDelayRetryStrategyCancellation() {
        final ManuallyTriggeredScheduledExecutor scheduledExecutor =
                new ManuallyTriggeredScheduledExecutor();

        CompletableFuture<?> retryFuture =
                FutureUtils.retryWithDelay(
                        () ->
                                FutureUtils.completedExceptionally(
                                        new FlinkException("Test exception")),
                        new FixedRetryStrategy(1, TestingUtils.infiniteDuration()),
                        scheduledExecutor);

        assertThat(retryFuture).isNotDone();

        final Collection<ScheduledFuture<?>> scheduledTasks =
                scheduledExecutor.getActiveScheduledTasks();

        assertThat(scheduledTasks).isNotEmpty();

        final ScheduledFuture<?> scheduledFuture = scheduledTasks.iterator().next();

        assertThat(scheduledFuture.isDone()).isFalse();

        retryFuture.cancel(false);

        assertThat(retryFuture).isCancelled();
        assertThat(scheduledFuture.isCancelled()).isTrue();
    }

    /** Tests that a future is timed out after the specified timeout. */
    @Test
    void testOrTimeout() {
        final CompletableFuture<String> future = new CompletableFuture<>();
        final long timeout = 10L;

        final String expectedErrorMessage = "testOrTimeout";
        FutureUtils.orTimeout(future, timeout, TimeUnit.MILLISECONDS, expectedErrorMessage);

        assertThatFuture(future)
                .eventuallyFailsWith(ExecutionException.class)
                .withCauseInstanceOf(TimeoutException.class)
                .withMessageContaining(expectedErrorMessage);
    }

    @Test
    void testRetryWithDelayRetryStrategyAndPredicate() {
        final ScheduledExecutorService retryExecutor = EXECUTOR_RESOURCE.getExecutor();
        final String retryableExceptionMessage = "first exception";
        final String expectedErrorMessage = "should propagate";
        class TestStringSupplier implements Supplier<CompletableFuture<String>> {
            private final AtomicInteger counter = new AtomicInteger();

            @Override
            public CompletableFuture<String> get() {
                if (counter.getAndIncrement() == 0) {
                    return FutureUtils.completedExceptionally(
                            new RuntimeException(retryableExceptionMessage));
                } else {
                    return FutureUtils.completedExceptionally(
                            new RuntimeException(expectedErrorMessage));
                }
            }
        }

        final CompletableFuture<String> resultFuture =
                FutureUtils.retryWithDelay(
                        new TestStringSupplier(),
                        new FixedRetryStrategy(1, Duration.ZERO),
                        throwable ->
                                throwable instanceof RuntimeException
                                        && throwable
                                                .getMessage()
                                                .contains(retryableExceptionMessage),
                        new ScheduledExecutorServiceAdapter(retryExecutor));

        assertThatFuture(resultFuture)
                .eventuallyFailsWith(ExecutionException.class)
                .withMessageContaining(expectedErrorMessage);
    }

    @Test
    void testRunAfterwards() {
        final CompletableFuture<Void> inputFuture = new CompletableFuture<>();
        final OneShotLatch runnableLatch = new OneShotLatch();

        final CompletableFuture<Void> runFuture =
                FutureUtils.runAfterwards(inputFuture, runnableLatch::trigger);

        assertThat(runnableLatch.isTriggered()).isFalse();
        assertThat(runFuture).isNotDone();

        inputFuture.complete(null);

        assertThat(runnableLatch.isTriggered()).isTrue();
        assertThatFuture(runFuture).eventuallySucceeds();
    }

    @Test
    void testRunAfterwardsExceptional() {
        final CompletableFuture<Void> inputFuture = new CompletableFuture<>();
        final OneShotLatch runnableLatch = new OneShotLatch();
        final FlinkException testException = new FlinkException("Test exception");

        final CompletableFuture<Void> runFuture =
                FutureUtils.runAfterwards(inputFuture, runnableLatch::trigger);

        assertThat(runnableLatch.isTriggered()).isFalse();
        assertThat(runFuture).isNotDone();

        inputFuture.completeExceptionally(testException);

        assertThat(runnableLatch.isTriggered()).isTrue();
        assertThat(runFuture).isDone();

        assertThatFuture(runFuture)
                .eventuallyFailsWith(ExecutionException.class)
                .withCause(testException);
    }

    @Test
    void testComposeAfterwards() {
        final CompletableFuture<Void> inputFuture = new CompletableFuture<>();
        final OneShotLatch composeLatch = new OneShotLatch();

        final CompletableFuture<Void> composeFuture =
                FutureUtils.composeAfterwards(
                        inputFuture,
                        () -> {
                            composeLatch.trigger();
                            return CompletableFuture.completedFuture(null);
                        });

        assertThat(composeLatch.isTriggered()).isFalse();
        assertThat(composeFuture).isNotDone();

        inputFuture.complete(null);

        assertThat(composeLatch.isTriggered()).isTrue();

        assertThatFuture(composeFuture).eventuallySucceeds();
    }

    @Test
    void testComposeAfterwardsFirstExceptional() {
        final CompletableFuture<Void> inputFuture = new CompletableFuture<>();
        final OneShotLatch composeLatch = new OneShotLatch();
        final FlinkException testException = new FlinkException("Test exception");

        final CompletableFuture<Void> composeFuture =
                FutureUtils.composeAfterwards(
                        inputFuture,
                        () -> {
                            composeLatch.trigger();
                            return CompletableFuture.completedFuture(null);
                        });

        assertThat(composeLatch.isTriggered()).isFalse();
        assertThat(composeFuture).isNotDone();

        inputFuture.completeExceptionally(testException);

        assertThat(composeLatch.isTriggered()).isTrue();
        assertThat(composeFuture).isDone();

        assertThatFuture(composeFuture)
                .eventuallyFailsWith(ExecutionException.class)
                .withCause(testException);
    }

    @Test
    void testComposeAfterwardsSecondExceptional() {
        final CompletableFuture<Void> inputFuture = new CompletableFuture<>();
        final OneShotLatch composeLatch = new OneShotLatch();
        final FlinkException testException = new FlinkException("Test exception");

        final CompletableFuture<Void> composeFuture =
                FutureUtils.composeAfterwards(
                        inputFuture,
                        () -> {
                            composeLatch.trigger();
                            return FutureUtils.completedExceptionally(testException);
                        });

        assertThat(composeLatch.isTriggered()).isFalse();
        assertThat(composeFuture).isNotDone();

        inputFuture.complete(null);

        assertThat(composeLatch.isTriggered()).isTrue();
        assertThat(composeFuture).isDone();

        assertThatFuture(composeFuture)
                .eventuallyFailsWith(ExecutionException.class)
                .withCause(testException);
    }

    @Test
    void testComposeAfterwardsBothExceptional() {
        final CompletableFuture<Void> inputFuture = new CompletableFuture<>();
        final FlinkException testException1 = new FlinkException("Test exception1");
        final FlinkException testException2 = new FlinkException("Test exception2");
        final OneShotLatch composeLatch = new OneShotLatch();

        final CompletableFuture<Void> composeFuture =
                FutureUtils.composeAfterwards(
                        inputFuture,
                        () -> {
                            composeLatch.trigger();
                            return FutureUtils.completedExceptionally(testException2);
                        });

        assertThat(composeLatch.isTriggered()).isFalse();
        assertThat(composeFuture).isNotDone();

        inputFuture.completeExceptionally(testException1);

        assertThat(composeLatch.isTriggered()).isTrue();
        assertThat(composeFuture).isDone();

        assertThatFuture(composeFuture)
                .eventuallyFailsWith(ExecutionException.class)
                .extracting(Throwable::getCause)
                .isEqualTo(testException1)
                .satisfies(
                        cause -> assertThat(cause.getSuppressed()).containsExactly(testException2));
    }

    @Test
    void testCompleteAll() {
        final CompletableFuture<String> inputFuture1 = new CompletableFuture<>();
        final CompletableFuture<Integer> inputFuture2 = new CompletableFuture<>();

        final List<CompletableFuture<?>> futuresToComplete =
                Arrays.asList(inputFuture1, inputFuture2);
        final FutureUtils.ConjunctFuture<Void> completeFuture =
                FutureUtils.completeAll(futuresToComplete);

        assertThat(completeFuture).isNotDone();
        assertThat(completeFuture.getNumFuturesCompleted()).isZero();
        assertThat(completeFuture.getNumFuturesTotal()).isEqualTo(futuresToComplete.size());

        inputFuture2.complete(42);

        assertThat(completeFuture).isNotDone();
        assertThat(completeFuture.getNumFuturesCompleted()).isOne();

        inputFuture1.complete("foobar");

        assertThat(completeFuture).isDone();
        assertThat(completeFuture.getNumFuturesCompleted()).isEqualTo(2);

        assertThatFuture(completeFuture).eventuallySucceeds();
    }

    @Test
    void testCompleteAllPartialExceptional() {
        final CompletableFuture<String> inputFuture1 = new CompletableFuture<>();
        final CompletableFuture<Integer> inputFuture2 = new CompletableFuture<>();

        final List<CompletableFuture<?>> futuresToComplete =
                Arrays.asList(inputFuture1, inputFuture2);
        final FutureUtils.ConjunctFuture<Void> completeFuture =
                FutureUtils.completeAll(futuresToComplete);

        assertThat(completeFuture).isNotDone();
        assertThat(completeFuture.getNumFuturesCompleted()).isZero();
        assertThat(completeFuture.getNumFuturesTotal()).isEqualTo(futuresToComplete.size());

        final FlinkException testException1 = new FlinkException("Test exception 1");
        inputFuture2.completeExceptionally(testException1);

        assertThat(completeFuture).isNotDone();
        assertThat(completeFuture.getNumFuturesCompleted()).isOne();

        inputFuture1.complete("foobar");

        assertThat(completeFuture).isDone();
        assertThat(completeFuture.getNumFuturesCompleted()).isEqualTo(2);

        assertThatFuture(completeFuture)
                .eventuallyFailsWith(ExecutionException.class)
                .withCause(testException1);
    }

    @Test
    void testCompleteAllExceptional() {
        final CompletableFuture<String> inputFuture1 = new CompletableFuture<>();
        final CompletableFuture<Integer> inputFuture2 = new CompletableFuture<>();

        final List<CompletableFuture<?>> futuresToComplete =
                Arrays.asList(inputFuture1, inputFuture2);
        final FutureUtils.ConjunctFuture<Void> completeFuture =
                FutureUtils.completeAll(futuresToComplete);

        assertThat(completeFuture).isNotDone();
        assertThat(completeFuture.getNumFuturesCompleted()).isZero();
        assertThat(completeFuture.getNumFuturesTotal()).isEqualTo(futuresToComplete.size());

        final FlinkException testException1 = new FlinkException("Test exception 1");
        inputFuture1.completeExceptionally(testException1);

        assertThat(completeFuture).isNotDone();
        assertThat(completeFuture.getNumFuturesCompleted()).isOne();

        final FlinkException testException2 = new FlinkException("Test exception 2");
        inputFuture2.completeExceptionally(testException2);

        assertThat(completeFuture.getNumFuturesCompleted()).isEqualTo(2);
        assertThatFuture(completeFuture)
                .eventuallyFailsWith(ExecutionException.class)
                .withCauseInstanceOf(FlinkException.class)
                .extracting(Throwable::getCause)
                .satisfies(
                        e -> {
                            final Throwable[] actualSuppressedExceptions = e.getSuppressed();
                            final FlinkException expectedSuppressedException =
                                    e.equals(testException1) ? testException2 : testException1;

                            assertThat(actualSuppressedExceptions)
                                    .containsExactly(expectedSuppressedException);
                        });
    }

    @Test
    void testSupplyAsyncFailure() {
        final String exceptionMessage = "Test exception";
        final FlinkException testException = new FlinkException(exceptionMessage);
        final CompletableFuture<Object> future =
                FutureUtils.supplyAsync(
                        () -> {
                            throw testException;
                        },
                        EXECUTOR_RESOURCE.getExecutor());

        assertThatFuture(future)
                .eventuallyFailsWith(ExecutionException.class)
                .withCause(testException);
    }

    @Test
    void testSupplyAsync() {
        final Object expectedResult = new Object();
        final CompletableFuture<Object> future =
                FutureUtils.supplyAsync(() -> expectedResult, EXECUTOR_RESOURCE.getExecutor());

        assertThatFuture(future).eventuallySucceeds().isEqualTo(expectedResult);
    }

    @Test
    void testHandleAsyncIfNotDone() {
        testFutureContinuation(
                (CompletableFuture<?> future, Executor executor) ->
                        FutureUtils.handleAsyncIfNotDone(future, executor, (o, t) -> null));
    }

    @Test
    void testApplyAsyncIfNotDone() {
        testFutureContinuation(
                (CompletableFuture<?> future, Executor executor) ->
                        FutureUtils.thenApplyAsyncIfNotDone(future, executor, o -> null));
    }

    @Test
    void testComposeAsyncIfNotDone() {
        testFutureContinuation(
                (CompletableFuture<?> future, Executor executor) ->
                        FutureUtils.thenComposeAsyncIfNotDone(future, executor, o -> null));
    }

    @Test
    void testWhenCompleteAsyncIfNotDone() {
        testFutureContinuation(
                (CompletableFuture<?> future, Executor executor) ->
                        FutureUtils.whenCompleteAsyncIfNotDone(
                                future, executor, (o, throwable) -> {}));
    }

    @Test
    void testThenAcceptAsyncIfNotDone() {
        testFutureContinuation(
                (CompletableFuture<?> future, Executor executor) ->
                        FutureUtils.thenAcceptAsyncIfNotDone(future, executor, o -> {}));
    }

    private void testFutureContinuation(
            BiFunction<CompletableFuture<?>, Executor, CompletableFuture<?>>
                    testFunctionGenerator) {

        CompletableFuture<?> startFuture = new CompletableFuture<>();
        final AtomicBoolean runWithExecutor = new AtomicBoolean(false);

        Executor executor =
                r -> {
                    r.run();
                    runWithExecutor.set(true);
                };

        // branch for a start future that has not completed
        CompletableFuture<?> continuationFuture =
                testFunctionGenerator.apply(startFuture, executor);
        assertThat(continuationFuture).isNotDone();

        startFuture.complete(null);

        assertThat(runWithExecutor).isTrue();
        assertThat(continuationFuture).isDone();

        // branch for a start future that was completed
        runWithExecutor.set(false);

        continuationFuture = testFunctionGenerator.apply(startFuture, executor);

        assertThat(runWithExecutor).isFalse();
        assertThat(continuationFuture).isDone();
    }

    @Test
    void testHandleExceptionWithCompletedFuture() {
        final CompletableFuture<String> future = CompletableFuture.completedFuture("foobar");
        final CompletableFuture<String> handled =
                FutureUtils.handleException(future, Exception.class, exception -> "handled");

        assertThatFuture(handled).eventuallySucceeds().isEqualTo("foobar");
    }

    @Test
    void testHandleExceptionWithNormalCompletion() {
        final CompletableFuture<String> future = new CompletableFuture<>();
        final CompletableFuture<String> handled =
                FutureUtils.handleException(future, Exception.class, exception -> "handled");
        future.complete("foobar");

        assertThat(handled).isCompletedWithValue("foobar");
    }

    @Test
    void testHandleExceptionWithMatchingExceptionallyCompletedFuture() {
        final CompletableFuture<String> future = new CompletableFuture<>();
        final CompletableFuture<String> handled =
                FutureUtils.handleException(
                        future, UnsupportedOperationException.class, exception -> "handled");
        future.completeExceptionally(new UnsupportedOperationException("foobar"));

        assertThat(handled).isCompletedWithValue("handled");
    }

    @Test
    void testHandleExceptionWithNotMatchingExceptionallyCompletedFuture() {
        final CompletableFuture<String> future = new CompletableFuture<>();
        final CompletableFuture<String> handled =
                FutureUtils.handleException(
                        future, UnsupportedOperationException.class, exception -> "handled");
        final IllegalArgumentException futureException = new IllegalArgumentException("foobar");
        future.completeExceptionally(futureException);

        assertThatFuture(handled)
                .eventuallyFailsWith(ExecutionException.class)
                .withCause(futureException);
    }

    @Test
    void testHandleExceptionWithThrowingExceptionHandler() {
        final CompletableFuture<String> future = new CompletableFuture<>();
        final IllegalStateException handlerException =
                new IllegalStateException("something went terribly wrong");
        final CompletableFuture<String> handled =
                FutureUtils.handleException(
                        future,
                        UnsupportedOperationException.class,
                        exception -> {
                            throw handlerException;
                        });
        future.completeExceptionally(new UnsupportedOperationException("foobar"));

        assertThatFuture(handled)
                .eventuallyFailsWith(ExecutionException.class)
                .withCause(handlerException);
    }

    @Test
    void testHandleUncaughtExceptionWithCompletedFuture() {
        final CompletableFuture<String> future = CompletableFuture.completedFuture("foobar");
        final TestingUncaughtExceptionHandler uncaughtExceptionHandler =
                new TestingUncaughtExceptionHandler();

        FutureUtils.handleUncaughtException(future, uncaughtExceptionHandler);

        assertThat(uncaughtExceptionHandler.hasBeenCalled()).isFalse();
    }

    @Test
    void testHandleUncaughtExceptionWithNormalCompletion() {
        final CompletableFuture<String> future = new CompletableFuture<>();

        final TestingUncaughtExceptionHandler uncaughtExceptionHandler =
                new TestingUncaughtExceptionHandler();

        FutureUtils.handleUncaughtException(future, uncaughtExceptionHandler);
        future.complete("barfoo");

        assertThat(uncaughtExceptionHandler.hasBeenCalled()).isFalse();
    }

    @Test
    void testHandleUncaughtExceptionWithExceptionallyCompletedFuture() {
        final CompletableFuture<String> future =
                FutureUtils.completedExceptionally(new FlinkException("foobar"));

        final TestingUncaughtExceptionHandler uncaughtExceptionHandler =
                new TestingUncaughtExceptionHandler();

        FutureUtils.handleUncaughtException(future, uncaughtExceptionHandler);

        assertThat(uncaughtExceptionHandler.hasBeenCalled()).isTrue();
    }

    @Test
    void testHandleUncaughtExceptionWithExceptionallyCompletion() {
        final CompletableFuture<String> future = new CompletableFuture<>();

        final TestingUncaughtExceptionHandler uncaughtExceptionHandler =
                new TestingUncaughtExceptionHandler();

        FutureUtils.handleUncaughtException(future, uncaughtExceptionHandler);

        assertThat(uncaughtExceptionHandler.hasBeenCalled()).isFalse();

        future.completeExceptionally(new FlinkException("barfoo"));
        assertThat(uncaughtExceptionHandler.hasBeenCalled()).isTrue();
    }

    /**
     * Tests the behavior of {@link FutureUtils#handleUncaughtException(CompletableFuture,
     * Thread.UncaughtExceptionHandler)} with a custom fallback exception handler to avoid
     * triggering {@code System.exit}.
     */
    @Test
    void testHandleUncaughtExceptionWithBuggyErrorHandlingCode() {
        final Exception actualProductionCodeError =
                new Exception(
                        "Actual production code error that should be caught by the error handler.");

        final RuntimeException errorHandlingException =
                new RuntimeException("Expected test error in error handling code.");
        final Thread.UncaughtExceptionHandler buggyActualExceptionHandler =
                (thread, ignoredActualException) -> {
                    throw errorHandlingException;
                };

        final AtomicReference<Throwable> caughtErrorHandlingException = new AtomicReference<>();
        final Thread.UncaughtExceptionHandler fallbackExceptionHandler =
                (thread, errorHandlingEx) -> caughtErrorHandlingException.set(errorHandlingEx);

        FutureUtils.handleUncaughtException(
                FutureUtils.completedExceptionally(actualProductionCodeError),
                buggyActualExceptionHandler,
                fallbackExceptionHandler);

        assertThat(caughtErrorHandlingException)
                .hasValueSatisfying(
                        actualError -> {
                            assertThat(actualError)
                                    .isInstanceOf(IllegalStateException.class)
                                    .hasRootCause(errorHandlingException)
                                    .satisfies(
                                            cause ->
                                                    assertThat(cause.getSuppressed())
                                                            .containsExactly(
                                                                    actualProductionCodeError));
                        });
    }

    private static class TestingUncaughtExceptionHandler
            implements Thread.UncaughtExceptionHandler {

        private Throwable exception = null;

        @Override
        public void uncaughtException(Thread t, Throwable e) {
            exception = e;
        }

        private boolean hasBeenCalled() {
            return exception != null;
        }
    }

    @Test
    void testForwardNormal() throws Exception {
        final CompletableFuture<String> source = new CompletableFuture<>();
        final CompletableFuture<String> target = new CompletableFuture<>();

        FutureUtils.forward(source, target);

        assertThat(source).isNotDone();
        assertThat(target).isNotDone();

        source.complete("foobar");

        assertThat(source).isDone();
        assertThat(target).isDone();

        assertThat(source.get()).isEqualTo(target.get());
    }

    @Test
    void testForwardExceptionally() {
        final CompletableFuture<String> source = new CompletableFuture<>();
        final CompletableFuture<String> target = new CompletableFuture<>();

        FutureUtils.forward(source, target);

        assertThat(source).isNotDone();
        assertThat(target).isNotDone();

        final FlinkException expectedCause = new FlinkException("Expected exception");
        source.completeExceptionally(expectedCause);

        assertThat(source).isDone();
        assertThat(target).isDone();

        assertThatFuture(source)
                .eventuallyFailsWith(ExecutionException.class)
                .extracting(Throwable::getCause)
                .isEqualTo(expectedCause);
        assertThatFuture(target)
                .eventuallyFailsWith(ExecutionException.class)
                .extracting(Throwable::getCause)
                .isEqualTo(expectedCause);
    }

    @Test
    void testForwardAsync() {
        final CompletableFuture<String> source = new CompletableFuture<>();
        final CompletableFuture<String> target = new CompletableFuture<>();
        final ManuallyTriggeredScheduledExecutor executor =
                new ManuallyTriggeredScheduledExecutor();

        FutureUtils.forwardAsync(source, target, executor);

        final String expectedValue = "foobar";
        source.complete(expectedValue);

        assertThat(target).isNotDone();

        // execute the forward action
        executor.triggerAll();

        assertThatFuture(target).eventuallySucceeds().isEqualTo(expectedValue);
    }

    @Test
    void testGetWithoutException() {
        final int expectedValue = 1;
        final CompletableFuture<Integer> completableFuture = new CompletableFuture<>();
        completableFuture.complete(expectedValue);

        assertThat(FutureUtils.getWithoutException(completableFuture)).isEqualTo(expectedValue);
    }

    @Test
    void testGetWithoutExceptionWithAnException() {
        final CompletableFuture<Integer> completableFuture = new CompletableFuture<>();
        completableFuture.completeExceptionally(new RuntimeException("expected"));

        assertThat(FutureUtils.getWithoutException(completableFuture)).isNull();
    }

    @Test
    void testGetWithoutExceptionWithoutFinishing() {
        final CompletableFuture<Integer> completableFuture = new CompletableFuture<>();

        assertThat(FutureUtils.getWithoutException(completableFuture)).isNull();
    }

    @Test
    void testSwitchExecutorForNormallyCompletedFuture() {
        final CompletableFuture<String> source = new CompletableFuture<>();

        final ExecutorService singleThreadExecutor = EXECUTOR_RESOURCE.getExecutor();

        final CompletableFuture<String> resultFuture =
                FutureUtils.switchExecutor(source, singleThreadExecutor);

        final String expectedThreadName =
                FutureUtils.supplyAsync(
                                () -> Thread.currentThread().getName(), singleThreadExecutor)
                        .join();
        final String expectedValue = "foobar";

        final CompletableFuture<Void> assertionFuture =
                resultFuture.handle(
                        (s, throwable) -> {
                            assertThat(s).isEqualTo(expectedValue);
                            assertThat(Thread.currentThread().getName())
                                    .isEqualTo(expectedThreadName);

                            return null;
                        });
        source.complete(expectedValue);

        assertionFuture.join();
    }

    @Test
    void testSwitchExecutorForExceptionallyCompletedFuture() {
        final CompletableFuture<String> source = new CompletableFuture<>();

        final ExecutorService singleThreadExecutor = EXECUTOR_RESOURCE.getExecutor();

        final CompletableFuture<String> resultFuture =
                FutureUtils.switchExecutor(source, singleThreadExecutor);

        final String expectedThreadName =
                FutureUtils.supplyAsync(
                                () -> Thread.currentThread().getName(), singleThreadExecutor)
                        .join();
        final Exception expectedException = new Exception("foobar");

        final CompletableFuture<Void> assertionFuture =
                resultFuture.handle(
                        (s, throwable) -> {
                            assertThat(throwable)
                                    .isInstanceOf(CompletionException.class)
                                    .extracting(Throwable::getCause)
                                    .isEqualTo(expectedException);
                            assertThat(Thread.currentThread().getName())
                                    .isEqualTo(expectedThreadName);

                            return null;
                        });
        source.completeExceptionally(expectedException);

        assertionFuture.join();
    }
}
