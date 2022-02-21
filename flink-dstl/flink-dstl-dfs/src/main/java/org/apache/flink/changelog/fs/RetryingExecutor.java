/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.changelog.fs;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.util.function.RunnableWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.flink.util.ExceptionUtils.firstOrSuppressed;

/**
 * A {@link RetriableAction} executor that schedules a next attempt upon timeout based on {@link
 * RetryPolicy}. Aimed to curb tail latencies
 */
class RetryingExecutor implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(RetryingExecutor.class);

    private final ScheduledExecutorService timer; // schedule timeouts
    private final ScheduledExecutorService blockingExecutor; // schedule and run actual uploads
    private final Histogram attemptsPerTaskHistogram;

    RetryingExecutor(int nThreads, Histogram attemptsPerTaskHistogram) {
        this(
                SchedulerFactory.create(1, "ChangelogRetryScheduler", LOG),
                SchedulerFactory.create(nThreads, "ChangelogBlockingExecutor", LOG),
                attemptsPerTaskHistogram);
    }

    @VisibleForTesting
    RetryingExecutor(ScheduledExecutorService executor, Histogram attemptsPerTaskHistogram) {
        this(executor, executor, attemptsPerTaskHistogram);
    }

    RetryingExecutor(
            ScheduledExecutorService timer,
            ScheduledExecutorService blockingExecutor,
            Histogram attemptsPerTaskHistogram) {
        this.timer = timer;
        this.blockingExecutor = blockingExecutor;
        this.attemptsPerTaskHistogram = attemptsPerTaskHistogram;
    }

    /**
     * Execute the given action according to the retry policy.
     *
     * <p>NOTE: the action must be idempotent because multiple instances of it can be executed
     * concurrently (if the policy allows retries).
     */
    void execute(
            RetryPolicy retryPolicy, RetriableAction action, Consumer<Throwable> failureCallback) {
        LOG.debug("execute with retryPolicy: {}", retryPolicy);
        RetriableTask task =
                RetriableTask.initialize(
                        action,
                        retryPolicy,
                        blockingExecutor,
                        attemptsPerTaskHistogram,
                        timer,
                        failureCallback);
        blockingExecutor.submit(task);
    }

    @Override
    public void close() throws Exception {
        LOG.debug("close");
        Exception closeException = null;
        try {
            timer.shutdownNow();
        } catch (Exception e) {
            closeException = e;
        }
        try {
            blockingExecutor.shutdownNow();
        } catch (Exception e) {
            closeException = firstOrSuppressed(e, closeException);
        }
        if (!timer.awaitTermination(1, TimeUnit.SECONDS)) {
            LOG.warn("Unable to cleanly shutdown scheduler in 1s");
        }
        if (!blockingExecutor.awaitTermination(1, TimeUnit.SECONDS)) {
            LOG.warn("Unable to cleanly shutdown blockingExecutor in 1s");
        }
        if (closeException != null) {
            throw closeException;
        }
    }

    /**
     * An action to be performed by {@link RetryingExecutor}, potentially with multiple attempts,
     * potentially concurrently.
     *
     * <p>NOTE: the action must be idempotent because of potential concurrent attempts.
     */
    interface RetriableAction extends RunnableWithException {}

    private static final class RetriableTask implements Runnable {
        private final RetriableAction runnable;
        private final Consumer<Throwable> failureCallback;
        private final ScheduledExecutorService blockingExecutor;
        private final ScheduledExecutorService timer;
        private final int current;
        private final RetryPolicy retryPolicy;
        /**
         * The flag shared across all attempts to execute the same {#link #runnable action}
         * signifying whether it was completed already or not. Used to prevent scheduling a new
         * attempt or starting it if another attempt has already completed the action.
         */
        private final AtomicBoolean actionCompleted;

        /**
         * The flag private to <b>this</b> attempt signifying whether it has completed or not. Used
         * to prevent double finalization ({@link #handleError}) by the executing thread and
         * timeouting thread.
         */
        private final AtomicBoolean attemptCompleted;

        private final AtomicInteger activeAttempts;

        private final Histogram attemptsPerTaskHistogram;

        private RetriableTask(
                int current,
                AtomicBoolean actionCompleted,
                RetriableAction runnable,
                RetryPolicy retryPolicy,
                ScheduledExecutorService blockingExecutor,
                ScheduledExecutorService timer,
                Consumer<Throwable> failureCallback,
                AtomicInteger activeAttempts,
                Histogram attemptsPerTaskHistogram) {
            this.current = current;
            this.runnable = runnable;
            this.failureCallback = failureCallback;
            this.retryPolicy = retryPolicy;
            this.blockingExecutor = blockingExecutor;
            this.actionCompleted = actionCompleted;
            this.attemptsPerTaskHistogram = attemptsPerTaskHistogram;
            this.timer = timer;
            this.activeAttempts = activeAttempts;
            this.attemptCompleted = new AtomicBoolean(false);
        }

        @Override
        public void run() {
            LOG.debug("starting attempt {}", current);
            if (!actionCompleted.get()) {
                Optional<ScheduledFuture<?>> timeoutFuture = scheduleTimeout();
                try {
                    runnable.run();
                    if (actionCompleted.compareAndSet(false, true)) {
                        LOG.debug("succeeded with {} attempts", current);
                        attemptsPerTaskHistogram.update(current);
                    }
                    attemptCompleted.set(true);
                } catch (Exception e) {
                    handleError(e);
                } finally {
                    timeoutFuture.ifPresent(f -> f.cancel(true));
                }
            }
        }

        private void handleError(Exception e) {
            if (!attemptCompleted.compareAndSet(false, true) || actionCompleted.get()) {
                // either this attempt was already completed (e.g. timed out);
                // or another attempt completed the task
                return;
            }
            LOG.debug("execution attempt {} failed: {}", current, e.getMessage());
            long nextAttemptDelay = retryPolicy.retryAfter(current, e);
            if (nextAttemptDelay >= 0L) {
                activeAttempts.incrementAndGet();
                scheduleNext(nextAttemptDelay, next());
            }
            if (activeAttempts.decrementAndGet() == 0
                    && actionCompleted.compareAndSet(false, true)) {
                LOG.info("failed with {} attempts: {}", current, e.getMessage());
                failureCallback.accept(e);
            }
        }

        private void scheduleNext(long nextAttemptDelay, RetriableTask next) {
            if (nextAttemptDelay == 0L) {
                blockingExecutor.submit(next);
            } else if (nextAttemptDelay > 0L) {
                blockingExecutor.schedule(next, nextAttemptDelay, MILLISECONDS);
            }
        }

        private static RetriableTask initialize(
                RetriableAction runnable,
                RetryPolicy retryPolicy,
                ScheduledExecutorService blockingExecutor,
                Histogram attemptsPerTaskHistogram,
                ScheduledExecutorService timer,
                Consumer<Throwable> failureCallback) {
            return new RetriableTask(
                    1,
                    new AtomicBoolean(false),
                    runnable,
                    retryPolicy,
                    blockingExecutor,
                    timer,
                    failureCallback,
                    new AtomicInteger(1),
                    attemptsPerTaskHistogram);
        }

        private RetriableTask next() {
            return new RetriableTask(
                    current + 1,
                    actionCompleted,
                    runnable,
                    retryPolicy,
                    blockingExecutor,
                    timer,
                    failureCallback,
                    activeAttempts,
                    attemptsPerTaskHistogram);
        }

        private Optional<ScheduledFuture<?>> scheduleTimeout() {
            long timeout = retryPolicy.timeoutFor(current);
            return timeout <= 0
                    ? Optional.empty()
                    : Optional.of(
                            timer.schedule(
                                    () -> handleError(fmtError(timeout)), timeout, MILLISECONDS));
        }

        private TimeoutException fmtError(long timeout) {
            return new TimeoutException(
                    String.format("Attempt %d timed out after %dms", current, timeout));
        }
    }
}
