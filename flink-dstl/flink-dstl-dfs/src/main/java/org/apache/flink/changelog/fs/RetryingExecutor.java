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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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
    <T> void execute(RetryPolicy retryPolicy, RetriableAction<T> action) {
        LOG.debug("execute with retryPolicy: {}", retryPolicy);
        RetriableActionAttempt<T> task =
                RetriableActionAttempt.initialize(
                        action, retryPolicy, blockingExecutor, attemptsPerTaskHistogram, timer);
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
    interface RetriableAction<Result> {
        /**
         * Make an attempt to execute this action.
         *
         * @return result of execution to be used in either {@link #completeWithResult(Object)} or
         *     {@link #discardResult(Object)}.
         * @throws Exception any intermediate state should be cleaned up inside this method in case
         *     of failure
         */
        Result tryExecute() throws Exception;

        /**
         * Complete the action with the given result, e.g. by notifying waiting parties. Called on
         * successful execution once per action, regardless of the number of execution attempts.
         */
        void completeWithResult(Result result);

        /**
         * Discard the execution results, e.g. because another execution attempt has completed
         * earlier. This result will not be passed to {@link #completeWithResult(Object)} or
         * otherwise used.
         */
        void discardResult(Result result) throws Exception;

        /**
         * Handle this action failure, which means that an un-recoverable failure has occurred in
         * {@link #tryExecute()} or retry limit has been reached. No further execution attempts will
         * be performed.
         */
        void handleFailure(Throwable throwable);
    }

    private static final class RetriableActionAttempt<Result> implements Runnable {
        private final RetriableAction<Result> action;
        private final ScheduledExecutorService blockingExecutor;
        private final ScheduledExecutorService timer;
        private final int attemptNumber;
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

        private RetriableActionAttempt(
                int attemptNumber,
                AtomicBoolean actionCompleted,
                RetriableAction<Result> action,
                RetryPolicy retryPolicy,
                ScheduledExecutorService blockingExecutor,
                ScheduledExecutorService timer,
                AtomicInteger activeAttempts,
                Histogram attemptsPerTaskHistogram) {
            this.attemptNumber = attemptNumber;
            this.action = action;
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
            LOG.debug("starting attempt {}", attemptNumber);
            if (actionCompleted.get()) {
                return;
            }
            Optional<ScheduledFuture<?>> timeoutFuture = scheduleTimeout();
            try {
                Result result = action.tryExecute();
                if (actionCompleted.compareAndSet(false, true)) {
                    LOG.debug("succeeded with {} attempts", attemptNumber);
                    action.completeWithResult(result);
                    attemptsPerTaskHistogram.update(attemptNumber);
                } else {
                    LOG.debug("discard unnecessarily uploaded state, attempt {}", attemptNumber);
                    try {
                        action.discardResult(result);
                    } catch (Exception e) {
                        LOG.warn("unable to discard execution attempt result", e);
                    }
                }
            } catch (Exception e) {
                handleError(e);
            } finally {
                timeoutFuture.ifPresent(f -> f.cancel(true));
            }
        }

        private void handleError(Exception e) {
            if (!attemptCompleted.compareAndSet(false, true) || actionCompleted.get()) {
                // either this attempt was already completed (e.g. timed out);
                // or another attempt completed the task
                return;
            }
            LOG.debug("execution attempt {} failed: {}", attemptNumber, e.getMessage());
            long nextAttemptDelay = retryPolicy.retryAfter(attemptNumber, e);
            if (nextAttemptDelay >= 0L) {
                activeAttempts.incrementAndGet();
                scheduleNext(nextAttemptDelay, next());
            }
            if (activeAttempts.decrementAndGet() == 0
                    && actionCompleted.compareAndSet(false, true)) {
                LOG.info("failed with {} attempts: {}", attemptNumber, e.getMessage());
                action.handleFailure(e);
            }
        }

        private void scheduleNext(long nextAttemptDelay, RetriableActionAttempt<Result> next) {
            if (nextAttemptDelay == 0L) {
                blockingExecutor.submit(next);
            } else if (nextAttemptDelay > 0L) {
                blockingExecutor.schedule(next, nextAttemptDelay, MILLISECONDS);
            }
        }

        private static <T> RetriableActionAttempt<T> initialize(
                RetriableAction<T> runnable,
                RetryPolicy retryPolicy,
                ScheduledExecutorService blockingExecutor,
                Histogram attemptsPerTaskHistogram,
                ScheduledExecutorService timer) {
            return new RetriableActionAttempt(
                    1,
                    new AtomicBoolean(false),
                    runnable,
                    retryPolicy,
                    blockingExecutor,
                    timer,
                    new AtomicInteger(1),
                    attemptsPerTaskHistogram);
        }

        private RetriableActionAttempt<Result> next() {
            return new RetriableActionAttempt<>(
                    attemptNumber + 1,
                    actionCompleted,
                    action,
                    retryPolicy,
                    blockingExecutor,
                    timer,
                    activeAttempts,
                    attemptsPerTaskHistogram);
        }

        private Optional<ScheduledFuture<?>> scheduleTimeout() {
            long timeout = retryPolicy.timeoutFor(attemptNumber);
            return timeout <= 0
                    ? Optional.empty()
                    : Optional.of(
                            timer.schedule(
                                    () -> handleError(fmtError(timeout)), timeout, MILLISECONDS));
        }

        private TimeoutException fmtError(long timeout) {
            return new TimeoutException(
                    String.format("Attempt %d timed out after %dms", attemptNumber, timeout));
        }
    }
}
