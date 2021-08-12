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

import org.apache.flink.util.function.RunnableWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * A {@link RetriableAction} executor that schedules a next attempt upon timeout based on {@link
 * RetryPolicy}. Aimed to curb tail latencies
 */
class RetryingExecutor implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(RetryingExecutor.class);

    private final ScheduledExecutorService scheduler;

    RetryingExecutor(int nThreads) {
        this(SchedulerFactory.create(nThreads, "ChangelogRetryScheduler", LOG));
    }

    RetryingExecutor(ScheduledExecutorService scheduler) {
        this.scheduler = scheduler;
    }

    /**
     * Execute the given action according to the retry policy.
     *
     * <p>NOTE: the action must be idempotent because multiple instances of it can be executed
     * concurrently (if the policy allows retries).
     */
    void execute(RetryPolicy retryPolicy, RetriableAction action) {
        LOG.debug("execute with retryPolicy: {}", retryPolicy);
        RetriableTask task = new RetriableTask(action, retryPolicy, scheduler);
        scheduler.submit(task);
    }

    @Override
    public void close() throws Exception {
        LOG.debug("close");
        scheduler.shutdownNow();
        if (!scheduler.awaitTermination(1, TimeUnit.SECONDS)) {
            LOG.warn("Unable to cleanly shutdown executorService in 1s");
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
        private final ScheduledExecutorService executorService;
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
        private final AtomicBoolean attemptCompleted = new AtomicBoolean(false);

        RetriableTask(
                RetriableAction runnable,
                RetryPolicy retryPolicy,
                ScheduledExecutorService executorService) {
            this(1, new AtomicBoolean(false), runnable, retryPolicy, executorService);
        }

        private RetriableTask(
                int current,
                AtomicBoolean actionCompleted,
                RetriableAction runnable,
                RetryPolicy retryPolicy,
                ScheduledExecutorService executorService) {
            this.current = current;
            this.runnable = runnable;
            this.retryPolicy = retryPolicy;
            this.executorService = executorService;
            this.actionCompleted = actionCompleted;
        }

        @Override
        public void run() {
            if (!actionCompleted.get()) {
                Optional<ScheduledFuture<?>> timeoutFuture = scheduleTimeout();
                try {
                    runnable.run();
                    actionCompleted.set(true);
                    attemptCompleted.set(true);
                } catch (Exception e) {
                    handleError(e);
                } finally {
                    timeoutFuture.ifPresent(f -> f.cancel(true));
                }
            }
        }

        private void handleError(Exception e) {
            LOG.info("execution attempt {} failed: {}", current, e.getMessage());
            // prevent double completion in case of a timeout and another failure
            boolean attemptTransition = attemptCompleted.compareAndSet(false, true);
            if (attemptTransition && !actionCompleted.get()) {
                long nextAttemptDelay = retryPolicy.retryAfter(current, e);
                if (nextAttemptDelay == 0L) {
                    executorService.submit(next());
                } else if (nextAttemptDelay > 0L) {
                    executorService.schedule(next(), nextAttemptDelay, MILLISECONDS);
                } else {
                    actionCompleted.set(true);
                }
            }
        }

        private RetriableTask next() {
            return new RetriableTask(
                    current + 1, actionCompleted, runnable, retryPolicy, executorService);
        }

        private Optional<ScheduledFuture<?>> scheduleTimeout() {
            long timeout = retryPolicy.timeoutFor(current);
            return timeout <= 0
                    ? Optional.empty()
                    : Optional.of(
                            executorService.schedule(
                                    () -> handleError(fmtError(timeout)), timeout, MILLISECONDS));
        }

        private TimeoutException fmtError(long timeout) {
            return new TimeoutException(
                    String.format("Attempt %d timed out after %dms", current, timeout));
        }
    }
}
