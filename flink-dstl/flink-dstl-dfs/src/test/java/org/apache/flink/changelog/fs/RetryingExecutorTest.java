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

import org.apache.flink.core.testutils.CompletedScheduledFuture;
import org.apache.flink.runtime.testutils.DirectScheduledExecutorService;
import org.apache.flink.util.function.ThrowingConsumer;

import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/** {@link RetryingExecutor} test. */
public class RetryingExecutorTest {

    private static final ThrowingConsumer<Integer, Exception> FAILING_TASK =
            attempt -> {
                throw new IOException();
            };

    @Test
    public void testNoRetries() throws Exception {
        testPolicy(1, RetryPolicy.NONE, FAILING_TASK);
    }

    @Test
    public void testFixedRetryLimit() throws Exception {
        testPolicy(5, RetryPolicy.fixed(5, 0, 0), FAILING_TASK);
    }

    @Test
    public void testFixedRetrySuccess() throws Exception {
        int successfulAttempt = 3;
        int maxAttempts = successfulAttempt * 2;
        testPolicy(
                successfulAttempt,
                RetryPolicy.fixed(maxAttempts, 0, 0),
                attempt -> {
                    if (attempt < successfulAttempt) {
                        throw new IOException();
                    }
                });
    }

    @Test
    public void testNonRetryableException() throws Exception {
        testPolicy(
                1,
                RetryPolicy.fixed(Integer.MAX_VALUE, 0, 0),
                ignored -> {
                    throw new RuntimeException();
                });
    }

    @Test
    public void testRetryDelay() throws Exception {
        int delayAfterFailure = 123;
        int numAttempts = 2;
        testPolicy(
                numAttempts,
                RetryPolicy.fixed(Integer.MAX_VALUE, 0, delayAfterFailure),
                a -> {
                    if (a < numAttempts) {
                        throw new IOException();
                    }
                },
                new DirectScheduledExecutorService() {
                    @Override
                    public ScheduledFuture<?> schedule(
                            Runnable command, long delay, TimeUnit unit) {
                        assertEquals(delayAfterFailure, delay);
                        command.run();
                        return CompletedScheduledFuture.create(null);
                    }
                });
    }

    @Test
    public void testNoRetryDelayIfTimeout() throws Exception {
        int delayAfterFailure = 123;
        int numAttempts = 2;
        testPolicy(
                numAttempts,
                RetryPolicy.fixed(Integer.MAX_VALUE, 0, delayAfterFailure),
                a -> {
                    if (a < numAttempts) {
                        throw new TimeoutException();
                    }
                },
                new DirectScheduledExecutorService() {
                    @Override
                    public ScheduledFuture<?> schedule(
                            Runnable command, long delay, TimeUnit unit) {
                        fail("task should be executed directly without delay after timeout");
                        return CompletedScheduledFuture.create(null);
                    }
                });
    }

    @Test
    public void testTimeout() throws Exception {
        int numAttempts = 2;
        int timeout = 500;
        CompletableFuture<Long> firstStart = new CompletableFuture<>();
        CompletableFuture<Long> secondStart = new CompletableFuture<>();
        testPolicy(
                numAttempts,
                RetryPolicy.fixed(Integer.MAX_VALUE, timeout, 0),
                a -> {
                    long now = System.nanoTime();
                    if (a < numAttempts) {
                        firstStart.complete(now);
                        secondStart.get(); // cause timeout
                    } else {
                        secondStart.complete(now);
                    }
                },
                Executors.newScheduledThreadPool(2));
        assertEquals(
                timeout,
                ((double) secondStart.get() - firstStart.get()) / 1_000_000,
                timeout
                        * 0.75d /* future completion can be delayed arbitrarily causing start delta be less than timeout */);
    }

    private void testPolicy(
            int expectedAttempts, RetryPolicy policy, ThrowingConsumer<Integer, Exception> task)
            throws Exception {
        testPolicy(expectedAttempts, policy, task, new DirectScheduledExecutorService());
    }

    private void testPolicy(
            int expectedAttempts,
            RetryPolicy policy,
            ThrowingConsumer<Integer, Exception> task,
            ScheduledExecutorService scheduler)
            throws Exception {
        AtomicInteger attemptsMade = new AtomicInteger(0);
        CountDownLatch firstAttemptCompletedLatch = new CountDownLatch(1);
        try (RetryingExecutor executor = new RetryingExecutor(scheduler)) {
            executor.execute(
                    policy,
                    () -> {
                        try {
                            task.accept(attemptsMade.incrementAndGet());
                        } finally {
                            firstAttemptCompletedLatch.countDown();
                        }
                    });
            firstAttemptCompletedLatch.await(); // before closing executor
        }
        assertEquals(expectedAttempts, attemptsMade.get());
    }
}
