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

package org.apache.flink.runtime.util;

import org.apache.flink.shaded.guava18.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Timeout;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.hamcrest.MatcherAssert;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

public class RunnablesTest {

    private static final int TIMEOUT_MS = 100;

    // ------------------------------------------------------------------------
    // Test ExecutorService/ScheduledExecutorService behaviour.
    // ------------------------------------------------------------------------

    @Test
    public void testExecutorService_uncaughtExceptionHandler() throws InterruptedException {
        final CountDownLatch handlerCalled = new CountDownLatch(1);
        final ThreadFactory threadFactory =
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setUncaughtExceptionHandler((t, e) -> handlerCalled.countDown())
                        .build();
        final ExecutorService scheduledExecutorService =
                Executors.newSingleThreadExecutor(threadFactory);
        scheduledExecutorService.execute(
                () -> {
                    throw new RuntimeException("foo");
                });
        Assertions.assertTrue(                handlerCalled.await(TIMEOUT_MS, TimeUnit.MILLISECONDS),                "Expected handler to be called.");
    }

    @Test
    public void testScheduledExecutorService_uncaughtExceptionHandler()
            throws InterruptedException {
        final CountDownLatch handlerCalled = new CountDownLatch(1);
        final ThreadFactory threadFactory =
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setUncaughtExceptionHandler((t, e) -> handlerCalled.countDown())
                        .build();
        final ScheduledExecutorService scheduledExecutorService =
                Executors.newSingleThreadScheduledExecutor(threadFactory);
        scheduledExecutorService.execute(
                () -> {
                    throw new RuntimeException("foo");
                });
        Assertions.assertFalse(                handlerCalled.await(TIMEOUT_MS, TimeUnit.MILLISECONDS),                "Expected handler not to be called.");
    }

    // ------------------------------------------------------------------------
    // Test ScheduledFutures.
    // ------------------------------------------------------------------------

    @Test
    public void testWithUncaughtExceptionHandler_runtimeException() throws InterruptedException {
        final RuntimeException expected = new RuntimeException("foo");
        testWithUncaughtExceptionHandler(
                () -> {
                    throw expected;
                },
                expected);
    }

    @Test
    public void testWithUncaughtExceptionHandler_error() throws InterruptedException {
        final Error expected = new Error("foo");
        testWithUncaughtExceptionHandler(
                () -> {
                    throw expected;
                },
                expected);
    }

    private static void testWithUncaughtExceptionHandler(Runnable runnable, Throwable expected)
            throws InterruptedException {
        final ThreadFactory threadFactory =
                new ThreadFactoryBuilder().setDaemon(true).setNameFormat("ueh-test-%d").build();
        ScheduledExecutorService scheduledExecutorService =
                Executors.newSingleThreadScheduledExecutor(threadFactory);
        final AtomicReference<Thread> thread = new AtomicReference<>();
        final AtomicReference<Throwable> throwable = new AtomicReference<>();
        final CountDownLatch handlerCalled = new CountDownLatch(1);
        final Runnable guardedRunnable =
                Runnables.withUncaughtExceptionHandler(
                        runnable,
                        (t, e) -> {
                            thread.set(t);
                            throwable.set(e);
                            handlerCalled.countDown();
                        });
        scheduledExecutorService.execute(guardedRunnable);
        Assertions.assertTrue(handlerCalled.await(100, TimeUnit.MILLISECONDS));
        Assertions.assertNotNull(thread.get());
        Assertions.assertNotNull(throwable.get());
        Assertions.assertEquals("ueh-test-0", thread.get().getName());
        Assertions.assertEquals(expected.getClass(), throwable.get().getClass());
        Assertions.assertEquals("foo", throwable.get().getMessage());
    }
}
