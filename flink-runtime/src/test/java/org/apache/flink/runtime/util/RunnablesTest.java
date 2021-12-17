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

import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava30.com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class RunnablesTest extends TestLogger {

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
        final ExecutorService executorService = Executors.newSingleThreadExecutor(threadFactory);
        executorService.execute(
                () -> {
                    throw new RuntimeException("foo");
                });

        // expect handler to be called
        handlerCalled.await();
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
        Assert.assertFalse(
                "Expected handler not to be called.",
                handlerCalled.await(TIMEOUT_MS, TimeUnit.MILLISECONDS));
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
        Assert.assertTrue(handlerCalled.await(100, TimeUnit.MILLISECONDS));
        Assert.assertNotNull(thread.get());
        Assert.assertNotNull(throwable.get());
        Assert.assertEquals("ueh-test-0", thread.get().getName());
        Assert.assertEquals(expected.getClass(), throwable.get().getClass());
        Assert.assertEquals("foo", throwable.get().getMessage());
    }
}
