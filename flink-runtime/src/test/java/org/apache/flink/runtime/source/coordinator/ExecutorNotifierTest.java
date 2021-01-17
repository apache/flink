/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package org.apache.flink.runtime.source.coordinator;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/** Unit tests for ExecutorNotifier. */
public class ExecutorNotifierTest {
    private ScheduledExecutorService workerExecutor;
    private ExecutorService executorToNotify;
    private ExecutorNotifier notifier;
    private Throwable exceptionInHandler;
    private CountDownLatch exceptionInHandlerLatch;

    @Before
    public void setup() {
        this.exceptionInHandler = null;
        this.exceptionInHandlerLatch = new CountDownLatch(1);
        this.workerExecutor =
                Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "worker-thread"));
        this.executorToNotify =
                Executors.newSingleThreadExecutor(
                        r -> {
                            Thread t = new Thread(r, "main-thread");
                            t.setUncaughtExceptionHandler(
                                    (thread, e) -> {
                                        exceptionInHandler = e;
                                        exceptionInHandlerLatch.countDown();
                                    });
                            return t;
                        });
        this.notifier = new ExecutorNotifier(this.workerExecutor, this.executorToNotify);
    }

    @After
    public void tearDown() throws InterruptedException {
        notifier.close();
        closeExecutorToNotify();
    }

    @Test
    public void testBasic() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger result = new AtomicInteger(0);
        notifier.notifyReadyAsync(
                () -> 1234,
                (v, e) -> {
                    result.set(v);
                    latch.countDown();
                });
        latch.await();
        closeExecutorToNotify();
        assertEquals(1234, result.get());
    }

    @Test
    public void testExceptionInCallable() {
        Exception exception = new Exception("Expected exception.");
        notifier.notifyReadyAsync(
                () -> {
                    throw exception;
                },
                (v, e) -> {
                    assertEquals(exception, e);
                    assertNull(v);
                });
    }

    @Test
    public void testExceptionInHandlerWhenHandlingException() throws InterruptedException {
        Exception exception1 = new Exception("Expected exception.");
        RuntimeException exception2 = new RuntimeException("Expected exception.");
        CountDownLatch latch = new CountDownLatch(1);
        notifier.notifyReadyAsync(
                () -> {
                    throw exception1;
                },
                (v, e) -> {
                    assertEquals(exception1, e);
                    assertNull(v);
                    latch.countDown();
                    throw exception2;
                });
        latch.await();
        closeExecutorToNotify();
        // The uncaught exception handler may fire after the executor has shutdown.
        // We need to wait on the countdown latch here.
        exceptionInHandlerLatch.await(10000L, TimeUnit.MILLISECONDS);
        assertEquals(exception2, exceptionInHandler);
    }

    @Test
    public void testExceptionInHandlerWhenHandlingResult() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        RuntimeException exception = new RuntimeException("Expected exception.");
        notifier.notifyReadyAsync(
                () -> 1234,
                (v, e) -> {
                    latch.countDown();
                    throw exception;
                });
        latch.await();
        closeExecutorToNotify();
        // The uncaught exception handler may fire after the executor has shutdown.
        // We need to wait on the countdown latch here.
        exceptionInHandlerLatch.await(10000L, TimeUnit.MILLISECONDS);
        assertEquals(exception, exceptionInHandler);
    }

    private void closeExecutorToNotify() throws InterruptedException {
        executorToNotify.shutdown();
        executorToNotify.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }
}
