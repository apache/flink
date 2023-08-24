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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.runtime.operators.coordination.ComponentClosingUtils.shutdownExecutorForcefully;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for ExecutorNotifier. */
class ExecutorNotifierTest {
    private ScheduledExecutorService workerExecutor;
    private ExecutorService executorToNotify;
    private ExecutorNotifier notifier;
    private Throwable exceptionInHandler;
    private CountDownLatch exceptionInHandlerLatch;

    @BeforeEach
    void setup() {
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

    @AfterEach
    void tearDown() throws InterruptedException {
        shutdownExecutorForcefully(workerExecutor, Duration.ofNanos(Long.MAX_VALUE));
        shutdownExecutorForcefully(executorToNotify, Duration.ofNanos(Long.MAX_VALUE));
    }

    @Test
    void testBasic() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger result = new AtomicInteger(0);
        notifier.notifyReadyAsync(
                () -> 1234,
                (v, e) -> {
                    result.set(v);
                    latch.countDown();
                });
        latch.await();
        assertThat(result.get()).isEqualTo(1234);
    }

    @Test
    void testExceptionInCallable() {
        Exception exception = new Exception("Expected exception.");
        notifier.notifyReadyAsync(
                () -> {
                    throw exception;
                },
                (v, e) -> {
                    assertThat(e).isEqualTo(exception);
                    assertThat(v).isNull();
                });
    }

    @Test
    void testExceptionInHandlerWhenHandlingException() throws InterruptedException {
        Exception exception1 = new Exception("Expected exception.");
        RuntimeException exception2 = new RuntimeException("Expected exception.");
        CountDownLatch latch = new CountDownLatch(1);
        notifier.notifyReadyAsync(
                () -> {
                    throw exception1;
                },
                (v, e) -> {
                    assertThat(e).isEqualTo(exception1);
                    assertThat(v).isNull();
                    latch.countDown();
                    throw exception2;
                });
        latch.await();
        // The uncaught exception handler may fire after the executor has shutdown.
        // We need to wait on the countdown latch here.
        exceptionInHandlerLatch.await(10000L, TimeUnit.MILLISECONDS);
        assertThat(exceptionInHandler).isEqualTo(exception2);
    }

    @Test
    void testExceptionInHandlerWhenHandlingResult() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        RuntimeException exception = new RuntimeException("Expected exception.");
        notifier.notifyReadyAsync(
                () -> 1234,
                (v, e) -> {
                    latch.countDown();
                    throw exception;
                });
        latch.await();
        // The uncaught exception handler may fire after the executor has shutdown.
        // We need to wait on the countdown latch here.
        exceptionInHandlerLatch.await(10000L, TimeUnit.MILLISECONDS);
        assertThat(exceptionInHandler).isEqualTo(exception);
    }
}
