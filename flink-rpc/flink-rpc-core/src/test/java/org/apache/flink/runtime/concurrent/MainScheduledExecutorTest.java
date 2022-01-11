/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.concurrent;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.rpc.MainThreadExecutable;

import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Unit tests for {@link MainScheduledExecutor}. */
public class MainScheduledExecutorTest {
    /** Test schedule runnable. */
    @Test
    public void testScheduleRunnable() throws Exception {
        MainScheduledExecutor mainScheduledExecutor =
                new MainScheduledExecutor(new TestRunnableMainThreadExecutable());
        final int timeDelay = 1;
        CompletableFuture<Integer> future = new CompletableFuture<>();
        mainScheduledExecutor.schedule(
                () -> {
                    future.complete(timeDelay);
                },
                timeDelay,
                TimeUnit.SECONDS);
        assertEquals(timeDelay, (int) future.get(timeDelay * 2, TimeUnit.SECONDS));
    }

    /** Test schedule runnable after close. */
    @Test
    public void testScheduleRunnableAfterClose() {
        MainScheduledExecutor mainScheduledExecutor =
                new MainScheduledExecutor(new TestRunnableMainThreadExecutable());
        final int timeDelay = 1;
        CompletableFuture<Integer> future = new CompletableFuture<>();
        mainScheduledExecutor.close();
        ScheduledFuture<?> scheduledFuture =
                mainScheduledExecutor.schedule(
                        () -> {
                            future.complete(timeDelay);
                        },
                        timeDelay,
                        TimeUnit.SECONDS);
        assertTrue(scheduledFuture instanceof ThrowingScheduledFuture);
    }

    /** Test schedule callable. */
    @Test
    public void testScheduleCallable() throws Exception {
        MainScheduledExecutor mainScheduledExecutor =
                new MainScheduledExecutor(new TestRunnableMainThreadExecutable());
        final int timeDelay = 1;
        ScheduledFuture<Integer> scheduledFuture =
                mainScheduledExecutor.schedule(() -> timeDelay, timeDelay, TimeUnit.SECONDS);
        assertEquals(timeDelay, (int) scheduledFuture.get(timeDelay * 2, TimeUnit.SECONDS));
    }

    /** Test schedule callable after close. */
    @Test
    public void testScheduleCallableAfterClose() {
        MainScheduledExecutor mainScheduledExecutor =
                new MainScheduledExecutor(new TestRunnableMainThreadExecutable());
        mainScheduledExecutor.close();
        final int timeDelay = 1;
        ScheduledFuture<Integer> scheduledFuture =
                mainScheduledExecutor.schedule(() -> timeDelay, timeDelay, TimeUnit.SECONDS);
        assertTrue(scheduledFuture instanceof ThrowingScheduledFuture);
    }

    private static class TestRunnableMainThreadExecutable implements MainThreadExecutable {
        @Override
        public void runAsync(Runnable runnable) {
            runnable.run();
        }

        @Override
        public <V> CompletableFuture<V> callAsync(Callable<V> callable, Time callTimeout) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void scheduleRunAsync(Runnable runnable, long delay) {
            throw new UnsupportedOperationException();
        }
    }
}
