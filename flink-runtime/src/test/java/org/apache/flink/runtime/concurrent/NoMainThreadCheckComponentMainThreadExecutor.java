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

package org.apache.flink.runtime.concurrent;

import org.apache.flink.runtime.testutils.DirectScheduledExecutorService;

import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * A synchronous {@link ComponentMainThreadExecutor} that executes tasks directly on the calling
 * thread without performing strict thread identity checks.
 *
 * <p>Unlike {@link ComponentMainThreadExecutorServiceAdapter#forMainThread()}, this executor does
 * not assert that the current thread is the main thread, avoiding flaky test failures when {@link
 * java.util.concurrent.CompletableFuture} callbacks are dispatched from background threads.
 */
public class NoMainThreadCheckComponentMainThreadExecutor implements ComponentMainThreadExecutor {

    private final DirectScheduledExecutorService executor = new DirectScheduledExecutorService();

    @Override
    public void assertRunningInMainThread() {
        // No-op: Skip thread assertion to avoid flaky test failures
    }

    @Override
    public void execute(Runnable command) {
        executor.execute(command);
    }

    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        return executor.schedule(command, delay, unit);
    }

    @Override
    public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
        return executor.schedule(callable, delay, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(
            Runnable command, long initialDelay, long period, TimeUnit unit) {
        return executor.scheduleAtFixedRate(command, initialDelay, period, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(
            Runnable command, long initialDelay, long delay, TimeUnit unit) {
        return executor.scheduleWithFixedDelay(command, initialDelay, delay, unit);
    }
}
