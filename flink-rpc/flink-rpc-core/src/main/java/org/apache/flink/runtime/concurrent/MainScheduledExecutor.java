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

import org.apache.flink.runtime.rpc.MainThreadExecutable;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.Closeable;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * The main scheduled executor will manage the scheduled tasks. When the specified time arrives, the
 * executor will send these tasks to gateway and execute them.
 */
public class MainScheduledExecutor implements ScheduledExecutor, Closeable {
    private static final Logger log = LoggerFactory.getLogger(MainScheduledExecutor.class);

    private final ScheduledExecutorService scheduledExecutorService;
    private final MainThreadExecutable gateway;

    public MainScheduledExecutor(MainThreadExecutable gateway) {
        this.gateway = gateway;
        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    }

    /**
     * {@link ScheduledExecutorService} manages the task and sends it to the gateway after the given
     * delay.
     *
     * @param command the task to execute in the future
     * @param delay the time from now to delay the execution
     * @param unit the time unit of the delay parameter
     * @return a ScheduledFuture representing the completion of the scheduled task
     */
    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        if (scheduledExecutorService.isShutdown()) {
            log.warn("The scheduled executor for periodic tasks is shutdown.");
            return ThrowingScheduledFuture.create();
        } else {
            return this.scheduledExecutorService.schedule(
                    () -> gateway.runAsync(command), delay, unit);
        }
    }

    /**
     * {@link ScheduledExecutorService} manages the given callable and sends it to the gateway after
     * the given delay. The result of the callable is returned as a {@link ScheduledFuture}.
     *
     * @param callable the callable to execute
     * @param delay the time from now to delay the execution
     * @param unit the time unit of the delay parameter
     * @param <V> result type of the callable
     * @return a ScheduledFuture which holds the future value of the given callable
     */
    @Override
    public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
        if (scheduledExecutorService.isShutdown()) {
            log.warn("The scheduled executor for periodic tasks is shutdown.");
            return ThrowingScheduledFuture.create();
        } else {
            final long delayMillis = TimeUnit.MILLISECONDS.convert(delay, unit);
            FutureTask<V> ft = new FutureTask<>(callable);
            this.scheduledExecutorService.schedule(() -> gateway.runAsync(ft), delay, unit);
            return new ScheduledFutureAdapter<>(ft, delayMillis, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(
            Runnable command, long initialDelay, long period, TimeUnit unit) {
        throw new UnsupportedOperationException(
                "Not implemented because the method is currently not required.");
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(
            Runnable command, long initialDelay, long delay, TimeUnit unit) {
        throw new UnsupportedOperationException(
                "Not implemented because the method is currently not required.");
    }

    @Override
    public void execute(@Nonnull Runnable command) {
        throw new UnsupportedOperationException(
                "Not implemented because the method is currently not required.");
    }

    /** Shutdown the {@link ScheduledExecutorService} and remove all the pending tasks. */
    @Override
    public void close() {
        scheduledExecutorService.shutdownNow().clear();
    }
}
