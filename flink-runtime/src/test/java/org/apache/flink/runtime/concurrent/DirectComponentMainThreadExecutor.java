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

import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.Executors;

import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * A test implementation of the {@link ComponentMainThreadExecutor} that executes any immediate
 * tasks right away but collects the scheduled tasks to trigger them from within the test.
 */
public class DirectComponentMainThreadExecutor implements ComponentMainThreadExecutor {

    private Map<CompletableFuture<Void>, Future<?>> scheduledTasks = new LinkedHashMap<>();

    private final Thread thread;
    private final Executor directExecutor = Executors.directExecutor();

    public DirectComponentMainThreadExecutor() {
        this(Thread.currentThread());
    }

    public DirectComponentMainThreadExecutor(Thread thread) {
        this.thread = thread;
    }

    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        return schedule(
                () -> {
                    command.run();
                    return null;
                },
                delay,
                unit);
    }

    @Override
    public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
        final CompletableFuture<Void> scheduleTriggerFuture = new CompletableFuture<>();
        final ScheduledFuture<V> scheduledFuture =
                new ScheduledFutureAdapter<>(
                        scheduleTriggerFuture.thenApply(
                                ignored -> {
                                    try {
                                        return callable.call();
                                    } catch (Exception e) {
                                        throw new CompletionException(e);
                                    }
                                }),
                        delay,
                        unit);

        scheduledTasks.put(scheduleTriggerFuture, scheduledFuture);
        return scheduledFuture;
    }

    public Collection<Future<?>> getScheduledTasks() {
        return scheduledTasks.values();
    }

    public void triggerAllScheduledTasks() {
        // re-instantiate a new instance that can collect scheduled tasks while the older scheduled
        // tasks are triggered
        final Map<CompletableFuture<Void>, Future<?>> tasksToExecute = scheduledTasks;
        scheduledTasks = new LinkedHashMap<>();
        tasksToExecute.keySet().forEach(ft -> ft.complete(null));
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(
            Runnable command, long initialDelay, long period, TimeUnit unit) {
        throw new UnsupportedOperationException(
                "This method is not supported analogously to RpcEndpoint#MainThreadExecutor.");
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(
            Runnable command, long initialDelay, long delay, TimeUnit unit) {
        throw new UnsupportedOperationException(
                "This method is not supported analogously to RpcEndpoint#MainThreadExecutor.");
    }

    @Override
    public void assertRunningInMainThread() {
        Preconditions.checkState(Thread.currentThread() == this.thread);
    }

    @Override
    public void execute(@NotNull Runnable command) {
        directExecutor.execute(command);
    }
}
