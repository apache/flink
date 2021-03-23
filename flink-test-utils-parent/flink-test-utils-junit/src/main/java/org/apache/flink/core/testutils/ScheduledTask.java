/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.core.testutils;

import javax.annotation.Nonnull;

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * ScheduledTask represents a task which is executed at a later point in time.
 *
 * @param <T> type of the result
 */
public final class ScheduledTask<T> implements ScheduledFuture<T> {

    private final Callable<T> callable;

    private final long delay;

    private final long period;

    private final CompletableFuture<T> result;

    public ScheduledTask(Callable<T> callable, long delay) {
        this(callable, delay, 0);
    }

    public ScheduledTask(Callable<T> callable, long delay, long period) {
        this.callable = Objects.requireNonNull(callable);
        this.result = new CompletableFuture<>();
        this.delay = delay;
        this.period = period;
    }

    private boolean isPeriodic() {
        return period > 0;
    }

    public void execute() {
        if (!result.isDone()) {
            if (!isPeriodic()) {
                try {
                    result.complete(callable.call());
                } catch (Exception e) {
                    result.completeExceptionally(e);
                }
            } else {
                try {
                    callable.call();
                } catch (Exception e) {
                    result.completeExceptionally(e);
                }
            }
        }
    }

    @Override
    public long getDelay(TimeUnit unit) {
        return unit.convert(delay, TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed o) {
        return Long.compare(getDelay(TimeUnit.MILLISECONDS), o.getDelay(TimeUnit.MILLISECONDS));
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return result.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
        return result.isCancelled();
    }

    @Override
    public boolean isDone() {
        return result.isDone();
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
        return result.get();
    }

    @Override
    public T get(long timeout, @Nonnull TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        return result.get(timeout, unit);
    }
}
