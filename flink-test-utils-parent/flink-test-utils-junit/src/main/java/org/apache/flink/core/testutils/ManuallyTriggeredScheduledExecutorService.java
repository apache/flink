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

package org.apache.flink.core.testutils;

import javax.annotation.Nonnull;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Simple {@link ScheduledExecutorService} implementation for testing purposes. It spawns no
 * threads, but lets you trigger the execution of tasks manually.
 *
 * <p>This class is helpful when implementing tests tasks synchronous and control when they run,
 * which would otherwise asynchronous and require complex triggers and latches to test.
 */
public class ManuallyTriggeredScheduledExecutorService implements ScheduledExecutorService {

    private final ArrayDeque<Runnable> queuedRunnables = new ArrayDeque<>();

    private final ConcurrentLinkedQueue<ScheduledTask<?>> nonPeriodicScheduledTasks =
            new ConcurrentLinkedQueue<>();

    private final ConcurrentLinkedQueue<ScheduledTask<?>> periodicScheduledTasks =
            new ConcurrentLinkedQueue<>();

    private boolean shutdown;

    // ------------------------------------------------------------------------
    //  (scheduled) execution
    // ------------------------------------------------------------------------

    @Override
    public void execute(@Nonnull Runnable command) {
        synchronized (queuedRunnables) {
            queuedRunnables.addLast(command);
        }
    }

    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        return insertNonPeriodicTask(command, delay, unit);
    }

    @Override
    public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
        return insertNonPeriodicTask(callable, delay, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(
            Runnable command, long initialDelay, long period, TimeUnit unit) {
        return insertPeriodicRunnable(command, initialDelay, period, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(
            Runnable command, long initialDelay, long delay, TimeUnit unit) {
        return insertPeriodicRunnable(command, initialDelay, delay, unit);
    }

    // ------------------------------------------------------------------------
    //  service shutdown
    // ------------------------------------------------------------------------

    @Override
    public void shutdown() {
        shutdown = true;
    }

    @Override
    public List<Runnable> shutdownNow() {
        shutdown();
        return Collections.emptyList();
    }

    @Override
    public boolean isShutdown() {
        return false;
    }

    @Override
    public boolean isTerminated() {
        return shutdown;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return true;
    }

    // ------------------------------------------------------------------------
    //  non-implemented future task methods
    // ------------------------------------------------------------------------

    @Override
    public <T> Future<T> submit(Callable<T> task) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Future<?> submit(Runnable task) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> List<Future<T>> invokeAll(
            Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) {
        throw new UnsupportedOperationException();
    }

    // ------------------------------------------------------------------------
    // Execution triggering and access to the queued tasks
    // ------------------------------------------------------------------------

    /**
     * Executes all runnable and scheduled non-periodic tasks until none are left to run. This is
     * essentially a combination of {@link #triggerAll()} and {@link
     * #triggerNonPeriodicScheduledTasks()} that allows making a test agnostic of how exactly a
     * runnable is passed to the executor.
     */
    public void triggerAllNonPeriodicTasks() {
        while (numQueuedRunnables() > 0 || !nonPeriodicScheduledTasks.isEmpty()) {
            triggerAll();
            triggerNonPeriodicScheduledTasks();
        }
    }

    /** Triggers all {@code queuedRunnables}. */
    public void triggerAll() {
        while (numQueuedRunnables() > 0) {
            trigger();
        }
    }

    /**
     * Triggers the next queued runnable and executes it synchronously. This method throws an
     * exception if no Runnable is currently queued.
     */
    public void trigger() {
        final Runnable next;

        synchronized (queuedRunnables) {
            next = queuedRunnables.removeFirst();
        }

        next.run();
    }

    /** Gets the number of Runnables currently queued. */
    public int numQueuedRunnables() {
        synchronized (queuedRunnables) {
            return queuedRunnables.size();
        }
    }

    public Collection<ScheduledFuture<?>> getActiveScheduledTasks() {
        final ArrayList<ScheduledFuture<?>> scheduledTasks =
                new ArrayList<>(nonPeriodicScheduledTasks.size() + periodicScheduledTasks.size());
        scheduledTasks.addAll(getActiveNonPeriodicScheduledTask());
        scheduledTasks.addAll(getActivePeriodicScheduledTask());
        return scheduledTasks;
    }

    public Collection<ScheduledFuture<?>> getActivePeriodicScheduledTask() {
        return periodicScheduledTasks.stream()
                .filter(scheduledTask -> !scheduledTask.isCancelled())
                .collect(Collectors.toList());
    }

    public Collection<ScheduledFuture<?>> getActiveNonPeriodicScheduledTask() {
        return nonPeriodicScheduledTasks.stream()
                .filter(scheduledTask -> !scheduledTask.isCancelled())
                .collect(Collectors.toList());
    }

    public List<ScheduledFuture<?>> getAllScheduledTasks() {
        final ArrayList<ScheduledFuture<?>> scheduledTasks =
                new ArrayList<>(nonPeriodicScheduledTasks.size() + periodicScheduledTasks.size());
        scheduledTasks.addAll(getAllNonPeriodicScheduledTask());
        scheduledTasks.addAll(getAllPeriodicScheduledTask());
        return scheduledTasks;
    }

    public List<ScheduledFuture<?>> getAllPeriodicScheduledTask() {
        return new ArrayList<>(periodicScheduledTasks);
    }

    public List<ScheduledFuture<?>> getAllNonPeriodicScheduledTask() {
        return new ArrayList<>(nonPeriodicScheduledTasks);
    }

    /** Triggers all registered tasks. */
    public void triggerScheduledTasks() {
        triggerPeriodicScheduledTasks();
        triggerNonPeriodicScheduledTasks();
    }

    /**
     * Triggers a single non-periodically scheduled task.
     *
     * @throws NoSuchElementException If there is no such task.
     */
    public void triggerNonPeriodicScheduledTask() {
        final ScheduledTask<?> poll = nonPeriodicScheduledTasks.remove();
        if (poll != null) {
            poll.execute();
        }
    }

    /**
     * Triggers all non-periodically scheduled tasks. In contrast to {@link
     * #triggerNonPeriodicScheduledTasks()}, if such a task schedules another non-periodically
     * schedule task, then this new task will also be triggered.
     */
    public void triggerNonPeriodicScheduledTasksWithRecursion() {
        while (!nonPeriodicScheduledTasks.isEmpty()) {
            final ScheduledTask<?> scheduledTask = nonPeriodicScheduledTasks.poll();

            if (!scheduledTask.isCancelled()) {
                scheduledTask.execute();
            }
        }
    }

    public void triggerNonPeriodicScheduledTasks() {
        final Iterator<ScheduledTask<?>> iterator = nonPeriodicScheduledTasks.iterator();

        while (iterator.hasNext()) {
            final ScheduledTask<?> scheduledTask = iterator.next();

            if (!scheduledTask.isCancelled()) {
                scheduledTask.execute();
            }
            iterator.remove();
        }
    }

    public void triggerPeriodicScheduledTasks() {
        for (ScheduledTask<?> scheduledTask : periodicScheduledTasks) {
            if (!scheduledTask.isCancelled()) {
                scheduledTask.execute();
            }
        }
    }

    private ScheduledFuture<?> insertPeriodicRunnable(
            Runnable command, long delay, long period, TimeUnit unit) {

        final ScheduledTask<?> scheduledTask =
                new ScheduledTask<>(
                        () -> {
                            command.run();
                            return null;
                        },
                        unit.convert(delay, TimeUnit.MILLISECONDS),
                        unit.convert(period, TimeUnit.MILLISECONDS));

        periodicScheduledTasks.offer(scheduledTask);

        return scheduledTask;
    }

    private ScheduledFuture<?> insertNonPeriodicTask(Runnable command, long delay, TimeUnit unit) {
        return insertNonPeriodicTask(
                () -> {
                    command.run();
                    return null;
                },
                delay,
                unit);
    }

    private <V> ScheduledFuture<V> insertNonPeriodicTask(
            Callable<V> callable, long delay, TimeUnit unit) {
        final ScheduledTask<V> scheduledTask =
                new ScheduledTask<>(callable, unit.convert(delay, TimeUnit.MILLISECONDS));

        nonPeriodicScheduledTasks.offer(scheduledTask);

        return scheduledTask;
    }
}
