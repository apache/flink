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

package org.apache.flink.runtime.scheduler.adaptive;

import org.apache.flink.util.FlinkException;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.SupplierWithException;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

/**
 * BackgroundTask encapsulates an asynchronous operation which can produce a result. The result can
 * be accessed via {@link BackgroundTask#getResultFuture()}. Additionally, the task allows to track
 * its completion via {@link BackgroundTask#getTerminationFuture()}.
 *
 * <p>In order to ensure the order of background tasks, one can use the {@link
 * BackgroundTask#runAfter} to schedule tasks which are executed after this task has completed.
 * Tasks which are executed sequentially like this won't be affected by the outcome of previous
 * tasks. This means that a failed task won't stop succeeding tasks from being executed.
 *
 * @param <T> type of the produced result
 */
final class BackgroundTask<T> {
    private final CompletableFuture<Void> terminationFuture;

    private final CompletableFuture<T> resultFuture;

    private volatile boolean isAborted = false;

    private BackgroundTask(
            CompletableFuture<Void> previousTerminationFuture,
            SupplierWithException<? extends T, ? extends Exception> task,
            Executor executor) {
        resultFuture =
                previousTerminationFuture.thenApplyAsync(
                        ignored -> {
                            if (!isAborted) {
                                try {
                                    return task.get();
                                } catch (Exception exception) {
                                    throw new CompletionException(exception);
                                }
                            } else {
                                throw new CompletionException(
                                        new FlinkException("Background task has been aborted."));
                            }
                        },
                        executor);

        terminationFuture = resultFuture.handle((ignored, ignoredThrowable) -> null);
    }

    private BackgroundTask() {
        terminationFuture = FutureUtils.completedVoidFuture();
        resultFuture =
                FutureUtils.completedExceptionally(
                        new FlinkException(
                                "No result has been created because it is a finished background task."));
    }

    /**
     * Abort the execution of this background task. This method has only an effect if the background
     * task has not been started yet.
     */
    void abort() {
        isAborted = true;
    }

    public CompletableFuture<T> getResultFuture() {
        return resultFuture;
    }

    CompletableFuture<Void> getTerminationFuture() {
        return terminationFuture;
    }

    /**
     * Runs the given task after this background task has completed (normally or exceptionally).
     *
     * @param task task to run after this background task has completed
     * @param executor executor to run the task
     * @param <V> type of the result
     * @return new {@link BackgroundTask} representing the new task to execute
     */
    <V> BackgroundTask<V> runAfter(
            SupplierWithException<? extends V, ? extends Exception> task, Executor executor) {
        return new BackgroundTask<>(terminationFuture, task, executor);
    }

    /**
     * Creates a finished background task which can be used as the start of a background task chain.
     *
     * @param <V> type of the background task
     * @return A finished background task
     */
    static <V> BackgroundTask<V> finishedBackgroundTask() {
        return new BackgroundTask<>();
    }

    /**
     * Creates an initial background task. This means that this background task has no predecessor.
     *
     * @param task task to run
     * @param executor executor to run the task
     * @param <V> type of the result
     * @return initial {@link BackgroundTask} representing the task to execute
     */
    static <V> BackgroundTask<V> initialBackgroundTask(
            SupplierWithException<? extends V, ? extends Exception> task, Executor executor) {
        return new BackgroundTask<>(FutureUtils.completedVoidFuture(), task, executor);
    }
}
