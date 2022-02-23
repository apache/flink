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

package org.apache.flink.runtime.dispatcher.cleanup;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.RetryStrategy;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

/**
 * {@code DefaultResourceCleaner} is the default implementation of {@link ResourceCleaner}. It will
 * try to clean up any resource that was added. Failure will result in an individual retry of the
 * cleanup. The overall cleanup result succeeds after all subtasks succeeded.
 */
public class DefaultResourceCleaner<T> implements ResourceCleaner {

    private final ComponentMainThreadExecutor mainThreadExecutor;
    private final Executor cleanupExecutor;
    private final CleanupFn<T> cleanupFn;

    private final Collection<T> prioritizedCleanup;
    private final Collection<T> regularCleanup;

    private final RetryStrategy retryStrategy;

    public static Builder<LocallyCleanableResource> forLocallyCleanableResources(
            ComponentMainThreadExecutor mainThreadExecutor,
            Executor cleanupExecutor,
            RetryStrategy retryStrategy) {
        return forCleanableResources(
                mainThreadExecutor,
                cleanupExecutor,
                LocallyCleanableResource::localCleanupAsync,
                retryStrategy);
    }

    public static Builder<GloballyCleanableResource> forGloballyCleanableResources(
            ComponentMainThreadExecutor mainThreadExecutor,
            Executor cleanupExecutor,
            RetryStrategy retryStrategy) {
        return forCleanableResources(
                mainThreadExecutor,
                cleanupExecutor,
                GloballyCleanableResource::globalCleanupAsync,
                retryStrategy);
    }

    @VisibleForTesting
    static <T> Builder<T> forCleanableResources(
            ComponentMainThreadExecutor mainThreadExecutor,
            Executor cleanupExecutor,
            CleanupFn<T> cleanupFunction,
            RetryStrategy retryStrategy) {
        return new Builder<>(mainThreadExecutor, cleanupExecutor, cleanupFunction, retryStrategy);
    }

    @VisibleForTesting
    @FunctionalInterface
    interface CleanupFn<T> {
        CompletableFuture<Void> cleanupAsync(T resource, JobID jobId, Executor cleanupExecutor);
    }

    /**
     * {@code Builder} for creating {@code DefaultResourceCleaner} instances.
     *
     * @param <T> The functional interface that's being translated into the internally used {@link
     *     CleanupFn}.
     */
    public static class Builder<T> {

        private final ComponentMainThreadExecutor mainThreadExecutor;
        private final Executor cleanupExecutor;
        private final CleanupFn<T> cleanupFn;

        private final RetryStrategy retryStrategy;

        private final Collection<T> prioritizedCleanup = new ArrayList<>();
        private final Collection<T> regularCleanup = new ArrayList<>();

        private Builder(
                ComponentMainThreadExecutor mainThreadExecutor,
                Executor cleanupExecutor,
                CleanupFn<T> cleanupFn,
                RetryStrategy retryStrategy) {
            this.mainThreadExecutor = mainThreadExecutor;
            this.cleanupExecutor = cleanupExecutor;
            this.cleanupFn = cleanupFn;
            this.retryStrategy = retryStrategy;
        }

        /**
         * Prioritized cleanups run before their regular counterparts. This method enables the
         * caller to model dependencies between cleanup tasks. The order in which cleanable
         * resources are added matters, i.e. if two cleanable resources are added as prioritized
         * cleanup tasks, the resource being added first will block the cleanup of the second
         * resource. All prioritized cleanup resources will run and finish before any resource that
         * is added using {@link #withRegularCleanup(Object)} is started.
         */
        public Builder<T> withPrioritizedCleanup(T prioritizedCleanup) {
            this.prioritizedCleanup.add(prioritizedCleanup);
            return this;
        }

        /**
         * Regular cleanups are resources for which the cleanup is triggered after all prioritized
         * cleanups succeeded. All added regular cleanups will run concurrently to each other.
         *
         * @see #withPrioritizedCleanup(Object)
         */
        public Builder<T> withRegularCleanup(T regularCleanup) {
            this.regularCleanup.add(regularCleanup);
            return this;
        }

        public DefaultResourceCleaner<T> build() {
            return new DefaultResourceCleaner<>(
                    mainThreadExecutor,
                    cleanupExecutor,
                    cleanupFn,
                    prioritizedCleanup,
                    regularCleanup,
                    retryStrategy);
        }
    }

    private DefaultResourceCleaner(
            ComponentMainThreadExecutor mainThreadExecutor,
            Executor cleanupExecutor,
            CleanupFn<T> cleanupFn,
            Collection<T> prioritizedCleanup,
            Collection<T> regularCleanup,
            RetryStrategy retryStrategy) {
        this.mainThreadExecutor = mainThreadExecutor;
        this.cleanupExecutor = cleanupExecutor;
        this.cleanupFn = cleanupFn;
        this.prioritizedCleanup = prioritizedCleanup;
        this.regularCleanup = regularCleanup;
        this.retryStrategy = retryStrategy;
    }

    @Override
    public CompletableFuture<Void> cleanupAsync(JobID jobId) {
        mainThreadExecutor.assertRunningInMainThread();

        CompletableFuture<Void> cleanupFuture = FutureUtils.completedVoidFuture();
        for (T cleanup : prioritizedCleanup) {
            cleanupFuture = cleanupFuture.thenCompose(ignoredValue -> withRetry(jobId, cleanup));
        }

        return cleanupFuture.thenCompose(
                ignoredValue ->
                        FutureUtils.completeAll(
                                regularCleanup.stream()
                                        .map(cleanup -> withRetry(jobId, cleanup))
                                        .collect(Collectors.toList())));
    }

    private CompletableFuture<Void> withRetry(JobID jobId, T cleanup) {
        return FutureUtils.retryWithDelay(
                () -> cleanupFn.cleanupAsync(cleanup, jobId, cleanupExecutor),
                retryStrategy,
                mainThreadExecutor);
    }
}
