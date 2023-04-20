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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static final Logger LOG = LoggerFactory.getLogger(DefaultResourceCleaner.class);

    private final ComponentMainThreadExecutor mainThreadExecutor;
    private final Executor cleanupExecutor;
    private final CleanupFn<T> cleanupFn;

    private final Collection<CleanupWithLabel<T>> prioritizedCleanup;
    private final Collection<CleanupWithLabel<T>> regularCleanup;

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

        private final Collection<CleanupWithLabel<T>> prioritizedCleanup = new ArrayList<>();
        private final Collection<CleanupWithLabel<T>> regularCleanup = new ArrayList<>();

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
         * is added using {@link #withRegularCleanup(String, Object)} is started.
         *
         * @param label The label being used when logging errors in the given cleanup.
         * @param prioritizedCleanup The cleanup callback that is going to be prioritized.
         */
        public Builder<T> withPrioritizedCleanup(String label, T prioritizedCleanup) {
            this.prioritizedCleanup.add(new CleanupWithLabel<>(prioritizedCleanup, label));
            return this;
        }

        /**
         * Regular cleanups are resources for which the cleanup is triggered after all prioritized
         * cleanups succeeded. All added regular cleanups will run concurrently to each other.
         *
         * @param label The label being used when logging errors in the given cleanup.
         * @param regularCleanup The cleanup callback that is going to run after all prioritized
         *     cleanups are finished.
         * @see #withPrioritizedCleanup(String, Object)
         */
        public Builder<T> withRegularCleanup(String label, T regularCleanup) {
            this.regularCleanup.add(new CleanupWithLabel<>(regularCleanup, label));
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
            Collection<CleanupWithLabel<T>> prioritizedCleanup,
            Collection<CleanupWithLabel<T>> regularCleanup,
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
        for (CleanupWithLabel<T> cleanupWithLabel : prioritizedCleanup) {
            cleanupFuture =
                    cleanupFuture.thenCompose(
                            ignoredValue ->
                                    withRetry(
                                            jobId,
                                            cleanupWithLabel.getLabel(),
                                            cleanupWithLabel.getCleanup()));
        }

        return cleanupFuture.thenCompose(
                ignoredValue ->
                        FutureUtils.completeAll(
                                regularCleanup.stream()
                                        .map(
                                                cleanupWithLabel ->
                                                        withRetry(
                                                                jobId,
                                                                cleanupWithLabel.getLabel(),
                                                                cleanupWithLabel.getCleanup()))
                                        .collect(Collectors.toList())));
    }

    private CompletableFuture<Void> withRetry(JobID jobId, String label, T cleanup) {
        return FutureUtils.retryWithDelay(
                () ->
                        cleanupFn
                                .cleanupAsync(cleanup, jobId, cleanupExecutor)
                                .whenComplete(
                                        (value, throwable) -> {
                                            if (throwable != null) {
                                                final String logMessage =
                                                        String.format(
                                                                "Cleanup of %s failed for job %s due to a %s: %s",
                                                                label,
                                                                jobId,
                                                                throwable
                                                                        .getClass()
                                                                        .getSimpleName(),
                                                                throwable.getMessage());
                                                if (LOG.isTraceEnabled()) {
                                                    LOG.warn(logMessage, throwable);
                                                } else {
                                                    LOG.warn(logMessage);
                                                }
                                            }
                                        }),
                retryStrategy,
                mainThreadExecutor);
    }

    /**
     * {@code CleanupWithLabel} makes it possible to attach a label to a given cleanup that can be
     * used as human-readable representation of the corresponding cleanup.
     *
     * @param <CLEANUP_TYPE> The type of cleanup.
     */
    private static class CleanupWithLabel<CLEANUP_TYPE> {

        private final CLEANUP_TYPE cleanup;
        private final String label;

        public CleanupWithLabel(CLEANUP_TYPE cleanup, String label) {
            this.cleanup = cleanup;
            this.label = label;
        }

        public CLEANUP_TYPE getCleanup() {
            return cleanup;
        }

        public String getLabel() {
            return label;
        }
    }
}
