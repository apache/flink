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

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

/** {@code DefaultResourceCleaner} is the default implementation of {@link ResourceCleaner}. */
public class DefaultResourceCleaner<T> implements ResourceCleaner {

    private final ComponentMainThreadExecutor mainThreadExecutor;
    private final Executor cleanupExecutor;
    private final CleanupFn<T> cleanupFn;

    private final Collection<T> prioritizedCleanup;
    private final Collection<T> regularCleanup;

    public static Builder<LocallyCleanableResource> forLocallyCleanableResources(
            ComponentMainThreadExecutor mainThreadExecutor, Executor cleanupExecutor) {
        return forCleanableResources(
                mainThreadExecutor, cleanupExecutor, LocallyCleanableResource::localCleanupAsync);
    }

    public static Builder<GloballyCleanableResource> forGloballyCleanableResources(
            ComponentMainThreadExecutor mainThreadExecutor, Executor cleanupExecutor) {
        return forCleanableResources(
                mainThreadExecutor, cleanupExecutor, GloballyCleanableResource::globalCleanupAsync);
    }

    @VisibleForTesting
    static <T> Builder<T> forCleanableResources(
            ComponentMainThreadExecutor mainThreadExecutor,
            Executor cleanupExecutor,
            CleanupFn<T> cleanupFunction) {
        return new Builder<>(mainThreadExecutor, cleanupExecutor, cleanupFunction);
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

        private final Collection<T> prioritizedCleanup = new ArrayList<>();
        private final Collection<T> regularCleanup = new ArrayList<>();

        private Builder(
                ComponentMainThreadExecutor mainThreadExecutor,
                Executor cleanupExecutor,
                CleanupFn<T> cleanupFn) {
            this.mainThreadExecutor = mainThreadExecutor;
            this.cleanupExecutor = cleanupExecutor;
            this.cleanupFn = cleanupFn;
        }

        public Builder<T> withPrioritizedCleanup(T prioritizedCleanup) {
            this.prioritizedCleanup.add(prioritizedCleanup);
            return this;
        }

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
                    regularCleanup);
        }
    }

    private DefaultResourceCleaner(
            ComponentMainThreadExecutor mainThreadExecutor,
            Executor cleanupExecutor,
            CleanupFn<T> cleanupFn,
            Collection<T> prioritizedCleanup,
            Collection<T> regularCleanup) {
        this.mainThreadExecutor = mainThreadExecutor;
        this.cleanupExecutor = cleanupExecutor;
        this.cleanupFn = cleanupFn;
        this.prioritizedCleanup = prioritizedCleanup;
        this.regularCleanup = regularCleanup;
    }

    @Override
    public CompletableFuture<Void> cleanupAsync(JobID jobId) {
        mainThreadExecutor.assertRunningInMainThread();
        CompletableFuture<Void> cleanupFuture = FutureUtils.completedVoidFuture();
        for (T cleanup : prioritizedCleanup) {
            cleanupFuture =
                    cleanupFuture.thenCompose(
                            ignoredValue ->
                                    cleanupFn.cleanupAsync(cleanup, jobId, cleanupExecutor));
        }
        return cleanupFuture.thenCompose(
                ignoredValue ->
                        FutureUtils.completeAll(
                                regularCleanup.stream()
                                        .map(
                                                cleanup ->
                                                        cleanupFn.cleanupAsync(
                                                                cleanup, jobId, cleanupExecutor))
                                        .collect(Collectors.toList())));
    }
}
