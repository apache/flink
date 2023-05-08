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

package org.apache.flink.runtime.rest.handler.async;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.util.AutoCloseableAsync;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;

import org.apache.flink.shaded.guava31.com.google.common.base.Ticker;
import org.apache.flink.shaded.guava31.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava31.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.guava31.com.google.common.cache.RemovalListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.Serializable;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Cache to manage ongoing operations.
 *
 * <p>The cache allows to register ongoing operations by calling {@link #registerOngoingOperation(K,
 * CompletableFuture)}, where the {@code CompletableFuture} contains the operation result. Completed
 * operations will be removed from the cache automatically after a fixed timeout.
 */
@ThreadSafe
public class CompletedOperationCache<K extends OperationKey, R extends Serializable>
        implements AutoCloseableAsync {

    private static final Logger LOGGER = LoggerFactory.getLogger(CompletedOperationCache.class);

    /** In-progress asynchronous operations. */
    private final Map<K, ResultAccessTracker<R>> registeredOperationTriggers =
            new ConcurrentHashMap<>();

    /** Caches the result of completed operations. */
    private final Cache<K, ResultAccessTracker<R>> completedOperations;

    private final Object lock = new Object();

    @Nullable private CompletableFuture<Void> terminationFuture;
    private Duration cacheDuration;

    public CompletedOperationCache(final Duration cacheDuration) {
        this(cacheDuration, Ticker.systemTicker());
    }

    @VisibleForTesting
    CompletedOperationCache(final Ticker ticker) {
        this(RestOptions.ASYNC_OPERATION_STORE_DURATION.defaultValue(), ticker);
    }

    @VisibleForTesting
    CompletedOperationCache(final Duration cacheDuration, final Ticker ticker) {
        this.cacheDuration = Preconditions.checkNotNull(cacheDuration);

        CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder();

        if (cacheDuration.getSeconds() != 0) {
            cacheBuilder = cacheBuilder.expireAfterWrite(cacheDuration);
        }

        completedOperations =
                cacheBuilder
                        .removalListener(
                                (RemovalListener<K, ResultAccessTracker<R>>)
                                        removalNotification -> {
                                            if (removalNotification.wasEvicted()) {
                                                Preconditions.checkState(
                                                        removalNotification.getKey() != null);
                                                Preconditions.checkState(
                                                        removalNotification.getValue() != null);

                                                // When shutting down the cache, we wait until all
                                                // results are accessed.
                                                // When a result gets evicted from the cache, it
                                                // will not be possible to access
                                                // it any longer, and we might be in the process of
                                                // shutting down, so we mark
                                                // the result as accessed to avoid waiting
                                                // indefinitely.
                                                removalNotification.getValue().markAccessed();

                                                LOGGER.info(
                                                        "Evicted result with trigger id {} because its TTL of {}s has expired.",
                                                        removalNotification.getKey().getTriggerId(),
                                                        cacheDuration.getSeconds());
                                            }
                                        })
                        .ticker(ticker)
                        .build();
    }

    /**
     * Registers an ongoing operation with the cache.
     *
     * @param operationResultFuture A future containing the operation result.
     * @throws IllegalStateException if the cache is already shutting down
     */
    public void registerOngoingOperation(
            final K operationKey, final CompletableFuture<R> operationResultFuture) {
        final ResultAccessTracker<R> inProgress = ResultAccessTracker.inProgress();

        synchronized (lock) {
            checkState(isRunning(), "The CompletedOperationCache has already been closed.");
            registeredOperationTriggers.put(operationKey, inProgress);
        }

        operationResultFuture.whenComplete(
                (result, error) -> {
                    if (error == null) {
                        completedOperations.put(
                                operationKey,
                                inProgress.finishOperation(OperationResult.success(result)));
                    } else {
                        completedOperations.put(
                                operationKey,
                                inProgress.finishOperation(OperationResult.failure(error)));
                    }
                    registeredOperationTriggers.remove(operationKey);
                });
    }

    @GuardedBy("lock")
    private boolean isRunning() {
        return terminationFuture == null;
    }

    /** Returns whether this cache contains an operation under the given operation key. */
    public boolean containsOperation(final K operationKey) {
        return registeredOperationTriggers.containsKey(operationKey)
                || completedOperations.getIfPresent(operationKey) != null;
    }

    /**
     * Returns an optional containing the {@link OperationResult} for the specified key, or an empty
     * optional if no operation is registered under the key.
     */
    public Optional<OperationResult<R>> get(final K operationKey) {
        ResultAccessTracker<R> resultAccessTracker;
        if ((resultAccessTracker = registeredOperationTriggers.get(operationKey)) == null
                && (resultAccessTracker = completedOperations.getIfPresent(operationKey)) == null) {
            return Optional.empty();
        }

        return Optional.of(resultAccessTracker.accessOperationResultOrError());
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        synchronized (lock) {
            if (isRunning()) {
                terminationFuture =
                        FutureUtils.orTimeout(
                                asyncWaitForResultsToBeAccessed(),
                                cacheDuration.getSeconds(),
                                TimeUnit.SECONDS,
                                String.format(
                                        "Waiting for results to be accessed timed out after %s seconds.",
                                        cacheDuration.getSeconds()));
            }

            return terminationFuture;
        }
    }

    private CompletableFuture<Void> asyncWaitForResultsToBeAccessed() {
        return FutureUtils.waitForAll(
                Stream.concat(
                                registeredOperationTriggers.values().stream(),
                                completedOperations.asMap().values().stream())
                        .map(ResultAccessTracker::getAccessedFuture)
                        .collect(Collectors.toList()));
    }

    @VisibleForTesting
    void cleanUp() {
        completedOperations.cleanUp();
    }

    /** Stores the result of an asynchronous operation, and tracks accesses to it. */
    private static class ResultAccessTracker<R extends Serializable> {

        /** Result of an asynchronous operation. */
        private final OperationResult<R> operationResult;

        /** Future that completes if {@link #operationResult} is accessed after it finished. */
        private final CompletableFuture<Void> accessed;

        private static <R extends Serializable> ResultAccessTracker<R> inProgress() {
            return new ResultAccessTracker<>();
        }

        private ResultAccessTracker() {
            this.operationResult = OperationResult.inProgress();
            this.accessed = new CompletableFuture<>();
        }

        private ResultAccessTracker(
                final OperationResult<R> operationResult, final CompletableFuture<Void> accessed) {
            this.operationResult = checkNotNull(operationResult);
            this.accessed = checkNotNull(accessed);
        }

        /**
         * Creates a new instance of the tracker with the result of the asynchronous operation set.
         */
        public ResultAccessTracker<R> finishOperation(final OperationResult<R> operationResult) {
            checkState(!this.operationResult.isFinished());

            return new ResultAccessTracker<>(checkNotNull(operationResult), this.accessed);
        }

        /**
         * Returns the {@link OperationResult} of the asynchronous operation. If the operation is
         * finished, marks the result as accessed.
         */
        public OperationResult<R> accessOperationResultOrError() {
            if (operationResult.isFinished()) {
                markAccessed();
            }
            return operationResult;
        }

        public CompletableFuture<Void> getAccessedFuture() {
            return accessed;
        }

        private void markAccessed() {
            accessed.complete(null);
        }
    }
}
