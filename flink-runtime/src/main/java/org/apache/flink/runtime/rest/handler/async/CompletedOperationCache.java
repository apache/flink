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
import org.apache.flink.types.Either;
import org.apache.flink.util.AutoCloseableAsync;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;

import org.apache.flink.shaded.guava30.com.google.common.base.Ticker;
import org.apache.flink.shaded.guava30.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava30.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.guava30.com.google.common.cache.RemovalListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.Map;
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
class CompletedOperationCache<K extends OperationKey, R> implements AutoCloseableAsync {

    private static final long COMPLETED_OPERATION_RESULT_CACHE_DURATION_SECONDS = 300L;

    private static final Logger LOGGER = LoggerFactory.getLogger(CompletedOperationCache.class);

    /** In-progress asynchronous operations. */
    private final Map<K, ResultAccessTracker<R>> registeredOperationTriggers =
            new ConcurrentHashMap<>();

    /** Caches the result of completed operations. */
    private final Cache<K, ResultAccessTracker<R>> completedOperations;

    private final Object lock = new Object();

    @Nullable private CompletableFuture<Void> terminationFuture;

    CompletedOperationCache() {
        this(Ticker.systemTicker());
    }

    @VisibleForTesting
    CompletedOperationCache(final Ticker ticker) {
        completedOperations =
                CacheBuilder.newBuilder()
                        .expireAfterWrite(
                                COMPLETED_OPERATION_RESULT_CACHE_DURATION_SECONDS, TimeUnit.SECONDS)
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
                                                        COMPLETED_OPERATION_RESULT_CACHE_DURATION_SECONDS);
                                            }
                                        })
                        .ticker(ticker)
                        .build();
    }

    /**
     * Registers an ongoing operation with the cache.
     *
     * @param operationResultFuture A future containing the operation result.
     * @throw IllegalStateException if the cache is already shutting down
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
                                operationKey, inProgress.finishOperation(Either.Right(result)));
                    } else {
                        completedOperations.put(
                                operationKey, inProgress.finishOperation(Either.Left(error)));
                    }
                    registeredOperationTriggers.remove(operationKey);
                });
    }

    @GuardedBy("lock")
    private boolean isRunning() {
        return terminationFuture == null;
    }

    /**
     * Returns the operation result or a {@code Throwable} if the {@code CompletableFuture}
     * finished, otherwise {@code null}.
     *
     * @throws UnknownOperationKeyException If the operation is not found, and there is no ongoing
     *     operation under the provided key.
     */
    @Nullable
    public Either<Throwable, R> get(final K operationKey) throws UnknownOperationKeyException {
        ResultAccessTracker<R> resultAccessTracker;
        if ((resultAccessTracker = registeredOperationTriggers.get(operationKey)) == null
                && (resultAccessTracker = completedOperations.getIfPresent(operationKey)) == null) {
            throw new UnknownOperationKeyException(operationKey);
        }

        return resultAccessTracker.accessOperationResultOrError();
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        synchronized (lock) {
            if (isRunning()) {
                terminationFuture =
                        FutureUtils.orTimeout(
                                asyncWaitForResultsToBeAccessed(),
                                COMPLETED_OPERATION_RESULT_CACHE_DURATION_SECONDS,
                                TimeUnit.SECONDS);
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
    private static class ResultAccessTracker<R> {

        /** Result of an asynchronous operation. Null if operation is in progress. */
        @Nullable private final Either<Throwable, R> operationResultOrError;

        /** Future that completes if a non-null {@link #operationResultOrError} is accessed. */
        private final CompletableFuture<Void> accessed;

        private static <R> ResultAccessTracker<R> inProgress() {
            return new ResultAccessTracker<>();
        }

        private ResultAccessTracker() {
            this.operationResultOrError = null;
            this.accessed = new CompletableFuture<>();
        }

        private ResultAccessTracker(
                final Either<Throwable, R> operationResultOrError,
                final CompletableFuture<Void> accessed) {
            this.operationResultOrError = checkNotNull(operationResultOrError);
            this.accessed = checkNotNull(accessed);
        }

        /**
         * Creates a new instance of the tracker with the result of the asynchronous operation set.
         */
        public ResultAccessTracker<R> finishOperation(
                final Either<Throwable, R> operationResultOrError) {
            checkState(this.operationResultOrError == null);

            return new ResultAccessTracker<>(checkNotNull(operationResultOrError), this.accessed);
        }

        /**
         * If present, returns the result of the asynchronous operation, and marks the result as
         * accessed. If the result is not present, this method returns null.
         */
        @Nullable
        public Either<Throwable, R> accessOperationResultOrError() {
            if (operationResultOrError != null) {
                markAccessed();
            }
            return operationResultOrError;
        }

        public CompletableFuture<Void> getAccessedFuture() {
            return accessed;
        }

        private void markAccessed() {
            accessed.complete(null);
        }
    }
}
