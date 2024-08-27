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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.Internal;

import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.concurrent.FutureUtils.assertNoException;

/**
 * This class is semi-thread safe. Only method {@link #notifyCompletion()} is allowed to be executed
 * from an outside of the task thread.
 *
 * <p>It solves a problem of a potential memory leak as described in FLINK-25728. In short we have
 * to ensure, that if there is one input (future) that rarely (or never) completes, that such future
 * would not prevent previously returned combined futures (like {@link
 * CompletableFuture#anyOf(CompletableFuture[])} from being garbage collected. Additionally, we
 * don't want to accumulate more and more completion stages on such rarely completed future, so we
 * are registering {@link CompletableFuture#thenRun(Runnable)} only if it has not already been done.
 *
 * <p>Note {@link #resetToUnAvailable()} doesn't de register previously registered futures. If
 * future was registered in the past, but for whatever reason now it is not, such future can still
 * complete the newly created future.
 *
 * <p>It might be no longer needed after upgrading to JDK9
 * (https://bugs.openjdk.java.net/browse/JDK-8160402).
 */
@Internal
public class MultipleFuturesAvailabilityHelper {
    private final CompletableFuture<?>[] futuresToCombine;

    private volatile CompletableFuture<?> availableFuture = new CompletableFuture<>();

    public MultipleFuturesAvailabilityHelper(int size) {
        futuresToCombine = new CompletableFuture[size];
    }

    /** @return combined future using anyOf logic */
    public CompletableFuture<?> getAvailableFuture() {
        return availableFuture;
    }

    public void resetToUnAvailable() {
        if (availableFuture.isDone()) {
            availableFuture = new CompletableFuture<>();
        }
    }

    private void notifyCompletion() {
        availableFuture.complete(null);
    }

    /**
     * Combine {@code availabilityFuture} using anyOf logic with other previously registered
     * futures.
     */
    public void anyOf(final int idx, CompletableFuture<?> availabilityFuture) {
        if (futuresToCombine[idx] == null || futuresToCombine[idx].isDone()) {
            futuresToCombine[idx] = availabilityFuture;
            assertNoException(availabilityFuture.thenRun(this::notifyCompletion));
        }
    }
}
