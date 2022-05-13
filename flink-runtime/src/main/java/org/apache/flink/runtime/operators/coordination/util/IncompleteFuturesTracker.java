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

package org.apache.flink.runtime.operators.coordination.util;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This tracker remembers CompletableFutures as long as they are incomplete and allows us to fail
 * them later.
 */
public final class IncompleteFuturesTracker {

    private final ReentrantLock lock = new ReentrantLock();

    private final HashSet<CompletableFuture<?>> incompleteFutures = new HashSet<>();

    @Nullable private Throwable failureCause;

    public void trackFutureWhileIncomplete(CompletableFuture<?> future) {
        // this is only a best-effort shortcut for efficiency
        if (future.isDone()) {
            return;
        }

        lock.lock();
        try {
            if (failureCause != null) {
                future.completeExceptionally(failureCause);
                return;
            }

            incompleteFutures.add(future);
        } finally {
            lock.unlock();
        }

        // when the future completes, it removes itself from the set
        future.whenComplete((success, failure) -> removeFromSet(future));
    }

    public Collection<CompletableFuture<?>> getCurrentIncompleteAndReset() {
        lock.lock();
        try {
            if (incompleteFutures.isEmpty()) {
                return Collections.emptySet();
            }

            final ArrayList<CompletableFuture<?>> futures = new ArrayList<>(incompleteFutures);
            incompleteFutures.clear();
            return futures;

        } finally {
            lock.unlock();
        }
    }

    public void failAllFutures(Throwable cause) {
        final Collection<CompletableFuture<?>> futuresToFail;

        lock.lock();
        try {
            if (failureCause != null) {
                // already failed
                return;
            }
            failureCause = cause;
            futuresToFail = new ArrayList<>(incompleteFutures);
            incompleteFutures.clear();
        } finally {
            lock.unlock();
        }

        // we do the actual failing outside the lock scope for efficiency, because there
        // may be synchronous actions triggered on the future
        for (CompletableFuture<?> future : futuresToFail) {
            future.completeExceptionally(cause);
        }
    }

    void removeFromSet(CompletableFuture<?> future) {
        lock.lock();
        try {
            incompleteFutures.remove(future);
        } finally {
            lock.unlock();
        }
    }
}
