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

package org.apache.flink.core.state;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.state.v2.StateFuture;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A collection of utilities that expand the usage of {@link StateFuture}. All the methods can only
 * be accessed within user function provided to {@code KeyedStream}.
 */
@Experimental
public class StateFutureUtils {
    /** Returns a completed future that does nothing and return null. */
    public static <V> StateFuture<V> completedVoidFuture() {
        return new CompletedStateFuture<>(null);
    }

    /** Returns a completed future that does nothing and return provided result. */
    public static <V> StateFuture<V> completedFuture(V result) {
        return new CompletedStateFuture<>(result);
    }

    /**
     * Creates a future that is complete once multiple other futures completed. Upon successful
     * completion, the future returns the collection of the futures' results.
     *
     * @param futures The futures that make up the conjunction. No null entries are allowed,
     *     otherwise a IllegalArgumentException will be thrown.
     * @return The StateFuture that completes once all given futures are complete.
     */
    @SuppressWarnings("unchecked")
    public static <T> StateFuture<Collection<T>> combineAll(
            Collection<? extends StateFuture<? extends T>> futures) {
        int count = futures.size();
        if (count == 0) {
            return new CompletedStateFuture<>(Collections.emptyList());
        } else if (count == 1) {
            StateFuture<? extends T> firstFuture = futures.stream().findFirst().get();
            return firstFuture.thenCompose(
                    (t) -> StateFutureUtils.completedFuture(Collections.singletonList(t)));
        }

        // multiple futures

        final T[] results = (T[]) new Object[count];

        StateFutureImpl<? extends T> pendingFuture = null;
        for (StateFuture<? extends T> future : futures) {
            if (future instanceof StateFutureImpl) {
                pendingFuture = (StateFutureImpl<? extends T>) future;
                break;
            }
        }

        if (pendingFuture == null) {
            int i = 0;
            for (StateFuture<? extends T> future : futures) {
                final int index = i;
                ((InternalStateFuture<? extends T>) future)
                        .thenSyncAccept(
                                (t) -> {
                                    results[index] = t;
                                });
                i++;
            }
            return new CompletedStateFuture<>(Arrays.asList(results));
        } else {
            int i = 0;
            AtomicInteger countDown = new AtomicInteger(count);
            StateFutureImpl<Collection<T>> ret = pendingFuture.makeNewStateFuture();
            for (StateFuture<? extends T> future : futures) {
                final int index = i;
                ((InternalStateFuture<? extends T>) future)
                        .thenSyncAccept(
                                (t) -> {
                                    results[index] = t;
                                    if (countDown.decrementAndGet() == 0) {
                                        ret.complete(Arrays.asList(results));
                                    }
                                });
                i++;
            }
            return ret;
        }
    }
}
