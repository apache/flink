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

import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.api.common.state.v2.StateIterator;
import org.apache.flink.core.asyncprocessing.InternalAsyncFuture;
import org.apache.flink.util.function.FunctionWithException;
import org.apache.flink.util.function.ThrowingConsumer;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link StateFutureUtils}. */
class StateFutureUtilsTest {

    @Test
    void testToIterableWithNonInternalStateIterator() {
        // A StateIterator that is not an InternalStateIterator, like the async TTL wrappers.
        List<String> elements = Arrays.asList("a", "b", "c");
        StateFuture<StateIterator<String>> future =
                StateFutureUtils.completedFuture(new ListStateIterator<>(elements));

        Iterable<String> result = sync(StateFutureUtils.toIterable(future));

        assertThat(result).containsExactly("a", "b", "c");
    }

    @Test
    void testToIterableWithEmptyNonInternalStateIterator() {
        StateFuture<StateIterator<String>> future =
                StateFutureUtils.completedFuture(new ListStateIterator<>(new ArrayList<>()));

        Iterable<String> result = sync(StateFutureUtils.toIterable(future));

        assertThat(result).isEmpty();
    }

    @Test
    void testToIterableWithFullyLoadedInternalStateIterator() {
        List<String> cache = Arrays.asList("x", "y");
        StateFuture<StateIterator<String>> future =
                StateFutureUtils.completedFuture(new CachedInternalStateIterator<>(cache, false));

        Iterable<String> result = sync(StateFutureUtils.toIterable(future));

        assertThat(result).containsExactly("x", "y");
    }

    @Test
    void testToIterableWithLoadingInternalStateIterator() {
        List<String> cache = Arrays.asList("x", "y");
        StateFuture<StateIterator<String>> future =
                StateFutureUtils.completedFuture(new CachedInternalStateIterator<>(cache, true));

        Iterable<String> result = sync(StateFutureUtils.toIterable(future));

        assertThat(result).containsExactly("x", "y");
    }

    @Test
    void testToIterableWithNullIterator() {
        StateFuture<StateIterator<String>> future = StateFutureUtils.completedFuture(null);

        Iterable<String> result = sync(StateFutureUtils.toIterable(future));

        assertThat(result).isEmpty();
    }

    private static <T> T sync(StateFuture<T> future) {
        return ((InternalAsyncFuture<T>) future).get();
    }

    /** A minimal {@link StateIterator} that is intentionally not an InternalStateIterator. */
    private static class ListStateIterator<T> implements StateIterator<T> {
        private final List<T> elements;

        private ListStateIterator(List<T> elements) {
            this.elements = elements;
        }

        @Override
        public <U> StateFuture<Collection<U>> onNext(
                FunctionWithException<T, StateFuture<? extends U>, Exception> iterating) {
            List<StateFuture<? extends U>> futures = new ArrayList<>();
            for (T element : elements) {
                futures.add(FunctionWithException.unchecked(iterating).apply(element));
            }
            return StateFutureUtils.combineAll(futures);
        }

        @Override
        public StateFuture<Void> onNext(ThrowingConsumer<T, Exception> iterating) {
            for (T element : elements) {
                ThrowingConsumer.unchecked(iterating).accept(element);
            }
            return StateFutureUtils.completedVoidFuture();
        }

        @Override
        public boolean isEmpty() {
            return elements.isEmpty();
        }
    }

    /** An {@link InternalStateIterator} backed by an in-memory cache. */
    private static class CachedInternalStateIterator<T> extends ListStateIterator<T>
            implements InternalStateIterator<T> {
        private final List<T> cache;
        private final boolean hasNextLoading;

        private CachedInternalStateIterator(List<T> cache, boolean hasNextLoading) {
            super(cache);
            this.cache = cache;
            this.hasNextLoading = hasNextLoading;
        }

        @Override
        public boolean hasNextLoading() {
            return hasNextLoading;
        }

        @Override
        public Iterable<T> getCurrentCache() {
            return cache;
        }
    }
}
