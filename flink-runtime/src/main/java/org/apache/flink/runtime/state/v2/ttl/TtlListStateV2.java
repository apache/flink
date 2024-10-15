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

package org.apache.flink.runtime.state.v2.ttl;

import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.api.common.state.v2.StateIterator;
import org.apache.flink.runtime.state.ttl.TtlStateContext;
import org.apache.flink.runtime.state.ttl.TtlUtils;
import org.apache.flink.runtime.state.ttl.TtlValue;
import org.apache.flink.runtime.state.v2.adaptor.CompleteStateIterator;
import org.apache.flink.runtime.state.v2.internal.InternalListState;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class wraps list state with TTL logic.
 *
 * @param <K> The type of key the state is associated to
 * @param <N> The type of the namespace
 * @param <T> Type of the user entry value of state with TTL
 */
class TtlListStateV2<K, N, T>
        extends AbstractTtlStateV2<K, N, T, TtlValue<T>, InternalListState<K, N, TtlValue<T>>>
        implements InternalListState<K, N, T> {

    protected TtlListStateV2(
            TtlStateContext<InternalListState<K, N, TtlValue<T>>, T> ttlStateContext) {
        super(ttlStateContext);
    }

    @Override
    public StateFuture<Void> asyncUpdate(List<T> values) {
        Preconditions.checkNotNull(values, "List of values to add cannot be null.");
        return original.asyncUpdate(withTs(values));
    }

    @Override
    public StateFuture<Void> asyncAddAll(List<T> values) {
        Preconditions.checkNotNull(values, "List of values to add cannot be null.");
        return original.asyncAddAll(withTs(values));
    }

    @Override
    public StateFuture<StateIterator<T>> asyncGet() {
        final List<T> result = new ArrayList<>();
        AtomicBoolean encounteredNull = new AtomicBoolean(false);
        AtomicBoolean anyUnexpired = new AtomicBoolean(false);
        return original.asyncGet()
                .thenAccept(
                        stateIter -> {
                            stateIter.onNext(
                                    item -> {
                                        if (item == null) {
                                            encounteredNull.set(true);
                                        } else if (!encounteredNull.get()) {
                                            boolean unexpired = !expired(item);
                                            if (unexpired) {
                                                anyUnexpired.set(true);
                                            }
                                            if (unexpired || returnExpired) {
                                                result.add(item.getUserValue());
                                            }
                                        }
                                    });
                        })
                .thenAccept(
                        v -> {
                            if (!anyUnexpired.get()) {
                                original.asyncClear();
                            }
                        })
                .thenApply(v -> new CompleteStateIterator<T>(result));
    }

    @Override
    public StateFuture<Void> asyncAdd(T value) {
        return original.asyncAdd(value == null ? null : wrapWithTs(value));
    }

    @Override
    public Iterable<T> get() {
        Iterable<TtlValue<T>> ttlValue = original.get();
        ttlValue = ttlValue == null ? Collections.emptyList() : ttlValue;
        if (updateTsOnRead) {
            List<TtlValue<T>> collected = collect(ttlValue);
            ttlValue = collected;
            // the underlying state in backend is iterated in updateTs anyways
            // to avoid reiterating backend in IteratorWithCleanup
            // it is collected and iterated next time in memory
            updateTs(collected);
        }
        final Iterable<TtlValue<T>> finalResult = ttlValue;
        return () -> new IteratorWithCleanup(finalResult.iterator());
    }

    @Override
    public void add(T value) {
        original.add(value == null ? null : wrapWithTs(value));
    }

    @Override
    public void update(List<T> values) {
        Preconditions.checkNotNull(values, "List of values to add cannot be null.");
        original.update(withTs(values));
    }

    @Override
    public void addAll(List<T> values) {
        Preconditions.checkNotNull(values, "List of values to add cannot be null.");
        original.addAll(withTs(values));
    }

    private <E> List<E> collect(Iterable<E> iterable) {
        if (iterable instanceof List) {
            return (List<E>) iterable;
        } else {
            List<E> list = new ArrayList<>();
            for (E element : iterable) {
                list.add(element);
            }
            return list;
        }
    }

    private List<TtlValue<T>> withTs(List<T> values) {
        long currentTimestamp = timeProvider.currentTimestamp();
        List<TtlValue<T>> withTs = new ArrayList<>(values.size());
        for (T value : values) {
            Preconditions.checkNotNull(value, "You cannot have null element in a ListState.");
            withTs.add(TtlUtils.wrapWithTs(value, currentTimestamp));
        }
        return withTs;
    }

    private void updateTs(List<TtlValue<T>> ttlValues) {
        List<TtlValue<T>> unexpiredWithUpdatedTs = new ArrayList<>(ttlValues.size());
        long currentTimestamp = timeProvider.currentTimestamp();
        for (TtlValue<T> ttlValue : ttlValues) {
            if (!TtlUtils.expired(ttlValue, ttl, currentTimestamp)) {
                unexpiredWithUpdatedTs.add(
                        TtlUtils.wrapWithTs(ttlValue.getUserValue(), currentTimestamp));
            }
        }
        if (!unexpiredWithUpdatedTs.isEmpty()) {
            original.update(unexpiredWithUpdatedTs);
        }
    }

    private class IteratorWithCleanup implements Iterator<T> {
        private final Iterator<TtlValue<T>> originalIterator;
        private boolean anyUnexpired = false;
        private boolean uncleared = true;
        private T nextUnexpired = null;

        private IteratorWithCleanup(Iterator<TtlValue<T>> ttlIterator) {
            this.originalIterator = ttlIterator;
        }

        @Override
        public boolean hasNext() {
            findNextUnexpired();
            cleanupIfEmpty();
            return nextUnexpired != null;
        }

        private void cleanupIfEmpty() {
            boolean endOfIter = !originalIterator.hasNext() && nextUnexpired == null;
            if (uncleared && !anyUnexpired && endOfIter) {
                original.clear();
                uncleared = false;
            }
        }

        @Override
        public T next() {
            if (hasNext()) {
                T result = nextUnexpired;
                nextUnexpired = null;
                return result;
            }
            throw new NoSuchElementException();
        }

        // Once a null element is encountered, the subsequent elements will no longer be returned.
        private void findNextUnexpired() {
            while (nextUnexpired == null && originalIterator.hasNext()) {
                TtlValue<T> ttlValue = originalIterator.next();
                if (ttlValue == null) {
                    break;
                }
                boolean unexpired = !expired(ttlValue);
                if (unexpired) {
                    anyUnexpired = true;
                }
                if (unexpired || returnExpired) {
                    nextUnexpired = ttlValue.getUserValue();
                }
            }
        }
    }
}
