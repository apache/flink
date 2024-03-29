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

package org.apache.flink.runtime.state.ttl;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * This class wraps list state with TTL logic.
 *
 * @param <K> The type of key the state is associated to
 * @param <N> The type of the namespace
 * @param <T> Type of the user entry value of state with TTL
 */
class TtlListState<K, N, T>
        extends AbstractTtlState<
                K, N, List<T>, List<TtlValue<T>>, InternalListState<K, N, TtlValue<T>>>
        implements InternalListState<K, N, T> {

    TtlListState(TtlStateContext<InternalListState<K, N, TtlValue<T>>, List<T>> ttlStateContext) {
        super(ttlStateContext);
    }

    @Override
    public void update(List<T> values) throws Exception {
        accessCallback.run();
        updateInternal(values);
    }

    @Override
    public void addAll(List<T> values) throws Exception {
        accessCallback.run();
        Preconditions.checkNotNull(values, "List of values to add cannot be null.");
        original.addAll(withTs(values));
    }

    @Override
    public Iterable<T> get() throws Exception {
        accessCallback.run();
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

    private void updateTs(List<TtlValue<T>> ttlValues) throws Exception {
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

    @Override
    public void add(T value) throws Exception {
        accessCallback.run();
        Preconditions.checkNotNull(value, "You cannot add null to a ListState.");
        original.add(wrapWithTs(value));
    }

    @Nullable
    @Override
    public List<TtlValue<T>> getUnexpiredOrNull(@Nonnull List<TtlValue<T>> ttlValues) {
        // the update operation will clear the whole state if the list becomes empty after init
        if (ttlValues.isEmpty()) {
            return ttlValues;
        }

        long currentTimestamp = timeProvider.currentTimestamp();
        TypeSerializer<TtlValue<T>> elementSerializer =
                ((ListSerializer<TtlValue<T>>) original.getValueSerializer())
                        .getElementSerializer();
        int firstExpireElementIndex = -1;
        for (int i = 0; i < ttlValues.size(); i++) {
            TtlValue<T> ttlValue = ttlValues.get(i);
            if (TtlUtils.expired(ttlValue, ttl, currentTimestamp)) {
                firstExpireElementIndex = i;
                break;
            }
        }
        if (firstExpireElementIndex == -1) {
            return ttlValues;
        }

        List<TtlValue<T>> unexpired = new ArrayList<>(ttlValues.size());
        for (int i = 0; i < ttlValues.size(); i++) {
            TtlValue<T> ttlValue = ttlValues.get(i);
            if (i < firstExpireElementIndex
                    || (i > firstExpireElementIndex
                            && !TtlUtils.expired(ttlValue, ttl, currentTimestamp))) {
                // we have to do the defensive copy to update the value
                unexpired.add(elementSerializer.copy(ttlValue));
            }
        }
        if (!unexpired.isEmpty()) {
            return unexpired;
        } else {
            // list is not empty and all expired
            return null;
        }
    }

    @Override
    public void clear() {
        original.clear();
    }

    @Override
    public void mergeNamespaces(N target, Collection<N> sources) throws Exception {
        original.mergeNamespaces(target, sources);
    }

    @Override
    public List<T> getInternal() throws Exception {
        return collect(get());
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

    @Override
    public void updateInternal(List<T> valueToStore) throws Exception {
        Preconditions.checkNotNull(valueToStore, "List of values to update cannot be null.");
        original.update(withTs(valueToStore));
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
