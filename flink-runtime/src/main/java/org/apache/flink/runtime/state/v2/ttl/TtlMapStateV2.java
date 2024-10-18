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
import org.apache.flink.runtime.state.v2.internal.InternalMapState;

import javax.annotation.Nonnull;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Function;

/**
 * This class wraps map state with TTL logic.
 *
 * @param <K> The type of key the state is associated to
 * @param <N> The type of the namespace
 * @param <UK> Type of the user entry key of state with TTL
 * @param <UV> Type of the user entry value of state with TTL
 */
class TtlMapStateV2<K, N, UK, UV>
        extends AbstractTtlStateV2<K, N, UV, TtlValue<UV>, InternalMapState<K, N, UK, TtlValue<UV>>>
        implements InternalMapState<K, N, UK, UV> {

    protected TtlMapStateV2(
            TtlStateContext<InternalMapState<K, N, UK, TtlValue<UV>>, UV> ttlStateContext) {
        super(ttlStateContext);
    }

    @Override
    public void clear() {
        original.clear();
    }

    @Override
    public StateFuture<UV> asyncGet(UK key) {
        return original.asyncGet(key)
                .thenApply(
                        ttlValue ->
                                getElementWithTtlCheck(
                                        ttlValue, (newTtl) -> original.asyncPut(key, newTtl)));
    }

    @Override
    public StateFuture<Void> asyncPut(UK key, UV value) {
        return original.asyncPut(key, value == null ? null : wrapWithTs(value));
    }

    @Override
    public StateFuture<Void> asyncPutAll(Map<UK, UV> map) {
        Map<UK, TtlValue<UV>> withTs = new HashMap();
        for (Map.Entry<UK, UV> entry : map.entrySet()) {
            withTs.put(
                    entry.getKey(), entry.getValue() == null ? null : wrapWithTs(entry.getValue()));
        }
        return original.asyncPutAll(withTs);
    }

    @Override
    public StateFuture<Void> asyncRemove(UK key) {
        return original.asyncRemove(key);
    }

    @Override
    public StateFuture<Boolean> asyncContains(UK key) {
        return original.asyncGet(key)
                .thenApply(
                        ttlValue -> {
                            if (ttlValue == null) {
                                return false;
                            }
                            boolean unexpired = !expired(ttlValue);
                            if (!unexpired || returnExpired) {
                                if (updateTsOnRead) {
                                    original.put(key, rewrapWithNewTs(ttlValue));
                                }
                                return true;
                            }
                            return false;
                        });
    }

    @Override
    public StateFuture<StateIterator<Map.Entry<UK, UV>>> asyncEntries() {
        final Map<UK, UV> result = new HashMap<>();
        return original.asyncEntries()
                .thenAccept(
                        iter -> {
                            iter.onNext(
                                    entry -> {
                                        UV value =
                                                getElementWithTtlCheck(
                                                        entry.getValue(),
                                                        (newTtl) ->
                                                                original.asyncPut(
                                                                        entry.getKey(), newTtl));
                                        if (value != null) {
                                            result.put(entry.getKey(), value);
                                        }
                                    });
                        })
                .thenApply(v -> new CompleteStateIterator<>(result.entrySet()));
    }

    @Override
    public StateFuture<StateIterator<UK>> asyncKeys() {
        final List<UK> result = new ArrayList<>();
        return original.asyncEntries()
                .thenAccept(
                        iter -> {
                            iter.onNext(
                                    entry -> {
                                        UV value =
                                                getElementWithTtlCheck(
                                                        entry.getValue(),
                                                        (newTtl) ->
                                                                original.asyncPut(
                                                                        entry.getKey(), newTtl));
                                        if (value != null) {
                                            result.add(entry.getKey());
                                        }
                                    });
                        })
                .thenApply(v -> new CompleteStateIterator<>(result));
    }

    @Override
    public StateFuture<StateIterator<UV>> asyncValues() {
        final List<UV> result = new ArrayList<>();
        return original.asyncEntries()
                .thenAccept(
                        iter -> {
                            iter.onNext(
                                    entry -> {
                                        UV value =
                                                getElementWithTtlCheck(
                                                        entry.getValue(),
                                                        (newTtl) ->
                                                                original.put(
                                                                        entry.getKey(), newTtl));
                                        if (value != null) {
                                            result.add(value);
                                        }
                                    });
                        })
                .thenApply(v -> new CompleteStateIterator<>(result));
    }

    @Override
    public StateFuture<Boolean> asyncIsEmpty() {
        // the result may be wrong if state is expired.
        return original.asyncIsEmpty();
    }

    @Override
    public UV get(UK key) {
        TtlValue<UV> ttlValue = original.get(key);
        if (ttlValue == null) {
            return null;
        } else if (expired(ttlValue)) {
            if (!returnExpired) {
                return null;
            }
        } else if (updateTsOnRead) {
            original.put(key, rewrapWithNewTs(ttlValue));
        }
        return ttlValue.getUserValue();
    }

    @Override
    public void put(UK key, UV value) {
        original.put(key, value == null ? null : wrapWithTs(value));
    }

    @Override
    public void putAll(Map<UK, UV> map) {
        Map<UK, TtlValue<UV>> withTs = new HashMap();
        long currentTimestamp = timeProvider.currentTimestamp();
        for (Map.Entry<UK, UV> entry : map.entrySet()) {
            withTs.put(
                    entry.getKey(),
                    entry.getValue() == null
                            ? null
                            : TtlUtils.wrapWithTs(entry.getValue(), currentTimestamp));
        }
        original.putAll(withTs);
    }

    @Override
    public void remove(UK key) {
        original.remove(key);
    }

    @Override
    public boolean contains(UK key) {
        TtlValue<UV> ttlValue = original.get(key);
        if (ttlValue == null) {
            return false;
        }
        boolean unexpired = !expired(ttlValue);
        if (!unexpired || returnExpired) {
            if (updateTsOnRead) {
                original.put(key, rewrapWithNewTs(ttlValue));
            }
            return true;
        }
        return false;
    }

    @Override
    public Iterable<Map.Entry<UK, UV>> entries() {
        return entries(e -> e);
    }

    @Override
    public Iterable<UK> keys() {
        return entries(e -> e.getKey());
    }

    @Override
    public Iterable<UV> values() {
        return entries(e -> e.getValue());
    }

    private <R> Iterable<R> entries(Function<Map.Entry<UK, UV>, R> resultMapper) {
        Iterable<Map.Entry<UK, TtlValue<UV>>> withTs = original.entries();
        return () ->
                new EntriesIterator<>(
                        withTs == null ? Collections.emptyList() : withTs, resultMapper);
    }

    @Override
    public Iterator<Map.Entry<UK, UV>> iterator() {
        return entries().iterator();
    }

    @Override
    public boolean isEmpty() {
        // todo: poor performance, if return `original.isEmpty()` directly, the result may be wrong.
        return iterator().hasNext();
    }

    private class EntriesIterator<R> implements Iterator<R> {
        private final Iterator<Map.Entry<UK, TtlValue<UV>>> originalIterator;
        private final Function<Map.Entry<UK, UV>, R> resultMapper;
        private Map.Entry<UK, UV> nextUnexpired = null;
        private boolean rightAfterNextIsCalled = false;

        private EntriesIterator(
                @Nonnull Iterable<Map.Entry<UK, TtlValue<UV>>> withTs,
                @Nonnull Function<Map.Entry<UK, UV>, R> resultMapper) {
            this.originalIterator = withTs.iterator();
            this.resultMapper = resultMapper;
        }

        @Override
        public boolean hasNext() {
            rightAfterNextIsCalled = false;
            while (nextUnexpired == null && originalIterator.hasNext()) {
                Map.Entry<UK, TtlValue<UV>> ttlEntry = originalIterator.next();
                UV value =
                        getElementWithTtlCheck(
                                ttlEntry.getValue(),
                                (newTtl) -> original.put(ttlEntry.getKey(), newTtl));
                nextUnexpired =
                        value == null
                                ? null
                                : new AbstractMap.SimpleEntry<>(ttlEntry.getKey(), value);
            }
            return nextUnexpired != null;
        }

        @Override
        public R next() {
            if (hasNext()) {
                rightAfterNextIsCalled = true;
                R result = resultMapper.apply(nextUnexpired);
                nextUnexpired = null;
                return result;
            }
            throw new NoSuchElementException();
        }

        @Override
        public void remove() {
            if (rightAfterNextIsCalled) {
                originalIterator.remove();
            } else {
                throw new IllegalStateException(
                        "next() has not been called or hasNext() has been called afterwards,"
                                + " remove() is supported only right after calling next()");
            }
        }
    }
}
