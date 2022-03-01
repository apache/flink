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

package org.apache.flink.state.changelog;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.changelog.StateChange;
import org.apache.flink.runtime.state.heap.InternalKeyContext;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.state.changelog.restore.ChangelogApplierFactory;
import org.apache.flink.state.changelog.restore.StateChangeApplier;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.function.ThrowingConsumer;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * Delegated partitioned {@link MapState} that forwards changes to {@link StateChange} upon {@link
 * MapState} is updated.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <UK> The type of the keys in the state.
 * @param <UV> The type of the values in the state.
 */
class ChangelogMapState<K, N, UK, UV>
        extends AbstractChangelogState<K, N, Map<UK, UV>, InternalMapState<K, N, UK, UV>>
        implements InternalMapState<K, N, UK, UV> {

    private final InternalKeyContext<K> keyContext;

    ChangelogMapState(
            InternalMapState<K, N, UK, UV> delegatedState,
            KvStateChangeLogger<Map<UK, UV>, N> changeLogger,
            InternalKeyContext<K> keyContext) {
        super(delegatedState, changeLogger);
        this.keyContext = keyContext;
    }

    private Map.Entry<UK, UV> loggingMapEntry(
            Map.Entry<UK, UV> entry, KvStateChangeLogger<Map<UK, UV>, N> changeLogger, N ns) {
        return new Map.Entry<UK, UV>() {
            @Override
            public UK getKey() {
                return entry.getKey();
            }

            @Override
            public UV getValue() {
                return entry.getValue();
            }

            @Override
            public UV setValue(UV value) {
                UV oldValue = entry.setValue(value);
                try {
                    changeLogger.valueElementAddedOrUpdated(
                            getWriter(entry.getKey(), entry.getValue()), ns);
                } catch (IOException e) {
                    ExceptionUtils.rethrow(e);
                }
                return oldValue;
            }
        };
    }

    @Override
    public UV get(UK key) throws Exception {
        return delegatedState.get(key);
    }

    @Override
    public void put(UK key, UV value) throws Exception {
        delegatedState.put(key, value);
        changeLogger.valueElementAddedOrUpdated(getWriter(key, value), getCurrentNamespace());
    }

    @Override
    public void putAll(Map<UK, UV> map) throws Exception {
        delegatedState.putAll(map);
        changeLogger.valueAdded(map, getCurrentNamespace());
    }

    @Override
    public void remove(UK key) throws Exception {
        delegatedState.remove(key);
        changeLogger.valueElementRemoved(out -> serializeKey(key, out), getCurrentNamespace());
    }

    @Override
    public boolean contains(UK key) throws Exception {
        return delegatedState.contains(key);
    }

    @Override
    public Iterable<Map.Entry<UK, UV>> entries() throws Exception {
        Iterator<Map.Entry<UK, UV>> iterator = delegatedState.iterator();
        return () -> getEntryIterator(iterator);
    }

    private Iterator<Map.Entry<UK, UV>> getEntryIterator(Iterator<Map.Entry<UK, UV>> iterator) {
        final N currentNamespace = getCurrentNamespace();
        return StateChangeLoggingIterator.create(
                CloseableIterator.adapterForIterator(
                        new Iterator<Map.Entry<UK, UV>>() {
                            @Override
                            public Map.Entry<UK, UV> next() {
                                return loggingMapEntry(
                                        iterator.next(), changeLogger, currentNamespace);
                            }

                            @Override
                            public boolean hasNext() {
                                return iterator.hasNext();
                            }

                            @Override
                            public void remove() {
                                iterator.remove();
                            }
                        }),
                changeLogger,
                (entry, out) -> serializeKey(entry.getKey(), out),
                currentNamespace);
    }

    @Override
    public Iterable<UK> keys() throws Exception {
        Iterable<UK> iterable = delegatedState.keys();
        return () ->
                StateChangeLoggingIterator.create(
                        CloseableIterator.adapterForIterator(iterable.iterator()),
                        changeLogger,
                        this::serializeKey,
                        getCurrentNamespace());
    }

    @Override
    public Iterable<UV> values() throws Exception {
        Iterator<Map.Entry<UK, UV>> iterator = entries().iterator();
        return () ->
                new Iterator<UV>() {
                    @Override
                    public boolean hasNext() {
                        return iterator.hasNext();
                    }

                    @Override
                    public UV next() {
                        return iterator.next().getValue();
                    }

                    @Override
                    public void remove() {
                        iterator.remove();
                    }
                };
    }

    @Override
    public Iterator<Map.Entry<UK, UV>> iterator() throws Exception {
        return getEntryIterator(delegatedState.iterator());
    }

    @Override
    public boolean isEmpty() throws Exception {
        return delegatedState.isEmpty();
    }

    @Override
    public void clear() {
        delegatedState.clear();
        try {
            changeLogger.valueCleared(getCurrentNamespace());
        } catch (IOException e) {
            ExceptionUtils.rethrow(e);
        }
    }

    private void serializeValue(UV value, DataOutputViewStreamWrapper out) throws IOException {
        getMapSerializer().getValueSerializer().serialize(value, out);
    }

    private void serializeKey(UK key, DataOutputViewStreamWrapper out) throws IOException {
        getMapSerializer().getKeySerializer().serialize(key, out);
    }

    private ThrowingConsumer<DataOutputViewStreamWrapper, IOException> getWriter(UK key, UV value) {
        return out -> {
            serializeKey(key, out);
            serializeValue(value, out);
        };
    }

    private MapSerializer<UK, UV> getMapSerializer() {
        return (MapSerializer<UK, UV>) getValueSerializer();
    }

    @SuppressWarnings("unchecked")
    static <UK, UV, K, N, SV, S extends State, IS extends S> IS create(
            InternalKvState<K, N, SV> mapState,
            KvStateChangeLogger<SV, N> changeLogger,
            InternalKeyContext<K> keyContext) {
        return (IS)
                new ChangelogMapState<>(
                        (InternalMapState<K, N, UK, UV>) mapState,
                        (KvStateChangeLogger<Map<UK, UV>, N>) changeLogger,
                        keyContext);
    }

    @Override
    public StateChangeApplier getChangeApplier(ChangelogApplierFactory factory) {
        return factory.forMap(delegatedState, keyContext);
    }
}
