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

package org.apache.flink.cep.utils;

import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.cep.nfa.sharedbuffer.SharedBuffer;

import org.apache.flink.shaded.guava18.com.google.common.collect.Iterators;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Extends {@link SharedBuffer} with methods for checking the number of state accesses. It does not
 * use a proper StateBackend, but uses stubs over java collections.
 */
public class TestSharedBuffer<V> extends SharedBuffer<V> {

    private final MockKeyedStateStore keyedStateStore;

    private TestSharedBuffer(MockKeyedStateStore stateStore, TypeSerializer<V> valueSerializer) {
        super(stateStore, valueSerializer);
        this.keyedStateStore = stateStore;
    }

    public long getStateWrites() {
        return keyedStateStore.stateWrites;
    }

    public long getStateReads() {
        return keyedStateStore.stateReads;
    }

    public long getStateAccesses() {
        return getStateWrites() + getStateReads();
    }

    /**
     * Creates instance of {@link TestSharedBuffer}.
     *
     * @param typeSerializer serializer used to serialize incoming events
     * @param <T> type of incoming events
     * @return TestSharedBuffer instance
     */
    public static <T> TestSharedBuffer<T> createTestBuffer(TypeSerializer<T> typeSerializer) {
        return new TestSharedBuffer<>(new MockKeyedStateStore(), typeSerializer);
    }

    private static class MockKeyedStateStore implements KeyedStateStore {

        private long stateWrites = 0;
        private long stateReads = 0;

        @Override
        public <T> ValueState<T> getState(ValueStateDescriptor<T> stateProperties) {
            return new ValueState<T>() {

                private T value;

                @Override
                public T value() throws IOException {
                    stateReads++;
                    return value;
                }

                @Override
                public void update(T value) throws IOException {
                    stateWrites++;
                    this.value = value;
                }

                @Override
                public void clear() {
                    this.value = null;
                }
            };
        }

        @Override
        public <T> ListState<T> getListState(ListStateDescriptor<T> stateProperties) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> ReducingState<T> getReducingState(ReducingStateDescriptor<T> stateProperties) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <IN, ACC, OUT> AggregatingState<IN, OUT> getAggregatingState(
                AggregatingStateDescriptor<IN, ACC, OUT> stateProperties) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <UK, UV> MapState<UK, UV> getMapState(MapStateDescriptor<UK, UV> stateProperties) {
            return new MapState<UK, UV>() {

                private Map<UK, UV> values;

                private Map<UK, UV> getOrSetMap() {
                    if (values == null) {
                        this.values = new HashMap<>();
                    }
                    return values;
                }

                @Override
                public UV get(UK key) throws Exception {
                    stateReads++;
                    if (values == null) {
                        return null;
                    }

                    return values.get(key);
                }

                @Override
                public void put(UK key, UV value) throws Exception {
                    stateWrites++;
                    getOrSetMap().put(key, value);
                }

                @Override
                public void putAll(Map<UK, UV> map) throws Exception {
                    stateWrites++;
                    getOrSetMap().putAll(map);
                }

                @Override
                public void remove(UK key) throws Exception {
                    if (values == null) {
                        return;
                    }

                    stateWrites++;
                    values.remove(key);
                }

                @Override
                public boolean contains(UK key) throws Exception {
                    if (values == null) {
                        return false;
                    }

                    stateReads++;
                    return values.containsKey(key);
                }

                @Override
                public Iterable<Map.Entry<UK, UV>> entries() throws Exception {
                    if (values == null) {
                        return Collections.emptyList();
                    }

                    return () -> new CountingIterator<>(values.entrySet().iterator());
                }

                @Override
                public Iterable<UK> keys() throws Exception {
                    if (values == null) {
                        return Collections.emptyList();
                    }

                    return () -> new CountingIterator<>(values.keySet().iterator());
                }

                @Override
                public Iterable<UV> values() throws Exception {
                    if (values == null) {
                        return Collections.emptyList();
                    }

                    return () -> new CountingIterator<>(values.values().iterator());
                }

                @Override
                public Iterator<Map.Entry<UK, UV>> iterator() throws Exception {
                    if (values == null) {
                        return Iterators.emptyIterator();
                    }

                    return new CountingIterator<>(values.entrySet().iterator());
                }

                @Override
                public boolean isEmpty() throws Exception {
                    if (values == null) {
                        return true;
                    }

                    return values.isEmpty();
                }

                @Override
                public void clear() {
                    stateWrites++;
                    this.values = null;
                }
            };
        }

        private class CountingIterator<T> implements Iterator<T> {

            private final Iterator<T> iterator;

            CountingIterator(Iterator<T> iterator) {
                this.iterator = iterator;
            }

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public T next() {
                stateReads++;
                return iterator.next();
            }

            @Override
            public void remove() {
                stateWrites++;
                iterator.remove();
            }
        }
    }
}
