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

package org.apache.flink.table.runtime.operators.process;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.table.annotation.ArgumentTrait;
import org.apache.flink.table.annotation.StateKind;
import org.apache.flink.table.api.TableRuntimeException;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.function.SupplierWithException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;

/**
 * Adapters around Flink's {@link BroadcastState} for {@link StateKind#BROADCAST} state entries of
 * PTFs that declare a table argument with {@link ArgumentTrait#BROADCAST_SEMANTIC_TABLE}.
 *
 * <p>Broadcast state is exposed as {@link MapState} or {@link ValueState} such that the regular
 * data views and the eager value state logic can be reused. Write access is only granted while
 * processing a broadcast table.
 */
@Internal
public final class BroadcastStateAdapters {

    /** {@link MapState} backed by {@link BroadcastState}. */
    public static final class BroadcastMapState<K, V> implements MapState<K, V> {

        private final String stateName;
        private final BroadcastState<K, V> state;
        private final BooleanSupplier isProcessingBroadcast;

        public BroadcastMapState(
                String stateName,
                BroadcastState<K, V> state,
                BooleanSupplier isProcessingBroadcast) {
            this.stateName = stateName;
            this.state = state;
            this.isProcessingBroadcast = isProcessingBroadcast;
        }

        @Override
        public V get(K key) throws Exception {
            return state.get(key);
        }

        @Override
        public void put(K key, V value) throws Exception {
            checkWriteAccess();
            state.put(key, value);
        }

        @Override
        public void putAll(Map<K, V> map) throws Exception {
            checkWriteAccess();
            state.putAll(map);
        }

        @Override
        public void remove(K key) throws Exception {
            checkWriteAccess();
            state.remove(key);
        }

        @Override
        public boolean contains(K key) throws Exception {
            return state.contains(key);
        }

        @Override
        public Iterable<Map.Entry<K, V>> entries() throws Exception {
            // Entries of the heap-based broadcast state are mutable
            return isProcessingBroadcast.getAsBoolean()
                    ? state.entries()
                    : state.immutableEntries();
        }

        @Override
        public Iterable<K> keys() throws Exception {
            final List<K> keys = new ArrayList<>();
            for (Map.Entry<K, V> entry : state.immutableEntries()) {
                keys.add(entry.getKey());
            }
            return keys;
        }

        @Override
        public Iterable<V> values() throws Exception {
            final List<V> values = new ArrayList<>();
            for (Map.Entry<K, V> entry : state.immutableEntries()) {
                values.add(entry.getValue());
            }
            return values;
        }

        @Override
        public Iterator<Map.Entry<K, V>> iterator() throws Exception {
            return entries().iterator();
        }

        @Override
        public boolean isEmpty() throws Exception {
            return !state.immutableEntries().iterator().hasNext();
        }

        @Override
        public void clear() {
            checkWriteAccess();
            state.clear();
        }

        private void checkWriteAccess() {
            if (!isProcessingBroadcast.getAsBoolean()) {
                throw broadcastStateIsReadOnly(stateName);
            }
        }
    }

    /**
     * {@link ValueState} backed by {@link BroadcastState}. The value is stored as a single entry.
     */
    public static final class BroadcastValueState<V> implements ValueState<V> {

        private final String stateName;
        private final BroadcastState<VoidNamespace, V> state;
        private final BooleanSupplier isProcessingBroadcast;

        public BroadcastValueState(
                String stateName,
                BroadcastState<VoidNamespace, V> state,
                BooleanSupplier isProcessingBroadcast) {
            this.stateName = stateName;
            this.state = state;
            this.isProcessingBroadcast = isProcessingBroadcast;
        }

        @Override
        public V value() throws IOException {
            return rethrow(() -> state.get(VoidNamespace.INSTANCE));
        }

        @Override
        public void update(V value) throws IOException {
            checkWriteAccess();
            rethrow(
                    () -> {
                        if (value == null) {
                            state.remove(VoidNamespace.INSTANCE);
                        } else {
                            state.put(VoidNamespace.INSTANCE, value);
                        }
                        return null;
                    });
        }

        @Override
        public void clear() {
            checkWriteAccess();
            try {
                rethrow(
                        () -> {
                            state.remove(VoidNamespace.INSTANCE);
                            return null;
                        });
            } catch (IOException e) {
                throw new FlinkRuntimeException(e);
            }
        }

        private void checkWriteAccess() {
            if (!isProcessingBroadcast.getAsBoolean()) {
                throw broadcastStateIsReadOnly(stateName);
            }
        }
    }

    // --------------------------------------------------------------------------------------------

    /**
     * Calls the given state access. I/O and runtime exceptions of the state backend are forwarded
     * as-is, other checked exceptions are wrapped.
     */
    private static <T> T rethrow(SupplierWithException<T, Exception> stateAccess)
            throws IOException {
        try {
            return stateAccess.get();
        } catch (IOException | RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new FlinkRuntimeException(e);
        }
    }

    public static TableRuntimeException broadcastStateIsReadOnly(String stateName) {
        return new TableRuntimeException(
                String.format(
                        "Broadcast state entry '%s' is read-only while processing a table "
                                + "with row or set semantics.",
                        stateName));
    }

    private BroadcastStateAdapters() {
        // no instantiation
    }
}
