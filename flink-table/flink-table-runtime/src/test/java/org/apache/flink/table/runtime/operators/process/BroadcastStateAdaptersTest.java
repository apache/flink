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

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.table.api.TableRuntimeException;
import org.apache.flink.table.runtime.operators.process.BroadcastStateAdapters.BroadcastMapState;
import org.apache.flink.table.runtime.operators.process.BroadcastStateAdapters.BroadcastValueState;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link BroadcastStateAdapters}. */
class BroadcastStateAdaptersTest {

    private boolean processingBroadcast;

    @Test
    void testBroadcastMapState() throws Exception {
        final TestBroadcastState<String, Integer> backing = new TestBroadcastState<>();
        final BroadcastMapState<String, Integer> state =
                new BroadcastMapState<>("s", backing, () -> processingBroadcast);

        processingBroadcast = true;
        state.put("a", 1);
        state.putAll(Map.of("b", 2, "c", 3));
        state.remove("c");
        assertThat(backing.map).containsExactlyInAnyOrderEntriesOf(Map.of("a", 1, "b", 2));
        // Entries are mutable while processing a broadcast table
        state.entries().iterator().next().setValue(42);

        processingBroadcast = false;
        assertThat(state.get("b")).isEqualTo(2);
        assertThat(state.contains("a")).isTrue();
        assertThat(state.keys()).containsExactlyInAnyOrder("a", "b");
        assertThat(state.values()).contains(2, 42);
        assertThat(state.isEmpty()).isFalse();
        assertThatThrownBy(() -> state.put("x", 1)).satisfies(this::isReadOnly);
        assertThatThrownBy(() -> state.putAll(Map.of("x", 1))).satisfies(this::isReadOnly);
        assertThatThrownBy(() -> state.remove("a")).satisfies(this::isReadOnly);
        assertThatThrownBy(state::clear).satisfies(this::isReadOnly);
        // Entries are immutable while processing other tables
        assertThatThrownBy(() -> state.entries().iterator().next().setValue(1))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(
                        () -> {
                            final Iterator<Map.Entry<String, Integer>> it = state.iterator();
                            it.next();
                            it.remove();
                        })
                .isInstanceOf(UnsupportedOperationException.class);

        processingBroadcast = true;
        state.clear();
        assertThat(state.isEmpty()).isTrue();
    }

    @Test
    void testBroadcastValueState() throws Exception {
        final TestBroadcastState<VoidNamespace, String> backing = new TestBroadcastState<>();
        final BroadcastValueState<String> state =
                new BroadcastValueState<>("s", backing, () -> processingBroadcast);

        assertThat(state.value()).isNull();
        assertThatThrownBy(() -> state.update("a")).satisfies(this::isReadOnly);
        assertThatThrownBy(state::clear).satisfies(this::isReadOnly);

        processingBroadcast = true;
        state.update("a");
        assertThat(backing.map).containsExactlyEntriesOf(Map.of(VoidNamespace.INSTANCE, "a"));

        processingBroadcast = false;
        assertThat(state.value()).isEqualTo("a");
        assertThatThrownBy(state::clear).satisfies(this::isReadOnly);
        assertThatThrownBy(() -> state.update(null)).satisfies(this::isReadOnly);

        processingBroadcast = true;
        state.update(null);
        assertThat(backing.map).isEmpty();
    }

    private void isReadOnly(Throwable t) {
        assertThat(t)
                .isInstanceOf(TableRuntimeException.class)
                .hasMessageContaining("Broadcast state entry 's' is read-only");
    }

    // --------------------------------------------------------------------------------------------

    private static class TestBroadcastState<K, V> implements BroadcastState<K, V> {

        final Map<K, V> map = new HashMap<>();

        @Override
        public void put(K key, V value) {
            map.put(key, value);
        }

        @Override
        public void putAll(Map<K, V> map) {
            this.map.putAll(map);
        }

        @Override
        public void remove(K key) {
            map.remove(key);
        }

        @Override
        public Iterator<Map.Entry<K, V>> iterator() {
            return map.entrySet().iterator();
        }

        @Override
        public Iterable<Map.Entry<K, V>> entries() {
            return map.entrySet();
        }

        @Override
        public V get(K key) {
            return map.get(key);
        }

        @Override
        public boolean contains(K key) {
            return map.containsKey(key);
        }

        @Override
        public Iterable<Map.Entry<K, V>> immutableEntries() {
            return Collections.unmodifiableMap(map).entrySet();
        }

        @Override
        public void clear() {
            map.clear();
        }
    }
}
