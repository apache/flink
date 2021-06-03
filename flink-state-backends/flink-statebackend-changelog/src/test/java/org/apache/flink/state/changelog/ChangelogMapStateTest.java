/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.changelog;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.heap.InternalKeyContextImpl;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.util.function.FunctionWithException;
import org.apache.flink.util.function.ThrowingConsumer;

import org.junit.Test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Consumer;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** ChangelogMapState Test. */
@SuppressWarnings({"rawtypes", "unchecked"})
public class ChangelogMapStateTest {

    @Test
    public void testValuesIterator() throws Exception {
        testIterator(singletonMap("key", "value"), state -> state.values().iterator(), "value");
    }

    @Test
    public void testKeysIterator() throws Exception {
        testIterator(singletonMap("key", "value"), state -> state.keys().iterator(), "key");
    }

    @Test
    public void testEntriesIterator() throws Exception {
        Map<String, String> map = singletonMap("key", "value");
        Map.Entry<String, String> entry = map.entrySet().iterator().next();
        testIterator(map, state -> state.entries().iterator(), entry);
    }

    @Test
    public void testEntryUpdateRecorded() throws Exception {
        testRecorded(
                singletonMap("x", "y"),
                state ->
                        ((Iterable<Map.Entry<String, String>>) state.entries())
                                .iterator()
                                .next()
                                .setValue("z"),
                logger -> assertTrue(logger.stateElementChanged));
    }

    @Test
    public void testPutRecorded() throws Exception {
        testRecorded(
                emptyMap(),
                state -> state.put("x", "y"),
                logger -> assertTrue(logger.stateElementChanged));
    }

    @Test
    public void testPutAllRecorded() throws Exception {
        Map<String, String> map = singletonMap("x", "y");
        testRecorded(
                emptyMap(), state -> state.putAll(map), logger -> assertEquals(map, logger.state));
    }

    @Test
    public void testRemoveRecorded() throws Exception {
        testRecorded(
                singletonMap("x", "y"),
                state -> state.remove("x"),
                logger -> assertTrue(logger.stateElementRemoved));
    }

    @Test
    public void testGetNotRecorded() throws Exception {
        testRecorded(
                singletonMap("x", "y"),
                state -> state.get("x"),
                logger -> assertFalse(logger.anythingChanged()));
    }

    @Test
    public void testClearRecorded() throws Exception {
        testRecorded(
                singletonMap("x", "y"),
                ChangelogMapState::clear,
                logger -> assertTrue(logger.stateCleared));
    }

    private <T> void testIterator(
            Map<String, String> data,
            FunctionWithException<ChangelogMapState, Iterator<T>, Exception> iteratorSupplier,
            T... elements)
            throws Exception {
        TestChangeLoggerKv logger = TestChangeLoggerKv.forMap(data);
        ChangelogMapState state = createState(data, logger);

        Iterator iterator = iteratorSupplier.apply(state);
        for (T el : elements) {
            assertTrue(iterator.hasNext());
            assertEquals(el, iterator.next());
            iterator.remove();
        }

        assertFalse(iterator.hasNext());
        assertTrue(state.isEmpty());
        assertTrue(logger.stateElementRemoved);
    }

    private void testRecorded(
            Map<String, String> data,
            ThrowingConsumer<ChangelogMapState, Exception> action,
            Consumer<TestChangeLoggerKv<Map<String, String>>> assertion)
            throws Exception {
        TestChangeLoggerKv logger = TestChangeLoggerKv.forMap(data);
        ChangelogMapState state = createState(data, logger);
        action.accept(state);
        assertion.accept(logger);
    }

    private static class TestingInternalMapState
            implements InternalMapState<String, String, String, String> {

        private final TypeSerializer<String> serializer;
        private final MapSerializer<String, String> mapSerializer;
        private final Map<String, String> map;

        public TestingInternalMapState(Map<String, String> stringStringMap) {
            serializer = TypeInformation.of(String.class).createSerializer(new ExecutionConfig());
            mapSerializer = new MapSerializer<>(serializer, serializer);
            map = new HashMap<>(stringStringMap);
        }

        @Override
        public String get(String key) {
            return map.get(key);
        }

        @Override
        public void put(String key, String value) {
            map.put(key, value);
        }

        @Override
        public void putAll(Map<String, String> map) {
            this.map.putAll(map);
        }

        @Override
        public void remove(String key) {
            map.remove(key);
        }

        @Override
        public boolean contains(String key) {
            return map.containsKey(key);
        }

        @Override
        public Iterable<Map.Entry<String, String>> entries() {
            return map.entrySet();
        }

        @Override
        public Iterable<String> keys() {
            return map.keySet();
        }

        @Override
        public Iterable<String> values() {
            return map.values();
        }

        @Override
        public Iterator<Map.Entry<String, String>> iterator() {
            return map.entrySet().iterator();
        }

        @Override
        public boolean isEmpty() {
            return map.isEmpty();
        }

        @Override
        public TypeSerializer<String> getKeySerializer() {
            return serializer;
        }

        @Override
        public TypeSerializer<String> getNamespaceSerializer() {
            return serializer;
        }

        @Override
        public TypeSerializer<Map<String, String>> getValueSerializer() {
            return mapSerializer;
        }

        @Override
        public void setCurrentNamespace(String namespace) {}

        @Override
        public byte[] getSerializedValue(
                byte[] serializedKeyAndNamespace,
                TypeSerializer<String> safeKeySerializer,
                TypeSerializer<String> safeNamespaceSerializer,
                TypeSerializer<Map<String, String>> safeValueSerializer) {
            return new byte[0];
        }

        @Override
        public StateIncrementalVisitor<String, String, Map<String, String>>
                getStateIncrementalVisitor(int recommendedMaxNumberOfReturnedRecords) {
            return null;
        }

        @Override
        public void clear() {
            map.clear();
        }
    }

    private static ChangelogMapState createState(
            Map<String, String> data, TestChangeLoggerKv logger) {
        ChangelogMapState state =
                new ChangelogMapState<>(
                        new TestingInternalMapState(data),
                        logger,
                        new InternalKeyContextImpl<>(KeyGroupRange.EMPTY_KEY_GROUP_RANGE, 0));
        state.setCurrentNamespace("ns0");
        return state;
    }
}
