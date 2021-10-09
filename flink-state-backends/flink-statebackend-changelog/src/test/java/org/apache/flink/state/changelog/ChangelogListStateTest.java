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
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.heap.InternalKeyContextImpl;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.util.function.FunctionWithException;
import org.apache.flink.util.function.ThrowingConsumer;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** ChangelogListState Test. */
@SuppressWarnings({"rawtypes", "unchecked"})
public class ChangelogListStateTest {

    @Test
    public void testValuesIterator() throws Exception {
        testIterator(singletonList("value"), state -> state.get().iterator(), "value");
    }

    @Test
    public void testPutRecorded() throws Exception {
        testRecorded(
                emptyList(),
                state -> state.add("x"),
                logger -> assertTrue(logger.stateElementAdded));
    }

    @Test
    public void testAddAllRecorded() throws Exception {
        List<String> list = Arrays.asList("a", "b", "c");
        testRecorded(
                emptyList(),
                state -> state.addAll(list),
                logger -> assertEquals(list, logger.state));
    }

    @Test
    public void testGetNotRecorded() throws Exception {
        testRecorded(
                singletonList("x"),
                ChangelogListState::get,
                logger -> assertFalse(logger.anythingChanged()));
    }

    @Test
    public void testClearRecorded() throws Exception {
        testRecorded(
                singletonList("x"),
                ChangelogListState::clear,
                logger -> assertTrue(logger.stateCleared));
    }

    private <T> void testIterator(
            List<String> data,
            FunctionWithException<ChangelogListState, Iterator<T>, Exception> iteratorSupplier,
            T... elements)
            throws Exception {
        TestChangeLoggerKv logger = TestChangeLoggerKv.forList(data);
        ChangelogListState state = createState(data, logger);

        Iterator iterator = iteratorSupplier.apply(state);
        for (T el : elements) {
            assertTrue(iterator.hasNext());
            assertEquals(el, iterator.next());
            iterator.remove();
        }

        assertFalse(iterator.hasNext());
        assertTrue(state.getInternal().isEmpty());
        // changes to the rocksdb list iterator are not propagated back - expect the same here
        assertFalse(logger.stateElementRemoved);
    }

    private void testRecorded(
            List<String> data,
            ThrowingConsumer<ChangelogListState, Exception> action,
            Consumer<TestChangeLoggerKv> assertion)
            throws Exception {
        TestChangeLoggerKv logger = TestChangeLoggerKv.forList(data);
        ChangelogListState state = createState(data, logger);
        action.accept(state);
        assertion.accept(logger);
    }

    private static class TestingInternalListState
            implements InternalListState<String, String, String> {

        private final TypeSerializer<String> serializer;
        private final ListSerializer<String> listSerializer;
        private final List<String> list;

        public TestingInternalListState(List<String> stringStringList) {
            serializer = TypeInformation.of(String.class).createSerializer(new ExecutionConfig());
            listSerializer = new ListSerializer<>(serializer);
            list = new ArrayList<>(stringStringList);
        }

        @Override
        public List<String> get() {
            return list;
        }

        @Override
        public void add(String value) {
            list.add(value);
        }

        @Override
        public void update(List<String> values) throws Exception {}

        @Override
        public void addAll(List<String> list) {
            this.list.addAll(list);
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
        public TypeSerializer<List<String>> getValueSerializer() {
            return listSerializer;
        }

        @Override
        public void setCurrentNamespace(String namespace) {}

        @Override
        public byte[] getSerializedValue(
                byte[] serializedKeyAndNamespace,
                TypeSerializer<String> safeKeySerializer,
                TypeSerializer<String> safeNamespaceSerializer,
                TypeSerializer<List<String>> safeValueSerializer) {
            return new byte[0];
        }

        @Override
        public StateIncrementalVisitor<String, String, List<String>> getStateIncrementalVisitor(
                int recommendedMaxNumberOfReturnedRecords) {
            return null;
        }

        @Override
        public void clear() {
            list.clear();
        }

        @Override
        public List<String> getInternal() {
            return list;
        }

        @Override
        public void updateInternal(List<String> valueToStore) {
            list.clear();
            list.addAll(valueToStore);
        }

        @Override
        public void mergeNamespaces(String target, Collection<String> sources) {
            throw new UnsupportedOperationException();
        }
    }

    private static ChangelogListState createState(List<String> data, TestChangeLoggerKv logger) {
        ChangelogListState state =
                new ChangelogListState<>(
                        new TestingInternalListState(data),
                        logger,
                        new InternalKeyContextImpl<>(KeyGroupRange.EMPTY_KEY_GROUP_RANGE, 0));
        state.setCurrentNamespace("ns0");
        return state;
    }
}
