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
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.function.FunctionWithException;
import org.apache.flink.util.function.ThrowingConsumer;

import org.junit.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** ChangelogKeyGroupedPriorityQueue Test. */
@SuppressWarnings({"rawtypes", "unchecked"})
public class ChangelogPqStateTest {

    @Test
    public void testValuesIterator() throws Exception {
        testIterator(singletonList("value"), ChangelogKeyGroupedPriorityQueue::iterator, "value");
    }

    @Test
    public void testPutRecorded() throws Exception {
        testRecorded(
                emptyList(),
                state -> state.add(StringKey.of("x")),
                logger -> assertTrue(logger.stateElementAdded));
    }

    @Test
    public void testPollRecorded() throws Exception {
        testRecorded(
                singletonList("x"),
                ChangelogKeyGroupedPriorityQueue::poll,
                logger -> assertTrue(logger.stateElementRemoved));
    }

    @Test
    public void testRemoveRecorded() throws Exception {
        testRecorded(
                singletonList("x"),
                state -> state.remove(StringKey.of("x")),
                logger -> assertTrue(logger.stateElementRemoved));
    }

    @Test
    public void testAddAllRecorded() throws Exception {
        testRecorded(
                emptyList(),
                state -> state.addAll(singletonList("x")),
                logger -> assertTrue(logger.stateElementAdded));
    }

    @Test
    public void testGetNotRecorded() throws Exception {
        testRecorded(
                singletonList("x"),
                ChangelogKeyGroupedPriorityQueue::peek,
                logger -> assertFalse(logger.anythingChanged()));
    }

    private <T> void testIterator(
            List<String> data,
            FunctionWithException<ChangelogKeyGroupedPriorityQueue, Iterator<T>, Exception>
                    iteratorSupplier,
            T... elements)
            throws Exception {
        TestPriorityQueueChangeLogger<String, StringKey> logger =
                new TestPriorityQueueChangeLogger<>();
        ChangelogKeyGroupedPriorityQueue<String, StringKey> state =
                new ChangelogKeyGroupedPriorityQueue<>(
                        new TestingInternalQueueState(data),
                        logger,
                        TypeInformation.of(StringKey.class)
                                .createSerializer(new ExecutionConfig()));

        Iterator iterator = iteratorSupplier.apply(state);
        for (T el : elements) {
            assertTrue(iterator.hasNext());
            assertEquals(el, ((StringKey) iterator.next()).key);
            iterator.remove();
        }

        assertFalse(iterator.hasNext());
        assertTrue(state.isEmpty());
        assertTrue(logger.stateElementRemoved);
    }

    private void testRecorded(
            List<String> data,
            ThrowingConsumer<ChangelogKeyGroupedPriorityQueue, Exception> action,
            Consumer<TestPriorityQueueChangeLogger> assertion)
            throws Exception {
        TestPriorityQueueChangeLogger logger = new TestPriorityQueueChangeLogger();
        ChangelogKeyGroupedPriorityQueue state =
                new ChangelogKeyGroupedPriorityQueue<String, StringKey>(
                        new TestingInternalQueueState(data),
                        logger,
                        TypeInformation.of(StringKey.class)
                                .createSerializer(new ExecutionConfig()));
        action.accept(state);
        assertion.accept(logger);
    }

    private static class TestPriorityQueueChangeLogger<K, T>
            implements PriorityQueueStateChangeLogger<K, T, Void> {
        public boolean stateElementChanged;
        public boolean stateCleared;
        public boolean stateElementRemoved;
        public boolean stateElementAdded;

        @Override
        public void valueUpdated(T newState, Void ns) {
            stateElementChanged = true;
        }

        @Override
        public void valueUpdatedInternal(T newState, Void ns) {
            stateElementChanged = true;
        }

        @Override
        public void valueAdded(T addedState, Void ns) {
            stateElementChanged = true;
        }

        @Override
        public void valueCleared(Void ns) {
            stateCleared = true;
        }

        @Override
        public void valueElementAdded(
                ThrowingConsumer<DataOutputViewStreamWrapper, IOException> dataSerializer,
                Void ns) {
            stateElementAdded = true;
        }

        @Override
        public void valueElementAddedOrUpdated(
                ThrowingConsumer<DataOutputViewStreamWrapper, IOException> dataSerializer,
                Void ns) {
            stateElementChanged = true;
        }

        @Override
        public void valueElementRemoved(
                ThrowingConsumer<DataOutputViewStreamWrapper, IOException> dataSerializer,
                Void ns) {
            stateElementRemoved = true;
        }

        @Override
        public void valueElementRemoved(
                K key,
                ThrowingConsumer<DataOutputViewStreamWrapper, IOException> dataSerializer,
                Void unused) {
            stateElementRemoved = true;
        }

        @Override
        public void resetWritingMetaFlag() {}

        public boolean anythingChanged() {
            return stateElementChanged || stateElementRemoved || stateCleared;
        }

        @Override
        public void close() {}
    }

    private static class TestingInternalQueueState
            implements KeyGroupedInternalPriorityQueue<StringKey> {
        private final Queue<StringKey> queue;

        public TestingInternalQueueState(List<String> data) {
            this.queue =
                    data.stream()
                            .map(StringKey::of)
                            .collect(Collectors.toCollection(LinkedList::new));
        }

        @Nullable
        @Override
        public StringKey poll() {
            return queue.poll();
        }

        @Nullable
        @Override
        public StringKey peek() {
            return queue.peek();
        }

        @Override
        public boolean add(@Nonnull StringKey toAdd) {
            return queue.offer(toAdd);
        }

        @Override
        public boolean remove(@Nonnull StringKey toRemove) {
            return queue.remove(toRemove);
        }

        @Override
        public boolean isEmpty() {
            return queue.isEmpty();
        }

        @Override
        public int size() {
            return queue.size();
        }

        @Override
        public void addAll(@Nullable Collection<? extends StringKey> toAdd) {
            if (toAdd != null) {
                queue.addAll(toAdd);
            }
        }

        @Nonnull
        @Override
        public CloseableIterator<StringKey> iterator() {
            return CloseableIterator.adapterForIterator(queue.iterator());
        }

        @Nonnull
        @Override
        public Set<StringKey> getSubsetForKeyGroup(int keyGroupId) {
            throw new UnsupportedOperationException();
        }
    }

    private static class StringKey implements Keyed<String> {
        public static StringKey of(String key) {
            return new StringKey(key);
        }

        private final String key;

        StringKey(String key) {
            this.key = key;
        }

        @Override
        public String getKey() {
            return key;
        }
    }
}
