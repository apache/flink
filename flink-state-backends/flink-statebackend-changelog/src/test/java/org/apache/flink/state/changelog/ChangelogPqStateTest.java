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

import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
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
                state -> state.add("x"),
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
                state -> state.remove("x"),
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
        TestPriorityQueueChangeLogger logger = new TestPriorityQueueChangeLogger();
        ChangelogKeyGroupedPriorityQueue<String> state =
                new ChangelogKeyGroupedPriorityQueue<String>(
                        new TestingInternalQueueState(data), logger, new StringSerializer());

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
            List<String> data,
            ThrowingConsumer<ChangelogKeyGroupedPriorityQueue, Exception> action,
            Consumer<TestPriorityQueueChangeLogger> assertion)
            throws Exception {
        TestPriorityQueueChangeLogger logger = new TestPriorityQueueChangeLogger();
        ChangelogKeyGroupedPriorityQueue state =
                new ChangelogKeyGroupedPriorityQueue<String>(
                        new TestingInternalQueueState(data), logger, new StringSerializer());
        action.accept(state);
        assertion.accept(logger);
    }

    private static class TestPriorityQueueChangeLogger<T> implements StateChangeLogger<T, Void> {
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
                ThrowingConsumer<DataOutputView, IOException> dataSerializer, Void ns) {
            stateElementAdded = true;
        }

        @Override
        public void valueElementAddedOrUpdated(
                ThrowingConsumer<DataOutputView, IOException> dataSerializer, Void ns) {
            stateElementChanged = true;
        }

        @Override
        public void valueElementRemoved(
                ThrowingConsumer<DataOutputView, IOException> dataSerializer, Void ns) {
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
            implements KeyGroupedInternalPriorityQueue<String> {
        private final Queue<String> queue;

        public TestingInternalQueueState(List<String> data) {
            this.queue = data instanceof Queue ? (Queue<String>) data : new LinkedList<>(data);
        }

        @Nullable
        @Override
        public String poll() {
            return queue.poll();
        }

        @Nullable
        @Override
        public String peek() {
            return queue.peek();
        }

        @Override
        public boolean add(@Nonnull String toAdd) {
            return queue.offer(toAdd);
        }

        @Override
        public boolean remove(@Nonnull String toRemove) {
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
        public void addAll(@Nullable Collection<? extends String> toAdd) {
            if (toAdd != null) {
                queue.addAll(toAdd);
            }
        }

        @Nonnull
        @Override
        public CloseableIterator<String> iterator() {
            return CloseableIterator.adapterForIterator(queue.iterator());
        }

        @Nonnull
        @Override
        public Set<String> getSubsetForKeyGroup(int keyGroupId) {
            throw new UnsupportedOperationException();
        }
    }
}
