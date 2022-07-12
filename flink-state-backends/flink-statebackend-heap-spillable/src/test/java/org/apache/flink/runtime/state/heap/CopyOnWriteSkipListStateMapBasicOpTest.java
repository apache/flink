/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.state.StateEntry;
import org.apache.flink.runtime.state.StateTransformationFunction;
import org.apache.flink.runtime.state.heap.space.Allocator;
import org.apache.flink.runtime.state.heap.space.Chunk;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.IOUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import static org.apache.flink.runtime.state.heap.CopyOnWriteSkipListStateMapTestUtils.addToReferenceState;
import static org.apache.flink.runtime.state.heap.CopyOnWriteSkipListStateMapTestUtils.createEmptyStateMap;
import static org.apache.flink.runtime.state.heap.CopyOnWriteSkipListStateMapTestUtils.removeFromReferenceState;
import static org.apache.flink.runtime.state.heap.CopyOnWriteSkipListStateMapTestUtils.verifyState;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.HamcrestCondition.matching;
import static org.hamcrest.Matchers.contains;

/** Test basic operations of {@link CopyOnWriteSkipListStateMap}. */
class CopyOnWriteSkipListStateMapBasicOpTest {
    private final long namespace = 1L;
    private TestAllocator allocator;
    private CopyOnWriteSkipListStateMap<Integer, Long, String> stateMap;

    @BeforeEach
    void setUp() {
        allocator = new TestAllocator(256);
        stateMap = createEmptyStateMap(0, 0.0f, allocator);
    }

    @AfterEach
    void tearDown() {
        stateMap.close();
        IOUtils.closeQuietly(allocator);
    }

    /** Test initialization of state map. */
    @Test
    void testInitStateMap() {
        assertThat(stateMap.isEmpty()).isTrue();
        assertThat(stateMap.size()).isEqualTo(0);
        assertThat(stateMap.totalSize()).isEqualTo(0);
        assertThat(stateMap.getRequestCount()).isEqualTo(0);
        assertThat(stateMap.getLogicallyRemovedNodes()).isEmpty();
        assertThat(stateMap.getHighestRequiredSnapshotVersionPlusOne()).isEqualTo(0);
        assertThat(stateMap.getHighestFinishedSnapshotVersion()).isEqualTo(0);
        assertThat(stateMap.getSnapshotVersions()).isEmpty();
        assertThat(stateMap.getPruningValueNodes()).isEmpty();
        assertThat(stateMap.getResourceGuard().getLeaseCount()).isEqualTo(0);
        assertThat(stateMap.getResourceGuard().isClosed()).isFalse();
        assertThat(stateMap.isClosed()).isFalse();

        assertThat(stateMap.get(0, 0L)).isNull();
        assertThat(stateMap.containsKey(1, 2L)).isFalse();
        assertThat(stateMap.removeAndGetOld(3, 4L)).isNull();
        assertThat(stateMap.getKeys(-92L).iterator().hasNext()).isFalse();
        assertThat(stateMap.sizeOfNamespace(8L)).isEqualTo(0);
        assertThat(stateMap.iterator().hasNext()).isFalse();
        assertThat(stateMap.getStateIncrementalVisitor(100).hasNext()).isFalse();
    }

    /** Test state put and get. */
    @Test
    void testPutAndGetState() {
        int totalSize = 0;
        // test put empty skip list and get
        totalSize = putAndVerify(2, "2", totalSize, true);
        // put state at tail of the skip list (new key is bigger than all existing keys) and get
        totalSize = putAndVerify(4, "4", totalSize, true);
        // put state at head of the skip list (new key is smaller than all existing keys) and get
        totalSize = putAndVerify(1, "1", totalSize, true);
        // put state in the middle and get
        putAndVerify(3, "3", totalSize, true);
    }

    private int putAndVerify(int key, String state, int initTotalSize, boolean isNewKey) {
        stateMap.put(key, namespace, state);
        int totalSize = isNewKey ? initTotalSize + 1 : initTotalSize;
        assertThat(stateMap.get(key, namespace)).isEqualTo(state);
        assertThat(stateMap.size()).isEqualTo(totalSize);
        assertThat(stateMap.totalSize()).isEqualTo(totalSize);
        assertThat(allocator.getTotalSpaceNumber()).isEqualTo(totalSize * 2);
        return totalSize;
    }

    /** Test state update (put existing key). */
    @Test
    void testUpdateState() {
        int totalSize = 3;
        for (int i = 1; i <= totalSize; i++) {
            stateMap.put(i, namespace, String.valueOf(i));
        }
        // update state at tail of the skip list (new key is bigger than all existing keys)
        totalSize = putAndVerify(3, "33", totalSize, false);
        // update state at head of the skip list (new key is smaller than all existing keys)
        totalSize = putAndVerify(1, "11", totalSize, false);
        // update state in the middle
        putAndVerify(2, "22", totalSize, false);
    }

    /** Test state putAndGetOld. */
    @Test
    void testPutAndGetOldState() {
        int totalSize = 0;
        // test put empty skip list and get
        totalSize = putAndGetOldVerify(2, "2", totalSize);
        // put state at tail of the skip list (new key is bigger than all existing keys) and get
        totalSize = putAndGetOldVerify(4, "4", totalSize);
        // put state at head of the skip list (new key is smaller than all existing keys) and get
        totalSize = putAndGetOldVerify(1, "1", totalSize);
        // put state in the middle and get
        putAndGetOldVerify(3, "3", totalSize);
    }

    private int putAndGetOldVerify(int key, String state, int initTotalSize) {
        int totalSize = initTotalSize + 1;
        String oldState = stateMap.get(key, namespace);
        assertThat(stateMap.putAndGetOld(key, namespace, state)).isEqualTo(oldState);
        assertThat(stateMap.get(key, namespace)).isEqualTo(state);
        assertThat(stateMap.size()).isEqualTo(totalSize);
        assertThat(stateMap.totalSize()).isEqualTo(totalSize);
        assertThat(allocator.getTotalSpaceNumber()).isEqualTo(totalSize * 2);
        return totalSize;
    }

    /** Test state remove. */
    @Test
    void testRemoveState() {
        int totalSize = 4;
        for (int i = 1; i <= totalSize; i++) {
            stateMap.put(i, namespace, String.valueOf(i));
        }
        // test remove state in the middle
        totalSize = removeAndVerify(2, totalSize, true);
        // test remove state at tail
        totalSize = removeAndVerify(4, totalSize, true);
        // test remove state at head
        totalSize = removeAndVerify(1, totalSize, true);
        // test remove the single state
        totalSize = removeAndVerify(3, totalSize, true);
        // test remove none-existing state
        removeAndVerify(3, totalSize, false);
    }

    private int removeAndVerify(int key, int initTotalSize, boolean keyExists) {
        stateMap.remove(key, namespace);
        int totalSize = keyExists ? initTotalSize - 1 : initTotalSize;
        assertThat(stateMap.get(key, namespace)).isNull();
        assertThat(stateMap.size()).isEqualTo(totalSize);
        assertThat(stateMap.totalSize()).isEqualTo(totalSize);
        assertThat(allocator.getTotalSpaceNumber()).isEqualTo(totalSize * 2);
        return totalSize;
    }

    /** Test state removeAndGetOld. */
    @Test
    void testRemoveAndGetOldState() {
        int totalSize = 4;
        for (int i = 1; i <= totalSize; i++) {
            stateMap.put(i, namespace, String.valueOf(i));
        }
        // test remove state in the middle
        totalSize = removeAndGetOldVerify(2, totalSize);
        // test remove state at tail
        totalSize = removeAndGetOldVerify(4, totalSize);
        // test remove state at head
        totalSize = removeAndGetOldVerify(1, totalSize);
        // test remove the single state
        removeAndGetOldVerify(3, totalSize);
    }

    private int removeAndGetOldVerify(int key, int initTotalSize) {
        int totalSize = initTotalSize - 1;
        String oldState = stateMap.get(key, namespace);
        assertThat(stateMap.removeAndGetOld(key, namespace)).isEqualTo(oldState);
        assertThat(stateMap.get(key, namespace)).isNull();
        assertThat(stateMap.size()).isEqualTo(totalSize);
        assertThat(stateMap.totalSize()).isEqualTo(totalSize);
        assertThat(allocator.getTotalSpaceNumber()).isEqualTo(totalSize * 2);
        return totalSize;
    }

    /** Test state transform with existing key. */
    @Test
    void testTransformExistingState() throws Exception {
        final int key = 1;
        final String oldState = "1";
        final int delta = 1;
        StateTransformationFunction<String, Integer> function =
                (String prevState, Integer value) ->
                        prevState == null ? String.valueOf(value) : prevState + value;
        stateMap.put(key, namespace, oldState);
        String expectedState = function.apply(oldState, delta);
        stateMap.transform(key, namespace, delta, function);
        assertThat(stateMap.get(key, namespace)).isEqualTo(expectedState);
        assertThat(stateMap.size()).isEqualTo(1);
        assertThat(stateMap.totalSize()).isEqualTo(1);
        assertThat(allocator.getTotalSpaceNumber()).isEqualTo(2);
    }

    /** Test state transform with new key. */
    @Test
    void testTransformAbsentState() throws Exception {
        final int key = 1;
        final int delta = 1;
        StateTransformationFunction<String, Integer> function =
                (String prevState, Integer value) ->
                        prevState == null ? String.valueOf(value) : prevState + value;
        String expectedState = function.apply(null, delta);
        stateMap.transform(key, namespace, delta, function);
        assertThat(stateMap.get(key, namespace)).isEqualTo(expectedState);
        assertThat(stateMap.size()).isEqualTo(1);
        assertThat(stateMap.totalSize()).isEqualTo(1);
        assertThat(allocator.getTotalSpaceNumber()).isEqualTo(2);
    }

    /** Test close of state map. */
    @Test
    void testCloseStateMap() {
        stateMap.close();
        assertThat(stateMap.isClosed()).isTrue();
        assertThat(stateMap.size()).isEqualTo(0);
        assertThat(stateMap.totalSize()).isEqualTo(0);
        assertThat(allocator.getTotalSpaceNumber()).isEqualTo(0);
        // make sure double close won't cause problem
        stateMap.close();
    }

    /** Make sure exception will be thrown with allocation failure rather than swallowed. */
    @Test
    void testPutWithAllocationFailure() {
        Allocator exceptionalAllocator =
                new Allocator() {
                    @Override
                    public long allocate(int size) {
                        throw new RuntimeException("Exception on purpose");
                    }

                    @Override
                    public void free(long address) {}

                    @Override
                    @Nullable
                    public Chunk getChunkById(int chunkId) {
                        return null;
                    }

                    @Override
                    public void close() {}
                };
        try (CopyOnWriteSkipListStateMap<Integer, Long, String> stateMap =
                createEmptyStateMap(0, 0.0f, exceptionalAllocator)) {
            assertThatThrownBy(() -> stateMap.put(1, 1L, "test-value"))
                    .as("Should have thrown exception when space allocation fails")
                    .isInstanceOf(FlinkRuntimeException.class);
        }
    }

    /**
     * This tests the internal capability of using partial {@link ByteBuffer}, making sure the
     * internal methods works when put/get state with a key stored at a none-zero offset of a
     * ByteBuffer.
     */
    @Test
    void testPutAndGetNodeWithNoneZeroOffset() {
        final int key = 10;
        final long namespace = 0L;
        final String valueString = "test";
        SkipListKeySerializer<Integer, Long> skipListKeySerializer =
                new SkipListKeySerializer<>(IntSerializer.INSTANCE, LongSerializer.INSTANCE);
        SkipListValueSerializer<String> skipListValueSerializer =
                new SkipListValueSerializer<>(StringSerializer.INSTANCE);
        byte[] keyBytes = skipListKeySerializer.serialize(key, namespace);
        byte[] constructedKeyBytes = new byte[keyBytes.length + 1];
        System.arraycopy(keyBytes, 0, constructedKeyBytes, 1, keyBytes.length);
        MemorySegment keySegment = MemorySegmentFactory.wrap(constructedKeyBytes);
        int keyLen = keyBytes.length;
        byte[] value = skipListValueSerializer.serialize(valueString);
        stateMap.putValue(keySegment, 1, keyLen, value, false);
        String state = stateMap.getNode(keySegment, 1, keyLen);
        assertThat(state).isEqualTo(valueString);
    }

    /** Test next/update/remove during global iteration of StateIncrementalVisitor. */
    @Test
    void testStateIncrementalVisitor() {
        // map to store expected states, namespace -> key -> state
        Map<Long, Map<Integer, String>> referenceStates = new HashMap<>();

        // put some states
        for (long namespace = 0; namespace < 15; namespace++) {
            for (int key = 0; key < 20; key++) {
                String state = String.valueOf(namespace * key);
                stateMap.put(key, namespace, state);
                addToReferenceState(referenceStates, key, namespace, state);
            }
        }
        verifyState(referenceStates, stateMap);

        InternalKvState.StateIncrementalVisitor<Integer, Long, String> visitor =
                stateMap.getStateIncrementalVisitor(5);
        int op = 0;
        while (visitor.hasNext()) {
            for (StateEntry<Integer, Long, String> stateEntry : visitor.nextEntries()) {
                int key = stateEntry.getKey();
                long namespace = stateEntry.getNamespace();
                String state = stateEntry.getState();
                assertThat(stateMap.get(key, namespace)).isEqualTo(state);
                switch (op) {
                    case 0:
                        visitor.remove(stateEntry);
                        removeFromReferenceState(referenceStates, key, namespace);
                        op = 1;
                        break;
                    case 1:
                        String newState = state + "-update";
                        visitor.update(stateEntry, newState);
                        addToReferenceState(referenceStates, key, namespace, newState);
                        op = 2;
                        break;
                    default:
                        op = 0;
                        break;
                }
            }
        }
        verifyState(referenceStates, stateMap);
    }

    /** Test StateIncrementalVisitor with closed state map. */
    @Test
    void testStateIncrementalVisitorWithClosedStateMap() {
        CopyOnWriteSkipListStateMap<Integer, Long, String> stateMap = createEmptyStateMap();
        // put some states
        for (long namespace = 0; namespace < 15; namespace++) {
            for (int key = 0; key < 20; key++) {
                String state = String.valueOf(namespace * key);
                stateMap.put(key, namespace, state);
            }
        }
        InternalKvState.StateIncrementalVisitor<Integer, Long, String> closedVisitor =
                stateMap.getStateIncrementalVisitor(5);
        assertThat(closedVisitor.hasNext()).isTrue();

        stateMap.close();
        // the visitor will be invalid after state map is closed
        assertThat(closedVisitor.hasNext()).isFalse();
    }

    /** Test basic snapshot correctness. */
    @Test
    void testBasicSnapshot() {
        // take an empty snapshot
        CopyOnWriteSkipListStateMapSnapshot<Integer, Long, String> snapshot =
                stateMap.stateSnapshot();
        snapshot.release();
        // put some states
        stateMap.put(1, namespace, "1");
        // take the 2nd snapshot with data
        CopyOnWriteSkipListStateMapSnapshot<Integer, Long, String> snapshot2 =
                stateMap.stateSnapshot();
        assertThat(stateMap.getStateMapVersion()).isEqualTo(2);
        assertThat(stateMap.getHighestRequiredSnapshotVersionPlusOne()).isEqualTo(2);
        assertThat(stateMap.getSnapshotVersions()).hasSize(1);
        assertThat(stateMap.getSnapshotVersions()).satisfies(matching(contains(2)));
        assertThat(stateMap.getResourceGuard().getLeaseCount()).isEqualTo(1);
        snapshot2.release();
        stateMap.close();
    }

    /** Test snapshot empty state map. */
    @Test
    void testSnapshotEmptyStateMap() {
        // take snapshot on an empty state map
        CopyOnWriteSkipListStateMapSnapshot<Integer, Long, String> snapshot =
                stateMap.stateSnapshot();
        assertThat(stateMap.getHighestRequiredSnapshotVersionPlusOne()).isEqualTo(1);
        assertThat(stateMap.getSnapshotVersions()).hasSize(1);
        assertThat(stateMap.getSnapshotVersions()).satisfies(matching(contains(1)));
        assertThat(stateMap.getResourceGuard().getLeaseCount()).isEqualTo(1);
        snapshot.release();
    }

    /** Test snapshot empty state map. */
    @Test
    void testSnapshotClosedStateMap() {
        // close state map
        stateMap.close();
        assertThatThrownBy(() -> stateMap.stateSnapshot()).isInstanceOf(Exception.class);
    }

    /** Test snapshot release. */
    @Test
    void testReleaseSnapshot() {
        int expectedSnapshotVersion = 0;
        int round = 10;
        for (int i = 0; i < round; i++) {
            assertThat(stateMap.getStateMapVersion()).isEqualTo(expectedSnapshotVersion);
            assertThat(stateMap.getHighestFinishedSnapshotVersion())
                    .isEqualTo(expectedSnapshotVersion);
            CopyOnWriteSkipListStateMapSnapshot<Integer, Long, String> snapshot =
                    stateMap.stateSnapshot();
            expectedSnapshotVersion++;
            snapshot.release();
            assertThat(stateMap.getHighestRequiredSnapshotVersionPlusOne()).isEqualTo(0);
            assertThat(stateMap.getSnapshotVersions()).isEmpty();
            assertThat(stateMap.getResourceGuard().getLeaseCount()).isEqualTo(0);
        }
    }

    /** Test state map iterator illegal next call. */
    @Test
    void testStateMapIteratorIllegalNextInvocation() {
        Iterator iterator = stateMap.iterator();
        while (iterator.hasNext()) {
            iterator.next();
        }
        assertThatThrownBy(() -> iterator.next()).isInstanceOf(NoSuchElementException.class);
    }

    /** Test state map iterator illegal next call. */
    @Test
    void testNamespaceNodeIteratorIllegalNextInvocation() {
        SkipListKeySerializer<Integer, Long> skipListKeySerializer =
                new SkipListKeySerializer<>(IntSerializer.INSTANCE, LongSerializer.INSTANCE);
        byte[] namespaceBytes = skipListKeySerializer.serializeNamespace(namespace);
        MemorySegment namespaceSegment = MemorySegmentFactory.wrap(namespaceBytes);
        Iterator<Long> iterator =
                stateMap.new NamespaceNodeIterator(namespaceSegment, 0, namespaceBytes.length);
        while (iterator.hasNext()) {
            iterator.next();
        }
        assertThatThrownBy(() -> iterator.next()).isInstanceOf(NoSuchElementException.class);
    }

    /** Test snapshot node iterator illegal next call. */
    @Test
    void testSnapshotNodeIteratorIllegalNextInvocation() {
        CopyOnWriteSkipListStateMapSnapshot<Integer, Long, String> snapshot =
                stateMap.stateSnapshot();
        CopyOnWriteSkipListStateMapSnapshot.SnapshotNodeIterator iterator =
                snapshot.new SnapshotNodeIterator(false);
        while (iterator.hasNext()) {
            iterator.next();
        }
        try {
            assertThatThrownBy(() -> iterator.next()).isInstanceOf(NoSuchElementException.class);
        } finally {
            snapshot.release();
        }
    }
}
