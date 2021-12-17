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
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Test basic operations of {@link CopyOnWriteSkipListStateMap}. */
public class CopyOnWriteSkipListStateMapBasicOpTest extends TestLogger {
    private final long namespace = 1L;
    private TestAllocator allocator;
    private CopyOnWriteSkipListStateMap<Integer, Long, String> stateMap;

    @Before
    public void setUp() {
        allocator = new TestAllocator(256);
        stateMap = createEmptyStateMap(0, 0.0f, allocator);
    }

    @After
    public void tearDown() {
        stateMap.close();
        IOUtils.closeQuietly(allocator);
    }

    /** Test initialization of state map. */
    @Test
    public void testInitStateMap() {
        assertTrue(stateMap.isEmpty());
        assertEquals(0, stateMap.size());
        assertEquals(0, stateMap.totalSize());
        assertEquals(0, stateMap.getRequestCount());
        assertTrue(stateMap.getLogicallyRemovedNodes().isEmpty());
        assertEquals(0, stateMap.getHighestRequiredSnapshotVersionPlusOne());
        assertEquals(0, stateMap.getHighestFinishedSnapshotVersion());
        assertTrue(stateMap.getSnapshotVersions().isEmpty());
        assertTrue(stateMap.getPruningValueNodes().isEmpty());
        assertEquals(0, stateMap.getResourceGuard().getLeaseCount());
        assertFalse(stateMap.getResourceGuard().isClosed());
        assertFalse(stateMap.isClosed());

        assertNull(stateMap.get(0, 0L));
        assertFalse(stateMap.containsKey(1, 2L));
        assertNull(stateMap.removeAndGetOld(3, 4L));
        assertFalse(stateMap.getKeys(-92L).iterator().hasNext());
        assertEquals(0, stateMap.sizeOfNamespace(8L));
        assertFalse(stateMap.iterator().hasNext());
        assertFalse(stateMap.getStateIncrementalVisitor(100).hasNext());
    }

    /** Test state put and get. */
    @Test
    public void testPutAndGetState() {
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
        assertThat(stateMap.get(key, namespace), is(state));
        assertThat(stateMap.size(), is(totalSize));
        assertThat(stateMap.totalSize(), is(totalSize));
        assertThat(allocator.getTotalSpaceNumber(), is(totalSize * 2));
        return totalSize;
    }

    /** Test state update (put existing key). */
    @Test
    public void testUpdateState() {
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
    public void testPutAndGetOldState() {
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
        assertThat(stateMap.putAndGetOld(key, namespace, state), is(oldState));
        assertThat(stateMap.get(key, namespace), is(state));
        assertThat(stateMap.size(), is(totalSize));
        assertThat(stateMap.totalSize(), is(totalSize));
        assertThat(allocator.getTotalSpaceNumber(), is(totalSize * 2));
        return totalSize;
    }

    /** Test state remove. */
    @Test
    public void testRemoveState() {
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
        assertThat(stateMap.get(key, namespace), nullValue());
        assertThat(stateMap.size(), is(totalSize));
        assertThat(stateMap.totalSize(), is(totalSize));
        assertThat(allocator.getTotalSpaceNumber(), is(totalSize * 2));
        return totalSize;
    }

    /** Test state removeAndGetOld. */
    @Test
    public void testRemoveAndGetOldState() {
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
        assertThat(stateMap.removeAndGetOld(key, namespace), is(oldState));
        assertThat(stateMap.get(key, namespace), nullValue());
        assertThat(stateMap.size(), is(totalSize));
        assertThat(stateMap.totalSize(), is(totalSize));
        assertThat(allocator.getTotalSpaceNumber(), is(totalSize * 2));
        return totalSize;
    }

    /** Test state transform with existing key. */
    @Test
    public void testTransformExistingState() throws Exception {
        final int key = 1;
        final String oldState = "1";
        final int delta = 1;
        StateTransformationFunction<String, Integer> function =
                (String prevState, Integer value) ->
                        prevState == null ? String.valueOf(value) : prevState + value;
        stateMap.put(key, namespace, oldState);
        String expectedState = function.apply(oldState, delta);
        stateMap.transform(key, namespace, delta, function);
        assertThat(stateMap.get(key, namespace), is(expectedState));
        assertThat(stateMap.size(), is(1));
        assertThat(stateMap.totalSize(), is(1));
        assertThat(allocator.getTotalSpaceNumber(), is(2));
    }

    /** Test state transform with new key. */
    @Test
    public void testTransformAbsentState() throws Exception {
        final int key = 1;
        final int delta = 1;
        StateTransformationFunction<String, Integer> function =
                (String prevState, Integer value) ->
                        prevState == null ? String.valueOf(value) : prevState + value;
        String expectedState = function.apply(null, delta);
        stateMap.transform(key, namespace, delta, function);
        assertThat(stateMap.get(key, namespace), is(expectedState));
        assertThat(stateMap.size(), is(1));
        assertThat(stateMap.totalSize(), is(1));
        assertThat(allocator.getTotalSpaceNumber(), is(2));
    }

    /** Test close of state map. */
    @Test
    public void testCloseStateMap() {
        stateMap.close();
        assertTrue(stateMap.isClosed());
        assertThat(stateMap.size(), is(0));
        assertThat(stateMap.totalSize(), is(0));
        assertThat(allocator.getTotalSpaceNumber(), is(0));
        // make sure double close won't cause problem
        stateMap.close();
    }

    /** Make sure exception will be thrown with allocation failure rather than swallowed. */
    @Test
    public void testPutWithAllocationFailure() {
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
            stateMap.put(1, 1L, "test-value");
            fail("Should have thrown exception when space allocation fails");
        } catch (FlinkRuntimeException e) {
            // expected
        }
    }

    /**
     * This tests the internal capability of using partial {@link ByteBuffer}, making sure the
     * internal methods works when put/get state with a key stored at a none-zero offset of a
     * ByteBuffer.
     */
    @Test
    public void testPutAndGetNodeWithNoneZeroOffset() {
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
        assertThat(state, is(valueString));
    }

    /** Test next/update/remove during global iteration of StateIncrementalVisitor. */
    @Test
    public void testStateIncrementalVisitor() {
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
                assertEquals(state, stateMap.get(key, namespace));
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
    public void testStateIncrementalVisitorWithClosedStateMap() {
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
        assertTrue(closedVisitor.hasNext());

        stateMap.close();
        // the visitor will be invalid after state map is closed
        assertFalse(closedVisitor.hasNext());
    }

    /** Test basic snapshot correctness. */
    @Test
    public void testBasicSnapshot() {
        // take an empty snapshot
        CopyOnWriteSkipListStateMapSnapshot<Integer, Long, String> snapshot =
                stateMap.stateSnapshot();
        snapshot.release();
        // put some states
        stateMap.put(1, namespace, "1");
        // take the 2nd snapshot with data
        CopyOnWriteSkipListStateMapSnapshot<Integer, Long, String> snapshot2 =
                stateMap.stateSnapshot();
        assertEquals(2, stateMap.getStateMapVersion());
        assertEquals(2, stateMap.getHighestRequiredSnapshotVersionPlusOne());
        assertEquals(1, stateMap.getSnapshotVersions().size());
        assertThat(stateMap.getSnapshotVersions(), contains(2));
        assertEquals(1, stateMap.getResourceGuard().getLeaseCount());
        snapshot2.release();
        stateMap.close();
    }

    /** Test snapshot empty state map. */
    @Test
    public void testSnapshotEmptyStateMap() {
        // take snapshot on an empty state map
        CopyOnWriteSkipListStateMapSnapshot<Integer, Long, String> snapshot =
                stateMap.stateSnapshot();
        assertEquals(1, stateMap.getHighestRequiredSnapshotVersionPlusOne());
        assertEquals(1, stateMap.getSnapshotVersions().size());
        assertThat(stateMap.getSnapshotVersions(), contains(1));
        assertEquals(1, stateMap.getResourceGuard().getLeaseCount());
        snapshot.release();
    }

    /** Test snapshot empty state map. */
    @Test
    public void testSnapshotClosedStateMap() {
        // close state map
        stateMap.close();
        try {
            stateMap.stateSnapshot();
            fail(
                    "Should have thrown exception when trying to snapshot an already closed state map.");
        } catch (Exception e) {
            // expected
        }
    }

    /** Test snapshot release. */
    @Test
    public void testReleaseSnapshot() {
        int expectedSnapshotVersion = 0;
        int round = 10;
        for (int i = 0; i < round; i++) {
            assertEquals(expectedSnapshotVersion, stateMap.getStateMapVersion());
            assertEquals(expectedSnapshotVersion, stateMap.getHighestFinishedSnapshotVersion());
            CopyOnWriteSkipListStateMapSnapshot<Integer, Long, String> snapshot =
                    stateMap.stateSnapshot();
            expectedSnapshotVersion++;
            snapshot.release();
            assertEquals(0, stateMap.getHighestRequiredSnapshotVersionPlusOne());
            assertTrue(stateMap.getSnapshotVersions().isEmpty());
            assertEquals(0, stateMap.getResourceGuard().getLeaseCount());
        }
    }

    /** Test state map iterator illegal next call. */
    @Test
    public void testStateMapIteratorIllegalNextInvocation() {
        Iterator iterator = stateMap.iterator();
        while (iterator.hasNext()) {
            iterator.next();
        }
        try {
            iterator.next();
            fail("Should have thrown NoSuchElementException.");
        } catch (NoSuchElementException e) {
            // expected
        }
    }

    /** Test state map iterator illegal next call. */
    @Test
    public void testNamespaceNodeIteratorIllegalNextInvocation() {
        SkipListKeySerializer<Integer, Long> skipListKeySerializer =
                new SkipListKeySerializer<>(IntSerializer.INSTANCE, LongSerializer.INSTANCE);
        byte[] namespaceBytes = skipListKeySerializer.serializeNamespace(namespace);
        MemorySegment namespaceSegment = MemorySegmentFactory.wrap(namespaceBytes);
        Iterator<Long> iterator =
                stateMap.new NamespaceNodeIterator(namespaceSegment, 0, namespaceBytes.length);
        while (iterator.hasNext()) {
            iterator.next();
        }
        try {
            iterator.next();
            fail("Should have thrown NoSuchElementException.");
        } catch (NoSuchElementException e) {
            // expected
        }
    }

    /** Test snapshot node iterator illegal next call. */
    @Test
    public void testSnapshotNodeIteratorIllegalNextInvocation() {
        CopyOnWriteSkipListStateMapSnapshot<Integer, Long, String> snapshot =
                stateMap.stateSnapshot();
        CopyOnWriteSkipListStateMapSnapshot.SnapshotNodeIterator iterator =
                snapshot.new SnapshotNodeIterator(false);
        while (iterator.hasNext()) {
            iterator.next();
        }
        try {
            iterator.next();
            fail("Should have thrown NoSuchElementException.");
        } catch (NoSuchElementException e) {
            // expected
        } finally {
            snapshot.release();
        }
    }
}
