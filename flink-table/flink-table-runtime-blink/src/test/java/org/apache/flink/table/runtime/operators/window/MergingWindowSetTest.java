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

package org.apache.flink.table.runtime.operators.window;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.window.assigners.MergingWindowAssigner;
import org.apache.flink.table.runtime.operators.window.assigners.SessionWindowAssigner;
import org.apache.flink.table.runtime.operators.window.internal.MergingWindowSet;

import org.hamcrest.core.Is;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for verifying that {@link MergingWindowSet} correctly merges windows in various situations
 * and that the merge callback is called with the correct sets of windows.
 */
public class MergingWindowSetTest {

    /**
     * This test uses a special (misbehaving) {@code MergingWindowAssigner} that produces cases
     * where windows that don't overlap with the newly added window are being merged. We verify that
     * the merging window set is nevertheless correct and contains all added windows.
     */
    @Test
    public void testNonEagerMerging() throws Exception {
        MapState<TimeWindow, TimeWindow> mockState = new HeapMapState<>();

        MergingWindowSet<TimeWindow> windowSet =
                new MergingWindowSet<>(new NonEagerlyMergingWindowAssigner(3000), mockState);
        windowSet.initializeCache("key1");

        TestingMergeFunction mergeFunction = new TestingMergeFunction();

        TimeWindow result;

        mergeFunction.reset();
        result = windowSet.addWindow(new TimeWindow(0, 2), mergeFunction);
        assertNotNull(windowSet.getStateWindow(result));

        mergeFunction.reset();
        result = windowSet.addWindow(new TimeWindow(2, 5), mergeFunction);
        assertNotNull(windowSet.getStateWindow(result));

        mergeFunction.reset();
        result = windowSet.addWindow(new TimeWindow(1, 2), mergeFunction);
        assertNotNull(windowSet.getStateWindow(result));

        mergeFunction.reset();
        result = windowSet.addWindow(new TimeWindow(10, 12), mergeFunction);
        assertNotNull(windowSet.getStateWindow(result));
    }

    @Test
    public void testIncrementalMerging() throws Exception {
        MapState<TimeWindow, TimeWindow> mockState = new HeapMapState<>();

        MergingWindowSet<TimeWindow> windowSet =
                new MergingWindowSet<>(
                        SessionWindowAssigner.withGap(Duration.ofMillis(3)), mockState);
        windowSet.initializeCache("key1");

        TestingMergeFunction mergeFunction = new TestingMergeFunction();

        // add initial window
        mergeFunction.reset();
        assertEquals(
                new TimeWindow(0, 4), windowSet.addWindow(new TimeWindow(0, 4), mergeFunction));
        assertFalse(mergeFunction.hasMerged());

        assertTrue(windowSet.getStateWindow(new TimeWindow(0, 4)).equals(new TimeWindow(0, 4)));

        // add some more windows
        mergeFunction.reset();
        assertEquals(
                new TimeWindow(0, 4), windowSet.addWindow(new TimeWindow(0, 4), mergeFunction));
        assertFalse(mergeFunction.hasMerged());

        mergeFunction.reset();
        assertEquals(
                new TimeWindow(0, 5), windowSet.addWindow(new TimeWindow(3, 5), mergeFunction));
        assertTrue(mergeFunction.hasMerged());
        assertEquals(new TimeWindow(0, 5), mergeFunction.mergeTarget());
        assertEquals(new TimeWindow(0, 4), mergeFunction.stateWindow());
        assertThat(mergeFunction.mergeSources(), containsInAnyOrder(new TimeWindow(0, 4)));
        assertTrue(mergeFunction.mergedStateWindows().isEmpty());

        mergeFunction.reset();
        assertEquals(
                new TimeWindow(0, 6), windowSet.addWindow(new TimeWindow(4, 6), mergeFunction));
        assertTrue(mergeFunction.hasMerged());
        assertEquals(new TimeWindow(0, 6), mergeFunction.mergeTarget());
        assertEquals(new TimeWindow(0, 4), mergeFunction.stateWindow());
        assertThat(mergeFunction.mergeSources(), containsInAnyOrder(new TimeWindow(0, 5)));
        assertTrue(mergeFunction.mergedStateWindows().isEmpty());

        assertEquals(new TimeWindow(0, 4), windowSet.getStateWindow(new TimeWindow(0, 6)));

        // add some windows that falls into the already merged region
        mergeFunction.reset();
        assertEquals(
                new TimeWindow(0, 6), windowSet.addWindow(new TimeWindow(1, 4), mergeFunction));
        assertFalse(mergeFunction.hasMerged());

        mergeFunction.reset();
        assertEquals(
                new TimeWindow(0, 6), windowSet.addWindow(new TimeWindow(0, 4), mergeFunction));
        assertFalse(mergeFunction.hasMerged());

        mergeFunction.reset();
        assertEquals(
                new TimeWindow(0, 6), windowSet.addWindow(new TimeWindow(3, 5), mergeFunction));
        assertFalse(mergeFunction.hasMerged());

        mergeFunction.reset();
        assertEquals(
                new TimeWindow(0, 6), windowSet.addWindow(new TimeWindow(4, 6), mergeFunction));
        assertFalse(mergeFunction.hasMerged());

        assertEquals(new TimeWindow(0, 4), windowSet.getStateWindow(new TimeWindow(0, 6)));

        // add some more windows that don't merge with the first bunch
        mergeFunction.reset();
        assertEquals(
                new TimeWindow(11, 14), windowSet.addWindow(new TimeWindow(11, 14), mergeFunction));
        assertFalse(mergeFunction.hasMerged());

        assertEquals(new TimeWindow(0, 4), windowSet.getStateWindow(new TimeWindow(0, 6)));

        assertEquals(new TimeWindow(11, 14), windowSet.getStateWindow(new TimeWindow(11, 14)));

        // add some more windows that merge with the second bunch

        mergeFunction.reset();
        assertEquals(
                new TimeWindow(10, 14), windowSet.addWindow(new TimeWindow(10, 13), mergeFunction));
        assertTrue(mergeFunction.hasMerged());
        assertEquals(new TimeWindow(10, 14), mergeFunction.mergeTarget());
        assertEquals(new TimeWindow(11, 14), mergeFunction.stateWindow());
        assertThat(mergeFunction.mergeSources(), containsInAnyOrder(new TimeWindow(11, 14)));
        assertTrue(mergeFunction.mergedStateWindows().isEmpty());

        mergeFunction.reset();
        assertEquals(
                new TimeWindow(10, 15), windowSet.addWindow(new TimeWindow(12, 15), mergeFunction));
        assertTrue(mergeFunction.hasMerged());
        assertEquals(new TimeWindow(10, 15), mergeFunction.mergeTarget());
        assertEquals(new TimeWindow(11, 14), mergeFunction.stateWindow());
        assertThat(mergeFunction.mergeSources(), containsInAnyOrder(new TimeWindow(10, 14)));
        assertTrue(mergeFunction.mergedStateWindows().isEmpty());

        mergeFunction.reset();
        assertEquals(
                new TimeWindow(10, 15), windowSet.addWindow(new TimeWindow(11, 14), mergeFunction));
        assertFalse(mergeFunction.hasMerged());

        assertEquals(new TimeWindow(0, 4), windowSet.getStateWindow(new TimeWindow(0, 6)));

        assertEquals(new TimeWindow(11, 14), windowSet.getStateWindow(new TimeWindow(10, 15)));

        // retire the first batch of windows
        windowSet.retireWindow(new TimeWindow(0, 6));

        assertTrue(windowSet.getStateWindow(new TimeWindow(0, 6)) == null);

        assertTrue(windowSet.getStateWindow(new TimeWindow(10, 15)).equals(new TimeWindow(11, 14)));
    }

    @Test
    public void testLateMerging() throws Exception {
        MapState<TimeWindow, TimeWindow> mockState = new HeapMapState<>();

        MergingWindowSet<TimeWindow> windowSet =
                new MergingWindowSet<>(
                        SessionWindowAssigner.withGap(Duration.ofMillis(3)), mockState);
        windowSet.initializeCache("key1");

        TestingMergeFunction mergeFunction = new TestingMergeFunction();

        // add several non-overlapping initial windoww

        mergeFunction.reset();
        assertEquals(
                new TimeWindow(0, 3), windowSet.addWindow(new TimeWindow(0, 3), mergeFunction));
        assertFalse(mergeFunction.hasMerged());
        assertEquals(new TimeWindow(0, 3), windowSet.getStateWindow(new TimeWindow(0, 3)));

        mergeFunction.reset();
        assertEquals(
                new TimeWindow(5, 8), windowSet.addWindow(new TimeWindow(5, 8), mergeFunction));
        assertFalse(mergeFunction.hasMerged());
        assertEquals(new TimeWindow(5, 8), windowSet.getStateWindow(new TimeWindow(5, 8)));

        mergeFunction.reset();
        assertEquals(
                new TimeWindow(10, 13), windowSet.addWindow(new TimeWindow(10, 13), mergeFunction));
        assertFalse(mergeFunction.hasMerged());
        assertEquals(new TimeWindow(10, 13), windowSet.getStateWindow(new TimeWindow(10, 13)));

        // add a window that merges the later two windows
        mergeFunction.reset();
        assertEquals(
                new TimeWindow(5, 13), windowSet.addWindow(new TimeWindow(8, 10), mergeFunction));
        assertTrue(mergeFunction.hasMerged());
        assertEquals(new TimeWindow(5, 13), mergeFunction.mergeTarget());
        assertThat(
                mergeFunction.stateWindow(),
                anyOf(Is.is(new TimeWindow(5, 8)), Is.is(new TimeWindow(10, 13))));
        assertThat(
                mergeFunction.mergeSources(),
                containsInAnyOrder(new TimeWindow(5, 8), new TimeWindow(10, 13)));
        assertThat(
                mergeFunction.mergedStateWindows(),
                anyOf(
                        containsInAnyOrder(new TimeWindow(10, 13)),
                        containsInAnyOrder(new TimeWindow(5, 8))));
        assertThat(mergeFunction.mergedStateWindows(), not(hasItem(mergeFunction.mergeTarget())));

        assertEquals(new TimeWindow(0, 3), windowSet.getStateWindow(new TimeWindow(0, 3)));

        mergeFunction.reset();
        assertEquals(
                new TimeWindow(5, 13), windowSet.addWindow(new TimeWindow(5, 8), mergeFunction));
        assertFalse(mergeFunction.hasMerged());

        mergeFunction.reset();
        assertEquals(
                new TimeWindow(5, 13), windowSet.addWindow(new TimeWindow(8, 10), mergeFunction));
        assertFalse(mergeFunction.hasMerged());

        mergeFunction.reset();
        assertEquals(
                new TimeWindow(5, 13), windowSet.addWindow(new TimeWindow(10, 13), mergeFunction));
        assertFalse(mergeFunction.hasMerged());

        assertThat(
                windowSet.getStateWindow(new TimeWindow(5, 13)),
                anyOf(Is.is(new TimeWindow(5, 8)), Is.is(new TimeWindow(10, 13))));

        // add a window that merges all of them together
        mergeFunction.reset();
        assertEquals(
                new TimeWindow(0, 13), windowSet.addWindow(new TimeWindow(3, 5), mergeFunction));
        assertTrue(mergeFunction.hasMerged());
        assertEquals(new TimeWindow(0, 13), mergeFunction.mergeTarget());
        assertThat(
                mergeFunction.stateWindow(),
                anyOf(
                        Is.is(new TimeWindow(0, 3)),
                        Is.is(new TimeWindow(5, 8)),
                        Is.is(new TimeWindow(10, 13))));
        assertThat(
                mergeFunction.mergeSources(),
                containsInAnyOrder(new TimeWindow(0, 3), new TimeWindow(5, 13)));
        assertThat(
                mergeFunction.mergedStateWindows(),
                anyOf(
                        containsInAnyOrder(new TimeWindow(0, 3)),
                        containsInAnyOrder(new TimeWindow(5, 8)),
                        containsInAnyOrder(new TimeWindow(10, 13))));
        assertThat(mergeFunction.mergedStateWindows(), not(hasItem(mergeFunction.mergeTarget())));

        assertThat(
                windowSet.getStateWindow(new TimeWindow(0, 13)),
                anyOf(
                        Is.is(new TimeWindow(0, 3)),
                        Is.is(new TimeWindow(5, 8)),
                        Is.is(new TimeWindow(10, 13))));
    }

    /** Test merging of a large new window that covers one existing windows. */
    @Test
    public void testMergeLargeWindowCoveringSingleWindow() throws Exception {
        MapState<TimeWindow, TimeWindow> mockState = new HeapMapState<>();

        MergingWindowSet<TimeWindow> windowSet =
                new MergingWindowSet<>(
                        SessionWindowAssigner.withGap(Duration.ofMillis(3)), mockState);
        windowSet.initializeCache("key1");

        TestingMergeFunction mergeFunction = new TestingMergeFunction();

        // add an initial small window

        mergeFunction.reset();
        assertEquals(
                new TimeWindow(1, 2), windowSet.addWindow(new TimeWindow(1, 2), mergeFunction));
        assertFalse(mergeFunction.hasMerged());
        assertEquals(new TimeWindow(1, 2), windowSet.getStateWindow(new TimeWindow(1, 2)));

        // add a new window that completely covers the existing window

        mergeFunction.reset();
        assertEquals(
                new TimeWindow(0, 3), windowSet.addWindow(new TimeWindow(0, 3), mergeFunction));
        assertTrue(mergeFunction.hasMerged());
        assertEquals(new TimeWindow(1, 2), windowSet.getStateWindow(new TimeWindow(0, 3)));
    }

    /**
     * Test adding a new window that is identical to an existing window. This should not cause a
     * merge.
     */
    @Test
    public void testAddingIdenticalWindows() throws Exception {
        MapState<TimeWindow, TimeWindow> mockState = new HeapMapState<>();

        MergingWindowSet<TimeWindow> windowSet =
                new MergingWindowSet<>(
                        SessionWindowAssigner.withGap(Duration.ofMillis(3)), mockState);
        windowSet.initializeCache("key1");

        TestingMergeFunction mergeFunction = new TestingMergeFunction();

        mergeFunction.reset();
        assertEquals(
                new TimeWindow(1, 2), windowSet.addWindow(new TimeWindow(1, 2), mergeFunction));
        assertFalse(mergeFunction.hasMerged());
        assertEquals(new TimeWindow(1, 2), windowSet.getStateWindow(new TimeWindow(1, 2)));

        mergeFunction.reset();
        assertEquals(
                new TimeWindow(1, 2), windowSet.addWindow(new TimeWindow(1, 2), mergeFunction));
        assertFalse(mergeFunction.hasMerged());
        assertEquals(new TimeWindow(1, 2), windowSet.getStateWindow(new TimeWindow(1, 2)));
    }

    /** Test merging of a large new window that covers multiple existing windows. */
    @Test
    public void testMergeLargeWindowCoveringMultipleWindows() throws Exception {
        MapState<TimeWindow, TimeWindow> mockState = new HeapMapState<>();

        MergingWindowSet<TimeWindow> windowSet =
                new MergingWindowSet<>(
                        SessionWindowAssigner.withGap(Duration.ofMillis(3)), mockState);
        windowSet.initializeCache("key1");

        TestingMergeFunction mergeFunction = new TestingMergeFunction();

        // add several non-overlapping initial windoww

        mergeFunction.reset();
        assertEquals(
                new TimeWindow(1, 3), windowSet.addWindow(new TimeWindow(1, 3), mergeFunction));
        assertFalse(mergeFunction.hasMerged());
        assertEquals(new TimeWindow(1, 3), windowSet.getStateWindow(new TimeWindow(1, 3)));

        mergeFunction.reset();
        assertEquals(
                new TimeWindow(5, 8), windowSet.addWindow(new TimeWindow(5, 8), mergeFunction));
        assertFalse(mergeFunction.hasMerged());
        assertEquals(new TimeWindow(5, 8), windowSet.getStateWindow(new TimeWindow(5, 8)));

        mergeFunction.reset();
        assertEquals(
                new TimeWindow(10, 13), windowSet.addWindow(new TimeWindow(10, 13), mergeFunction));
        assertFalse(mergeFunction.hasMerged());
        assertEquals(new TimeWindow(10, 13), windowSet.getStateWindow(new TimeWindow(10, 13)));

        // add a new window that completely covers the existing windows

        mergeFunction.reset();
        assertEquals(
                new TimeWindow(5, 13), windowSet.addWindow(new TimeWindow(5, 13), mergeFunction));
        assertTrue(mergeFunction.hasMerged());
        assertThat(
                mergeFunction.mergedStateWindows(),
                anyOf(
                        containsInAnyOrder(new TimeWindow(5, 8)),
                        containsInAnyOrder(new TimeWindow(10, 13))));
        assertThat(
                windowSet.getStateWindow(new TimeWindow(5, 13)),
                anyOf(Is.is(new TimeWindow(5, 8)), Is.is(new TimeWindow(10, 13))));
    }

    // ---------------------------------------------------------------------------------------

    private static class MockMapState<K, V> implements MapState<K, V> {

        private final Map<K, V> map = new HashMap<>();

        @Override
        public V get(K k) throws Exception {
            return map.get(k);
        }

        @Override
        public void put(K k, V v) throws Exception {
            map.put(k, v);
        }

        @Override
        public void putAll(Map<K, V> map) throws Exception {
            this.map.putAll(map);
        }

        @Override
        public void remove(K k) throws Exception {
            map.remove(k);
        }

        @Override
        public boolean contains(K k) throws Exception {
            return map.containsKey(k);
        }

        @Override
        public Iterable<Map.Entry<K, V>> entries() throws Exception {
            return map.entrySet();
        }

        @Override
        public Iterable<K> keys() throws Exception {
            return map.keySet();
        }

        @Override
        public Iterable<V> values() throws Exception {
            return map.values();
        }

        @Override
        public Iterator<Map.Entry<K, V>> iterator() throws Exception {
            return map.entrySet().iterator();
        }

        @Override
        public boolean isEmpty() throws Exception {
            return map.isEmpty();
        }

        @Override
        public void clear() {
            map.clear();
        }
    }

    private static class TestingMergeFunction
            implements MergingWindowSet.MergeFunction<TimeWindow> {
        private TimeWindow target = null;
        private Collection<TimeWindow> sources = null;

        private TimeWindow stateWindow = null;
        private Collection<TimeWindow> mergedStateWindows = null;

        public void reset() {
            target = null;
            sources = null;
            stateWindow = null;
            mergedStateWindows = null;
        }

        public boolean hasMerged() {
            return target != null;
        }

        public TimeWindow mergeTarget() {
            return target;
        }

        public Collection<TimeWindow> mergeSources() {
            return sources;
        }

        public TimeWindow stateWindow() {
            return stateWindow;
        }

        public Collection<TimeWindow> mergedStateWindows() {
            return mergedStateWindows;
        }

        @Override
        public void merge(
                TimeWindow mergeResult,
                Collection<TimeWindow> mergedWindows,
                TimeWindow stateWindowResult,
                Collection<TimeWindow> mergedStateWindows)
                throws Exception {

            if (target != null) {
                fail("More than one merge for adding a Window should not occur.");
            }
            this.stateWindow = stateWindowResult;
            this.target = mergeResult;
            this.mergedStateWindows = mergedStateWindows;
            this.sources = mergedWindows;
        }
    }

    /**
     * A special {@link MergingWindowAssigner} that let's windows get larger which leads to windows
     * being merged lazily.
     */
    static class NonEagerlyMergingWindowAssigner extends MergingWindowAssigner<TimeWindow> {
        private static final long serialVersionUID = 1L;

        protected long sessionTimeout;

        public NonEagerlyMergingWindowAssigner(long sessionTimeout) {
            this.sessionTimeout = sessionTimeout;
        }

        @Override
        public Collection<TimeWindow> assignWindows(RowData element, long timestamp) {
            return Collections.singletonList(new TimeWindow(timestamp, timestamp + sessionTimeout));
        }

        @Override
        public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
            return null;
        }

        @Override
        public boolean isEventTime() {
            return true;
        }

        /** Merge overlapping {@link TimeWindow}s. */
        @Override
        public void mergeWindows(
                TimeWindow newWindow,
                NavigableSet<TimeWindow> sortedWindows,
                MergeCallback<TimeWindow> callback) {
            TreeSet<TimeWindow> treeWindows = new TreeSet<>();
            treeWindows.add(newWindow);
            treeWindows.addAll(sortedWindows);

            TimeWindow earliestStart = treeWindows.first();

            List<TimeWindow> associatedWindows = new ArrayList<>();

            for (TimeWindow win : treeWindows) {
                if (win.getStart() < earliestStart.getEnd()
                        && win.getStart() >= earliestStart.getStart()) {
                    associatedWindows.add(win);
                }
            }

            TimeWindow target =
                    new TimeWindow(earliestStart.getStart(), earliestStart.getEnd() + 1);

            if (associatedWindows.size() > 1) {
                callback.merge(target, associatedWindows);
            }
        }

        @Override
        public String toString() {
            return "NonEagerlyMergingWindows";
        }
    }

    private static class HeapMapState<UK, UV> implements MapState<UK, UV> {

        private final Map<UK, UV> internalMap;

        HeapMapState() {
            internalMap = new HashMap<>();
        }

        @Override
        public UV get(UK key) throws Exception {
            return internalMap.get(key);
        }

        @Override
        public void put(UK key, UV value) throws Exception {
            internalMap.put(key, value);
        }

        @Override
        public void putAll(Map<UK, UV> map) throws Exception {
            internalMap.putAll(map);
        }

        @Override
        public void remove(UK key) throws Exception {
            internalMap.remove(key);
        }

        @Override
        public boolean contains(UK key) throws Exception {
            return internalMap.containsKey(key);
        }

        @Override
        public Iterable<Map.Entry<UK, UV>> entries() throws Exception {
            return internalMap.entrySet();
        }

        @Override
        public Iterable<UK> keys() throws Exception {
            return internalMap.keySet();
        }

        @Override
        public Iterable<UV> values() throws Exception {
            return internalMap.values();
        }

        @Override
        public Iterator<Map.Entry<UK, UV>> iterator() throws Exception {
            return internalMap.entrySet().iterator();
        }

        @Override
        public boolean isEmpty() {
            return internalMap.isEmpty();
        }

        @Override
        public void clear() {
            internalMap.clear();
        }
    }
}
