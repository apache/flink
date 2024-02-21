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

package org.apache.flink.streaming.runtime.operators.windowing;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.MergingWindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import org.apache.flink.shaded.guava31.com.google.common.collect.Lists;

import org.junit.Test;
import org.mockito.Matchers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
        @SuppressWarnings("unchecked")
        ListState<Tuple2<TimeWindow, TimeWindow>> mockState = mock(ListState.class);

        MergingWindowSet<TimeWindow> windowSet =
                new MergingWindowSet<>(new NonEagerlyMergingWindowAssigner(3000), mockState);

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
        @SuppressWarnings("unchecked")
        ListState<Tuple2<TimeWindow, TimeWindow>> mockState = mock(ListState.class);

        MergingWindowSet<TimeWindow> windowSet =
                new MergingWindowSet<>(
                        EventTimeSessionWindows.withGap(Time.milliseconds(3)), mockState);

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
        @SuppressWarnings("unchecked")
        ListState<Tuple2<TimeWindow, TimeWindow>> mockState = mock(ListState.class);

        MergingWindowSet<TimeWindow> windowSet =
                new MergingWindowSet<>(
                        EventTimeSessionWindows.withGap(Time.milliseconds(3)), mockState);

        TestingMergeFunction mergeFunction = new TestingMergeFunction();

        // add several non-overlapping initial windows

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
                anyOf(is(new TimeWindow(5, 8)), is(new TimeWindow(10, 13))));
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
                anyOf(is(new TimeWindow(5, 8)), is(new TimeWindow(10, 13))));

        // add a window that merges all of them together
        mergeFunction.reset();
        assertEquals(
                new TimeWindow(0, 13), windowSet.addWindow(new TimeWindow(3, 5), mergeFunction));
        assertTrue(mergeFunction.hasMerged());
        assertEquals(new TimeWindow(0, 13), mergeFunction.mergeTarget());
        assertThat(
                mergeFunction.stateWindow(),
                anyOf(
                        is(new TimeWindow(0, 3)),
                        is(new TimeWindow(5, 8)),
                        is(new TimeWindow(10, 13))));
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
                        is(new TimeWindow(0, 3)),
                        is(new TimeWindow(5, 8)),
                        is(new TimeWindow(10, 13))));
    }

    /** Test merging of a large new window that covers one existing windows. */
    @Test
    public void testMergeLargeWindowCoveringSingleWindow() throws Exception {
        @SuppressWarnings("unchecked")
        ListState<Tuple2<TimeWindow, TimeWindow>> mockState = mock(ListState.class);

        MergingWindowSet<TimeWindow> windowSet =
                new MergingWindowSet<>(
                        EventTimeSessionWindows.withGap(Time.milliseconds(3)), mockState);

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
        @SuppressWarnings("unchecked")
        ListState<Tuple2<TimeWindow, TimeWindow>> mockState = mock(ListState.class);

        MergingWindowSet<TimeWindow> windowSet =
                new MergingWindowSet<>(
                        EventTimeSessionWindows.withGap(Time.milliseconds(3)), mockState);

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
        @SuppressWarnings("unchecked")
        ListState<Tuple2<TimeWindow, TimeWindow>> mockState = mock(ListState.class);

        MergingWindowSet<TimeWindow> windowSet =
                new MergingWindowSet<>(
                        EventTimeSessionWindows.withGap(Time.milliseconds(3)), mockState);

        TestingMergeFunction mergeFunction = new TestingMergeFunction();

        // add several non-overlapping initial windows

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
                new TimeWindow(0, 13), windowSet.addWindow(new TimeWindow(0, 13), mergeFunction));
        assertTrue(mergeFunction.hasMerged());
        assertThat(
                mergeFunction.mergedStateWindows(),
                anyOf(
                        containsInAnyOrder(new TimeWindow(0, 3), new TimeWindow(5, 8)),
                        containsInAnyOrder(new TimeWindow(0, 3), new TimeWindow(10, 13)),
                        containsInAnyOrder(new TimeWindow(5, 8), new TimeWindow(10, 13))));
        assertThat(
                windowSet.getStateWindow(new TimeWindow(0, 13)),
                anyOf(
                        is(new TimeWindow(1, 3)),
                        is(new TimeWindow(5, 8)),
                        is(new TimeWindow(10, 13))));
    }

    @Test
    public void testRestoreFromState() throws Exception {
        @SuppressWarnings("unchecked")
        ListState<Tuple2<TimeWindow, TimeWindow>> mockState = mock(ListState.class);
        when(mockState.get())
                .thenReturn(
                        Lists.newArrayList(
                                new Tuple2<>(new TimeWindow(17, 42), new TimeWindow(42, 17)),
                                new Tuple2<>(new TimeWindow(1, 2), new TimeWindow(3, 4))));

        MergingWindowSet<TimeWindow> windowSet =
                new MergingWindowSet<>(
                        EventTimeSessionWindows.withGap(Time.milliseconds(3)), mockState);

        assertEquals(new TimeWindow(42, 17), windowSet.getStateWindow(new TimeWindow(17, 42)));
        assertEquals(new TimeWindow(3, 4), windowSet.getStateWindow(new TimeWindow(1, 2)));
    }

    @Test
    public void testPersist() throws Exception {
        @SuppressWarnings("unchecked")
        ListState<Tuple2<TimeWindow, TimeWindow>> mockState = mock(ListState.class);

        MergingWindowSet<TimeWindow> windowSet =
                new MergingWindowSet<>(
                        EventTimeSessionWindows.withGap(Time.milliseconds(3)), mockState);

        TestingMergeFunction mergeFunction = new TestingMergeFunction();

        windowSet.addWindow(new TimeWindow(1, 2), mergeFunction);
        windowSet.addWindow(new TimeWindow(17, 42), mergeFunction);

        assertEquals(new TimeWindow(1, 2), windowSet.getStateWindow(new TimeWindow(1, 2)));
        assertEquals(new TimeWindow(17, 42), windowSet.getStateWindow(new TimeWindow(17, 42)));

        windowSet.persist();

        verify(mockState)
                .update(
                        eq(
                                new ArrayList<>(
                                        Arrays.asList(
                                                new Tuple2<>(
                                                        new TimeWindow(1, 2), new TimeWindow(1, 2)),
                                                new Tuple2<>(
                                                        new TimeWindow(17, 42),
                                                        new TimeWindow(17, 42))))));

        verify(mockState, times(1)).update(Matchers.anyObject());
    }

    @Test
    public void testPersistOnlyIfHaveUpdates() throws Exception {
        @SuppressWarnings("unchecked")
        ListState<Tuple2<TimeWindow, TimeWindow>> mockState = mock(ListState.class);
        when(mockState.get())
                .thenReturn(
                        Lists.newArrayList(
                                new Tuple2<>(new TimeWindow(17, 42), new TimeWindow(42, 17)),
                                new Tuple2<>(new TimeWindow(1, 2), new TimeWindow(3, 4))));

        MergingWindowSet<TimeWindow> windowSet =
                new MergingWindowSet<>(
                        EventTimeSessionWindows.withGap(Time.milliseconds(3)), mockState);

        assertEquals(new TimeWindow(42, 17), windowSet.getStateWindow(new TimeWindow(17, 42)));
        assertEquals(new TimeWindow(3, 4), windowSet.getStateWindow(new TimeWindow(1, 2)));

        windowSet.persist();

        verify(mockState, times(0)).add(Matchers.<Tuple2<TimeWindow, TimeWindow>>anyObject());
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
    static class NonEagerlyMergingWindowAssigner extends MergingWindowAssigner<Object, TimeWindow> {
        private static final long serialVersionUID = 1L;

        protected long sessionTimeout;

        public NonEagerlyMergingWindowAssigner(long sessionTimeout) {
            this.sessionTimeout = sessionTimeout;
        }

        @Override
        public Collection<TimeWindow> assignWindows(
                Object element, long timestamp, WindowAssignerContext context) {
            return Collections.singletonList(new TimeWindow(timestamp, timestamp + sessionTimeout));
        }

        @Override
        public Trigger<Object, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
            throw new UnsupportedOperationException(
                    "This method is deprecated and shouldn't be invoked. Please use getDefaultTrigger() instead.");
        }

        @Override
        public Trigger<Object, TimeWindow> getDefaultTrigger() {
            return EventTimeTrigger.create();
        }

        @Override
        public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
            return new TimeWindow.Serializer();
        }

        @Override
        public boolean isEventTime() {
            return true;
        }

        /** Merge overlapping {@link TimeWindow}s. */
        @Override
        public void mergeWindows(
                Collection<TimeWindow> windows, MergingWindowAssigner.MergeCallback<TimeWindow> c) {

            TimeWindow earliestStart = null;

            for (TimeWindow win : windows) {
                if (earliestStart == null) {
                    earliestStart = win;
                } else if (win.getStart() < earliestStart.getStart()) {
                    earliestStart = win;
                }
            }

            List<TimeWindow> associatedWindows = new ArrayList<>();

            for (TimeWindow win : windows) {
                if (win.getStart() < earliestStart.getEnd()
                        && win.getStart() >= earliestStart.getStart()) {
                    associatedWindows.add(win);
                }
            }

            TimeWindow target =
                    new TimeWindow(earliestStart.getStart(), earliestStart.getEnd() + 1);

            if (associatedWindows.size() > 1) {
                c.merge(associatedWindows, target);
            }
        }
    }
}
