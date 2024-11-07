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
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.MergingWindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import org.apache.flink.shaded.guava32.com.google.common.collect.Lists;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for verifying that {@link MergingWindowSet} correctly merges windows in various situations
 * and that the merge callback is called with the correct sets of windows.
 */
class MergingWindowSetTest {

    /**
     * This test uses a special (misbehaving) {@code MergingWindowAssigner} that produces cases
     * where windows that don't overlap with the newly added window are being merged. We verify that
     * the merging window set is nevertheless correct and contains all added windows.
     */
    @Test
    void testNonEagerMerging() throws Exception {
        @SuppressWarnings("unchecked")
        ListState<Tuple2<TimeWindow, TimeWindow>> mockState = mock(ListState.class);

        MergingWindowSet<TimeWindow> windowSet =
                new MergingWindowSet<>(new NonEagerlyMergingWindowAssigner(3000), mockState);

        TestingMergeFunction mergeFunction = new TestingMergeFunction();

        TimeWindow result;

        mergeFunction.reset();
        result = windowSet.addWindow(new TimeWindow(0, 2), mergeFunction);
        assertThat(windowSet.getStateWindow(result)).isNotNull();

        mergeFunction.reset();
        result = windowSet.addWindow(new TimeWindow(2, 5), mergeFunction);
        assertThat(windowSet.getStateWindow(result)).isNotNull();

        mergeFunction.reset();
        result = windowSet.addWindow(new TimeWindow(1, 2), mergeFunction);
        assertThat(windowSet.getStateWindow(result)).isNotNull();

        mergeFunction.reset();
        result = windowSet.addWindow(new TimeWindow(10, 12), mergeFunction);
        assertThat(windowSet.getStateWindow(result)).isNotNull();
    }

    @Test
    void testIncrementalMerging() throws Exception {
        @SuppressWarnings("unchecked")
        ListState<Tuple2<TimeWindow, TimeWindow>> mockState = mock(ListState.class);

        MergingWindowSet<TimeWindow> windowSet =
                new MergingWindowSet<>(
                        EventTimeSessionWindows.withGap(Duration.ofMillis(3)), mockState);

        TestingMergeFunction mergeFunction = new TestingMergeFunction();

        // add initial window
        mergeFunction.reset();
        assertThat(windowSet.addWindow(new TimeWindow(0, 4), mergeFunction))
                .isEqualTo(new TimeWindow(0, 4));
        assertThat(mergeFunction.hasMerged()).isFalse();

        assertThat(windowSet.getStateWindow(new TimeWindow(0, 4))).isEqualTo(new TimeWindow(0, 4));

        // add some more windows
        mergeFunction.reset();
        assertThat(windowSet.addWindow(new TimeWindow(0, 4), mergeFunction))
                .isEqualTo(new TimeWindow(0, 4));
        assertThat(mergeFunction.hasMerged()).isFalse();

        mergeFunction.reset();
        assertThat(windowSet.addWindow(new TimeWindow(3, 5), mergeFunction))
                .isEqualTo(new TimeWindow(0, 5));
        assertThat(mergeFunction.hasMerged()).isTrue();
        assertThat(mergeFunction.mergeTarget()).isEqualTo(new TimeWindow(0, 5));
        assertThat(mergeFunction.stateWindow()).isEqualTo(new TimeWindow(0, 4));
        assertThat(mergeFunction.mergeSources()).containsExactly(new TimeWindow(0, 4));
        assertThat(mergeFunction.mergedStateWindows()).isEmpty();

        mergeFunction.reset();
        assertThat(windowSet.addWindow(new TimeWindow(4, 6), mergeFunction))
                .isEqualTo(new TimeWindow(0, 6));
        assertThat(mergeFunction.hasMerged()).isTrue();
        assertThat(mergeFunction.mergeTarget()).isEqualTo(new TimeWindow(0, 6));
        assertThat(mergeFunction.stateWindow()).isEqualTo(new TimeWindow(0, 4));
        assertThat(mergeFunction.mergeSources()).containsExactly(new TimeWindow(0, 5));
        assertThat(mergeFunction.mergedStateWindows()).isEmpty();

        assertThat(windowSet.getStateWindow(new TimeWindow(0, 6))).isEqualTo(new TimeWindow(0, 4));

        // add some windows that falls into the already merged region
        mergeFunction.reset();
        assertThat(windowSet.addWindow(new TimeWindow(1, 4), mergeFunction))
                .isEqualTo(new TimeWindow(0, 6));
        assertThat(mergeFunction.hasMerged()).isFalse();

        mergeFunction.reset();
        assertThat(windowSet.addWindow(new TimeWindow(0, 4), mergeFunction))
                .isEqualTo(new TimeWindow(0, 6));
        assertThat(mergeFunction.hasMerged()).isFalse();

        mergeFunction.reset();
        assertThat(windowSet.addWindow(new TimeWindow(3, 5), mergeFunction))
                .isEqualTo(new TimeWindow(0, 6));
        assertThat(mergeFunction.hasMerged()).isFalse();

        mergeFunction.reset();
        assertThat(windowSet.addWindow(new TimeWindow(4, 6), mergeFunction))
                .isEqualTo(new TimeWindow(0, 6));
        assertThat(mergeFunction.hasMerged()).isFalse();

        assertThat(windowSet.getStateWindow(new TimeWindow(0, 6))).isEqualTo(new TimeWindow(0, 4));

        // add some more windows that don't merge with the first bunch
        mergeFunction.reset();
        assertThat(windowSet.addWindow(new TimeWindow(11, 14), mergeFunction))
                .isEqualTo(new TimeWindow(11, 14));
        assertThat(mergeFunction.hasMerged()).isFalse();

        assertThat(windowSet.getStateWindow(new TimeWindow(0, 6))).isEqualTo(new TimeWindow(0, 4));

        assertThat(windowSet.getStateWindow(new TimeWindow(11, 14)))
                .isEqualTo(new TimeWindow(11, 14));

        // add some more windows that merge with the second bunch

        mergeFunction.reset();
        assertThat(windowSet.addWindow(new TimeWindow(10, 13), mergeFunction))
                .isEqualTo(new TimeWindow(10, 14));
        assertThat(mergeFunction.hasMerged()).isTrue();
        assertThat(mergeFunction.mergeTarget()).isEqualTo(new TimeWindow(10, 14));
        assertThat(mergeFunction.stateWindow()).isEqualTo(new TimeWindow(11, 14));
        assertThat(mergeFunction.mergeSources()).containsExactly(new TimeWindow(11, 14));
        assertThat(mergeFunction.mergedStateWindows()).isEmpty();

        mergeFunction.reset();
        assertThat(windowSet.addWindow(new TimeWindow(12, 15), mergeFunction))
                .isEqualTo(new TimeWindow(10, 15));
        assertThat(mergeFunction.hasMerged()).isTrue();
        assertThat(mergeFunction.mergeTarget()).isEqualTo(new TimeWindow(10, 15));
        assertThat(mergeFunction.stateWindow()).isEqualTo(new TimeWindow(11, 14));
        assertThat(mergeFunction.mergeSources()).containsExactly(new TimeWindow(10, 14));
        assertThat(mergeFunction.mergedStateWindows()).isEmpty();

        mergeFunction.reset();
        assertThat(windowSet.addWindow(new TimeWindow(11, 14), mergeFunction))
                .isEqualTo(new TimeWindow(10, 15));
        assertThat(mergeFunction.hasMerged()).isFalse();

        assertThat(windowSet.getStateWindow(new TimeWindow(0, 6))).isEqualTo(new TimeWindow(0, 4));

        assertThat(windowSet.getStateWindow(new TimeWindow(10, 15)))
                .isEqualTo(new TimeWindow(11, 14));

        // retire the first batch of windows
        windowSet.retireWindow(new TimeWindow(0, 6));

        assertThat(windowSet.getStateWindow(new TimeWindow(0, 6))).isNull();

        assertThat(windowSet.getStateWindow(new TimeWindow(10, 15)))
                .isEqualTo(new TimeWindow(11, 14));
    }

    @Test
    void testLateMerging() throws Exception {
        @SuppressWarnings("unchecked")
        ListState<Tuple2<TimeWindow, TimeWindow>> mockState = mock(ListState.class);

        MergingWindowSet<TimeWindow> windowSet =
                new MergingWindowSet<>(
                        EventTimeSessionWindows.withGap(Duration.ofMillis(3)), mockState);

        TestingMergeFunction mergeFunction = new TestingMergeFunction();

        // add several non-overlapping initial windows

        mergeFunction.reset();
        assertThat(windowSet.addWindow(new TimeWindow(0, 3), mergeFunction))
                .isEqualTo(new TimeWindow(0, 3));
        assertThat(mergeFunction.hasMerged()).isFalse();
        assertThat(windowSet.getStateWindow(new TimeWindow(0, 3))).isEqualTo(new TimeWindow(0, 3));

        mergeFunction.reset();
        assertThat(windowSet.addWindow(new TimeWindow(5, 8), mergeFunction))
                .isEqualTo(new TimeWindow(5, 8));
        assertThat(mergeFunction.hasMerged()).isFalse();
        assertThat(windowSet.getStateWindow(new TimeWindow(5, 8))).isEqualTo(new TimeWindow(5, 8));

        mergeFunction.reset();
        assertThat(windowSet.addWindow(new TimeWindow(10, 13), mergeFunction))
                .isEqualTo(new TimeWindow(10, 13));
        assertThat(mergeFunction.hasMerged()).isFalse();
        assertThat(windowSet.getStateWindow(new TimeWindow(10, 13)))
                .isEqualTo(new TimeWindow(10, 13));

        // add a window that merges the later two windows
        mergeFunction.reset();
        assertThat(windowSet.addWindow(new TimeWindow(8, 10), mergeFunction))
                .isEqualTo(new TimeWindow(5, 13));
        assertThat(mergeFunction.hasMerged()).isTrue();
        assertThat(mergeFunction.mergeTarget()).isEqualTo(new TimeWindow(5, 13));
        assertThat(mergeFunction.stateWindow())
                .satisfiesAnyOf(
                        w -> assertThat(w).isEqualTo(new TimeWindow(5, 8)),
                        w -> assertThat(w).isEqualTo(new TimeWindow(10, 13)));
        assertThat(mergeFunction.mergeSources())
                .containsExactlyInAnyOrder(new TimeWindow(5, 8), new TimeWindow(10, 13));
        assertThat(mergeFunction.mergedStateWindows())
                .containsAnyOf(new TimeWindow(5, 8), new TimeWindow(10, 13));

        assertThat(mergeFunction.mergedStateWindows().toArray())
                .satisfiesAnyOf(
                        o -> assertThat(o).containsExactly(new TimeWindow(10, 13)),
                        o -> assertThat(o).containsExactly(new TimeWindow(5, 8)));

        assertThat(mergeFunction.mergedStateWindows()).doesNotContain(mergeFunction.mergeTarget());

        assertThat(windowSet.getStateWindow(new TimeWindow(0, 3))).isEqualTo(new TimeWindow(0, 3));

        mergeFunction.reset();
        assertThat(windowSet.addWindow(new TimeWindow(5, 8), mergeFunction))
                .isEqualTo(new TimeWindow(5, 13));
        assertThat(mergeFunction.hasMerged()).isFalse();

        mergeFunction.reset();
        assertThat(windowSet.addWindow(new TimeWindow(8, 10), mergeFunction))
                .isEqualTo(new TimeWindow(5, 13));
        assertThat(mergeFunction.hasMerged()).isFalse();

        mergeFunction.reset();
        assertThat(windowSet.addWindow(new TimeWindow(10, 13), mergeFunction))
                .isEqualTo(new TimeWindow(5, 13));
        assertThat(mergeFunction.hasMerged()).isFalse();

        assertThat(windowSet.getStateWindow(new TimeWindow(5, 13)))
                .isIn(new TimeWindow(5, 8), new TimeWindow(10, 13));

        // add a window that merges all of them together
        mergeFunction.reset();
        assertThat(windowSet.addWindow(new TimeWindow(3, 5), mergeFunction))
                .isEqualTo(new TimeWindow(0, 13));
        assertThat(mergeFunction.hasMerged()).isTrue();
        assertThat(mergeFunction.mergeTarget()).isEqualTo(new TimeWindow(0, 13));
        assertThat(mergeFunction.stateWindow())
                .isIn(new TimeWindow(0, 3), new TimeWindow(5, 8), new TimeWindow(10, 13));

        assertThat(mergeFunction.mergeSources())
                .containsExactlyInAnyOrder(new TimeWindow(0, 3), new TimeWindow(5, 13));

        assertThat(mergeFunction.mergedStateWindows().toArray())
                .satisfiesAnyOf(
                        o -> assertThat(o).containsExactly(new TimeWindow(0, 3)),
                        o -> assertThat(o).containsExactly(new TimeWindow(5, 8)),
                        o -> assertThat(o).containsExactly(new TimeWindow(10, 13)));

        assertThat(mergeFunction.mergedStateWindows()).doesNotContain(mergeFunction.mergeTarget());
        assertThat(windowSet.getStateWindow(new TimeWindow(0, 13)))
                .isIn(new TimeWindow(0, 3), new TimeWindow(5, 8), new TimeWindow(10, 13));
    }

    /** Test merging of a large new window that covers one existing windows. */
    @Test
    void testMergeLargeWindowCoveringSingleWindow() throws Exception {
        @SuppressWarnings("unchecked")
        ListState<Tuple2<TimeWindow, TimeWindow>> mockState = mock(ListState.class);

        MergingWindowSet<TimeWindow> windowSet =
                new MergingWindowSet<>(
                        EventTimeSessionWindows.withGap(Duration.ofMillis(3)), mockState);

        TestingMergeFunction mergeFunction = new TestingMergeFunction();

        // add an initial small window

        mergeFunction.reset();
        assertThat(windowSet.addWindow(new TimeWindow(1, 2), mergeFunction))
                .isEqualTo(new TimeWindow(1, 2));
        assertThat(mergeFunction.hasMerged()).isFalse();
        assertThat(windowSet.getStateWindow(new TimeWindow(1, 2))).isEqualTo(new TimeWindow(1, 2));

        // add a new window that completely covers the existing window

        mergeFunction.reset();
        assertThat(windowSet.addWindow(new TimeWindow(0, 3), mergeFunction))
                .isEqualTo(new TimeWindow(0, 3));
        assertThat(mergeFunction.hasMerged()).isTrue();
        assertThat(windowSet.getStateWindow(new TimeWindow(0, 3))).isEqualTo(new TimeWindow(1, 2));
    }

    /**
     * Test adding a new window that is identical to an existing window. This should not cause a
     * merge.
     */
    @Test
    void testAddingIdenticalWindows() throws Exception {
        @SuppressWarnings("unchecked")
        ListState<Tuple2<TimeWindow, TimeWindow>> mockState = mock(ListState.class);

        MergingWindowSet<TimeWindow> windowSet =
                new MergingWindowSet<>(
                        EventTimeSessionWindows.withGap(Duration.ofMillis(3)), mockState);

        TestingMergeFunction mergeFunction = new TestingMergeFunction();

        mergeFunction.reset();
        assertThat(windowSet.addWindow(new TimeWindow(1, 2), mergeFunction))
                .isEqualTo(new TimeWindow(1, 2));
        assertThat(mergeFunction.hasMerged()).isFalse();
        assertThat(windowSet.getStateWindow(new TimeWindow(1, 2))).isEqualTo(new TimeWindow(1, 2));

        mergeFunction.reset();
        assertThat(windowSet.addWindow(new TimeWindow(1, 2), mergeFunction))
                .isEqualTo(new TimeWindow(1, 2));
        assertThat(mergeFunction.hasMerged()).isFalse();
        assertThat(windowSet.getStateWindow(new TimeWindow(1, 2))).isEqualTo(new TimeWindow(1, 2));
    }

    /** Test merging of a large new window that covers multiple existing windows. */
    @Test
    void testMergeLargeWindowCoveringMultipleWindows() throws Exception {
        @SuppressWarnings("unchecked")
        ListState<Tuple2<TimeWindow, TimeWindow>> mockState = mock(ListState.class);

        MergingWindowSet<TimeWindow> windowSet =
                new MergingWindowSet<>(
                        EventTimeSessionWindows.withGap(Duration.ofMillis(3)), mockState);

        TestingMergeFunction mergeFunction = new TestingMergeFunction();

        // add several non-overlapping initial windows

        mergeFunction.reset();
        assertThat(windowSet.addWindow(new TimeWindow(1, 3), mergeFunction))
                .isEqualTo(new TimeWindow(1, 3));
        assertThat(mergeFunction.hasMerged()).isFalse();
        assertThat(windowSet.getStateWindow(new TimeWindow(1, 3))).isEqualTo(new TimeWindow(1, 3));

        mergeFunction.reset();
        assertThat(windowSet.addWindow(new TimeWindow(5, 8), mergeFunction))
                .isEqualTo(new TimeWindow(5, 8));
        assertThat(mergeFunction.hasMerged()).isFalse();
        assertThat(windowSet.getStateWindow(new TimeWindow(5, 8))).isEqualTo(new TimeWindow(5, 8));

        mergeFunction.reset();
        assertThat(windowSet.addWindow(new TimeWindow(10, 13), mergeFunction))
                .isEqualTo(new TimeWindow(10, 13));
        assertThat(mergeFunction.hasMerged()).isFalse();
        assertThat(windowSet.getStateWindow(new TimeWindow(10, 13)))
                .isEqualTo(new TimeWindow(10, 13));

        // add a new window that completely covers the existing windows

        mergeFunction.reset();
        assertThat(windowSet.addWindow(new TimeWindow(0, 13), mergeFunction))
                .isEqualTo(new TimeWindow(0, 13));
        assertThat(mergeFunction.hasMerged()).isTrue();

        assertThat(mergeFunction.mergedStateWindows().toArray())
                .satisfiesAnyOf(
                        o ->
                                assertThat(o)
                                        .containsExactlyInAnyOrder(
                                                new TimeWindow(0, 3), new TimeWindow(5, 8)),
                        o ->
                                assertThat(o)
                                        .containsExactlyInAnyOrder(
                                                new TimeWindow(0, 3), new TimeWindow(10, 13)),
                        o ->
                                assertThat(o)
                                        .containsExactlyInAnyOrder(
                                                new TimeWindow(5, 8), new TimeWindow(10, 13)));

        assertThat(windowSet.getStateWindow(new TimeWindow(0, 13)))
                .isIn(new TimeWindow(1, 3), new TimeWindow(5, 8), new TimeWindow(10, 13));
    }

    @Test
    void testRestoreFromState() throws Exception {
        @SuppressWarnings("unchecked")
        ListState<Tuple2<TimeWindow, TimeWindow>> mockState = mock(ListState.class);
        when(mockState.get())
                .thenReturn(
                        Lists.newArrayList(
                                new Tuple2<>(new TimeWindow(17, 42), new TimeWindow(42, 17)),
                                new Tuple2<>(new TimeWindow(1, 2), new TimeWindow(3, 4))));

        MergingWindowSet<TimeWindow> windowSet =
                new MergingWindowSet<>(
                        EventTimeSessionWindows.withGap(Duration.ofMillis(3)), mockState);

        assertThat(windowSet.getStateWindow(new TimeWindow(17, 42)))
                .isEqualTo(new TimeWindow(42, 17));
        assertThat(windowSet.getStateWindow(new TimeWindow(1, 2))).isEqualTo(new TimeWindow(3, 4));
    }

    @Test
    void testPersist() throws Exception {
        @SuppressWarnings("unchecked")
        ListState<Tuple2<TimeWindow, TimeWindow>> mockState = mock(ListState.class);

        MergingWindowSet<TimeWindow> windowSet =
                new MergingWindowSet<>(
                        EventTimeSessionWindows.withGap(Duration.ofMillis(3)), mockState);

        TestingMergeFunction mergeFunction = new TestingMergeFunction();

        windowSet.addWindow(new TimeWindow(1, 2), mergeFunction);
        windowSet.addWindow(new TimeWindow(17, 42), mergeFunction);

        assertThat(windowSet.getStateWindow(new TimeWindow(1, 2))).isEqualTo(new TimeWindow(1, 2));
        assertThat(windowSet.getStateWindow(new TimeWindow(17, 42)))
                .isEqualTo(new TimeWindow(17, 42));

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

        verify(mockState, times(1)).update(ArgumentMatchers.any());
    }

    @Test
    void testPersistOnlyIfHaveUpdates() throws Exception {
        @SuppressWarnings("unchecked")
        ListState<Tuple2<TimeWindow, TimeWindow>> mockState = mock(ListState.class);
        when(mockState.get())
                .thenReturn(
                        Lists.newArrayList(
                                new Tuple2<>(new TimeWindow(17, 42), new TimeWindow(42, 17)),
                                new Tuple2<>(new TimeWindow(1, 2), new TimeWindow(3, 4))));

        MergingWindowSet<TimeWindow> windowSet =
                new MergingWindowSet<>(
                        EventTimeSessionWindows.withGap(Duration.ofMillis(3)), mockState);

        assertThat(windowSet.getStateWindow(new TimeWindow(17, 42)))
                .isEqualTo(new TimeWindow(42, 17));
        assertThat(windowSet.getStateWindow(new TimeWindow(1, 2))).isEqualTo(new TimeWindow(3, 4));

        windowSet.persist();

        verify(mockState, times(0)).add(ArgumentMatchers.any());
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
            assertThat(target)
                    .as("More than one merge for adding a Window should not occur.")
                    .isNull();
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
