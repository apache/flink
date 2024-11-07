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

package org.apache.flink.table.runtime.operators.window.groupwindow.assigners;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.window.MergeCallback;
import org.apache.flink.table.runtime.operators.window.TimeWindow;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatcher;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/** Tests for {@link SessionWindowAssigner}. */
class SessionWindowAssignerTest {

    private static final RowData ELEMENT = GenericRowData.of("String");

    @Test
    void testWindowAssignment() {
        final int sessionGap = 5000;

        SessionWindowAssigner assigner =
                SessionWindowAssigner.withGap(Duration.ofMillis(sessionGap));

        assertThat(assigner.assignWindows(ELEMENT, 0L)).contains(new TimeWindow(0, sessionGap));
        assertThat(assigner.assignWindows(ELEMENT, 4999L))
                .contains(new TimeWindow(4999, 4999 + sessionGap));
        assertThat(assigner.assignWindows(ELEMENT, 5000L))
                .contains(new TimeWindow(5000, 5000 + sessionGap));
    }

    @SuppressWarnings("unchecked")
    @Test
    void testMergeEmptyWindow() throws Exception {
        MergeCallback<TimeWindow, Collection<TimeWindow>> callback = mock(MergeCallback.class);
        SessionWindowAssigner assigner = SessionWindowAssigner.withGap(Duration.ofMillis(5000));

        assigner.mergeWindows(TimeWindow.of(0, 1), new TreeSet<>(), callback);

        verify(callback, never()).merge(any(), anyCollection());
    }

    @SuppressWarnings("unchecked")
    @Test
    void testMergeSingleWindow() throws Exception {
        MergeCallback<TimeWindow, Collection<TimeWindow>> callback = mock(MergeCallback.class);
        SessionWindowAssigner assigner = SessionWindowAssigner.withGap(Duration.ofMillis(5000));

        TreeSet<TimeWindow> sortedWindows = new TreeSet<>();
        sortedWindows.add(TimeWindow.of(6000, 6001));
        assigner.mergeWindows(TimeWindow.of(0, 1), sortedWindows, callback);

        verify(callback, never()).merge(any(), anyCollection());
    }

    @SuppressWarnings("unchecked")
    @Test
    void testMergeConsecutiveWindows() throws Exception {
        MergeCallback<TimeWindow, Collection<TimeWindow>> callback = mock(MergeCallback.class);
        SessionWindowAssigner assigner = SessionWindowAssigner.withGap(Duration.ofMillis(5000));

        TreeSet<TimeWindow> sortedWindows = new TreeSet<>();
        sortedWindows.addAll(
                Arrays.asList(
                        new TimeWindow(0, 1),
                        new TimeWindow(2, 3),
                        new TimeWindow(4, 5),
                        new TimeWindow(7, 8)));
        assigner.mergeWindows(new TimeWindow(1, 2), sortedWindows, callback);

        verify(callback, times(1))
                .merge(
                        eq(new TimeWindow(0, 3)),
                        argThat(
                                (ArgumentMatcher<Collection<TimeWindow>>)
                                        timeWindows ->
                                                containsInAnyOrder(
                                                                new TimeWindow(0, 1),
                                                                new TimeWindow(1, 2),
                                                                new TimeWindow(2, 3))
                                                        .matches(timeWindows)));
    }

    @SuppressWarnings("unchecked")
    @Test
    void testMergeCoveringWindow() throws Exception {
        MergeCallback<TimeWindow, Collection<TimeWindow>> callback = mock(MergeCallback.class);
        SessionWindowAssigner assigner = SessionWindowAssigner.withGap(Duration.ofMillis(5000));

        TreeSet<TimeWindow> sortedWindows = new TreeSet<>();
        sortedWindows.addAll(
                Arrays.asList(new TimeWindow(1, 4), new TimeWindow(5, 7), new TimeWindow(9, 10)));
        assigner.mergeWindows(new TimeWindow(3, 6), sortedWindows, callback);

        verify(callback, times(1))
                .merge(
                        eq(new TimeWindow(1, 7)),
                        argThat(
                                (ArgumentMatcher<Collection<TimeWindow>>)
                                        timeWindows ->
                                                containsInAnyOrder(
                                                                new TimeWindow(1, 4),
                                                                new TimeWindow(5, 7),
                                                                new TimeWindow(3, 6))
                                                        .matches(timeWindows)));
    }

    @Test
    void testProperties() {
        SessionWindowAssigner assigner = SessionWindowAssigner.withGap(Duration.ofMillis(5000));

        assertThat(assigner.isEventTime()).isTrue();
        assertThat(assigner.getWindowSerializer(new ExecutionConfig()))
                .isEqualTo(new TimeWindow.Serializer());

        assertThat(assigner.withEventTime().isEventTime()).isTrue();
        assertThat(assigner.withProcessingTime().isEventTime()).isFalse();
    }
}
