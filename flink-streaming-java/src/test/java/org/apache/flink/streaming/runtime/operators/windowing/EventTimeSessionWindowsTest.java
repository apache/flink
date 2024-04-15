/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.operators.windowing;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.streaming.api.windowing.assigners.DynamicEventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.MergingWindowAssigner;
import org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import org.junit.jupiter.api.Test;
import org.mockito.Matchers;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.Matchers.anyCollection;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;

/** Tests for {@link EventTimeSessionWindows}. */
class EventTimeSessionWindowsTest {

    @Test
    void testWindowAssignment() {
        final int sessionGap = 5000;

        WindowAssigner.WindowAssignerContext mockContext =
                mock(WindowAssigner.WindowAssignerContext.class);

        EventTimeSessionWindows assigner =
                EventTimeSessionWindows.withGap(Time.milliseconds(sessionGap));

        assertThat(assigner.assignWindows("String", 0L, mockContext))
                .containsExactly(new TimeWindow(0, 0 + sessionGap));
        assertThat(assigner.assignWindows("String", 4999L, mockContext))
                .containsExactly(new TimeWindow(4999, 4999 + sessionGap));
        assertThat(assigner.assignWindows("String", 5000L, mockContext))
                .containsExactly(new TimeWindow(5000, 5000 + sessionGap));
    }

    @Test
    void testMergeSinglePointWindow() {
        MergingWindowAssigner.MergeCallback callback =
                mock(MergingWindowAssigner.MergeCallback.class);

        EventTimeSessionWindows assigner = EventTimeSessionWindows.withGap(Time.milliseconds(5000));

        assigner.mergeWindows(Collections.singletonList(new TimeWindow(0, 0)), callback);

        verify(callback, never()).merge(anyCollection(), Matchers.anyObject());
    }

    @Test
    void testMergeSingleWindow() {
        MergingWindowAssigner.MergeCallback callback =
                mock(MergingWindowAssigner.MergeCallback.class);

        EventTimeSessionWindows assigner = EventTimeSessionWindows.withGap(Time.milliseconds(5000));

        assigner.mergeWindows(Collections.singletonList(new TimeWindow(0, 1)), callback);

        verify(callback, never()).merge(anyCollection(), Matchers.anyObject());
    }

    @Test
    void testMergeConsecutiveWindows() {
        MergingWindowAssigner.MergeCallback callback =
                mock(MergingWindowAssigner.MergeCallback.class);

        EventTimeSessionWindows assigner = EventTimeSessionWindows.withGap(Time.milliseconds(5000));

        assigner.mergeWindows(
                Arrays.asList(
                        new TimeWindow(0, 1),
                        new TimeWindow(1, 2),
                        new TimeWindow(2, 3),
                        new TimeWindow(4, 5),
                        new TimeWindow(5, 6)),
                callback);

        verify(callback, times(1))
                .merge(
                        (Collection<TimeWindow>)
                                argThat(
                                        containsInAnyOrder(
                                                new TimeWindow(0, 1),
                                                new TimeWindow(1, 2),
                                                new TimeWindow(2, 3))),
                        eq(new TimeWindow(0, 3)));

        verify(callback, times(1))
                .merge(
                        (Collection<TimeWindow>)
                                argThat(
                                        containsInAnyOrder(
                                                new TimeWindow(4, 5), new TimeWindow(5, 6))),
                        eq(new TimeWindow(4, 6)));

        verify(callback, times(2)).merge(anyCollection(), Matchers.anyObject());
    }

    @Test
    void testMergeCoveringWindow() {
        MergingWindowAssigner.MergeCallback callback =
                mock(MergingWindowAssigner.MergeCallback.class);

        EventTimeSessionWindows assigner = EventTimeSessionWindows.withGap(Time.milliseconds(5000));

        assigner.mergeWindows(
                Arrays.asList(
                        new TimeWindow(1, 1),
                        new TimeWindow(0, 2),
                        new TimeWindow(4, 7),
                        new TimeWindow(5, 6)),
                callback);

        verify(callback, times(1))
                .merge(
                        (Collection<TimeWindow>)
                                argThat(
                                        containsInAnyOrder(
                                                new TimeWindow(1, 1), new TimeWindow(0, 2))),
                        eq(new TimeWindow(0, 2)));

        verify(callback, times(1))
                .merge(
                        (Collection<TimeWindow>)
                                argThat(
                                        containsInAnyOrder(
                                                new TimeWindow(5, 6), new TimeWindow(4, 7))),
                        eq(new TimeWindow(4, 7)));

        verify(callback, times(2)).merge(anyCollection(), Matchers.anyObject());
    }

    @Test
    void testTimeUnits() {
        // sanity check with one other time unit

        final int sessionGap = 5000;

        WindowAssigner.WindowAssignerContext mockContext =
                mock(WindowAssigner.WindowAssignerContext.class);

        EventTimeSessionWindows assigner =
                EventTimeSessionWindows.withGap(Time.seconds(sessionGap / 1000));

        assertThat(assigner.assignWindows("String", 0L, mockContext))
                .containsExactly(new TimeWindow(0, 0 + sessionGap));
        assertThat(assigner.assignWindows("String", 4999L, mockContext))
                .containsExactly(new TimeWindow(4999, 4999 + sessionGap));
        assertThat(assigner.assignWindows("String", 5000L, mockContext))
                .containsExactly(new TimeWindow(5000, 5000 + sessionGap));
    }

    @Test
    void testInvalidParameters() {
        assertThatThrownBy(() -> EventTimeSessionWindows.withGap(Time.seconds(-1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("0 < size");

        assertThatThrownBy(() -> EventTimeSessionWindows.withGap(Time.seconds(0)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("0 < size");
    }

    @Test
    void testProperties() {
        EventTimeSessionWindows assigner = EventTimeSessionWindows.withGap(Time.seconds(5));

        assertThat(assigner.isEventTime()).isTrue();
        assertThat(assigner.getWindowSerializer(new ExecutionConfig()))
                .isEqualTo(new TimeWindow.Serializer());
        assertThat(assigner.getDefaultTrigger()).isInstanceOf(EventTimeTrigger.class);
    }

    @Test
    void testDynamicGapProperties() {
        SessionWindowTimeGapExtractor<String> extractor = mock(SessionWindowTimeGapExtractor.class);
        DynamicEventTimeSessionWindows<String> assigner =
                EventTimeSessionWindows.withDynamicGap(extractor);

        assertThat(assigner).isNotNull();
        assertThat(assigner.isEventTime()).isTrue();
    }
}
