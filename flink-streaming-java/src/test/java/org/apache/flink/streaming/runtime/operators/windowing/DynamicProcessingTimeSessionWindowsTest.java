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
import org.apache.flink.streaming.api.windowing.assigners.DynamicProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.MergingWindowAssigner;
import org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;

/** Tests for {@link DynamicProcessingTimeSessionWindows}. */
class DynamicProcessingTimeSessionWindowsTest {

    @Test
    void testWindowAssignment() {

        WindowAssigner.WindowAssignerContext mockContext =
                mock(WindowAssigner.WindowAssignerContext.class);
        SessionWindowTimeGapExtractor<String> extractor = mock(SessionWindowTimeGapExtractor.class);
        when(extractor.extract(eq("gap5000"))).thenReturn(5000L);
        when(extractor.extract(eq("gap4000"))).thenReturn(4000L);
        when(extractor.extract(eq("gap9000"))).thenReturn(9000L);

        DynamicProcessingTimeSessionWindows<String> assigner =
                DynamicProcessingTimeSessionWindows.withDynamicGap(extractor);

        when(mockContext.getCurrentProcessingTime()).thenReturn(0L);
        assertThat(assigner.assignWindows("gap5000", Long.MIN_VALUE, mockContext))
                .containsExactly(new TimeWindow(0, 5000));

        when(mockContext.getCurrentProcessingTime()).thenReturn(4999L);
        assertThat(assigner.assignWindows("gap4000", Long.MIN_VALUE, mockContext))
                .containsExactly(new TimeWindow(4999, 8999));

        when(mockContext.getCurrentProcessingTime()).thenReturn(5000L);
        assertThat(assigner.assignWindows("gap9000", Long.MIN_VALUE, mockContext))
                .containsExactly(new TimeWindow(5000, 14000));
    }

    @Test
    void testMergeSinglePointWindow() {
        MergingWindowAssigner.MergeCallback callback =
                mock(MergingWindowAssigner.MergeCallback.class);
        SessionWindowTimeGapExtractor extractor = mock(SessionWindowTimeGapExtractor.class);
        when(extractor.extract(any())).thenReturn(5000L);

        DynamicProcessingTimeSessionWindows assigner =
                DynamicProcessingTimeSessionWindows.withDynamicGap(extractor);

        assigner.mergeWindows(Collections.singletonList(new TimeWindow(0, 0)), callback);

        verify(callback, never()).merge(anyCollection(), ArgumentMatchers.any());
    }

    @Test
    void testMergeSingleWindow() {
        MergingWindowAssigner.MergeCallback callback =
                mock(MergingWindowAssigner.MergeCallback.class);
        SessionWindowTimeGapExtractor extractor = mock(SessionWindowTimeGapExtractor.class);
        when(extractor.extract(any())).thenReturn(5000L);

        DynamicProcessingTimeSessionWindows assigner =
                DynamicProcessingTimeSessionWindows.withDynamicGap(extractor);

        assigner.mergeWindows(Collections.singletonList(new TimeWindow(0, 1)), callback);

        verify(callback, never()).merge(anyCollection(), ArgumentMatchers.any());
    }

    @Test
    void testMergeConsecutiveWindows() {
        MergingWindowAssigner.MergeCallback callback =
                mock(MergingWindowAssigner.MergeCallback.class);
        SessionWindowTimeGapExtractor extractor = mock(SessionWindowTimeGapExtractor.class);
        when(extractor.extract(any())).thenReturn(5000L);

        DynamicProcessingTimeSessionWindows assigner =
                DynamicProcessingTimeSessionWindows.withDynamicGap(extractor);

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

        verify(callback, times(2)).merge(anyCollection(), ArgumentMatchers.any());
    }

    @Test
    void testMergeCoveringWindow() {
        MergingWindowAssigner.MergeCallback callback =
                mock(MergingWindowAssigner.MergeCallback.class);
        SessionWindowTimeGapExtractor extractor = mock(SessionWindowTimeGapExtractor.class);
        when(extractor.extract(any())).thenReturn(5000L);

        DynamicProcessingTimeSessionWindows assigner =
                DynamicProcessingTimeSessionWindows.withDynamicGap(extractor);

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

        verify(callback, times(2)).merge(anyCollection(), ArgumentMatchers.any());
    }

    @Test
    void testInvalidParameters() {
        WindowAssigner.WindowAssignerContext mockContext =
                mock(WindowAssigner.WindowAssignerContext.class);
        SessionWindowTimeGapExtractor extractor = mock(SessionWindowTimeGapExtractor.class);
        when(extractor.extract(any())).thenReturn(-1L);

        DynamicProcessingTimeSessionWindows assigner =
                DynamicProcessingTimeSessionWindows.withDynamicGap(extractor);

        assertThatThrownBy(
                        () ->
                                assigner.assignWindows(
                                        Collections.singletonList(new Object()), 1, mockContext))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("0 < gap");
    }

    @Test
    void testProperties() {
        SessionWindowTimeGapExtractor extractor = mock(SessionWindowTimeGapExtractor.class);
        when(extractor.extract(any())).thenReturn(5000L);

        DynamicProcessingTimeSessionWindows assigner =
                DynamicProcessingTimeSessionWindows.withDynamicGap(extractor);

        assertThat(assigner.isEventTime()).isFalse();
        assertThat(assigner.getWindowSerializer(new ExecutionConfig()))
                .isEqualTo(new TimeWindow.Serializer());
        assertThat(assigner.getDefaultTrigger()).isInstanceOf(ProcessingTimeTrigger.class);
    }
}
