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
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.assigners.WindowStagger;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests for {@link TumblingEventTimeWindows}. */
class TumblingEventTimeWindowsTest {

    @Test
    void testWindowAssignment() {
        WindowAssigner.WindowAssignerContext mockContext =
                mock(WindowAssigner.WindowAssignerContext.class);

        TumblingEventTimeWindows assigner = TumblingEventTimeWindows.of(Time.milliseconds(5000));

        assertThat(assigner.assignWindows("String", 0L, mockContext))
                .containsExactly(new TimeWindow(0, 5000));
        assertThat(assigner.assignWindows("String", 4999L, mockContext))
                .containsExactly(new TimeWindow(0, 5000));
        assertThat(assigner.assignWindows("String", 5000L, mockContext))
                .containsExactly(new TimeWindow(5000, 10000));
    }

    @Test
    void testWindowAssignmentWithStagger() {
        WindowAssigner.WindowAssignerContext mockContext =
                mock(WindowAssigner.WindowAssignerContext.class);

        TumblingEventTimeWindows assigner =
                TumblingEventTimeWindows.of(
                        Time.milliseconds(5000), Time.milliseconds(0), WindowStagger.NATURAL);

        when(mockContext.getCurrentProcessingTime()).thenReturn(150L);
        assertThat(assigner.assignWindows("String", 150L, mockContext))
                .containsExactly(new TimeWindow(150, 5150));
        assertThat(assigner.assignWindows("String", 5099L, mockContext))
                .containsExactly(new TimeWindow(150, 5150));
        assertThat(assigner.assignWindows("String", 5300L, mockContext))
                .containsExactly(new TimeWindow(5150, 10150));
    }

    @Test
    void testWindowAssignmentWithGlobalOffset() {
        WindowAssigner.WindowAssignerContext mockContext =
                mock(WindowAssigner.WindowAssignerContext.class);

        TumblingEventTimeWindows assigner =
                TumblingEventTimeWindows.of(Time.milliseconds(5000), Time.milliseconds(100));

        assertThat(assigner.assignWindows("String", 100L, mockContext))
                .containsExactly(new TimeWindow(100, 5100));
        assertThat(assigner.assignWindows("String", 5099L, mockContext))
                .containsExactly(new TimeWindow(100, 5100));
        assertThat(assigner.assignWindows("String", 5100L, mockContext))
                .containsExactly(new TimeWindow(5100, 10100));
    }

    @Test
    void testWindowAssignmentWithNegativeGlobalOffset() {
        WindowAssigner.WindowAssignerContext mockContext =
                mock(WindowAssigner.WindowAssignerContext.class);

        TumblingEventTimeWindows assigner =
                TumblingEventTimeWindows.of(Time.milliseconds(5000), Time.milliseconds(-100));

        assertThat(assigner.assignWindows("String", 0L, mockContext))
                .containsExactly(new TimeWindow(-100, 4900));
        assertThat(assigner.assignWindows("String", 4899L, mockContext))
                .containsExactly(new TimeWindow(-100, 4900));
        assertThat(assigner.assignWindows("String", 4900L, mockContext))
                .containsExactly(new TimeWindow(4900, 9900));
    }

    @Test
    void testTimeUnits() {
        // sanity check with one other time unit

        WindowAssigner.WindowAssignerContext mockContext =
                mock(WindowAssigner.WindowAssignerContext.class);

        TumblingEventTimeWindows assigner =
                TumblingEventTimeWindows.of(Time.seconds(5), Time.seconds(1));

        assertThat(assigner.assignWindows("String", 1000L, mockContext))
                .containsExactly(new TimeWindow(1000, 6000));
        assertThat(assigner.assignWindows("String", 5999L, mockContext))
                .containsExactly(new TimeWindow(1000, 6000));
        assertThat(assigner.assignWindows("String", 6000L, mockContext))
                .containsExactly(new TimeWindow(6000, 11000));
    }

    @Test
    void testInvalidParameters() {

        assertThatThrownBy(() -> TumblingEventTimeWindows.of(Time.seconds(-1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("abs(offset) < size");

        assertThatThrownBy(() -> TumblingEventTimeWindows.of(Time.seconds(10), Time.seconds(20)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("abs(offset) < size");

        assertThatThrownBy(() -> TumblingEventTimeWindows.of(Time.seconds(10), Time.seconds(-11)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("abs(offset) < size");
    }

    @Test
    void testProperties() {
        TumblingEventTimeWindows assigner =
                TumblingEventTimeWindows.of(Time.seconds(5), Time.milliseconds(100));

        assertThat(assigner.isEventTime()).isTrue();
        assertThat(assigner.getWindowSerializer(new ExecutionConfig()))
                .isEqualTo(new TimeWindow.Serializer());
        assertThat(assigner.getDefaultTrigger()).isInstanceOf(EventTimeTrigger.class);
    }
}
