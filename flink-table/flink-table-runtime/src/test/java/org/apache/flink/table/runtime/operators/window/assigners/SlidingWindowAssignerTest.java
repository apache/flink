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

package org.apache.flink.table.runtime.operators.window.assigners;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.window.TimeWindow;

import org.junit.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link SlidingWindowAssigner}. */
public class SlidingWindowAssignerTest {

    private static final RowData ELEMENT = GenericRowData.of("String");

    @Test
    public void testWindowAssignment() {
        SlidingWindowAssigner assigner =
                SlidingWindowAssigner.of(Duration.ofMillis(5000), Duration.ofMillis(1000));

        assertThat(assigner.assignWindows(ELEMENT, 0L))
                .containsExactlyInAnyOrder(
                        new TimeWindow(-4000, 1000),
                        new TimeWindow(-3000, 2000),
                        new TimeWindow(-2000, 3000),
                        new TimeWindow(-1000, 4000),
                        new TimeWindow(0, 5000));
        assertThat(assigner.assignWindows(ELEMENT, 4999L))
                .containsExactlyInAnyOrder(
                        new TimeWindow(0, 5000),
                        new TimeWindow(1000, 6000),
                        new TimeWindow(2000, 7000),
                        new TimeWindow(3000, 8000),
                        new TimeWindow(4000, 9000));
        assertThat(assigner.assignWindows(ELEMENT, 5000L))
                .containsExactlyInAnyOrder(
                        new TimeWindow(1000, 6000),
                        new TimeWindow(2000, 7000),
                        new TimeWindow(3000, 8000),
                        new TimeWindow(4000, 9000),
                        new TimeWindow(5000, 10000));

        // test pane
        assertThat(new TimeWindow(0, 1000)).isEqualTo(assigner.assignPane(ELEMENT, 0L));
        assertThat(new TimeWindow(4000, 5000)).isEqualTo(assigner.assignPane(ELEMENT, 4999L));
        assertThat(new TimeWindow(5000, 6000)).isEqualTo(assigner.assignPane(ELEMENT, 5000L));

        assertThat(assigner.splitIntoPanes(new TimeWindow(0, 5000)))
                .contains(
                        new TimeWindow(0, 1000),
                        new TimeWindow(1000, 2000),
                        new TimeWindow(2000, 3000),
                        new TimeWindow(3000, 4000),
                        new TimeWindow(4000, 5000));

        assertThat(assigner.splitIntoPanes(new TimeWindow(3000, 8000)))
                .contains(
                        new TimeWindow(3000, 4000),
                        new TimeWindow(4000, 5000),
                        new TimeWindow(5000, 6000),
                        new TimeWindow(6000, 7000),
                        new TimeWindow(7000, 8000));

        assertThat(new TimeWindow(4000, 9000))
                .isEqualTo(assigner.getLastWindow(new TimeWindow(4000, 5000)));
        assertThat(new TimeWindow(2000, 7000))
                .isEqualTo(assigner.getLastWindow(new TimeWindow(2000, 3000)));
    }

    @Test
    public void testWindowAssignmentWithOffset() {
        SlidingWindowAssigner assigner =
                SlidingWindowAssigner.of(Duration.ofMillis(5000), Duration.ofMillis(1000))
                        .withOffset(Duration.ofMillis(100));

        assertThat(assigner.assignWindows(ELEMENT, 100L))
                .containsExactlyInAnyOrder(
                        new TimeWindow(-3900, 1100),
                        new TimeWindow(-2900, 2100),
                        new TimeWindow(-1900, 3100),
                        new TimeWindow(-900, 4100),
                        new TimeWindow(100, 5100));
        assertThat(assigner.assignWindows(ELEMENT, 5099L))
                .containsExactlyInAnyOrder(
                        new TimeWindow(100, 5100),
                        new TimeWindow(1100, 6100),
                        new TimeWindow(2100, 7100),
                        new TimeWindow(3100, 8100),
                        new TimeWindow(4100, 9100));
        assertThat(assigner.assignWindows(ELEMENT, 5100L))
                .containsExactlyInAnyOrder(
                        new TimeWindow(1100, 6100),
                        new TimeWindow(2100, 7100),
                        new TimeWindow(3100, 8100),
                        new TimeWindow(4100, 9100),
                        new TimeWindow(5100, 10100));

        // test pane
        assertThat(new TimeWindow(100, 1100)).isEqualTo(assigner.assignPane(ELEMENT, 100L));
        assertThat(new TimeWindow(4100, 5100)).isEqualTo(assigner.assignPane(ELEMENT, 5099L));
        assertThat(new TimeWindow(5100, 6100)).isEqualTo(assigner.assignPane(ELEMENT, 5100L));

        assertThat(assigner.splitIntoPanes(new TimeWindow(1100, 6100)))
                .contains(
                        new TimeWindow(1100, 2100),
                        new TimeWindow(2100, 3100),
                        new TimeWindow(3100, 4100),
                        new TimeWindow(4100, 5100),
                        new TimeWindow(5100, 6100));

        assertThat(assigner.splitIntoPanes(new TimeWindow(3100, 8100)))
                .contains(
                        new TimeWindow(3100, 4100),
                        new TimeWindow(4100, 5100),
                        new TimeWindow(5100, 6100),
                        new TimeWindow(6100, 7100),
                        new TimeWindow(7100, 8100));

        assertThat(new TimeWindow(4100, 9100))
                .isEqualTo(assigner.getLastWindow(new TimeWindow(4100, 5100)));
        assertThat(new TimeWindow(2100, 7100))
                .isEqualTo(assigner.getLastWindow(new TimeWindow(2100, 3100)));
    }

    @Test
    public void testInvalidParameters() {
        assertThatThrownBy(
                        () ->
                                SlidingWindowAssigner.of(
                                        Duration.ofSeconds(-2), Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("slide > 0 and size > 0");

        assertThatThrownBy(
                        () ->
                                SlidingWindowAssigner.of(
                                        Duration.ofSeconds(2), Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("slide > 0 and size > 0");

        SlidingWindowAssigner.of(Duration.ofSeconds(20), Duration.ofSeconds(10))
                .withOffset(Duration.ofSeconds(-1));
    }

    @Test
    public void testProperties() {
        SlidingWindowAssigner assigner =
                SlidingWindowAssigner.of(Duration.ofMillis(5000), Duration.ofMillis(1000));

        assertThat(assigner.isEventTime()).isTrue();
        assertThat(assigner.getWindowSerializer(new ExecutionConfig()))
                .isEqualTo(new TimeWindow.Serializer());

        assertThat(assigner.withEventTime().isEventTime()).isTrue();
        assertThat(assigner.withProcessingTime().isEventTime()).isFalse();
    }
}
