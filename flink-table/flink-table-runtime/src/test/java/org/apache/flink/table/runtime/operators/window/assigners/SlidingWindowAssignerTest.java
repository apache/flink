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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.time.Duration;

import static org.apache.flink.table.runtime.operators.window.WindowTestUtils.timeWindow;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.HamcrestCondition.matching;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;

/** Tests for {@link SlidingWindowAssigner}. */
public class SlidingWindowAssignerTest {

    private static final RowData ELEMENT = GenericRowData.of("String");

    @Rule public ExpectedException thrown = ExpectedException.none();

    @SuppressWarnings("unchecked")
    @Test
    public void testWindowAssignment() {
        SlidingWindowAssigner assigner =
                SlidingWindowAssigner.of(Duration.ofMillis(5000), Duration.ofMillis(1000));

        assertThat(assigner.assignWindows(ELEMENT, 0L))
                .satisfies(
                        matching(
                                containsInAnyOrder(
                                        timeWindow(-4000, 1000),
                                        timeWindow(-3000, 2000),
                                        timeWindow(-2000, 3000),
                                        timeWindow(-1000, 4000),
                                        timeWindow(0, 5000))));
        assertThat(assigner.assignWindows(ELEMENT, 4999L))
                .satisfies(
                        matching(
                                containsInAnyOrder(
                                        timeWindow(0, 5000),
                                        timeWindow(1000, 6000),
                                        timeWindow(2000, 7000),
                                        timeWindow(3000, 8000),
                                        timeWindow(4000, 9000))));
        assertThat(assigner.assignWindows(ELEMENT, 5000L))
                .satisfies(
                        matching(
                                containsInAnyOrder(
                                        timeWindow(1000, 6000),
                                        timeWindow(2000, 7000),
                                        timeWindow(3000, 8000),
                                        timeWindow(4000, 9000),
                                        timeWindow(5000, 10000))));

        // test pane
        assertThat(new TimeWindow(0, 1000)).isEqualTo(assigner.assignPane(ELEMENT, 0L));
        assertThat(new TimeWindow(4000, 5000)).isEqualTo(assigner.assignPane(ELEMENT, 4999L));
        assertThat(new TimeWindow(5000, 6000)).isEqualTo(assigner.assignPane(ELEMENT, 5000L));

        assertThat(assigner.splitIntoPanes(new TimeWindow(0, 5000)))
                .satisfies(
                        matching(
                                contains(
                                        timeWindow(0, 1000),
                                        timeWindow(1000, 2000),
                                        timeWindow(2000, 3000),
                                        timeWindow(3000, 4000),
                                        timeWindow(4000, 5000))));

        assertThat(assigner.splitIntoPanes(new TimeWindow(3000, 8000)))
                .satisfies(
                        matching(
                                contains(
                                        timeWindow(3000, 4000),
                                        timeWindow(4000, 5000),
                                        timeWindow(5000, 6000),
                                        timeWindow(6000, 7000),
                                        timeWindow(7000, 8000))));

        assertThat(new TimeWindow(4000, 9000))
                .isEqualTo(assigner.getLastWindow(new TimeWindow(4000, 5000)));
        assertThat(new TimeWindow(2000, 7000))
                .isEqualTo(assigner.getLastWindow(new TimeWindow(2000, 3000)));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testWindowAssignmentWithOffset() {
        SlidingWindowAssigner assigner =
                SlidingWindowAssigner.of(Duration.ofMillis(5000), Duration.ofMillis(1000))
                        .withOffset(Duration.ofMillis(100));

        assertThat(assigner.assignWindows(ELEMENT, 100L))
                .satisfies(
                        matching(
                                containsInAnyOrder(
                                        timeWindow(-3900, 1100),
                                        timeWindow(-2900, 2100),
                                        timeWindow(-1900, 3100),
                                        timeWindow(-900, 4100),
                                        timeWindow(100, 5100))));
        assertThat(assigner.assignWindows(ELEMENT, 5099L))
                .satisfies(
                        matching(
                                containsInAnyOrder(
                                        timeWindow(100, 5100),
                                        timeWindow(1100, 6100),
                                        timeWindow(2100, 7100),
                                        timeWindow(3100, 8100),
                                        timeWindow(4100, 9100))));
        assertThat(assigner.assignWindows(ELEMENT, 5100L))
                .satisfies(
                        matching(
                                containsInAnyOrder(
                                        timeWindow(1100, 6100),
                                        timeWindow(2100, 7100),
                                        timeWindow(3100, 8100),
                                        timeWindow(4100, 9100),
                                        timeWindow(5100, 10100))));

        // test pane
        assertThat(new TimeWindow(100, 1100)).isEqualTo(assigner.assignPane(ELEMENT, 100L));
        assertThat(new TimeWindow(4100, 5100)).isEqualTo(assigner.assignPane(ELEMENT, 5099L));
        assertThat(new TimeWindow(5100, 6100)).isEqualTo(assigner.assignPane(ELEMENT, 5100L));

        assertThat(assigner.splitIntoPanes(new TimeWindow(1100, 6100)))
                .satisfies(
                        matching(
                                contains(
                                        timeWindow(1100, 2100),
                                        timeWindow(2100, 3100),
                                        timeWindow(3100, 4100),
                                        timeWindow(4100, 5100),
                                        timeWindow(5100, 6100))));

        assertThat(assigner.splitIntoPanes(new TimeWindow(3100, 8100)))
                .satisfies(
                        matching(
                                contains(
                                        timeWindow(3100, 4100),
                                        timeWindow(4100, 5100),
                                        timeWindow(5100, 6100),
                                        timeWindow(6100, 7100),
                                        timeWindow(7100, 8100))));

        assertThat(new TimeWindow(4100, 9100))
                .isEqualTo(assigner.getLastWindow(new TimeWindow(4100, 5100)));
        assertThat(new TimeWindow(2100, 7100))
                .isEqualTo(assigner.getLastWindow(new TimeWindow(2100, 3100)));
    }

    @Test
    public void testInvalidParameters() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("slide > 0 and size > 0");
        SlidingWindowAssigner.of(Duration.ofSeconds(-2), Duration.ofSeconds(1));

        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("slide > 0 and size > 0");
        SlidingWindowAssigner.of(Duration.ofSeconds(2), Duration.ofSeconds(-1));

        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("slide > 0 and size > 0");
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
