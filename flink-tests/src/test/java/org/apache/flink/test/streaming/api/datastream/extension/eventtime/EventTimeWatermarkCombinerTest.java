/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.streaming.api.datastream.extension.eventtime;

import org.apache.flink.api.common.watermark.BoolWatermark;
import org.apache.flink.api.common.watermark.LongWatermark;
import org.apache.flink.api.common.watermark.Watermark;
import org.apache.flink.datastream.api.extension.eventtime.EventTimeExtension;
import org.apache.flink.streaming.runtime.watermark.extension.eventtime.EventTimeWatermarkCombiner;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link EventTimeWatermarkCombiner}. */
class EventTimeWatermarkCombinerTest {

    private final List<Watermark> outputWatermarks = new ArrayList<>();
    private EventTimeWatermarkCombiner combiner;

    @BeforeEach
    void before() {
        combiner = new EventTimeWatermarkCombiner(2);
    }

    @AfterEach
    void after() {
        outputWatermarks.clear();
        combiner = null;
    }

    @Test
    void testCombinedResultIsMin() throws Exception {
        // The test scenario is as follows:
        // -----------------------------------------------------------------------------
        //               test scenario     |         expected result
        // -----------------------------------------------------------------------------
        //    Step | Channel 0 | Channel 1 | output event time | output idle status
        // -----------------------------------------------------------------------------
        //     1   |     1     |     2     |         [1]       |    []
        //     2   |     3     |           |       [1, 2]      |    []
        // -----------------------------------------------------------------------------
        // e.g. The step 1 means that Channel 0 will receive the event time watermark with value 1,
        // and the Channel 1 will receive the event time watermark with value 2.
        // After step 1 has been executed, the combiner should output an event time watermark with
        // value 1. And Step2 means that Channel 0 will receive the event time watermark with value
        // 3, After step 1 has been executed, the combiner should output an event time watermark
        // with value 2 again.

        // Step 1
        combiner.combineWatermark(
                EventTimeExtension.EVENT_TIME_WATERMARK_DECLARATION.newWatermark(1),
                0,
                outputWatermarks::add);
        combiner.combineWatermark(
                EventTimeExtension.EVENT_TIME_WATERMARK_DECLARATION.newWatermark(2),
                1,
                outputWatermarks::add);
        checkOutputEventTimeWatermarkValues(1L);

        // Step 2
        combiner.combineWatermark(
                EventTimeExtension.EVENT_TIME_WATERMARK_DECLARATION.newWatermark(3),
                0,
                outputWatermarks::add);
        checkOutputEventTimeWatermarkValues(1L, 2L);

        checkOutputIdleStatusWatermarkValues();
    }

    @Test
    void testCombineWhenPartialChannelsIdle() throws Exception {
        // The test scenario is as follows:
        // -----------------------------------------------------------------------------
        //               test scenario     |         expected result
        // -----------------------------------------------------------------------------
        //    Step | Channel 0 | Channel 1 | output event time | output idle status
        // -----------------------------------------------------------------------------
        //     1   |     1     |           |         []        |    []
        //     2   |           |   true    |         [1]       |    []
        //     3   |     2     |           |       [1,2]       |    []
        //     4   |           |   false   |       [1,2]       |    []
        //     5   |           |     3     |       [1,2]       |    []
        //     6   |     4     |           |      [1,2,3]      |    []
        // -----------------------------------------------------------------------------
        // e.g. The step 1 means that Channel 0 will receive the event time watermark with value 1.
        // After step 1 has been executed, the combiner should not output any event time watermark
        // as the combiner has not received event time watermark from all input channels.
        // The step 2 means that Channel 1 will receive the idle status watermark with value true.
        // After step 2 has been executed, the combiner should output an event time watermark with
        // value 1.

        // Step 1
        combiner.combineWatermark(
                EventTimeExtension.EVENT_TIME_WATERMARK_DECLARATION.newWatermark(1),
                0,
                outputWatermarks::add);
        checkOutputEventTimeWatermarkValues();

        // Step 2
        combiner.combineWatermark(
                EventTimeExtension.IDLE_STATUS_WATERMARK_DECLARATION.newWatermark(true),
                1,
                outputWatermarks::add);
        checkOutputEventTimeWatermarkValues(1L);

        // Step 3
        combiner.combineWatermark(
                EventTimeExtension.EVENT_TIME_WATERMARK_DECLARATION.newWatermark(2),
                0,
                outputWatermarks::add);
        checkOutputEventTimeWatermarkValues(1L, 2L);

        // Step 4
        combiner.combineWatermark(
                EventTimeExtension.IDLE_STATUS_WATERMARK_DECLARATION.newWatermark(false),
                1,
                outputWatermarks::add);
        checkOutputEventTimeWatermarkValues(1L, 2L);

        // Step 5
        combiner.combineWatermark(
                EventTimeExtension.EVENT_TIME_WATERMARK_DECLARATION.newWatermark(3),
                1,
                outputWatermarks::add);
        checkOutputEventTimeWatermarkValues(1L, 2L);

        // Step 6
        combiner.combineWatermark(
                EventTimeExtension.EVENT_TIME_WATERMARK_DECLARATION.newWatermark(4),
                0,
                outputWatermarks::add);
        checkOutputEventTimeWatermarkValues(1L, 2L, 3L);

        checkOutputIdleStatusWatermarkValues();
    }

    @Test
    void testCombineWhenAllChannelsIdle() throws Exception {
        // The test scenario is as follows:
        // -----------------------------------------------------------------------------
        //               test scenario     |         expected result
        // -----------------------------------------------------------------------------
        //    Step | Channel 0 | Channel 1 | output event time | output idle status
        // -----------------------------------------------------------------------------
        //     1   |     1     |     2     |         [1]       |    []
        //     2   |    true   |           |        [1,2]      |    []
        //     3   |           |   true    |        [1,2]      |    [true]
        //     4   |   false   |           |        [1,2]      |    [true, false]
        //     5   |     3     |           |       [1,2,3]     |    [true, false]
        //     6   |           |   false   |       [1,2,3]     |    [true, false]
        // -----------------------------------------------------------------------------

        // Step 1
        combiner.combineWatermark(
                EventTimeExtension.EVENT_TIME_WATERMARK_DECLARATION.newWatermark(1),
                0,
                outputWatermarks::add);
        combiner.combineWatermark(
                EventTimeExtension.EVENT_TIME_WATERMARK_DECLARATION.newWatermark(2),
                1,
                outputWatermarks::add);
        checkOutputEventTimeWatermarkValues(1L);

        // Step 2
        combiner.combineWatermark(
                EventTimeExtension.IDLE_STATUS_WATERMARK_DECLARATION.newWatermark(true),
                0,
                outputWatermarks::add);
        checkOutputEventTimeWatermarkValues(1L, 2L);
        checkOutputIdleStatusWatermarkValues();

        // Step 3
        combiner.combineWatermark(
                EventTimeExtension.IDLE_STATUS_WATERMARK_DECLARATION.newWatermark(true),
                1,
                outputWatermarks::add);
        checkOutputEventTimeWatermarkValues(1L, 2L);
        checkOutputIdleStatusWatermarkValues(true);

        // Step 4
        combiner.combineWatermark(
                EventTimeExtension.IDLE_STATUS_WATERMARK_DECLARATION.newWatermark(false),
                0,
                outputWatermarks::add);
        checkOutputIdleStatusWatermarkValues(true, false);

        // Step 5
        combiner.combineWatermark(
                EventTimeExtension.EVENT_TIME_WATERMARK_DECLARATION.newWatermark(3),
                0,
                outputWatermarks::add);
        checkOutputEventTimeWatermarkValues(1L, 2L, 3L);
        checkOutputIdleStatusWatermarkValues(true, false);

        // Step 6
        combiner.combineWatermark(
                EventTimeExtension.IDLE_STATUS_WATERMARK_DECLARATION.newWatermark(false),
                1,
                outputWatermarks::add);
        checkOutputIdleStatusWatermarkValues(true, false);
    }

    @Test
    void testCombineWaitForAllChannels() throws Exception {
        // The test scenario is as follows:
        // -----------------------------------------------------------------------------
        //               test scenario     |         expected result
        // -----------------------------------------------------------------------------
        //    Step | Channel 0 | Channel 1 | output event time | output idle status
        // -----------------------------------------------------------------------------
        //     1   |     1     |           |         []        |    []
        //     2   |     3     |           |         []        |    []
        //     3   |           |     2     |         [2]       |    []
        // -----------------------------------------------------------------------------

        // Step 1
        combiner.combineWatermark(
                EventTimeExtension.EVENT_TIME_WATERMARK_DECLARATION.newWatermark(1),
                0,
                outputWatermarks::add);
        checkOutputEventTimeWatermarkValues();

        // Step 2
        combiner.combineWatermark(
                EventTimeExtension.EVENT_TIME_WATERMARK_DECLARATION.newWatermark(3),
                0,
                outputWatermarks::add);
        checkOutputEventTimeWatermarkValues();

        // Step 3
        combiner.combineWatermark(
                EventTimeExtension.EVENT_TIME_WATERMARK_DECLARATION.newWatermark(2),
                1,
                outputWatermarks::add);
        checkOutputEventTimeWatermarkValues(2L);

        checkOutputIdleStatusWatermarkValues();
    }

    private void checkOutputEventTimeWatermarkValues(Long... expectedReceivedWatermarkValues) {
        assertThat(
                        outputWatermarks.stream()
                                .filter(w -> w instanceof LongWatermark)
                                .map(w -> ((LongWatermark) w).getValue()))
                .containsExactly(expectedReceivedWatermarkValues);
    }

    private void checkOutputIdleStatusWatermarkValues(Boolean... expectedReceivedWatermarkValues) {
        assertThat(
                        outputWatermarks.stream()
                                .filter(w -> w instanceof BoolWatermark)
                                .map(w -> ((BoolWatermark) w).getValue()))
                .containsExactly(expectedReceivedWatermarkValues);
    }
}
