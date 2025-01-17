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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.watermark.BoolWatermark;
import org.apache.flink.api.common.watermark.LongWatermark;
import org.apache.flink.api.common.watermark.Watermark;
import org.apache.flink.datastream.api.extension.eventtime.EventTimeExtension;
import org.apache.flink.runtime.event.WatermarkEvent;
import org.apache.flink.runtime.state.KeyedStateCheckpointOutputStream;
import org.apache.flink.streaming.api.operators.InternalTimeServiceManager;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributes;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermark.extension.eventtime.EventTimeWatermarkHandler;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.util.OutputTag;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link EventTimeWatermarkHandler}. */
class EventTimeWatermarkHandlerTest {

    private static final List<Watermark> outputWatermarks = new ArrayList<>();
    private static final List<Long> advancedEventTimes = new ArrayList<>();

    @AfterEach
    void after() {
        outputWatermarks.clear();
        advancedEventTimes.clear();
    }

    @Test
    void testOneInputWatermarkHandler() throws Exception {
        // The test scenario is as follows:
        // -----------------------------------------------------------------------------
        // test scenario|                   expected result
        // -----------------------------------------------------------------------------
        //  Step|Input 0|updateStatus|eventTimes| idleStatus |advancedEventTimes
        // -----------------------------------------------------------------------------
        //   1  |   1   |   true,1   |   [1]    |   []       |   [1]
        //   2  |   2   |   true,2   |   [1,2]  |   []       |   [1,2]
        //   3  |   1   |  false,-1  |   [1,2]  |   []       |   [1,2]
        //   4  |  true |  false,-1  |   [1,2]  |  [true]    |   [1,2]
        //   5  | false |  false,-1  |   [1,2]  |[true,false]|   [1,2]
        // -----------------------------------------------------------------------------
        // For example, Step 1 indicates that Input 0 will receive an event time watermark with a
        // value of 1.
        // After Step 1 is executed, the `updateStatus.isEventTimeUpdated` returned by the handler
        // should be true,
        // and `updateStatus.getNewEventTime` should be equal to 1.
        // Additionally, the handler should output an event time watermark with a value of 1 and
        // advance the current event time to 2.

        EventTimeWatermarkHandler watermarkHandler =
                new EventTimeWatermarkHandler(
                        1, new TestOutput(), new TestInternalTimeServiceManager());
        EventTimeWatermarkHandler.EventTimeUpdateStatus updateStatus;

        // Step 1
        updateStatus =
                watermarkHandler.processWatermark(
                        EventTimeExtension.EVENT_TIME_WATERMARK_DECLARATION.newWatermark(1L), 0);
        assertThat(updateStatus.isEventTimeUpdated()).isTrue();
        assertThat(updateStatus.getNewEventTime()).isEqualTo(1L);
        checkOutputEventTimeWatermarkValues(1L);
        checkOutputIdleStatusWatermarkValues();
        checkAdvancedEventTimes(1L);

        // Step 2
        updateStatus =
                watermarkHandler.processWatermark(
                        EventTimeExtension.EVENT_TIME_WATERMARK_DECLARATION.newWatermark(2L), 0);
        assertThat(updateStatus.isEventTimeUpdated()).isTrue();
        assertThat(updateStatus.getNewEventTime()).isEqualTo(2L);
        checkOutputEventTimeWatermarkValues(1L, 2L);
        checkOutputIdleStatusWatermarkValues();
        checkAdvancedEventTimes(1L, 2L);

        // Step 3
        updateStatus =
                watermarkHandler.processWatermark(
                        EventTimeExtension.EVENT_TIME_WATERMARK_DECLARATION.newWatermark(1L), 0);
        assertThat(updateStatus.isEventTimeUpdated()).isFalse();
        assertThat(updateStatus.getNewEventTime()).isEqualTo(-1L);
        checkOutputEventTimeWatermarkValues(1L, 2L);
        checkOutputIdleStatusWatermarkValues();
        checkAdvancedEventTimes(1L, 2L);

        // Step 4
        updateStatus =
                watermarkHandler.processWatermark(
                        EventTimeExtension.IDLE_STATUS_WATERMARK_DECLARATION.newWatermark(true), 0);
        assertThat(updateStatus.isEventTimeUpdated()).isFalse();
        checkOutputEventTimeWatermarkValues(1L, 2L);
        checkOutputIdleStatusWatermarkValues(true);
        checkAdvancedEventTimes(1L, 2L);

        // Step 5
        updateStatus =
                watermarkHandler.processWatermark(
                        EventTimeExtension.IDLE_STATUS_WATERMARK_DECLARATION.newWatermark(false),
                        0);
        assertThat(updateStatus.isEventTimeUpdated()).isFalse();
        checkOutputEventTimeWatermarkValues(1L, 2L);
        checkOutputIdleStatusWatermarkValues(true, false);
        checkAdvancedEventTimes(1L, 2L);
    }

    @Test
    void testTwoInputWatermarkHandler() throws Exception {
        // The test scenario is as follows:
        // ---------------------------------------------------------------------------------
        //     test scenario        |                   expected result
        // ---------------------------------------------------------------------------------
        //  Step| Input 0 | Input 1 |updateStatus|eventTimes| idleStatus |advancedEventTimes
        // ---------------------------------------------------------------------------------
        //   1  |   1     |         |  false,-1  |   []     |   []       |   []
        //   2  |         |    2    |  true,1    |  [1]     |   []       |   [1]
        //   3  |  true   |         |  false,-1  |  [1]     |   []       |   [1]
        //   4  |         |  true   |  false,-1  |  [1]     |   [true]   |   [1]
        //   5  |         |  false  |  false,-1  |  [1]     |[true,false]|   [1]
        // -----------------------------------------------------------------------------
        // For example, Step 1 indicates that Input 0 will receive an event time watermark with a
        // value of 1.
        // After Step 1 is executed, the `updateStatus.isEventTimeUpdated` returned by the handler
        // should be false,
        // and `updateStatus.getNewEventTime` should be equal to -1.
        // Additionally, the handler should not output any event time watermark and idle status
        // watermark.

        EventTimeWatermarkHandler watermarkHandler =
                new EventTimeWatermarkHandler(
                        2, new TestOutput(), new TestInternalTimeServiceManager());
        EventTimeWatermarkHandler.EventTimeUpdateStatus updateStatus;

        // Step 1
        updateStatus =
                watermarkHandler.processWatermark(
                        EventTimeExtension.EVENT_TIME_WATERMARK_DECLARATION.newWatermark(1L), 0);
        assertThat(updateStatus.isEventTimeUpdated()).isFalse();
        checkOutputEventTimeWatermarkValues();
        checkOutputIdleStatusWatermarkValues();
        checkAdvancedEventTimes();

        // Step 2
        updateStatus =
                watermarkHandler.processWatermark(
                        EventTimeExtension.EVENT_TIME_WATERMARK_DECLARATION.newWatermark(2L), 1);
        assertThat(updateStatus.isEventTimeUpdated()).isTrue();
        assertThat(updateStatus.getNewEventTime()).isEqualTo(1L);
        checkOutputEventTimeWatermarkValues(1L);
        checkOutputIdleStatusWatermarkValues();
        checkAdvancedEventTimes(1L);

        // Step 3
        updateStatus =
                watermarkHandler.processWatermark(
                        EventTimeExtension.IDLE_STATUS_WATERMARK_DECLARATION.newWatermark(true), 0);
        assertThat(updateStatus.isEventTimeUpdated()).isFalse();
        assertThat(updateStatus.getNewEventTime()).isEqualTo(-1L);
        checkOutputEventTimeWatermarkValues(1L);
        checkOutputIdleStatusWatermarkValues();
        checkAdvancedEventTimes(1L);

        // Step 4
        updateStatus =
                watermarkHandler.processWatermark(
                        EventTimeExtension.IDLE_STATUS_WATERMARK_DECLARATION.newWatermark(true), 1);
        assertThat(updateStatus.isEventTimeUpdated()).isFalse();
        assertThat(updateStatus.getNewEventTime()).isEqualTo(-1L);
        checkOutputEventTimeWatermarkValues(1L);
        checkOutputIdleStatusWatermarkValues(true);
        checkAdvancedEventTimes(1L);

        // Step 5
        updateStatus =
                watermarkHandler.processWatermark(
                        EventTimeExtension.IDLE_STATUS_WATERMARK_DECLARATION.newWatermark(false),
                        1);
        assertThat(updateStatus.isEventTimeUpdated()).isFalse();
        assertThat(updateStatus.getNewEventTime()).isEqualTo(-1L);
        checkOutputEventTimeWatermarkValues(1L);
        checkOutputIdleStatusWatermarkValues(true, false);
        checkAdvancedEventTimes(1L);
    }

    private static class TestOutput implements Output<Long> {
        @Override
        public void emitWatermark(org.apache.flink.streaming.api.watermark.Watermark mark) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void emitWatermarkStatus(WatermarkStatus watermarkStatus) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void emitLatencyMarker(LatencyMarker latencyMarker) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void emitRecordAttributes(RecordAttributes recordAttributes) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void emitWatermark(WatermarkEvent watermark) {
            outputWatermarks.add(watermark.getWatermark());
        }

        @Override
        public void collect(Long record) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {}
    }

    private static class TestInternalTimeServiceManager
            implements InternalTimeServiceManager<Long> {

        @Override
        public <N> InternalTimerService<N> getInternalTimerService(
                String name,
                TypeSerializer<Long> keySerializer,
                TypeSerializer<N> namespaceSerializer,
                Triggerable<Long, N> triggerable) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void advanceWatermark(org.apache.flink.streaming.api.watermark.Watermark watermark)
                throws Exception {
            advancedEventTimes.add(watermark.getTimestamp());
        }

        @Override
        public boolean tryAdvanceWatermark(
                org.apache.flink.streaming.api.watermark.Watermark watermark,
                ShouldStopAdvancingFn shouldStopAdvancingFn)
                throws Exception {
            throw new UnsupportedOperationException();
        }

        @Override
        public void snapshotToRawKeyedState(
                KeyedStateCheckpointOutputStream stateCheckpointOutputStream, String operatorName)
                throws Exception {
            throw new UnsupportedOperationException();
        }
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

    private void checkAdvancedEventTimes(Long... expectedAdvancedEventTimes) {
        assertThat(advancedEventTimes).containsExactly(expectedAdvancedEventTimes);
    }
}
