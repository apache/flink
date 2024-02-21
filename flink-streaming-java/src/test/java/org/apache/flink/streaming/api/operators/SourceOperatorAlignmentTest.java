/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.mocks.MockSourceSplit;
import org.apache.flink.api.connector.source.mocks.MockSourceSplitSerializer;
import org.apache.flink.runtime.io.network.api.StopMode;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.source.event.AddSplitEvent;
import org.apache.flink.runtime.source.event.ReportedWatermarkEvent;
import org.apache.flink.runtime.source.event.WatermarkAlignmentEvent;
import org.apache.flink.streaming.api.operators.source.CollectingDataOutput;
import org.apache.flink.streaming.runtime.io.DataInputStatus;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit test for {@link SourceOperator} watermark alignment. */
@SuppressWarnings("serial")
class SourceOperatorAlignmentTest {

    @Nullable private SourceOperatorTestContext context;
    @Nullable private SourceOperator<Integer, MockSourceSplit> operator;

    @BeforeEach
    void setup() throws Exception {
        context =
                new SourceOperatorTestContext(
                        false,
                        WatermarkStrategy.forGenerator(ctx -> new PunctuatedGenerator())
                                .withTimestampAssigner((r, t) -> r)
                                .withWatermarkAlignment(
                                        "group1", Duration.ofMillis(100), Duration.ofMillis(1)));
        operator = context.getOperator();
    }

    @AfterEach
    void tearDown() throws Exception {
        context.close();
        context = null;
        operator = null;
    }

    @Test
    void testWatermarkAlignment() throws Exception {
        operator.initializeState(context.createStateContext());
        operator.open();
        MockSourceSplit newSplit = new MockSourceSplit(2);
        int record1 = 1000;
        int record2 = 2000;
        int record3 = 3000;
        newSplit.addRecord(record1);
        newSplit.addRecord(record2);
        newSplit.addRecord(record3);

        operator.handleOperatorEvent(
                new AddSplitEvent<>(
                        Collections.singletonList(newSplit), new MockSourceSplitSerializer()));

        CollectingDataOutput<Integer> actualOutput = new CollectingDataOutput<>();
        List<Integer> expectedOutput = new ArrayList<>();

        assertThat(operator.emitNext(actualOutput)).isEqualTo(DataInputStatus.MORE_AVAILABLE);
        expectedOutput.add(record1);
        context.getTimeService().advance(1);
        assertLatestReportedWatermarkEvent(record1);
        assertOutput(actualOutput, expectedOutput);
        assertThat(operator.isAvailable()).isTrue();

        operator.handleOperatorEvent(new WatermarkAlignmentEvent(record1 - 1));

        assertThat(operator.isAvailable()).isFalse();
        assertThat(operator.emitNext(actualOutput)).isEqualTo(DataInputStatus.NOTHING_AVAILABLE);
        assertLatestReportedWatermarkEvent(record1);
        assertOutput(actualOutput, expectedOutput);
        assertThat(operator.isAvailable()).isFalse();

        operator.handleOperatorEvent(new WatermarkAlignmentEvent(record1 + 1));

        assertThat(operator.isAvailable()).isTrue();
        operator.emitNext(actualOutput);
        // Try to poll a record second time. Technically speaking previous emitNext call could have
        // already switch the operator status to unavailable, but that's an implementation detail.
        // However, this second call can not emit anything and should after that second call
        // operator must be unavailable.
        assertThat(operator.emitNext(actualOutput)).isEqualTo(DataInputStatus.NOTHING_AVAILABLE);
        expectedOutput.add(record2);
        context.getTimeService().advance(1);
        assertLatestReportedWatermarkEvent(record2);
        assertOutput(actualOutput, expectedOutput);
        assertThat(operator.isAvailable()).isFalse();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testWatermarkAlignmentWithIdleness(boolean allSubtasksIdle) throws Exception {
        // we use a separate context, because we need to enable idleness
        try (SourceOperatorTestContext context =
                new SourceOperatorTestContext(
                        true,
                        WatermarkStrategy.forGenerator(
                                        ctx ->
                                                new PunctuatedGenerator(
                                                        PunctuatedGenerator.GenerationMode.ODD))
                                .withWatermarkAlignment(
                                        "group1", Duration.ofMillis(100), Duration.ofMillis(1))
                                .withTimestampAssigner((r, t) -> r))) {
            final SourceOperator<Integer, MockSourceSplit> operator = context.getOperator();
            operator.initializeState(context.createStateContext());
            operator.open();
            MockSourceSplit newSplit = new MockSourceSplit(2);
            int record1 = 1;
            newSplit.addRecord(record1);

            operator.handleOperatorEvent(
                    new AddSplitEvent<>(
                            Collections.singletonList(newSplit), new MockSourceSplitSerializer()));

            CollectingDataOutput<Integer> actualOutput = new CollectingDataOutput<>();
            List<Integer> expectedOutput = new ArrayList<>();

            assertThat(operator.emitNext(actualOutput)).isEqualTo(DataInputStatus.MORE_AVAILABLE);
            expectedOutput.add(record1);
            context.getTimeService().advance(1);
            assertLatestReportedWatermarkEvent(context, record1);
            // mock WatermarkAlignmentEvent from SourceCoordinator
            operator.handleOperatorEvent(new WatermarkAlignmentEvent(record1 + 100));
            assertOutput(actualOutput, expectedOutput);
            assertThat(operator.isAvailable()).isTrue();

            // source becomes idle, it should report Long.MAX_VALUE as the watermark
            assertThat(operator.emitNext(actualOutput))
                    .isEqualTo(DataInputStatus.NOTHING_AVAILABLE);
            context.getTimeService().advance(1);
            assertLatestReportedWatermarkEvent(context, Long.MAX_VALUE);

            if (allSubtasksIdle) {
                // If all source subtasks of the watermark group are idle,
                // then the coordinator will report Long.MAX_VALUE
                operator.handleOperatorEvent(
                        new WatermarkAlignmentEvent(Watermark.MAX_WATERMARK.getTimestamp()));
            } else {
                // Other subtasks are not idle, so the watermark is increasing.
                operator.handleOperatorEvent(new WatermarkAlignmentEvent(record1 + 150));
            }

            // it is easier to create a new split than add records the old one. The old one is
            // serialized, when sending the AddSplitEvent, so it is not as easy as
            // newSplit.addRecord
            newSplit = new MockSourceSplit(3);
            int record2 = 2; // even timestamp -> no watermarks
            newSplit.addRecord(record2);

            operator.handleOperatorEvent(
                    new AddSplitEvent<>(
                            Collections.singletonList(newSplit), new MockSourceSplitSerializer()));

            assertThat(operator.emitNext(actualOutput)).isEqualTo(DataInputStatus.MORE_AVAILABLE);
            expectedOutput.add(record2);
            context.getTimeService().advance(1);
            // becomes active again, should go back to the previously emitted
            // watermark, as the record2 does not emit watermarks
            assertLatestReportedWatermarkEvent(context, record1);
            operator.handleOperatorEvent(new WatermarkAlignmentEvent(record1 + 100));
            assertOutput(actualOutput, expectedOutput);
            assertThat(operator.isAvailable()).isTrue();
        }
    }

    @Test
    void testWatermarkAlignmentWithoutSplit() throws Exception {
        operator.initializeState(context.createStateContext());
        operator.open();

        CollectingDataOutput<Integer> actualOutput = new CollectingDataOutput<>();
        assertThat(operator.emitNext(actualOutput)).isEqualTo(DataInputStatus.NOTHING_AVAILABLE);

        // Don't report any ReportedWatermarkEvent
        context.getTimeService().advance(1);
        assertNoReportedWatermarkEvent(context);

        context.getTimeService().advance(1);
        assertNoReportedWatermarkEvent(context);

        assertThat(operator.emitNext(actualOutput)).isEqualTo(DataInputStatus.NOTHING_AVAILABLE);

        MockSourceSplit newSplit = new MockSourceSplit(2);
        int record = 10;
        newSplit.addRecord(record);
        operator.handleOperatorEvent(
                new AddSplitEvent<>(
                        Collections.singletonList(newSplit), new MockSourceSplitSerializer()));

        List<Integer> expectedOutput = new ArrayList<>();
        assertThat(operator.emitNext(actualOutput)).isEqualTo(DataInputStatus.MORE_AVAILABLE);
        expectedOutput.add(record);

        context.getTimeService().advance(1);
        assertLatestReportedWatermarkEvent(record);
        assertOutput(actualOutput, expectedOutput);
    }

    @Test
    void testStopWhileWaitingForWatermarkAlignment() throws Exception {
        testWatermarkAlignment();

        CompletableFuture<?> availableFuture = operator.getAvailableFuture();
        assertThat(availableFuture).isNotDone();
        operator.stop(StopMode.NO_DRAIN);
        assertThat(availableFuture).isDone();
        assertThat(operator.isAvailable()).isTrue();
    }

    @Test
    void testReportedWatermarkDoNotDecrease() throws Exception {
        operator.initializeState(context.createStateContext());
        operator.open();
        MockSourceSplit split1 = new MockSourceSplit(2);
        MockSourceSplit split2 = new MockSourceSplit(3);
        int record1 = 2000;
        int record2 = 1000;
        split1.addRecord(record1);
        split2.addRecord(record2);

        operator.handleOperatorEvent(
                new AddSplitEvent<>(
                        Collections.singletonList(split1), new MockSourceSplitSerializer()));

        CollectingDataOutput<Integer> actualOutput = new CollectingDataOutput<>();

        operator.emitNext(actualOutput);
        context.getTimeService().advance(1);
        assertLatestReportedWatermarkEvent(record1);

        operator.handleOperatorEvent(
                new AddSplitEvent<>(
                        Collections.singletonList(split2), new MockSourceSplitSerializer()));

        operator.emitNext(actualOutput);
        context.getTimeService().advance(1);
        assertLatestReportedWatermarkEvent(record1);
    }

    private void assertOutput(
            CollectingDataOutput<Integer> actualOutput, List<Integer> expectedOutput) {
        assertThat(
                        actualOutput.getEvents().stream()
                                .filter(o -> o instanceof StreamRecord)
                                .mapToInt(object -> ((StreamRecord<Integer>) object).getValue())
                                .boxed()
                                .collect(Collectors.toList()))
                .containsExactly(expectedOutput.toArray(new Integer[0]));
    }

    private void assertLatestReportedWatermarkEvent(long expectedWatermark) {
        assertLatestReportedWatermarkEvent(this.context, expectedWatermark);
    }

    private void assertLatestReportedWatermarkEvent(
            SourceOperatorTestContext context, long expectedWatermark) {
        List<OperatorEvent> events =
                context.getGateway().getEventsSent().stream()
                        .filter(event -> event instanceof ReportedWatermarkEvent)
                        .collect(Collectors.toList());

        assertThat(events).isNotEmpty();
        assertThat(events.get(events.size() - 1))
                .isEqualTo(new ReportedWatermarkEvent(expectedWatermark));
    }

    private void assertNoReportedWatermarkEvent(SourceOperatorTestContext context) {
        List<OperatorEvent> events =
                context.getGateway().getEventsSent().stream()
                        .filter(event -> event instanceof ReportedWatermarkEvent)
                        .collect(Collectors.toList());
        assertThat(events).isEmpty();
    }

    private static class PunctuatedGenerator implements WatermarkGenerator<Integer> {

        private enum GenerationMode {
            ALL,
            ODD
        }

        private GenerationMode mode;

        public PunctuatedGenerator() {
            this(GenerationMode.ALL);
        }

        public PunctuatedGenerator(GenerationMode mode) {
            this.mode = mode;
        }

        @Override
        public void onEvent(Integer event, long eventTimestamp, WatermarkOutput output) {
            final boolean shouldGenerate;
            switch (mode) {
                case ALL:
                    shouldGenerate = true;
                    break;
                case ODD:
                    shouldGenerate = eventTimestamp % 2 == 1;
                    break;
                default:
                    throw new IllegalArgumentException("Unknown mode: " + mode);
            }

            if (shouldGenerate) {
                output.emitWatermark(new Watermark(eventTimestamp));
            }
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {}
    }
}
