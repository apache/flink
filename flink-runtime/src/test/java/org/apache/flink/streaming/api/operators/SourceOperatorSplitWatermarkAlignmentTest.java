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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.mocks.MockSourceReader;
import org.apache.flink.api.connector.source.mocks.MockSourceReader.WaitingForSplits;
import org.apache.flink.api.connector.source.mocks.MockSourceSplit;
import org.apache.flink.api.connector.source.mocks.MockSourceSplitSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.runtime.operators.coordination.MockOperatorEventGateway;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.runtime.source.event.AddSplitEvent;
import org.apache.flink.runtime.source.event.WatermarkAlignmentEvent;
import org.apache.flink.runtime.state.TestTaskStateManager;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.operators.source.CollectingDataOutput;
import org.apache.flink.streaming.api.operators.source.TestingSourceOperator;
import org.apache.flink.streaming.runtime.io.DataInputStatus;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.SourceOperatorStreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamMockEnvironment;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.streaming.util.MockOutput;
import org.apache.flink.streaming.util.MockStreamConfig;

import org.assertj.core.api.Condition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit test for split alignment in {@link SourceOperator}. */
class SourceOperatorSplitWatermarkAlignmentTest {

    @Test
    void testSplitWatermarkAlignment() throws Exception {

        MockSourceReader sourceReader =
                new MockSourceReader(WaitingForSplits.DO_NOT_WAIT_FOR_SPLITS, false, true);
        Environment env = getTestingEnvironment();
        SourceOperator<Integer, MockSourceSplit> operator =
                new TestingSourceOperator<>(
                        new StreamOperatorParameters<>(
                                new SourceOperatorStreamTask<Integer>(env),
                                new MockStreamConfig(new Configuration(), 1),
                                new MockOutput<>(new ArrayList<>()),
                                TestProcessingTimeService::new,
                                null,
                                null),
                        sourceReader,
                        WatermarkStrategy.forGenerator(ctx -> new TestWatermarkGenerator())
                                .withTimestampAssigner((r, l) -> r)
                                .withWatermarkAlignment("group-1", Duration.ofMillis(1)),
                        new TestProcessingTimeService(),
                        new MockOperatorEventGateway(),
                        1,
                        5,
                        true);
        operator.initializeState(
                new StreamTaskStateInitializerImpl(env, new HashMapStateBackend()));

        operator.open();
        MockSourceSplit split1 = new MockSourceSplit(0, 0, 10);
        MockSourceSplit split2 = new MockSourceSplit(1, 10, 20);
        split1.addRecord(5);
        split1.addRecord(11);
        split2.addRecord(3);
        split2.addRecord(12);

        operator.handleOperatorEvent(
                new AddSplitEvent<>(
                        Arrays.asList(split1, split2), new MockSourceSplitSerializer()));
        CollectingDataOutput<Integer> dataOutput = new CollectingDataOutput<>();

        operator.emitNext(dataOutput); // split 1 emits 5

        operator.handleOperatorEvent(
                new WatermarkAlignmentEvent(4)); // pause by coordinator message
        assertThat(sourceReader.getPausedSplits()).containsExactly("0");

        operator.handleOperatorEvent(new WatermarkAlignmentEvent(5));
        assertThat(sourceReader.getPausedSplits()).isEmpty();

        operator.emitNext(dataOutput); // split 1 emits 11
        operator.emitNext(dataOutput); // split 2 emits 3

        assertThat(sourceReader.getPausedSplits()).containsExactly("0");

        operator.emitNext(dataOutput); // split 2 emits 6

        assertThat(sourceReader.getPausedSplits()).containsExactly("0", "1");
    }

    @Test
    void testBackpressureAndIdleness() throws Exception {
        long idleTimeout = 100;
        MockSourceReader sourceReader =
                new MockSourceReader(WaitingForSplits.DO_NOT_WAIT_FOR_SPLITS, false, true);
        TestProcessingTimeService processingTimeService = new TestProcessingTimeService();
        SourceOperator<Integer, MockSourceSplit> operator =
                createAndOpenSourceOperatorWithIdleness(
                        sourceReader, processingTimeService, idleTimeout);

        /**
         * Intention behind this setup is that split0 emits a couple of records, while we keep
         * advancing processing time and keep firing timers. Normally split1 would switch to idle
         * first (it hasn't emitted any records), which would cause a watermark from split0 to be
         * emitted and then WatermarkStatus.IDLE should be emitted after split0 also switches to
         * idle. However we assert that neither watermark no idle status this doesn't happen due to
         * the back pressure status.
         */
        MockSourceSplit split0 = new MockSourceSplit(0, 0, 10).addRecord(42).addRecord(44);
        MockSourceSplit split1 = new MockSourceSplit(1, 10, 20);
        operator.handleOperatorEvent(
                new AddSplitEvent<>(
                        Arrays.asList(split0, split1), new MockSourceSplitSerializer()));

        CollectingDataOutput<Integer> dataOutput = new CollectingDataOutput<>();

        // Output is initialised by the SourceOperator on the first emitNext invocation
        operator.emitNext(dataOutput);

        TaskIOMetricGroup taskIOMetricGroup =
                operator.getContainingTask().getEnvironment().getMetricGroup().getIOMetricGroup();
        taskIOMetricGroup.getHardBackPressuredTimePerSecond().markStart();

        for (int i = 0; i < 10; i++) {
            processingTimeService.advance(idleTimeout);
            operator.emitNext(dataOutput);
        }
        assertThat(dataOutput.getEvents()).doesNotContain(WatermarkStatus.IDLE);
        assertThat(dataOutput.getEvents()).doNotHave(new AnyWatermark());

        taskIOMetricGroup.getHardBackPressuredTimePerSecond().markEnd();
        taskIOMetricGroup.getSoftBackPressuredTimePerSecond().markStart();

        for (int i = 0; i < 10; i++) {
            processingTimeService.advance(idleTimeout);
        }
        assertThat(dataOutput.getEvents()).doesNotContain(WatermarkStatus.IDLE);
        assertThat(dataOutput.getEvents()).doNotHave(new AnyWatermark());

        taskIOMetricGroup.getSoftBackPressuredTimePerSecond().markEnd();

        for (int i = 0; i < 10; i++) {
            processingTimeService.advance(idleTimeout);
        }

        assertThat(dataOutput.getEvents()).contains(WatermarkStatus.IDLE);
        assertThat(dataOutput.getEvents()).doNotHave(new AnyWatermark());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testSingleSplitWatermarkAlignmentAndIdleness(boolean usePerSplitOutputs) throws Exception {
        long idleTimeout = 100;
        MockSourceReader sourceReader =
                new MockSourceReader(
                        WaitingForSplits.DO_NOT_WAIT_FOR_SPLITS, false, usePerSplitOutputs);
        TestProcessingTimeService processingTimeService = new TestProcessingTimeService();
        processingTimeService.setCurrentTime(1);
        SourceOperator<Integer, MockSourceSplit> operator =
                createAndOpenSourceOperatorWithIdleness(
                        sourceReader, processingTimeService, idleTimeout);

        MockSourceSplit split0 = new MockSourceSplit(0, 0, 10);
        int maxAllowedWatermark = 4;
        int maxEmittedWatermark = maxAllowedWatermark + 1;
        // enough records should emit from split0 to make the mainSplit or perSplit is idle,
        // then split0 gets blocked and record (maxEmittedWatermark + 100) is never emitted from
        // split0
        split0.addRecord(1)
                .addRecord(1)
                .addRecord(1)
                .addRecord(1)
                .addRecord(maxEmittedWatermark)
                .addRecord(maxEmittedWatermark + 100);

        operator.handleOperatorEvent(
                new AddSplitEvent<>(
                        Collections.singletonList(split0), new MockSourceSplitSerializer()));
        CollectingDataOutput<Integer> dataOutput = new CollectingDataOutput<>();

        operator.handleOperatorEvent(
                new WatermarkAlignmentEvent(maxAllowedWatermark)); // blocks split0

        for (int i = 0; i < 10; i++) {
            operator.emitNext(dataOutput);
            processingTimeService.advance(idleTimeout);
        }
        assertThat(dataOutput.getEvents()).doesNotContain(WatermarkStatus.IDLE);
    }

    @Test
    void testMultiSplitWatermarkAlignmentAndIdleness() throws Exception {
        long idleTimeout = 100;
        MockSourceReader sourceReader =
                new MockSourceReader(WaitingForSplits.DO_NOT_WAIT_FOR_SPLITS, false, true);
        TestProcessingTimeService processingTimeService = new TestProcessingTimeService();
        SourceOperator<Integer, MockSourceSplit> operator =
                createAndOpenSourceOperatorWithIdleness(
                        sourceReader, processingTimeService, idleTimeout);

        MockSourceSplit split0 = new MockSourceSplit(0, 0, 10);
        MockSourceSplit split1 = new MockSourceSplit(1, 10, 20);
        int maxAllowedWatermark = 4;
        int maxEmittedWatermark = maxAllowedWatermark + 1;
        // the intention is that only first record from split0 gets emitted, then split0 gets
        // blocked and record (maxEmittedWatermark + 100) is never emitted from split0
        split0.addRecord(maxEmittedWatermark).addRecord(maxEmittedWatermark + 100);
        split1.addRecord(3)
                .addRecord(3)
                .addRecord(3)
                .addRecord(3)
                .addRecord(3)
                .addRecord(3)
                .addRecord(3);
        split1.addRecord(maxEmittedWatermark + 100);

        operator.handleOperatorEvent(
                new AddSplitEvent<>(
                        Arrays.asList(split0, split1), new MockSourceSplitSerializer()));
        CollectingDataOutput<Integer> dataOutput = new CollectingDataOutput<>();

        operator.emitNext(dataOutput); // split0 emits first (and only) record (maxEmittedWatermark)

        operator.handleOperatorEvent(
                new WatermarkAlignmentEvent(maxAllowedWatermark)); // blocks split0
        assertThat(sourceReader.getPausedSplits()).containsExactly("0");

        while (operator.isAvailable()) {
            // We are advancing a couple of times by (idleTimeout - 1) to make sure the active input
            // never switches idle, while giving plenty of time for the blocked split0 to evaluate
            // it's idle state
            processingTimeService.advance(idleTimeout - 1);
            operator.emitNext(dataOutput); // split1 keeps emitting records
        }
        // in the end, all records are emitted from split1. This shouldn't cause the watermark to
        // get bumped above maxEmittedWatermark, as split0 shouldn't be idle and it is still
        // blocked.
        assertThat(sourceReader.getPausedSplits()).containsExactly("0", "1");
        assertThat(dataOutput.getEvents()).doNotHave(new WatermarkAbove(maxEmittedWatermark));
    }

    @Test
    void testSplitWatermarkAlignmentWithFinishedSplit() throws Exception {
        long idleTimeout = 100;
        MockSourceReader sourceReader =
                new MockSourceReader(WaitingForSplits.DO_NOT_WAIT_FOR_SPLITS, false, true);
        TestProcessingTimeService processingTimeService = new TestProcessingTimeService();
        SourceOperator<Integer, MockSourceSplit> operator =
                createAndOpenSourceOperatorWithIdleness(
                        sourceReader, processingTimeService, idleTimeout);

        MockSourceSplit split0 = new MockSourceSplit(0, 0, 1);
        MockSourceSplit split1 = new MockSourceSplit(1, 10, 20);
        int maxAllowedWatermark = 4;
        int maxEmittedWatermark = maxAllowedWatermark + 1;
        // the intention is that only first record from split0 gets emitted, then split0 gets
        // blocked and record (maxEmittedWatermark + 100) is never emitted from split0
        split0.addRecord(maxEmittedWatermark);
        split1.addRecord(3);

        operator.handleOperatorEvent(
                new AddSplitEvent<>(
                        Arrays.asList(split0, split1), new MockSourceSplitSerializer()));
        CollectingDataOutput<Integer> dataOutput = new CollectingDataOutput<>();

        while (operator.emitNext(dataOutput) == DataInputStatus.MORE_AVAILABLE) {
            // split0 emits its only record and is finished/released
        }
        operator.handleOperatorEvent(
                new WatermarkAlignmentEvent(maxAllowedWatermark)); // blocks split0
        assertThat(sourceReader.getPausedSplits()).isEmpty();
    }

    @Test
    void testStateReportingForMultiSplitWatermarkAlignmentAndIdleness() throws Exception {
        long idleTimeout = 100;
        MockSourceReader sourceReader =
                new MockSourceReader(WaitingForSplits.DO_NOT_WAIT_FOR_SPLITS, false, true);
        TestProcessingTimeService processingTimeService = new TestProcessingTimeService();
        SourceOperator<Integer, MockSourceSplit> operator =
                createAndOpenSourceOperatorWithIdleness(
                        sourceReader, processingTimeService, idleTimeout);

        MockSourceSplit split0 = new MockSourceSplit(0, 0, 10);
        MockSourceSplit split1 = new MockSourceSplit(1, 10, 20);
        int allowedWatermark4 = 4;
        int allowedWatermark7 = 7;
        int allowedWatermark10 = 10;
        split0.addRecord(5);
        split1.addRecord(3);
        split0.addRecord(6);
        split1.addRecord(8);
        operator.handleOperatorEvent(
                new AddSplitEvent<>(
                        Arrays.asList(split0, split1), new MockSourceSplitSerializer()));
        CollectingDataOutput<Integer> dataOutput = new CollectingDataOutput<>();

        // at this point, both splits are neither paused nor idle
        assertThat(operator.getSplitMetricGroup(split0.splitId()).isActive()).isTrue();
        assertThat(operator.getSplitMetricGroup(split1.splitId()).isActive()).isTrue();

        operator.emitNext(dataOutput); // split0 emits 5
        operator.emitNext(dataOutput); // split1 emits 3

        operator.handleOperatorEvent(
                new WatermarkAlignmentEvent(allowedWatermark4)); // blocks split0
        assertThat(operator.getSplitMetricGroup(split0.splitId()).isPaused()).isTrue();
        assertThat(operator.getSplitMetricGroup(split1.splitId()).isActive()).isTrue();

        processingTimeService.advance(idleTimeout - 1);
        operator.emitNext(dataOutput); // split0 emits 6
        for (int i = 0; i < 10; i++) {
            processingTimeService.advance(idleTimeout); // split1 eventually turns idle
        }
        assertThat(operator.getSplitMetricGroup(split0.splitId()).isPaused()).isTrue();
        assertThat(operator.getSplitMetricGroup(split1.splitId()).isIdle()).isTrue();

        operator.handleOperatorEvent(
                new WatermarkAlignmentEvent(allowedWatermark7)); // unblocks split0
        assertThat(operator.getSplitMetricGroup(split0.splitId()).isActive()).isTrue();
        assertThat(operator.getSplitMetricGroup(split1.splitId()).isIdle()).isTrue();

        operator.emitNext(dataOutput); // split1 emits 8
        assertThat(operator.getSplitMetricGroup(split1.splitId()).isPaused()).isTrue();

        operator.handleOperatorEvent(
                new WatermarkAlignmentEvent(allowedWatermark10)); // unblocks split0
        assertThat(operator.getSplitMetricGroup(split0.splitId()).isActive()).isTrue();
        assertThat(operator.getSplitMetricGroup(split1.splitId()).isActive()).isTrue();
    }

    @Test
    void testStateReportingForSingleSplitWatermarkAlignmentAndIdleness() throws Exception {
        long idleTimeout = 100;
        MockSourceReader sourceReader =
                new MockSourceReader(WaitingForSplits.DO_NOT_WAIT_FOR_SPLITS, true, true);
        TestProcessingTimeService processingTimeService = new TestProcessingTimeService();
        SourceOperator<Integer, MockSourceSplit> operator =
                createAndOpenSourceOperatorWithIdleness(
                        sourceReader, processingTimeService, idleTimeout);

        MockSourceSplit split0 = new MockSourceSplit(0, 0, 10);
        int allowedWatermark4 = 4;
        int allowedWatermark5 = 5;
        int allowedWatermark7 = 7;
        split0.addRecord(5);
        split0.addRecord(6);
        split0.addRecord(7);
        operator.handleOperatorEvent(
                new AddSplitEvent<>(Arrays.asList(split0), new MockSourceSplitSerializer()));
        CollectingDataOutput<Integer> actualOutput = new CollectingDataOutput<>();

        operator.emitNext(actualOutput);
        assertOutput(actualOutput, Arrays.asList(5));
        assertThat(operator.getSplitMetricGroup(split0.splitId()).isActive()).isTrue();

        // testing transition active -> paused
        operator.handleOperatorEvent(new WatermarkAlignmentEvent(allowedWatermark4));
        assertThat(operator.getSplitMetricGroup(split0.splitId()).isPaused()).isTrue();

        // Testing the split doesn't become idle after idle timeout if paused
        for (int i = 0; i < 10; i++) {
            processingTimeService.advance(idleTimeout);
        }
        assertThat(operator.getSplitMetricGroup(split0.splitId()).isIdle()).isFalse();
        assertThat(operator.getSplitMetricGroup(split0.splitId()).isPaused()).isTrue();

        // testing transition paused -> active
        operator.handleOperatorEvent(
                new WatermarkAlignmentEvent(allowedWatermark5)); // unblocks split0
        assertThat(operator.getSplitMetricGroup(split0.splitId()).isActive()).isTrue();

        // testing transition active -> idle
        for (int i = 0; i < 10; i++) {
            processingTimeService.advance(idleTimeout);
        }
        assertThat(operator.getSplitMetricGroup(split0.splitId()).isIdle()).isTrue();

        // testing transition idle -> paused
        operator.emitNext(actualOutput);
        assertOutput(actualOutput, Arrays.asList(5, 6));
        assertThat(operator.getSplitMetricGroup(split0.splitId()).isPaused()).isTrue();
        assertThat(operator.getSplitMetricGroup(split0.splitId()).isIdle()).isFalse();

        operator.handleOperatorEvent(
                new WatermarkAlignmentEvent(allowedWatermark7)); // unblocks split0
        assertThat(operator.getSplitMetricGroup(split0.splitId()).isActive()).isTrue();

        // testing transition idle -> active
        for (int i = 0; i < 10; i++) {
            processingTimeService.advance(idleTimeout);
        }
        assertThat(operator.getSplitMetricGroup(split0.splitId()).isIdle()).isTrue();
        operator.emitNext(actualOutput);
        assertOutput(actualOutput, Arrays.asList(5, 6, 7));
        assertThat(operator.getSplitMetricGroup(split0.splitId()).isActive()).isTrue();
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

    private SourceOperator<Integer, MockSourceSplit> createAndOpenSourceOperatorWithIdleness(
            MockSourceReader sourceReader,
            TestProcessingTimeService processingTimeService,
            long idleTimeout)
            throws Exception {

        Environment env = getTestingEnvironment();
        SourceOperator<Integer, MockSourceSplit> operator =
                new TestingSourceOperator<>(
                        new StreamOperatorParameters<>(
                                new SourceOperatorStreamTask<Integer>(env),
                                new MockStreamConfig(new Configuration(), 1),
                                new MockOutput<>(new ArrayList<>()),
                                () -> processingTimeService,
                                null,
                                null),
                        sourceReader,
                        WatermarkStrategy.forGenerator(ctx -> new TestWatermarkGenerator())
                                .withTimestampAssigner((r, l) -> r)
                                .withWatermarkAlignment("group-1", Duration.ofMillis(1))
                                .withIdleness(Duration.ofMillis(idleTimeout)),
                        processingTimeService,
                        new MockOperatorEventGateway(),
                        1,
                        5,
                        true);
        operator.initializeState(
                new StreamTaskStateInitializerImpl(env, new HashMapStateBackend()));
        operator.open();
        return operator;
    }

    private Environment getTestingEnvironment() {
        return new StreamMockEnvironment(
                new Configuration(),
                new Configuration(),
                new ExecutionConfig(),
                1L,
                new MockInputSplitProvider(),
                1,
                new TestTaskStateManager());
    }

    private static class TestWatermarkGenerator implements WatermarkGenerator<Integer> {

        private long maxWatermark = Long.MIN_VALUE;

        @Override
        public void onEvent(Integer event, long eventTimestamp, WatermarkOutput output) {
            if (eventTimestamp > maxWatermark) {
                this.maxWatermark = eventTimestamp;
                output.emitWatermark(new Watermark(maxWatermark));
            }
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            output.emitWatermark(new Watermark(maxWatermark));
        }
    }

    /** Condition checking if there is no watermark above a certain value among StreamElements. */
    public static class WatermarkAbove extends Condition<Object> {
        public WatermarkAbove(int maxEmittedWatermark) {
            super(
                    event -> {
                        if (!(event
                                instanceof org.apache.flink.streaming.api.watermark.Watermark)) {
                            return false;
                        }
                        org.apache.flink.streaming.api.watermark.Watermark w =
                                (org.apache.flink.streaming.api.watermark.Watermark) event;
                        return w.getTimestamp() > maxEmittedWatermark;
                    },
                    "watermark value of greater than %d",
                    maxEmittedWatermark);
        }
    }

    /** Condition checking if there is any watermark among StreamElements. */
    public static class AnyWatermark extends Condition<Object> {
        public AnyWatermark() {
            super(
                    event -> event instanceof org.apache.flink.streaming.api.watermark.Watermark,
                    "any watermark");
        }
    }
}
