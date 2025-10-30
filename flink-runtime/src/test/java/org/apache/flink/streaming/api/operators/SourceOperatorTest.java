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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.mocks.MockSourceReader;
import org.apache.flink.api.connector.source.mocks.MockSourceSplit;
import org.apache.flink.api.connector.source.mocks.MockSourceSplitSerializer;
import org.apache.flink.runtime.event.WatermarkEvent;
import org.apache.flink.runtime.io.network.api.StopMode;
import org.apache.flink.runtime.operators.coordination.MockOperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.source.event.AddSplitEvent;
import org.apache.flink.runtime.source.event.IsProcessingBacklogEvent;
import org.apache.flink.runtime.source.event.ReaderRegistrationEvent;
import org.apache.flink.runtime.source.event.SourceEventWrapper;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContextSynchronousImpl;
import org.apache.flink.streaming.api.operators.source.CollectingDataOutput;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.DataInputStatus;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributes;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.streaming.util.CollectorOutput;
import org.apache.flink.streaming.util.MockOutput;
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/** Unit test for {@link SourceOperator}. */
@SuppressWarnings("serial")
class SourceOperatorTest {

    @Nullable private SourceOperatorTestContext context;
    @Nullable private SourceOperator<Integer, MockSourceSplit> operator;
    @Nullable private MockSourceReader mockSourceReader;
    @Nullable private MockOperatorEventGateway mockGateway;

    @BeforeEach
    void setup() throws Exception {
        context = new SourceOperatorTestContext();
        operator = context.getOperator();
        mockSourceReader = context.getSourceReader();
        mockGateway = context.getGateway();
    }

    @AfterEach
    void tearDown() throws Exception {
        context.close();
        context = null;
        operator = null;
        mockSourceReader = null;
        mockGateway = null;
    }

    @Test
    void testInitializeState() throws Exception {
        StateInitializationContext stateContext = context.createStateContext();
        operator.initializeState(stateContext);

        assertThat(
                        stateContext
                                .getOperatorStateStore()
                                .getListState(SourceOperator.SPLITS_STATE_DESC))
                .isNotNull();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testOpen(boolean supportsSplitReassignmentOnRecovery) throws Exception {
        try (SourceOperatorTestContext context =
                new SourceOperatorTestContext(
                        false,
                        false,
                        WatermarkStrategy.noWatermarks(),
                        new MockOutput<>(new ArrayList<>()),
                        supportsSplitReassignmentOnRecovery)) {
            SourceOperator<Integer, MockSourceSplit> operator = context.getOperator();
            // Initialize the operator.
            operator.initializeState(context.createStateContext());
            // Open the operator.
            operator.open();
            // The source reader should have been assigned a split.
            if (supportsSplitReassignmentOnRecovery) {
                assertThat(context.getSourceReader().getAssignedSplits()).isEmpty();
            } else {
                assertThat(context.getSourceReader().getAssignedSplits())
                        .containsExactly(SourceOperatorTestContext.MOCK_SPLIT);
            }

            // The source reader should have started.
            assertThat(context.getSourceReader().isStarted()).isTrue();

            // A ReaderRegistrationRequest should have been sent.
            assertThat(context.getGateway().getEventsSent()).hasSize(1);
            OperatorEvent operatorEvent = context.getGateway().getEventsSent().get(0);
            assertThat(operatorEvent).isInstanceOf(ReaderRegistrationEvent.class);
            ReaderRegistrationEvent registrationEvent = (ReaderRegistrationEvent) operatorEvent;
            assertThat(registrationEvent.subtaskId())
                    .isEqualTo(SourceOperatorTestContext.SUBTASK_INDEX);
            if (supportsSplitReassignmentOnRecovery) {
                assertThat(registrationEvent.splits(new MockSourceSplitSerializer()))
                        .containsExactly(SourceOperatorTestContext.MOCK_SPLIT);
            } else {
                assertThat(registrationEvent.splits(new MockSourceSplitSerializer())).isEmpty();
            }
        }
    }

    @Test
    void testStop() throws Exception {
        // Initialize the operator.
        operator.initializeState(context.createStateContext());
        // Open the operator.
        operator.open();
        // The source reader should have been assigned a split.
        assertThat(mockSourceReader.getAssignedSplits())
                .containsExactly(SourceOperatorTestContext.MOCK_SPLIT);

        CollectingDataOutput<Integer> dataOutput = new CollectingDataOutput<>();
        assertThat(operator.emitNext(dataOutput)).isEqualTo(DataInputStatus.NOTHING_AVAILABLE);
        assertThat(operator.isAvailable()).isFalse();

        CompletableFuture<Void> sourceStopped = operator.stop(StopMode.DRAIN);
        assertThat(operator.isAvailable()).isTrue();
        assertThat(sourceStopped).isNotDone();
        assertThat(operator.emitNext(dataOutput)).isEqualTo(DataInputStatus.END_OF_DATA);
        operator.finish();
        assertThat(sourceStopped).isDone();
    }

    @Test
    void testHandleAddSplitsEvent() throws Exception {
        operator.initializeState(context.createStateContext());
        operator.open();
        MockSourceSplit newSplit = new MockSourceSplit((2));
        operator.handleOperatorEvent(
                new AddSplitEvent<>(
                        Collections.singletonList(newSplit), new MockSourceSplitSerializer()));
        // The source reader should have been assigned two splits.
        assertThat(mockSourceReader.getAssignedSplits())
                .containsExactly(SourceOperatorTestContext.MOCK_SPLIT, newSplit);
    }

    @Test
    void testHandleAddSourceEvent() throws Exception {
        operator.initializeState(context.createStateContext());
        operator.open();
        SourceEvent event = new SourceEvent() {};
        operator.handleOperatorEvent(new SourceEventWrapper(event));
        // The source reader should have been assigned two splits.
        assertThat(mockSourceReader.getReceivedSourceEvents()).containsExactly(event);
    }

    @Test
    void testSnapshotState() throws Exception {
        StateInitializationContext stateContext = context.createStateContext();
        operator.initializeState(stateContext);
        operator.open();
        MockSourceSplit newSplit = new MockSourceSplit((2));
        operator.handleOperatorEvent(
                new AddSplitEvent<>(
                        Collections.singletonList(newSplit), new MockSourceSplitSerializer()));
        operator.snapshotState(new StateSnapshotContextSynchronousImpl(100L, 100L));

        // Verify the splits in state.
        List<MockSourceSplit> splitsInState =
                CollectionUtil.iterableToList(operator.getReaderState().get());
        assertThat(splitsInState).containsExactly(SourceOperatorTestContext.MOCK_SPLIT, newSplit);
    }

    @Test
    void testNotifyCheckpointComplete() throws Exception {
        StateInitializationContext stateContext = context.createStateContext();
        operator.initializeState(stateContext);
        operator.open();
        operator.snapshotState(new StateSnapshotContextSynchronousImpl(100L, 100L));
        operator.notifyCheckpointComplete(100L);
        assertThat(mockSourceReader.getCompletedCheckpoints().get(0)).isEqualTo(100L);
    }

    @Test
    void testNotifyCheckpointAborted() throws Exception {
        StateInitializationContext stateContext = context.createStateContext();
        operator.initializeState(stateContext);
        operator.open();
        operator.snapshotState(new StateSnapshotContextSynchronousImpl(100L, 100L));
        operator.notifyCheckpointAborted(100L);
        assertThat(mockSourceReader.getAbortedCheckpoints().get(0)).isEqualTo(100L);
    }

    @Test
    void testHandleBacklogEvent() throws Exception {
        List<StreamElement> outputStreamElements = new ArrayList<>();
        context =
                new SourceOperatorTestContext(
                        false,
                        false,
                        WatermarkStrategy.<Integer>forMonotonousTimestamps()
                                .withTimestampAssigner((element, recordTimestamp) -> element),
                        new CollectorOutput<>(outputStreamElements),
                        false);
        operator = context.getOperator();
        operator.initializeState(context.createStateContext());
        operator.open();

        MockSourceSplit newSplit = new MockSourceSplit(2);
        newSplit.addRecord(1);
        newSplit.addRecord(1001);
        operator.handleOperatorEvent(
                new AddSplitEvent<>(
                        Collections.singletonList(newSplit), new MockSourceSplitSerializer()));
        final DataOutputToOutput<Integer> output = new DataOutputToOutput<>(operator.output);
        operator.emitNext(output);
        operator.handleOperatorEvent(new IsProcessingBacklogEvent(true));

        operator.emitNext(output);
        operator.handleOperatorEvent(new IsProcessingBacklogEvent(false));

        assertThat(outputStreamElements)
                .containsExactly(
                        new StreamRecord<>(1, 1),
                        new Watermark(0),
                        new RecordAttributes(true),
                        new StreamRecord<>(1001, 1001),
                        new Watermark(1000),
                        new RecordAttributes(false));
    }

    @Test
    public void testMetricGroupIsCreatedForNewSplit() throws Exception {
        operator.initializeState(context.createStateContext());
        operator.open();
        MockSourceSplit newSplit = new MockSourceSplit((2));
        operator.handleOperatorEvent(
                new AddSplitEvent<>(
                        Collections.singletonList(newSplit), new MockSourceSplitSerializer()));
        assertNotNull(operator.getSplitMetricGroup(newSplit.splitId()));
    }

    @Test
    public void testMetricGroupIsCreatedForRestoredSplit() throws Exception {
        MockSourceSplit restoredSplit = new MockSourceSplit((1));
        StateInitializationContext stateContext =
                context.createStateContext(Collections.singletonList(restoredSplit));
        operator.initializeState(stateContext);
        operator.open();
        assertNotNull(operator.getSplitMetricGroup(restoredSplit.splitId()));
    }

    @Test
    public void testMetricGroupTracksSplitWatermark() throws Exception {
        long expectedWatermark = 1000;
        operator.initializeState(context.createStateContext());
        operator.open();
        MockSourceSplit split = new MockSourceSplit((2));
        operator.handleOperatorEvent(
                new AddSplitEvent<>(
                        Collections.singletonList(split), new MockSourceSplitSerializer()));
        operator.updateCurrentSplitWatermark(split.splitId(), expectedWatermark);
        assertEquals(
                expectedWatermark,
                operator.getSplitMetricGroup(split.splitId()).getCurrentWatermark());
    }

    @Test
    public void testMetricGroupReturnsDefaultIfNoSplitWatermark() throws Exception {
        long expectedWatermark = Watermark.UNINITIALIZED.getTimestamp();
        operator.initializeState(context.createStateContext());
        operator.open();
        MockSourceSplit split = new MockSourceSplit((2));
        operator.handleOperatorEvent(
                new AddSplitEvent<>(
                        Collections.singletonList(split), new MockSourceSplitSerializer()));
        assertEquals(
                expectedWatermark,
                operator.getSplitMetricGroup(split.splitId()).getCurrentWatermark());
    }

    @Test
    public void testMultipleMetricGroupsReturnWatermarkOrDefaultWatermark() throws Exception {
        long expectedWatermarkValueForSplit0 = Watermark.UNINITIALIZED.getTimestamp();
        long expectedWatermarkValueForSplit1 = 1000;
        operator.initializeState(context.createStateContext());
        operator.open();
        MockSourceSplit split0 = new MockSourceSplit((19));
        MockSourceSplit split1 = new MockSourceSplit((11));
        operator.handleOperatorEvent(
                new AddSplitEvent<>(
                        Arrays.asList(split0, split1), new MockSourceSplitSerializer()));
        operator.updateCurrentSplitWatermark(split1.splitId(), expectedWatermarkValueForSplit1);
        assertEquals(
                expectedWatermarkValueForSplit0,
                operator.getSplitMetricGroup(split0.splitId()).getCurrentWatermark());
        assertEquals(
                expectedWatermarkValueForSplit1,
                operator.getSplitMetricGroup(split1.splitId()).getCurrentWatermark());
    }

    private static class DataOutputToOutput<T> implements PushingAsyncDataInput.DataOutput<T> {

        private final Output<StreamRecord<T>> output;

        DataOutputToOutput(Output<StreamRecord<T>> output) {
            this.output = output;
        }

        @Override
        public void emitRecord(StreamRecord<T> streamRecord) throws Exception {
            output.collect(streamRecord);
        }

        @Override
        public void emitWatermark(Watermark watermark) throws Exception {
            output.emitWatermark(watermark);
        }

        @Override
        public void emitWatermarkStatus(WatermarkStatus watermarkStatus) throws Exception {
            output.emitWatermarkStatus(watermarkStatus);
        }

        @Override
        public void emitLatencyMarker(LatencyMarker latencyMarker) throws Exception {
            output.emitLatencyMarker(latencyMarker);
        }

        @Override
        public void emitRecordAttributes(RecordAttributes recordAttributes) throws Exception {
            output.emitRecordAttributes(recordAttributes);
        }

        @Override
        public void emitWatermark(WatermarkEvent watermark) throws Exception {
            output.emitWatermark(watermark);
        }
    }
}
