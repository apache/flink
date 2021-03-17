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

package org.apache.flink.streaming.runtime.operators.sink;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.util.SimpleVersionedListState;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TestHarnessUtil;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;

/** Tests for {@link StatefulSinkWriterOperator}. */
public class StatefulSinkWriterOperatorTest extends SinkWriterOperatorTestBase {

    @Override
    protected AbstractSinkWriterOperatorFactory createWriterOperator(TestSink sink) {
        return new StatefulSinkWriterOperatorFactory<>(sink);
    }

    @Test
    public void stateIsRestored() throws Exception {
        final long initialTime = 0;

        final OneInputStreamOperatorTestHarness<Integer, String> testHarness =
                createTestHarness(
                        TestSink.newBuilder()
                                .setWriter(new SnapshottingBufferingSinkWriter())
                                .withWriterState()
                                .build());

        testHarness.open();

        testHarness.processWatermark(initialTime);
        testHarness.processElement(1, initialTime + 1);
        testHarness.processElement(2, initialTime + 2);

        testHarness.prepareSnapshotPreBarrier(1L);
        OperatorSubtaskState snapshot = testHarness.snapshot(1L, 1L);

        // we only see the watermark, so the committables must be stored in state
        assertThat(testHarness.getOutput(), contains(new Watermark(initialTime)));

        testHarness.close();

        final OneInputStreamOperatorTestHarness<Integer, String> restoredTestHarness =
                createTestHarness(
                        TestSink.newBuilder()
                                .setWriter(new SnapshottingBufferingSinkWriter())
                                .withWriterState()
                                .build());

        restoredTestHarness.initializeState(snapshot);
        restoredTestHarness.open();

        // this will flush out the committables that were restored
        restoredTestHarness.endInput();

        assertThat(
                restoredTestHarness.getOutput(),
                contains(
                        new StreamRecord<>(Tuple3.of(1, initialTime + 1, initialTime).toString()),
                        new StreamRecord<>(Tuple3.of(2, initialTime + 2, initialTime).toString())));
    }

    @Test
    public void loadPreviousSinkState() throws Exception {
        // 1. Build previous sink state
        final List<String> previousSinkInputs =
                Arrays.asList(
                        "bit", "mention", "thick", "stick", "stir", "easy", "sleep", "forth",
                        "cost", "prompt");

        final OneInputStreamOperatorTestHarness<String, String> previousSink =
                new OneInputStreamOperatorTestHarness<>(
                        new DummySinkOperator(), StringSerializer.INSTANCE);

        OperatorSubtaskState previousSinkState =
                TestHarnessUtil.buildSubtaskState(previousSink, previousSinkInputs);

        // 2. Load previous sink state and verify the output
        final OneInputStreamOperatorTestHarness<Integer, String> compatibleWriterOperator =
                createCompatibleSinkOperator();

        final List<StreamRecord<String>> expectedOutput1 =
                previousSinkInputs.stream().map(StreamRecord::new).collect(Collectors.toList());
        expectedOutput1.add(new StreamRecord<>(Tuple3.of(1, 1, Long.MIN_VALUE).toString()));

        // load the state from previous sink
        compatibleWriterOperator.initializeState(previousSinkState);

        compatibleWriterOperator.open();

        compatibleWriterOperator.processElement(1, 1);

        // this will flush out the committables that were restored from previous sink
        compatibleWriterOperator.endInput();

        OperatorSubtaskState operatorStateWithoutPreviousState =
                compatibleWriterOperator.snapshot(1L, 1L);

        compatibleWriterOperator.close();

        assertThat(
                compatibleWriterOperator.getOutput(),
                containsInAnyOrder(expectedOutput1.toArray()));

        // 3. Restore the sink without previous sink's state
        final OneInputStreamOperatorTestHarness<Integer, String> restoredSinkOperator =
                createCompatibleSinkOperator();
        final List<StreamRecord<String>> expectedOutput2 =
                Arrays.asList(
                        new StreamRecord<>(Tuple3.of(2, 2, Long.MIN_VALUE).toString()),
                        new StreamRecord<>(Tuple3.of(3, 3, Long.MIN_VALUE).toString()));

        restoredSinkOperator.initializeState(operatorStateWithoutPreviousState);

        restoredSinkOperator.open();

        restoredSinkOperator.processElement(2, 2);
        restoredSinkOperator.processElement(3, 3);

        // this will flush out the committables that were restored
        restoredSinkOperator.endInput();

        assertThat(restoredSinkOperator.getOutput(), containsInAnyOrder(expectedOutput2.toArray()));
    }

    /** A {@link SinkWriter} buffers elements and snapshots them when asked. */
    static class SnapshottingBufferingSinkWriter extends BufferingSinkWriter {

        @Override
        public List<String> snapshotState() {
            return elements;
        }

        @Override
        void restoredFrom(List<String> states) {
            this.elements = states;
        }
    }

    static class DummySinkOperator extends AbstractStreamOperator<String>
            implements OneInputStreamOperator<String, String> {

        static final String DUMMY_SINK_STATE_NAME = "dummy_sink_state";

        static final ListStateDescriptor<byte[]> SINK_STATE_DESC =
                new ListStateDescriptor<>(
                        DUMMY_SINK_STATE_NAME, BytePrimitiveArraySerializer.INSTANCE);
        ListState<String> sinkState;

        public void initializeState(StateInitializationContext context) throws Exception {
            super.initializeState(context);
            sinkState =
                    new SimpleVersionedListState<>(
                            context.getOperatorStateStore().getListState(SINK_STATE_DESC),
                            TestSink.StringCommittableSerializer.INSTANCE);
        }

        @Override
        public void processElement(StreamRecord<String> element) throws Exception {
            sinkState.add(element.getValue());
        }
    }

    private OneInputStreamOperatorTestHarness<Integer, String> createCompatibleSinkOperator()
            throws Exception {
        return new OneInputStreamOperatorTestHarness<>(
                new StatefulSinkWriterOperatorFactory<>(
                        TestSink.newBuilder()
                                .setWriter(new SnapshottingBufferingSinkWriter())
                                .withWriterState()
                                .build(),
                        DummySinkOperator.DUMMY_SINK_STATE_NAME),
                IntSerializer.INSTANCE);
    }
}
