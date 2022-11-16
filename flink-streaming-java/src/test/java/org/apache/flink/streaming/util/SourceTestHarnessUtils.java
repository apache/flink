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

package org.apache.flink.streaming.util;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.streaming.api.operators.SourceOperator;
import org.apache.flink.streaming.api.operators.SourceOperatorFactory;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;

import java.util.ArrayList;
import java.util.List;

/**
 * Util class for performing tests with SourceOperator {@link AbstractStreamOperatorTestHarness}.
 */
public class SourceTestHarnessUtils {
    /**
     * Runs the specified bounded source, previously transferring the initial splits to it. After
     * emitting half of the values, the checkpoint behavior and operator recovery are simulated.
     *
     * @return record values emitted by source
     */
    public static <V, SplitT extends SourceSplit> List<V> testBoundedSourceWithHarness(
            Source<V, SplitT, ?> source, int elementsSize, List<SplitT> initialSplits)
            throws Exception {
        try (AbstractStreamOperatorTestHarness<V> testHarness = buildSourceHarness(source)) {
            testHarness.open();
            testHarness.setup();
            SourceOperator<V, SplitT> sourceOperator =
                    (SourceOperator<V, SplitT>) testHarness.getOperator();
            sourceOperator.getSourceReader().addSplits(initialSplits);

            RecordValueCollectingDataOutput<V> dataOutput = new RecordValueCollectingDataOutput<>();

            int elementsPerCycle = elementsSize / 2;
            for (int i = 0; i <= elementsPerCycle; ++i) {
                sourceOperator.emitNext(dataOutput);
            }
            OperatorSubtaskState state = testHarness.snapshot(1L, 1L);
            testHarness.close();

            AbstractStreamOperatorTestHarness<V> restoredHarness = buildSourceHarness(source);
            restoredHarness.initializeState(state);
            restoredHarness.open();
            restoredHarness.setup();
            sourceOperator = (SourceOperator<V, SplitT>) restoredHarness.getOperator();

            for (int i = elementsPerCycle; i <= elementsSize; ++i) {
                sourceOperator.emitNext(dataOutput);
            }
            return dataOutput.getRecordValues();
        }
    }

    private static <V, SplitT extends SourceSplit>
            AbstractStreamOperatorTestHarness<V> buildSourceHarness(Source<V, SplitT, ?> source)
                    throws Exception {
        return new AbstractStreamOperatorTestHarness<>(
                new SourceOperatorFactory<>(source, WatermarkStrategy.noWatermarks()),
                new MockEnvironmentBuilder().build());
    }

    /** DataOutput which saves all incoming record values in buffer. */
    public static final class RecordValueCollectingDataOutput<E>
            implements PushingAsyncDataInput.DataOutput<E> {

        final List<E> recordValues = new ArrayList<>();

        @Override
        public void emitWatermark(org.apache.flink.streaming.api.watermark.Watermark watermark)
                throws Exception {}

        @Override
        public void emitWatermarkStatus(WatermarkStatus watermarkStatus) throws Exception {}

        @Override
        public void emitRecord(StreamRecord<E> streamRecord) throws Exception {
            recordValues.add(streamRecord.getValue());
        }

        @Override
        public void emitLatencyMarker(LatencyMarker latencyMarker) throws Exception {}

        public List<E> getRecordValues() {
            return recordValues;
        }
    }
}
