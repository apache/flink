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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.mocks.MockSource;
import org.apache.flink.api.connector.source.mocks.MockSourceSplit;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.runtime.io.network.api.EndOfData;
import org.apache.flink.runtime.io.network.api.StopMode;
import org.apache.flink.streaming.api.operators.SourceOperatorFactory;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for FLINK-40499: a {@link SourceOperatorStreamTask} whose source went IDLE before finishing
 * must still emit {@link Watermark#MAX_WATERMARK} when the task is drained, so that downstream
 * event-time timers and windows fire at end of job.
 *
 * <p>This is the task-level counterpart of {@code ChainingOutputIdleMaxWatermarkTest}: {@code
 * RecordWriterOutput#emitWatermark} drops watermarks while the announced status is IDLE, so {@code
 * SourceOperatorStreamTask#advanceToEndOfEventTime()} must emit {@link WatermarkStatus#ACTIVE}
 * before the MAX watermark.
 */
class SourceOperatorStreamTaskIdleDrainTest {

    @Test
    void maxWatermarkIsEmittedAtDrainEvenIfSourceWentIdle() throws Exception {
        SourceOperatorFactory<Integer> sourceOperatorFactory =
                new SourceOperatorFactory<>(
                        new IdleBeforeFinishingSource(), WatermarkStrategy.noWatermarks());

        try (StreamTaskMailboxTestHarness<Integer> testHarness =
                new StreamTaskMailboxTestHarnessBuilder<>(
                                SourceOperatorStreamTask::new, BasicTypeInfo.INT_TYPE_INFO)
                        .setCollectNetworkEvents()
                        .setupOutputForSingletonOperatorChain(sourceOperatorFactory)
                        .build()) {

            testHarness.processAll();
            testHarness.finishProcessing();

            // sanity check: the source really announced idleness before finishing
            assertThat(testHarness.getOutput())
                    .as("Precondition: the source should have announced IDLE before finishing")
                    .contains(WatermarkStatus.IDLE);

            // the drain path must re-activate the (idle) output before the final MAX_WATERMARK,
            // otherwise the watermark is dropped by the idle gate in the output
            assertThat(testHarness.getOutput())
                    .as(
                            "At drain, WatermarkStatus.ACTIVE followed by MAX_WATERMARK must be "
                                    + "emitted before EndOfData even if the source's last "
                                    + "announced status was IDLE")
                    .containsExactly(
                            WatermarkStatus.IDLE,
                            WatermarkStatus.ACTIVE,
                            Watermark.MAX_WATERMARK,
                            new EndOfData(StopMode.DRAIN));
        }
    }

    /** A bounded source whose reader marks itself idle and then immediately finishes. */
    private static class IdleBeforeFinishingSource extends MockSource {
        private static final long serialVersionUID = 1L;

        IdleBeforeFinishingSource() {
            super(Boundedness.BOUNDED, 1);
        }

        @Override
        public SourceReader<Integer, MockSourceSplit> createReader(
                SourceReaderContext readerContext) {
            return new IdleThenFinishSourceReader();
        }
    }

    /**
     * A reader that marks the output IDLE (e.g. like a reader with no assigned work, or an idleness
     * timeout would) and then reaches end of input.
     */
    private static class IdleThenFinishSourceReader
            implements SourceReader<Integer, MockSourceSplit> {

        @Override
        public InputStatus pollNext(ReaderOutput<Integer> output) {
            output.markIdle();
            return InputStatus.END_OF_INPUT;
        }

        @Override
        public void start() {}

        @Override
        public List<MockSourceSplit> snapshotState(long checkpointId) {
            return Collections.emptyList();
        }

        @Override
        public CompletableFuture<Void> isAvailable() {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void addSplits(List<MockSourceSplit> splits) {}

        @Override
        public void notifyNoMoreSplits() {}

        @Override
        public void close() {}
    }
}
