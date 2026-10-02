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

package org.apache.flink.connector.base.source.hybrid;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.api.connector.source.mocks.MockSourceSplit;
import org.apache.flink.connector.base.source.reader.mocks.MockBaseSource;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Reproduces FLINK-39586: after a {@link HybridSource} switches from a bounded to an unbounded
 * source, a subtask that received no splits from the unbounded source neither advances its
 * watermark nor signals idleness, so the last bounded-era watermark of that subtask permanently
 * caps the downstream (combined) watermark.
 *
 * <p>The per-split watermark outputs of the bounded phase set {@code CombinedWatermarkStatus.idle =
 * false}. When the bounded splits finish, {@code SourceReaderBase.releaseOutputForSplit}
 * unregisters all per-split outputs, but {@code CombinedWatermarkStatus.updateCombinedWatermark()}
 * short-circuits on the empty output set without updating the idle flag. A subtask that gets no
 * splits in the unbounded phase is then stuck: not idle, never advancing.
 */
class HybridSourceWatermarkIdlenessITCase {

    private static final int PARALLELISM = 2;

    private static final int BOUNDED_START = 100;
    private static final int BOUNDED_RECORDS_PER_SPLIT = 10;
    private static final int UNBOUNDED_START = 1_000_000;
    private static final int UNBOUNDED_RECORDS = 10;
    private static final int TOTAL_RECORDS =
            PARALLELISM * BOUNDED_RECORDS_PER_SPLIT + UNBOUNDED_RECORDS;
    private static final int UNBOUNDED_SPLIT_ID = 100;

    private static final AtomicInteger RECORD_COUNT = new AtomicInteger();
    private static final AtomicLong MAX_SEEN_WATERMARK = new AtomicLong(Long.MIN_VALUE);

    @RegisterExtension
    private static final MiniClusterExtension miniClusterResource =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(PARALLELISM)
                            .build());

    @BeforeEach
    void resetObservations() {
        RECORD_COUNT.set(0);
        MAX_SEEN_WATERMARK.set(Long.MIN_VALUE);
    }

    @Test
    void testWatermarkAdvancesAfterSwitchLeavesSubtaskWithoutSplits() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);
        env.getConfig().setAutoWatermarkInterval(50);

        // Bounded phase: one split per subtask (MockSplitEnumerator assigns split i to subtask
        // i % parallelism), so every subtask emits per-split watermarks before its split
        // finishes. Unbounded phase: a single split assigned to subtask 0 only; subtask 1 keeps
        // running with no splits, like a Kafka reader that owns no partition.
        HybridSource<Integer> source =
                HybridSource.builder(
                                new MockBaseSource(
                                        PARALLELISM,
                                        BOUNDED_RECORDS_PER_SPLIT,
                                        BOUNDED_START,
                                        Boundedness.BOUNDED))
                        .addSource(new SingleSplitUnboundedSource())
                        .build();

        env.fromSource(source, perEventWatermarks(), "hybrid-source", Types.INT)
                .sinkTo(new WatermarkCapturingSink())
                .setParallelism(1);

        JobClient jobClient = env.executeAsync("HybridSource watermark after switch");
        try {
            // Sanity: records of both phases arrive, so the source switch completed and the
            // unbounded split is emitting. Record flow is unaffected by the watermark stall.
            CommonTestUtils.waitUtil(
                    () -> RECORD_COUNT.get() >= TOTAL_RECORDS,
                    Duration.ofSeconds(60),
                    "Not all records arrived; the HybridSource switch did not complete.");

            // FLINK-39586: the combined downstream watermark must eventually follow the subtask
            // that reads the unbounded split. It can only do so if the subtask that lost all its
            // splits either advances its watermark or signals idleness.
            CommonTestUtils.waitUtil(
                    () -> MAX_SEEN_WATERMARK.get() >= UNBOUNDED_START,
                    Duration.ofSeconds(30),
                    "Downstream watermark stalled at a bounded-era value: the subtask that lost"
                            + " all its splits at the source switch neither advanced its watermark"
                            + " nor went idle (FLINK-39586).");
        } finally {
            jobClient.cancel().get();
        }
    }

    /**
     * Emits a watermark for every event so that the per-split outputs are guaranteed to have
     * emitted a watermark (setting the combined status non-idle) before the splits finish,
     * independent of the periodic emit timing.
     */
    private static WatermarkStrategy<Integer> perEventWatermarks() {
        return WatermarkStrategy.<Integer>forGenerator(ctx -> new PerEventWatermarkGenerator())
                .withTimestampAssigner((value, ts) -> value);
    }

    private static final class PerEventWatermarkGenerator implements WatermarkGenerator<Integer> {
        @Override
        public void onEvent(Integer event, long eventTimestamp, WatermarkOutput output) {
            output.emitWatermark(new Watermark(eventTimestamp));
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {}
    }

    /**
     * An unbounded source with a single split that is assigned to subtask 0 and whose enumerator
     * never signals "no more splits" — like an external system where new splits may still be
     * discovered. Subtask 1 therefore never reaches END_OF_INPUT and never emits a final
     * MAX_WATERMARK.
     */
    private static final class SingleSplitUnboundedSource extends MockBaseSource {
        private static final long serialVersionUID = 1L;

        SingleSplitUnboundedSource() {
            super(1, UNBOUNDED_RECORDS, UNBOUNDED_START, Boundedness.CONTINUOUS_UNBOUNDED);
        }

        @Override
        public SplitEnumerator<MockSourceSplit, List<MockSourceSplit>> createEnumerator(
                SplitEnumeratorContext<MockSourceSplit> enumContext) {
            MockSourceSplit split = new MockSourceSplit(UNBOUNDED_SPLIT_ID, 0, Integer.MAX_VALUE);
            for (int i = 0; i < UNBOUNDED_RECORDS; i++) {
                split.addRecord(UNBOUNDED_START + i);
            }
            List<MockSourceSplit> splits = new ArrayList<>(Collections.singletonList(split));
            return new SingleReaderAssigningEnumerator(splits, enumContext);
        }
    }

    private static final class SingleReaderAssigningEnumerator
            implements SplitEnumerator<MockSourceSplit, List<MockSourceSplit>> {
        private final List<MockSourceSplit> unassignedSplits;
        private final SplitEnumeratorContext<MockSourceSplit> context;

        SingleReaderAssigningEnumerator(
                List<MockSourceSplit> splits, SplitEnumeratorContext<MockSourceSplit> context) {
            this.unassignedSplits = splits;
            this.context = context;
        }

        @Override
        public void addReader(int subtaskId) {
            if (subtaskId == 0 && !unassignedSplits.isEmpty()) {
                context.assignSplits(
                        new SplitsAssignment<>(
                                Collections.singletonMap(0, new ArrayList<>(unassignedSplits))));
                unassignedSplits.clear();
            }
            // Intentionally never calls context.signalNoMoreSplits(...).
        }

        @Override
        public void addSplitsBack(List<MockSourceSplit> splits, int subtaskId) {
            unassignedSplits.addAll(splits);
        }

        @Override
        public List<MockSourceSplit> snapshotState(long checkpointId) {
            return unassignedSplits;
        }

        @Override
        public void start() {}

        @Override
        public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {}

        @Override
        public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {}

        @Override
        public void close() {}
    }

    /** Captures the combined watermark of all source subtasks at parallelism 1. */
    private static final class WatermarkCapturingSink implements Sink<Integer> {
        private static final long serialVersionUID = 1L;

        @Override
        public SinkWriter<Integer> createWriter(WriterInitContext context) {
            return new SinkWriter<Integer>() {
                @Override
                public void write(Integer element, Context context) {
                    RECORD_COUNT.incrementAndGet();
                }

                @Override
                public void writeWatermark(Watermark watermark) {
                    MAX_SEEN_WATERMARK.accumulateAndGet(watermark.getTimestamp(), Math::max);
                }

                @Override
                public void flush(boolean endOfInput) {}

                @Override
                public void close() {}
            };
        }
    }
}
