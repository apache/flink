/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.streaming.api.datastream;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SupportsBatchSnapshot;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource.NumberSequenceSplit;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.util.FlinkRuntimeException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.assertj.core.api.Assertions.assertThat;

/** This ITCase class tests the behavior of task execution with watermark alignment. */
class WatermarkAlignmentITCase {

    /**
     * Test method to verify whether the watermark alignment works well with finished task.
     *
     * @throws Exception if any error occurs during the execution.
     */
    @Test
    void testTaskFinishedWithWatermarkAlignmentExecution() throws Exception {
        // Set up the execution environment with parallelism of 2
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // Create a stream from a custom source with watermark strategy
        DataStream<Long> stream =
                env.fromSource(
                                new EagerlyFinishingNumberSequenceSource(0, 100),
                                WatermarkStrategy.<Long>forMonotonousTimestamps()
                                        .withTimestampAssigner(
                                                (SerializableTimestampAssigner<Long>)
                                                        (aLong, l) -> aLong)
                                        .withWatermarkAlignment(
                                                "g1", Duration.ofMillis(10), Duration.ofMillis(1)),
                                "Sequence Source")
                        .filter(
                                (FilterFunction<Long>)
                                        aLong -> {
                                            Thread.sleep(10);
                                            return true;
                                        });

        // Execute the stream and collect the results
        List<Long> result = stream.executeAndCollect(101);
        Collections.sort(result);

        // Assert that the collected result contains all numbers from 0 to 100
        Assertions.assertIterableEquals(
                result, LongStream.rangeClosed(0, 100).boxed().collect(Collectors.toList()));
    }

    static class EagerlyFinishingNumberSequenceSource extends NumberSequenceSource {
        public EagerlyFinishingNumberSequenceSource(long from, long to) {
            super(from, to);
        }

        @Override
        public SplitEnumerator<NumberSequenceSplit, Collection<NumberSequenceSplit>>
                createEnumerator(SplitEnumeratorContext<NumberSequenceSplit> enumContext) {

            List<NumberSequenceSplit> splits =
                    splitNumberRange(getFrom(), getTo(), enumContext.currentParallelism());
            return new EagerlyFinishingIteratorSourceEnumerator(enumContext, splits);
        }
    }

    /**
     * Contrary to the {@link
     * org.apache.flink.api.connector.source.lib.util.IteratorSourceEnumerator}, this enumerator
     * signals no more available splits as soon as possible.
     */
    static class EagerlyFinishingIteratorSourceEnumerator
            implements SplitEnumerator<NumberSequenceSplit, Collection<NumberSequenceSplit>>,
                    SupportsBatchSnapshot {

        private final SplitEnumeratorContext<NumberSequenceSplit> context;
        private final Queue<NumberSequenceSplit> remainingSplits;

        public EagerlyFinishingIteratorSourceEnumerator(
                SplitEnumeratorContext<NumberSequenceSplit> context,
                Collection<NumberSequenceSplit> splits) {
            this.context = checkNotNull(context);
            this.remainingSplits = new ArrayDeque<>(splits);
            this.context
                    .metricGroup()
                    .setUnassignedSplitsGauge(() -> (long) remainingSplits.size());
        }

        @Override
        public void start() {}

        @Override
        public void close() {}

        @Override
        public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
            NumberSequenceSplit nextSplit = remainingSplits.poll();
            if (nextSplit != null) {
                context.assignSplit(nextSplit, subtaskId);
            }
            if (remainingSplits.size() == 0) {
                for (int i = 0; i < context.currentParallelism(); i++) {
                    context.signalNoMoreSplits(i);
                }
            }
        }

        @Override
        public void addSplitsBack(List<NumberSequenceSplit> splits, int subtaskId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Collection<NumberSequenceSplit> snapshotState(long checkpointId) throws Exception {
            return remainingSplits;
        }

        @Override
        public void addReader(int subtaskId) {}
    }

    // Tasks of a cancelled job can outlive cancel(), so the shared state is tied to one run.
    private static final AtomicInteger CURRENT_RUN = new AtomicInteger();
    private static final AtomicBoolean FAILURE_TRIGGERED = new AtomicBoolean();
    private static final AtomicLong MAX_TIMESTAMP_OF_PACED_SOURCE = new AtomicLong();

    /**
     * A source in an alignment group fails after its last record and stays silent after the
     * restore, so it never reports a watermark again. The other source of the group must not stay
     * held back by the failed attempt's watermark.
     */
    @ParameterizedTest(name = "globalFailover = {0}")
    @ValueSource(booleans = {false, true})
    void testSilentSourceAfterFailoverDoesNotHoldBackItsGroup(boolean globalFailover)
            throws Exception {
        final int run = CURRENT_RUN.incrementAndGet();
        FAILURE_TRIGGERED.set(false);
        MAX_TIMESTAMP_OF_PACED_SOURCE.set(Long.MIN_VALUE);

        Configuration configuration = new Configuration();
        // DataGeneratorSource does not pause single splits
        configuration.set(PipelineOptions.ALLOW_UNALIGNED_SOURCE_SPLITS, true);
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 3);
        configuration.set(
                RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofMillis(10));
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(1);
        env.enableCheckpointing(50);
        env.getConfig().setAutoWatermarkInterval(10);

        // two unconnected pipelines, so a task failure restarts only the silent source's region
        env.fromSource(
                        new SilentAfterRestoreSource(100, globalFailover, run),
                        WatermarkStrategy.<Long>forMonotonousTimestamps()
                                .withTimestampAssigner((value, ts) -> value)
                                .withIdleness(Duration.ofSeconds(3))
                                .withWatermarkAlignment(
                                        "group", Duration.ofMillis(1000), Duration.ofMillis(10)),
                        "Silent Source")
                .sinkTo(new DiscardingSink<>());
        env.fromSource(
                        new DataGeneratorSource<>(
                                index -> index * 10,
                                Long.MAX_VALUE,
                                RateLimiterStrategy.perSecond(1000),
                                Types.LONG),
                        WatermarkStrategy.<Long>forMonotonousTimestamps()
                                .withTimestampAssigner((value, ts) -> value)
                                .withWatermarkAlignment(
                                        "group", Duration.ofMillis(1000), Duration.ofMillis(10)),
                        "Paced Source")
                .map(new RecordMaxTimestamp(run))
                .sinkTo(new DiscardingSink<>());

        JobClient jobClient = env.executeAsync();
        try {
            // Held back by the silent source's last watermark (990), the paced source stays
            // around 990 + 1000 plus a few update intervals of records.
            CommonTestUtils.waitUntilCondition(
                    () -> FAILURE_TRIGGERED.get() && MAX_TIMESTAMP_OF_PACED_SOURCE.get() > 20_000,
                    100,
                    600);
        } finally {
            jobClient.cancel().get();
        }
        assertThat(FAILURE_TRIGGERED).isTrue();
    }

    private static class RecordMaxTimestamp implements MapFunction<Long, Long> {
        private final int run;

        RecordMaxTimestamp(int run) {
            this.run = run;
        }

        @Override
        public Long map(Long value) {
            if (run == CURRENT_RUN.get()) {
                MAX_TIMESTAMP_OF_PACED_SOURCE.accumulateAndGet(value, Math::max);
            }
            return value;
        }
    }

    /**
     * Emits timestamps 0, 10, ... for {@code numRecords} records and then nothing, without
     * finishing. Once a checkpoint that covers all records completes and the paced source is ahead
     * of it, it fails once: by throwing in the reader (region failover) or in the enumerator
     * (global failover). Restored from that checkpoint it emits no record and so no watermark.
     */
    private static class SilentAfterRestoreSource implements Source<Long, OffsetSplit, Integer> {
        private final int numRecords;
        private final boolean failGlobally;
        private final int run;

        SilentAfterRestoreSource(int numRecords, boolean failGlobally, int run) {
            this.numRecords = numRecords;
            this.failGlobally = failGlobally;
            this.run = run;
        }

        @Override
        public Boundedness getBoundedness() {
            return Boundedness.CONTINUOUS_UNBOUNDED;
        }

        @Override
        public SourceReader<Long, OffsetSplit> createReader(SourceReaderContext readerContext) {
            return new SilentAfterRestoreReader(readerContext, numRecords, failGlobally, run);
        }

        @Override
        public SplitEnumerator<OffsetSplit, Integer> createEnumerator(
                SplitEnumeratorContext<OffsetSplit> enumContext) {
            return new OneSplitPerRequestEnumerator(enumContext);
        }

        @Override
        public SplitEnumerator<OffsetSplit, Integer> restoreEnumerator(
                SplitEnumeratorContext<OffsetSplit> enumContext, Integer checkpoint) {
            return new OneSplitPerRequestEnumerator(enumContext);
        }

        @Override
        public SimpleVersionedSerializer<OffsetSplit> getSplitSerializer() {
            return new OffsetSplitSerializer();
        }

        @Override
        public SimpleVersionedSerializer<Integer> getEnumeratorCheckpointSerializer() {
            return new IntSerializer();
        }
    }

    private static class SilentAfterRestoreReader implements SourceReader<Long, OffsetSplit> {
        private final SourceReaderContext context;
        private final int numRecords;
        private final boolean failGlobally;
        private final int run;
        private final Set<Long> checkpointsWithAllRecords = new HashSet<>();
        private OffsetSplit split;
        private boolean failNow;
        private CompletableFuture<Void> available = new CompletableFuture<>();

        SilentAfterRestoreReader(
                SourceReaderContext context, int numRecords, boolean failGlobally, int run) {
            this.context = context;
            this.numRecords = numRecords;
            this.failGlobally = failGlobally;
            this.run = run;
        }

        @Override
        public void start() {
            if (split == null) {
                context.sendSplitRequest();
            }
        }

        @Override
        public InputStatus pollNext(ReaderOutput<Long> output) {
            if (failNow) {
                failNow = false;
                if (!failGlobally) {
                    throw new FlinkRuntimeException("Artificial task failure");
                }
                context.sendSourceEventToCoordinator(new FailEnumeratorEvent());
            }
            if (split == null || split.offset >= numRecords) {
                if (available.isDone()) {
                    available = new CompletableFuture<>();
                }
                return InputStatus.NOTHING_AVAILABLE;
            }
            long timestamp = split.offset * 10;
            output.collect(timestamp, timestamp);
            split.offset++;
            return InputStatus.MORE_AVAILABLE;
        }

        @Override
        public List<OffsetSplit> snapshotState(long checkpointId) {
            if (split == null) {
                return Collections.emptyList();
            }
            if (split.offset >= numRecords) {
                checkpointsWithAllRecords.add(checkpointId);
            }
            return Collections.singletonList(new OffsetSplit(split.id, split.offset));
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            // fail once the paced source is ahead, so this source's watermark holds the group back
            if (run == CURRENT_RUN.get()
                    && checkpointsWithAllRecords.contains(checkpointId)
                    && MAX_TIMESTAMP_OF_PACED_SOURCE.get() > 1500
                    && FAILURE_TRIGGERED.compareAndSet(false, true)) {
                failNow = true;
                available.complete(null);
            }
        }

        @Override
        public CompletableFuture<Void> isAvailable() {
            return available;
        }

        @Override
        public void addSplits(List<OffsetSplit> splits) {
            split = splits.get(0);
            available.complete(null);
        }

        @Override
        public void notifyNoMoreSplits() {}

        @Override
        public void pauseOrResumeSplits(
                Collection<String> splitsToPause, Collection<String> splitsToResume) {}

        @Override
        public void close() {}
    }

    private static class OneSplitPerRequestEnumerator
            implements SplitEnumerator<OffsetSplit, Integer> {
        private final SplitEnumeratorContext<OffsetSplit> context;

        OneSplitPerRequestEnumerator(SplitEnumeratorContext<OffsetSplit> context) {
            this.context = context;
        }

        @Override
        public void start() {}

        @Override
        public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
            context.assignSplit(new OffsetSplit(subtaskId, 0), subtaskId);
        }

        @Override
        public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
            if (sourceEvent instanceof FailEnumeratorEvent) {
                throw new FlinkRuntimeException("Artificial enumerator failure");
            }
        }

        @Override
        public void addSplitsBack(List<OffsetSplit> splits, int subtaskId) {}

        @Override
        public void addReader(int subtaskId) {}

        @Override
        public Integer snapshotState(long checkpointId) {
            return 0;
        }

        @Override
        public void close() {}
    }

    private static class FailEnumeratorEvent implements SourceEvent {}

    private static class OffsetSplit implements SourceSplit {
        private final int id;
        private long offset;

        OffsetSplit(int id, long offset) {
            this.id = id;
            this.offset = offset;
        }

        @Override
        public String splitId() {
            return "split-" + id;
        }
    }

    private static class OffsetSplitSerializer implements SimpleVersionedSerializer<OffsetSplit> {
        @Override
        public int getVersion() {
            return 1;
        }

        @Override
        public byte[] serialize(OffsetSplit split) throws IOException {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream out = new DataOutputStream(bytes)) {
                out.writeInt(split.id);
                out.writeLong(split.offset);
            }
            return bytes.toByteArray();
        }

        @Override
        public OffsetSplit deserialize(int version, byte[] serialized) throws IOException {
            try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(serialized))) {
                return new OffsetSplit(in.readInt(), in.readLong());
            }
        }
    }

    private static class IntSerializer implements SimpleVersionedSerializer<Integer> {
        @Override
        public int getVersion() {
            return 1;
        }

        @Override
        public byte[] serialize(Integer value) {
            return new byte[] {value.byteValue()};
        }

        @Override
        public Integer deserialize(int version, byte[] serialized) {
            return (int) serialized[0];
        }
    }
}
