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

package org.apache.flink.test.checkpointing;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.hybrid.HybridSource;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.test.util.NumberSequenceSourceWithWaitForCheckpoint;
import org.apache.flink.util.CloseableIterator;

import org.junit.After;
import org.junit.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL;
import static org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL_DURING_BACKLOG;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * A test suite that verifies the correctness of the configuration {@link
 * org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions#CHECKPOINTING_INTERVAL_DURING_BACKLOG}.
 */
public class CheckpointIntervalDuringBacklogITCase {
    private static final int NUM_SPLITS = 2;
    private static final int NUM_RECORDS = 40;
    private static final List<Long> EXPECTED_RESULT =
            LongStream.rangeClosed(0, NUM_RECORDS - 1).boxed().collect(Collectors.toList());

    @After
    public void tearDown() {
        CheckpointRecordingOperator.reset();
    }

    @Test
    public void testCheckpoint() throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(CHECKPOINTING_INTERVAL, Duration.ofMillis(100));
        configuration.set(CHECKPOINTING_INTERVAL_DURING_BACKLOG, Duration.ofMillis(200));
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(1);

        Source<Long, ?, ?> source =
                HybridSource.builder(
                                new NumberSequenceSourceWithWaitForCheckpoint(
                                        0, NUM_RECORDS / 2 - 1, NUM_SPLITS))
                        .addSource(
                                new NumberSequenceSourceWithWaitForCheckpoint(
                                        NUM_RECORDS / 2, NUM_RECORDS - 1, NUM_SPLITS))
                        .build();

        runAndVerifyResult(env, source);

        assertThat(CheckpointRecordingOperator.numCheckpointsBeforeSwitchSource.get())
                .isGreaterThan(0);
        assertThat(CheckpointRecordingOperator.numCheckpointsAfterSwitchSource.get())
                .isGreaterThan(0);
    }

    @Test
    public void testDefaultCheckpointIntervalDuringBacklog() throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(CHECKPOINTING_INTERVAL, Duration.ofMillis(100));
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(1);

        Source<Long, ?, ?> source =
                HybridSource.builder(
                                new NumberSequenceSourceWithWaitForCheckpoint(
                                        0, NUM_RECORDS / 2 - 1, NUM_SPLITS))
                        .addSource(new NumberSequenceSource(NUM_RECORDS / 2, NUM_RECORDS - 1))
                        .build();

        runAndVerifyResult(env, source);

        assertThat(CheckpointRecordingOperator.numCheckpointsBeforeSwitchSource.get())
                .isGreaterThan(0);
        assertThat(CheckpointRecordingOperator.numCheckpointsAfterSwitchSource.get())
                .isGreaterThan(0);
    }

    @Test
    public void testNoCheckpointDuringBacklog() throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(CHECKPOINTING_INTERVAL, Duration.ofMillis(100));
        configuration.set(CHECKPOINTING_INTERVAL_DURING_BACKLOG, Duration.ofMillis(0));
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(1);

        Source<Long, ?, ?> source =
                HybridSource.builder(new NumberSequenceSource(0, NUM_RECORDS / 2 - 1))
                        .addSource(new NumberSequenceSource(NUM_RECORDS / 2, NUM_RECORDS - 1))
                        .build();

        runAndVerifyResult(env, source);

        assertThat(CheckpointRecordingOperator.numCheckpointsBeforeSwitchSource.get()).isEqualTo(0);
        assertThat(CheckpointRecordingOperator.numCheckpointsAfterSwitchSource.get())
                .isGreaterThan(0);
    }

    @Test
    public void testExcludeFinishedOperatorBacklogStatus() throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(CHECKPOINTING_INTERVAL, Duration.ofMillis(100));
        configuration.set(CHECKPOINTING_INTERVAL_DURING_BACKLOG, Duration.ofMillis(0));
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(1);

        DataStream<Long> source2 =
                env.fromSource(
                                new SourceWithBacklogReport<>(
                                        new NumberSequenceSource(0L, 1L), true),
                                WatermarkStrategy.noWatermarks(),
                                "backlog-source")
                        .returns(Long.class);

        DataStream<Long> source =
                env.fromSource(
                        new NumberSequenceSourceWithWaitForCheckpoint(
                                2, NUM_RECORDS - 1, NUM_SPLITS),
                        WatermarkStrategy.noWatermarks(),
                        "non-backlog-source");

        final DataStream<Long> stream =
                source.union(source2)
                        .transform(
                                "CheckpointRecordingOperator",
                                Types.LONG,
                                new CheckpointRecordingOperator<>());

        final List<Long> result = new ArrayList<>();
        try (CloseableIterator<Long> iterator = stream.executeAndCollect()) {
            while (iterator.hasNext()) {
                result.add(iterator.next());
            }
        }

        Collections.sort(result);
        assertThat(result).containsExactly(EXPECTED_RESULT.toArray(new Long[0]));
    }

    private void runAndVerifyResult(StreamExecutionEnvironment env, Source<Long, ?, ?> source)
            throws Exception {
        final DataStream<Long> stream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "hybrid-source")
                        .returns(Long.class)
                        .transform(
                                "CheckpointRecordingOperator",
                                Types.LONG,
                                new CheckpointRecordingOperator<>());

        final List<Long> result = new ArrayList<>();
        try (CloseableIterator<Long> iterator = stream.executeAndCollect()) {
            while (iterator.hasNext()) {
                result.add(iterator.next());
            }
        }

        Collections.sort(result);
        assertThat(result).containsExactly(EXPECTED_RESULT.toArray(new Long[0]));
    }

    /**
     * A {@link Source} decorator that reports the configured backlog status of the source during
     * start.
     */
    private static class SourceWithBacklogReport<T, SplitT extends SourceSplit, EnumChkT>
            implements Source<T, SplitT, EnumChkT> {
        private final Source<T, SplitT, EnumChkT> source;
        private final boolean isBacklog;

        private SourceWithBacklogReport(Source<T, SplitT, EnumChkT> source, boolean isBacklog) {
            this.source = source;
            this.isBacklog = isBacklog;
        }

        @Override
        public Boundedness getBoundedness() {
            return source.getBoundedness();
        }

        @Override
        public SplitEnumerator<SplitT, EnumChkT> createEnumerator(
                SplitEnumeratorContext<SplitT> enumContext) throws Exception {
            SplitEnumerator<SplitT, EnumChkT> enumerator = source.createEnumerator(enumContext);
            return new EnumeratorWithBacklogReport<>(enumerator, enumContext, isBacklog);
        }

        @Override
        public SplitEnumerator<SplitT, EnumChkT> restoreEnumerator(
                SplitEnumeratorContext<SplitT> enumContext, EnumChkT checkpoint) throws Exception {
            SplitEnumerator<SplitT, EnumChkT> enumerator =
                    source.restoreEnumerator(enumContext, checkpoint);
            return new EnumeratorWithBacklogReport<>(enumerator, enumContext, isBacklog);
        }

        @Override
        public SimpleVersionedSerializer<SplitT> getSplitSerializer() {
            return source.getSplitSerializer();
        }

        @Override
        public SimpleVersionedSerializer<EnumChkT> getEnumeratorCheckpointSerializer() {
            return source.getEnumeratorCheckpointSerializer();
        }

        @Override
        public SourceReader<T, SplitT> createReader(SourceReaderContext readerContext)
                throws Exception {
            return source.createReader(readerContext);
        }
    }

    /**
     * A {@link SplitEnumerator} decorator that reports the configured backlog status of the source
     * during start.
     */
    private static class EnumeratorWithBacklogReport<SplitT extends SourceSplit, CheckpointT>
            implements SplitEnumerator<SplitT, CheckpointT> {
        private final SplitEnumerator<SplitT, CheckpointT> enumerator;
        private final SplitEnumeratorContext<SplitT> context;

        private final boolean isBacklog;

        private EnumeratorWithBacklogReport(
                SplitEnumerator<SplitT, CheckpointT> enumerator,
                SplitEnumeratorContext<SplitT> context,
                boolean isBacklog) {
            this.enumerator = enumerator;
            this.context = context;
            this.isBacklog = isBacklog;
        }

        @Override
        public void start() {
            this.enumerator.start();
            this.context.setIsProcessingBacklog(isBacklog);
        }

        @Override
        public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
            this.enumerator.handleSplitRequest(subtaskId, requesterHostname);
        }

        @Override
        public void addSplitsBack(List<SplitT> splits, int subtaskId) {
            this.enumerator.addSplitsBack(splits, subtaskId);
        }

        @Override
        public void addReader(int subtaskId) {
            this.enumerator.addReader(subtaskId);
        }

        @Override
        public CheckpointT snapshotState(long checkpointId) throws Exception {
            return this.enumerator.snapshotState(checkpointId);
        }

        @Override
        public void close() throws IOException {
            this.enumerator.close();
        }
    }

    private static class CheckpointRecordingOperator<T> extends AbstractStreamOperator<T>
            implements OneInputStreamOperator<T, T> {
        private static final AtomicInteger numCheckpointsBeforeSwitchSource = new AtomicInteger(0);
        private static final AtomicInteger numCheckpointsAfterSwitchSource = new AtomicInteger(0);

        private int numRecords;

        private CheckpointRecordingOperator() {
            numRecords = 0;
        }

        private static void reset() {
            numCheckpointsBeforeSwitchSource.set(0);
            numCheckpointsAfterSwitchSource.set(0);
        }

        @Override
        public void processElement(StreamRecord<T> element) {
            numRecords++;
            output.collect(element);
        }

        @Override
        public void snapshotState(StateSnapshotContext context) {
            if (numRecords < NUM_RECORDS / 2) {
                numCheckpointsBeforeSwitchSource.incrementAndGet();
            } else {
                numCheckpointsAfterSwitchSource.incrementAndGet();
            }
        }
    }
}
