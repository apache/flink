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

package org.apache.flink.streaming.connectors.kafka.internals;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;
import org.apache.flink.streaming.connectors.kafka.testutils.TestSourceContext;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.apache.flink.util.SerializedValue;

import org.junit.Test;

import javax.annotation.Nonnull;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Tests for the {@link AbstractFetcher}. */
@SuppressWarnings("serial")
public class AbstractFetcherTest {

    @Test
    public void testIgnorePartitionStateSentinelInSnapshot() throws Exception {
        final String testTopic = "test topic name";
        Map<KafkaTopicPartition, Long> originalPartitions = new HashMap<>();
        originalPartitions.put(
                new KafkaTopicPartition(testTopic, 1),
                KafkaTopicPartitionStateSentinel.LATEST_OFFSET);
        originalPartitions.put(
                new KafkaTopicPartition(testTopic, 2),
                KafkaTopicPartitionStateSentinel.GROUP_OFFSET);
        originalPartitions.put(
                new KafkaTopicPartition(testTopic, 3),
                KafkaTopicPartitionStateSentinel.EARLIEST_OFFSET);

        TestSourceContext<Long> sourceContext = new TestSourceContext<>();

        TestFetcher<Long> fetcher =
                new TestFetcher<>(
                        sourceContext,
                        originalPartitions,
                        null, /* watermark strategy */
                        new TestProcessingTimeService(),
                        0);

        synchronized (sourceContext.getCheckpointLock()) {
            HashMap<KafkaTopicPartition, Long> currentState = fetcher.snapshotCurrentState();
            fetcher.commitInternalOffsetsToKafka(
                    currentState,
                    new KafkaCommitCallback() {
                        @Override
                        public void onSuccess() {}

                        @Override
                        public void onException(Throwable cause) {
                            throw new RuntimeException("Callback failed", cause);
                        }
                    });

            assertTrue(fetcher.getLastCommittedOffsets().isPresent());
            assertEquals(Collections.emptyMap(), fetcher.getLastCommittedOffsets().get());
        }
    }

    // ------------------------------------------------------------------------
    //   Record emitting tests
    // ------------------------------------------------------------------------

    @Test
    public void testSkipCorruptedRecord() throws Exception {
        final String testTopic = "test topic name";
        Map<KafkaTopicPartition, Long> originalPartitions = new HashMap<>();
        originalPartitions.put(
                new KafkaTopicPartition(testTopic, 1),
                KafkaTopicPartitionStateSentinel.LATEST_OFFSET);

        TestSourceContext<Long> sourceContext = new TestSourceContext<>();

        TestFetcher<Long> fetcher =
                new TestFetcher<>(
                        sourceContext,
                        originalPartitions,
                        null, /* watermark strategy */
                        new TestProcessingTimeService(),
                        0);

        final KafkaTopicPartitionState<Long, Object> partitionStateHolder =
                fetcher.subscribedPartitionStates().get(0);

        emitRecord(fetcher, 1L, partitionStateHolder, 1L);
        emitRecord(fetcher, 2L, partitionStateHolder, 2L);
        assertEquals(2L, sourceContext.getLatestElement().getValue().longValue());
        assertEquals(2L, partitionStateHolder.getOffset());

        // emit no records
        fetcher.emitRecordsWithTimestamps(emptyQueue(), partitionStateHolder, 3L, Long.MIN_VALUE);
        assertEquals(
                2L,
                sourceContext
                        .getLatestElement()
                        .getValue()
                        .longValue()); // the null record should be skipped
        assertEquals(
                3L,
                partitionStateHolder.getOffset()); // the offset in state still should have advanced
    }

    @Test
    public void testConcurrentPartitionsDiscoveryAndLoopFetching() throws Exception {
        // test data
        final KafkaTopicPartition testPartition = new KafkaTopicPartition("test", 42);

        // ----- create the test fetcher -----

        SourceContext<String> sourceContext = new TestSourceContext<>();
        Map<KafkaTopicPartition, Long> partitionsWithInitialOffsets =
                Collections.singletonMap(
                        testPartition, KafkaTopicPartitionStateSentinel.GROUP_OFFSET);

        final OneShotLatch fetchLoopWaitLatch = new OneShotLatch();
        final OneShotLatch stateIterationBlockLatch = new OneShotLatch();

        final TestFetcher<String> fetcher =
                new TestFetcher<>(
                        sourceContext,
                        partitionsWithInitialOffsets,
                        null, /* watermark strategy */
                        new TestProcessingTimeService(),
                        10,
                        fetchLoopWaitLatch,
                        stateIterationBlockLatch);

        // ----- run the fetcher -----

        final CheckedThread checkedThread =
                new CheckedThread() {
                    @Override
                    public void go() throws Exception {
                        fetcher.runFetchLoop();
                    }
                };
        checkedThread.start();

        // wait until state iteration begins before adding discovered partitions
        fetchLoopWaitLatch.await();
        fetcher.addDiscoveredPartitions(Collections.singletonList(testPartition));

        stateIterationBlockLatch.trigger();
        checkedThread.sync();
    }

    // ------------------------------------------------------------------------
    //  Test mocks
    // ------------------------------------------------------------------------

    private static final class TestFetcher<T> extends AbstractFetcher<T, Object> {
        Map<KafkaTopicPartition, Long> lastCommittedOffsets = null;

        private final OneShotLatch fetchLoopWaitLatch;
        private final OneShotLatch stateIterationBlockLatch;

        TestFetcher(
                SourceContext<T> sourceContext,
                Map<KafkaTopicPartition, Long> assignedPartitionsWithStartOffsets,
                SerializedValue<WatermarkStrategy<T>> watermarkStrategy,
                ProcessingTimeService processingTimeProvider,
                long autoWatermarkInterval)
                throws Exception {

            this(
                    sourceContext,
                    assignedPartitionsWithStartOffsets,
                    watermarkStrategy,
                    processingTimeProvider,
                    autoWatermarkInterval,
                    null,
                    null);
        }

        TestFetcher(
                SourceContext<T> sourceContext,
                Map<KafkaTopicPartition, Long> assignedPartitionsWithStartOffsets,
                SerializedValue<WatermarkStrategy<T>> watermarkStrategy,
                ProcessingTimeService processingTimeProvider,
                long autoWatermarkInterval,
                OneShotLatch fetchLoopWaitLatch,
                OneShotLatch stateIterationBlockLatch)
                throws Exception {

            super(
                    sourceContext,
                    assignedPartitionsWithStartOffsets,
                    watermarkStrategy,
                    processingTimeProvider,
                    autoWatermarkInterval,
                    TestFetcher.class.getClassLoader(),
                    new UnregisteredMetricsGroup(),
                    false);

            this.fetchLoopWaitLatch = fetchLoopWaitLatch;
            this.stateIterationBlockLatch = stateIterationBlockLatch;
        }

        /**
         * Emulation of partition's iteration which is required for {@link
         * AbstractFetcherTest#testConcurrentPartitionsDiscoveryAndLoopFetching}.
         */
        @Override
        public void runFetchLoop() throws Exception {
            if (fetchLoopWaitLatch != null) {
                for (KafkaTopicPartitionState<?, ?> ignored : subscribedPartitionStates()) {
                    fetchLoopWaitLatch.trigger();
                    stateIterationBlockLatch.await();
                }
            } else {
                throw new UnsupportedOperationException();
            }
        }

        @Override
        public void cancel() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object createKafkaPartitionHandle(KafkaTopicPartition partition) {
            return new Object();
        }

        @Override
        protected void doCommitInternalOffsetsToKafka(
                Map<KafkaTopicPartition, Long> offsets, @Nonnull KafkaCommitCallback callback) {
            lastCommittedOffsets = offsets;
            callback.onSuccess();
        }

        public Optional<Map<KafkaTopicPartition, Long>> getLastCommittedOffsets() {
            return Optional.ofNullable(lastCommittedOffsets);
        }
    }

    // ------------------------------------------------------------------------

    private static <T, KPH> void emitRecord(
            AbstractFetcher<T, KPH> fetcher,
            T record,
            KafkaTopicPartitionState<T, KPH> partitionState,
            long offset) {
        ArrayDeque<T> recordQueue = new ArrayDeque<>();
        recordQueue.add(record);

        fetcher.emitRecordsWithTimestamps(recordQueue, partitionState, offset, Long.MIN_VALUE);
    }

    private static <T> Queue<T> emptyQueue() {
        return new ArrayDeque<>();
    }
}
