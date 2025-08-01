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
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SupportsBatchSnapshot;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource.NumberSequenceSplit;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.apache.flink.util.Preconditions.checkNotNull;

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
}
