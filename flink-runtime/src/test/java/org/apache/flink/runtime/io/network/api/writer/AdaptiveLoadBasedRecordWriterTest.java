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

package org.apache.flink.runtime.io.network.api.writer;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.StopMode;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionIndexSet;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link AdaptiveLoadBasedRecordWriter}. */
class AdaptiveLoadBasedRecordWriterTest {

    static Stream<Arguments> getTestingParams() {
        return Stream.of(
                // maxTraverseSize, bytesPerPartition, bufferPerPartition,
                // targetResultPartitionIndex
                Arguments.of(2, new long[] {1L, 2L, 3L}, new int[] {2, 3, 4}, 0),
                Arguments.of(2, new long[] {0L, 0L, 0L}, new int[] {2, 3, 4}, 0),
                Arguments.of(2, new long[] {0L, 0L, 0L}, new int[] {0, 0, 0}, 0),
                Arguments.of(3, new long[] {1L, 2L, 3L}, new int[] {2, 3, 4}, 0),
                Arguments.of(3, new long[] {0L, 0L, 0L}, new int[] {2, 3, 4}, 0),
                Arguments.of(3, new long[] {0L, 0L, 0L}, new int[] {0, 0, 0}, 0),
                Arguments.of(
                        2, new long[] {1L, 2L, 3L, 1L, 2L, 3L}, new int[] {2, 3, 4, 2, 3, 4}, 0),
                Arguments.of(
                        2, new long[] {0L, 0L, 3L, 1L, 2L, 3L}, new int[] {3, 2, 4, 2, 3, 4}, 1),
                Arguments.of(
                        2, new long[] {0L, 0L, 3L, 1L, 2L, 3L}, new int[] {0, 0, 4, 2, 3, 4}, 0),
                Arguments.of(
                        4, new long[] {1L, 2L, 3L, 0L, 2L, 3L}, new int[] {2, 3, 4, 2, 3, 4}, 3),
                Arguments.of(
                        4, new long[] {1L, 1L, 1L, 1L, 2L, 3L}, new int[] {2, 3, 4, 0, 3, 4}, 3),
                Arguments.of(
                        4, new long[] {0L, 0L, 0L, 0L, 2L, 3L}, new int[] {2, 3, 0, 2, 3, 4}, 2));
    }

    @ParameterizedTest(
            name =
                    "maxTraverseSize: {0}, bytesPerPartition: {1}, bufferPerPartition: {2}, targetResultPartitionIndex: {3}")
    @MethodSource("getTestingParams")
    void testGetIdlestChannelIndex(
            int maxTraverseSize,
            long[] bytesPerPartition,
            int[] buffersPerPartition,
            int targetResultPartitionIndex) {
        TestingResultPartitionWriter resultPartitionWriter =
                getTestingResultPartitionWriter(bytesPerPartition, buffersPerPartition);

        AdaptiveLoadBasedRecordWriter<IOReadableWritable> adaptiveLoadBasedRecordWriter =
                new AdaptiveLoadBasedRecordWriter<>(
                        resultPartitionWriter, 5L, "testingTask", maxTraverseSize);
        assertThat(adaptiveLoadBasedRecordWriter.getIdlestChannelIndex())
                .isEqualTo(targetResultPartitionIndex);
    }

    private static TestingResultPartitionWriter getTestingResultPartitionWriter(
            long[] bytesPerPartition, int[] buffersPerPartition) {
        final Map<Integer, Long> bytesPerPartitionMap = new HashMap<>();
        final Map<Integer, Integer> bufferPerPartitionMap = new HashMap<>();
        for (int i = 0; i < bytesPerPartition.length; i++) {
            bytesPerPartitionMap.put(i, bytesPerPartition[i]);
            bufferPerPartitionMap.put(i, buffersPerPartition[i]);
        }

        return new TestingResultPartitionWriter(
                buffersPerPartition.length, bytesPerPartitionMap, bufferPerPartitionMap);
    }

    /** Test utils class to simulate {@link ResultPartitionWriter}. */
    static final class TestingResultPartitionWriter implements ResultPartitionWriter {

        private final int numberOfSubpartitions;
        private final Map<Integer, Long> bytesPerPartition;
        private final Map<Integer, Integer> bufferPerPartition;

        TestingResultPartitionWriter(
                int numberOfSubpartitions,
                Map<Integer, Long> bytesPerPartition,
                Map<Integer, Integer> bufferPerPartition) {
            this.numberOfSubpartitions = numberOfSubpartitions;
            this.bytesPerPartition = bytesPerPartition;
            this.bufferPerPartition = bufferPerPartition;
        }

        // The methods that are used in the testing.

        @Override
        public long getBytesInQueueUnsafe(int targetSubpartition) {
            return bytesPerPartition.getOrDefault(targetSubpartition, 0L);
        }

        @Override
        public int getBuffersCountUnsafe(int targetSubpartition) {
            return bufferPerPartition.getOrDefault(targetSubpartition, 0);
        }

        @Override
        public int getNumberOfSubpartitions() {
            return numberOfSubpartitions;
        }

        // The methods that are not used.

        @Override
        public void setup() throws IOException {}

        @Override
        public ResultPartitionID getPartitionId() {
            return null;
        }

        @Override
        public int getNumTargetKeyGroups() {
            return 0;
        }

        @Override
        public void setMaxOverdraftBuffersPerGate(int maxOverdraftBuffersPerGate) {}

        @Override
        public void emitRecord(ByteBuffer record, int targetSubpartition) throws IOException {}

        @Override
        public void broadcastRecord(ByteBuffer record) throws IOException {}

        @Override
        public void broadcastEvent(AbstractEvent event, boolean isPriorityEvent)
                throws IOException {}

        @Override
        public void alignedBarrierTimeout(long checkpointId) throws IOException {}

        @Override
        public void abortCheckpoint(long checkpointId, CheckpointException cause) {}

        @Override
        public void notifyEndOfData(StopMode mode) throws IOException {}

        @Override
        public CompletableFuture<Void> getAllDataProcessedFuture() {
            return null;
        }

        @Override
        public void setMetricGroup(TaskIOMetricGroup metrics) {}

        @Override
        public ResultSubpartitionView createSubpartitionView(
                ResultSubpartitionIndexSet indexSet,
                BufferAvailabilityListener availabilityListener)
                throws IOException {
            return null;
        }

        @Override
        public void flushAll() {}

        @Override
        public void flush(int subpartitionIndex) {}

        @Override
        public void fail(@Nullable Throwable throwable) {}

        @Override
        public void finish() throws IOException {}

        @Override
        public boolean isFinished() {
            return false;
        }

        @Override
        public void release(Throwable cause) {}

        @Override
        public boolean isReleased() {
            return false;
        }

        @Override
        public void close() throws Exception {}

        @Override
        public CompletableFuture<?> getAvailableFuture() {
            return null;
        }
    }
}
