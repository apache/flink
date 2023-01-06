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

package org.apache.flink.runtime.scheduler.adaptivebatch;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.executiongraph.ResultPartitionBytes;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link DefaultVertexParallelismAndInputInfosDecider}. */
class DefaultVertexParallelismAndInputInfosDeciderTest {

    private static final long BYTE_256_MB = 256 * 1024 * 1024L;
    private static final long BYTE_512_MB = 512 * 1024 * 1024L;
    private static final long BYTE_1_GB = 1024 * 1024 * 1024L;
    private static final long BYTE_8_GB = 8 * 1024 * 1024 * 1024L;
    private static final long BYTE_1_TB = 1024 * 1024 * 1024 * 1024L;

    private static final int MAX_PARALLELISM = 100;
    private static final int MIN_PARALLELISM = 3;
    private static final int DEFAULT_SOURCE_PARALLELISM = 10;
    private static final long DATA_VOLUME_PER_TASK = 1024 * 1024 * 1024L;

    @Test
    void testNormalizedMaxAndMinParallelism() {
        final DefaultVertexParallelismAndInputInfosDecider decider =
                createDefaultVertexParallelismAndInputInfosDecider();
        assertThat(decider.getMaxParallelism()).isEqualTo(64);
        assertThat(decider.getMinParallelism()).isEqualTo(4);
    }

    @Test
    void testNormalizeParallelismDownToPowerOf2() {
        final DefaultVertexParallelismAndInputInfosDecider decider =
                createDefaultVertexParallelismAndInputInfosDecider();

        BlockingResultInfo resultInfo1 = createFromBroadcastResult(BYTE_256_MB);
        BlockingResultInfo resultInfo2 = createFromNonBroadcastResult(BYTE_256_MB + BYTE_8_GB);

        int parallelism =
                decider.decideParallelism(
                        new JobVertexID(), Arrays.asList(resultInfo1, resultInfo2));

        assertThat(parallelism).isEqualTo(8);
    }

    @Test
    void testNormalizeParallelismUpToPowerOf2() {
        final DefaultVertexParallelismAndInputInfosDecider decider =
                createDefaultVertexParallelismAndInputInfosDecider();

        BlockingResultInfo resultInfo1 = createFromBroadcastResult(BYTE_256_MB);
        BlockingResultInfo resultInfo2 = createFromNonBroadcastResult(BYTE_1_GB + BYTE_8_GB);

        int parallelism =
                decider.decideParallelism(
                        new JobVertexID(), Arrays.asList(resultInfo1, resultInfo2));

        assertThat(parallelism).isEqualTo(16);
    }

    @Test
    void testInitiallyNormalizedParallelismIsLargerThanMaxParallelism() {
        final DefaultVertexParallelismAndInputInfosDecider decider =
                createDefaultVertexParallelismAndInputInfosDecider();

        BlockingResultInfo resultInfo1 = createFromBroadcastResult(BYTE_256_MB);
        BlockingResultInfo resultInfo2 = createFromNonBroadcastResult(BYTE_8_GB + BYTE_1_TB);

        int parallelism =
                decider.decideParallelism(
                        new JobVertexID(), Arrays.asList(resultInfo1, resultInfo2));

        assertThat(parallelism).isEqualTo(64);
    }

    @Test
    void testInitiallyNormalizedParallelismIsSmallerThanMinParallelism() {
        final DefaultVertexParallelismAndInputInfosDecider decider =
                createDefaultVertexParallelismAndInputInfosDecider();

        BlockingResultInfo resultInfo1 = createFromBroadcastResult(BYTE_256_MB);
        BlockingResultInfo resultInfo2 = createFromNonBroadcastResult(BYTE_512_MB);

        int parallelism =
                decider.decideParallelism(
                        new JobVertexID(), Arrays.asList(resultInfo1, resultInfo2));

        assertThat(parallelism).isEqualTo(4);
    }

    @Test
    void testBroadcastRatioExceedsCapRatio() {
        final DefaultVertexParallelismAndInputInfosDecider decider =
                createDefaultVertexParallelismAndInputInfosDecider();

        BlockingResultInfo resultInfo1 = createFromBroadcastResult(BYTE_1_GB);
        BlockingResultInfo resultInfo2 = createFromNonBroadcastResult(BYTE_8_GB);

        int parallelism =
                decider.decideParallelism(
                        new JobVertexID(), Arrays.asList(resultInfo1, resultInfo2));

        assertThat(parallelism).isEqualTo(16);
    }

    @Test
    void testNonBroadcastBytesCanNotDividedEvenly() {
        final DefaultVertexParallelismAndInputInfosDecider decider =
                createDefaultVertexParallelismAndInputInfosDecider();

        BlockingResultInfo resultInfo1 = createFromBroadcastResult(BYTE_512_MB);
        BlockingResultInfo resultInfo2 = createFromNonBroadcastResult(BYTE_256_MB + BYTE_8_GB);

        int parallelism =
                decider.decideParallelism(
                        new JobVertexID(), Arrays.asList(resultInfo1, resultInfo2));

        assertThat(parallelism).isEqualTo(16);
    }

    private static DefaultVertexParallelismAndInputInfosDecider
            createDefaultVertexParallelismAndInputInfosDecider() {
        return createDefaultVertexParallelismAndInputInfosDecider(
                MIN_PARALLELISM, MAX_PARALLELISM, DATA_VOLUME_PER_TASK);
    }

    static DefaultVertexParallelismAndInputInfosDecider
            createDefaultVertexParallelismAndInputInfosDecider(
                    int minParallelism, int maxParallelism, long dataVolumePerTask) {
        Configuration configuration = new Configuration();

        configuration.setInteger(
                JobManagerOptions.ADAPTIVE_BATCH_SCHEDULER_MAX_PARALLELISM, maxParallelism);
        configuration.setInteger(
                JobManagerOptions.ADAPTIVE_BATCH_SCHEDULER_MIN_PARALLELISM, minParallelism);
        configuration.set(
                JobManagerOptions.ADAPTIVE_BATCH_SCHEDULER_AVG_DATA_VOLUME_PER_TASK,
                new MemorySize(dataVolumePerTask));
        configuration.setInteger(
                JobManagerOptions.ADAPTIVE_BATCH_SCHEDULER_DEFAULT_SOURCE_PARALLELISM,
                DEFAULT_SOURCE_PARALLELISM);

        return DefaultVertexParallelismAndInputInfosDecider.from(configuration);
    }

    private static class TestingBlockingResultInfo implements BlockingResultInfo {

        private final boolean isBroadcast;

        private final long producedBytes;

        private TestingBlockingResultInfo(boolean isBroadcast, long producedBytes) {
            this.isBroadcast = isBroadcast;
            this.producedBytes = producedBytes;
        }

        @Override
        public IntermediateDataSetID getResultId() {
            return new IntermediateDataSetID();
        }

        @Override
        public boolean isBroadcast() {
            return isBroadcast;
        }

        @Override
        public boolean isPointwise() {
            return false;
        }

        @Override
        public int getNumPartitions() {
            return 0;
        }

        @Override
        public int getNumSubpartitions(int partitionIndex) {
            return 0;
        }

        @Override
        public long getNumBytesProduced() {
            return producedBytes;
        }

        @Override
        public void recordPartitionInfo(int partitionIndex, ResultPartitionBytes partitionBytes) {}

        @Override
        public void resetPartitionInfo(int partitionIndex) {}
    }

    private static BlockingResultInfo createFromBroadcastResult(long producedBytes) {
        return new TestingBlockingResultInfo(true, producedBytes);
    }

    private static BlockingResultInfo createFromNonBroadcastResult(long producedBytes) {
        return new TestingBlockingResultInfo(false, producedBytes);
    }
}
