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

import org.apache.flink.runtime.executiongraph.IndexRange;
import org.apache.flink.runtime.executiongraph.ResultPartitionBytes;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link AllToAllBlockingResultInfo}. */
class AllToAllBlockingResultInfoTest {

    @Test
    void testGetNumBytesProducedForNonBroadcast() {
        testGetNumBytesProduced(false, 192L);
    }

    @Test
    void testGetNumBytesProducedForBroadcast() {
        testGetNumBytesProduced(true, 96L);
    }

    @Test
    void testGetNumBytesProducedWithIndexRange() {
        AllToAllBlockingResultInfo resultInfo =
                new AllToAllBlockingResultInfo(new IntermediateDataSetID(), 2, 2, false);
        resultInfo.recordPartitionInfo(0, new ResultPartitionBytes(new long[] {32L, 64L}));
        resultInfo.recordPartitionInfo(1, new ResultPartitionBytes(new long[] {128L, 256L}));

        IndexRange partitionIndexRange = new IndexRange(0, 1);
        IndexRange subpartitionIndexRange = new IndexRange(0, 0);

        assertThat(resultInfo.getNumBytesProduced(partitionIndexRange, subpartitionIndexRange))
                .isEqualTo(160L);
    }

    @Test
    void testGetAggregatedSubpartitionBytes() {
        AllToAllBlockingResultInfo resultInfo =
                new AllToAllBlockingResultInfo(new IntermediateDataSetID(), 2, 2, false);
        resultInfo.recordPartitionInfo(0, new ResultPartitionBytes(new long[] {32L, 64L}));
        resultInfo.recordPartitionInfo(1, new ResultPartitionBytes(new long[] {128L, 256L}));

        assertThat(resultInfo.getAggregatedSubpartitionBytes()).containsExactly(160L, 320L);
    }

    @Test
    void testGetBytesWithPartialPartitionInfos() {
        AllToAllBlockingResultInfo resultInfo =
                new AllToAllBlockingResultInfo(new IntermediateDataSetID(), 2, 2, false);
        resultInfo.recordPartitionInfo(0, new ResultPartitionBytes(new long[] {32L, 64L}));

        assertThatThrownBy(resultInfo::getNumBytesProduced)
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(resultInfo::getAggregatedSubpartitionBytes)
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testRecordPartitionInfoMultiTimes() {
        AllToAllBlockingResultInfo resultInfo =
                new AllToAllBlockingResultInfo(new IntermediateDataSetID(), 2, 2, false);

        ResultPartitionBytes partitionBytes1 = new ResultPartitionBytes(new long[] {32L, 64L});
        ResultPartitionBytes partitionBytes2 = new ResultPartitionBytes(new long[] {64L, 128L});
        ResultPartitionBytes partitionBytes3 = new ResultPartitionBytes(new long[] {128L, 256L});
        ResultPartitionBytes partitionBytes4 = new ResultPartitionBytes(new long[] {256L, 512L});

        // record partitionBytes1 for subtask 0 and then reset it
        resultInfo.recordPartitionInfo(0, partitionBytes1);
        assertThat(resultInfo.getNumOfRecordedPartitions()).isEqualTo(1);
        resultInfo.resetPartitionInfo(0);
        assertThat(resultInfo.getNumOfRecordedPartitions()).isEqualTo(0);

        // record partitionBytes2 for subtask 0 and record partitionBytes3 for subtask 1
        resultInfo.recordPartitionInfo(0, partitionBytes2);
        resultInfo.recordPartitionInfo(1, partitionBytes3);

        // The result info should be (partitionBytes2 + partitionBytes3)
        assertThat(resultInfo.getNumBytesProduced()).isEqualTo(576L);
        assertThat(resultInfo.getAggregatedSubpartitionBytes()).containsExactly(192L, 384L);
        // The raw info should be clear
        assertThat(resultInfo.getNumOfRecordedPartitions()).isEqualTo(0);

        // reset subtask 0 and then record partitionBytes4 for subtask 0
        resultInfo.resetPartitionInfo(0);
        resultInfo.recordPartitionInfo(0, partitionBytes4);

        // The result info should still be (partitionBytes2 + partitionBytes3)
        assertThat(resultInfo.getNumBytesProduced()).isEqualTo(576L);
        assertThat(resultInfo.getAggregatedSubpartitionBytes()).containsExactly(192L, 384L);
        assertThat(resultInfo.getNumOfRecordedPartitions()).isEqualTo(0);
    }

    private void testGetNumBytesProduced(boolean isBroadcast, long expectedBytes) {
        AllToAllBlockingResultInfo resultInfo =
                new AllToAllBlockingResultInfo(new IntermediateDataSetID(), 2, 2, isBroadcast);
        resultInfo.recordPartitionInfo(0, new ResultPartitionBytes(new long[] {32L, 32L}));
        resultInfo.recordPartitionInfo(1, new ResultPartitionBytes(new long[] {64L, 64L}));

        assertThat(resultInfo.getNumBytesProduced()).isEqualTo(expectedBytes);
    }
}
