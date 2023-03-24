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

/** Test for {@link PointwiseBlockingResultInfo}. */
class PointwiseBlockingResultInfoTest {

    @Test
    void testGetNumBytesProduced() {
        PointwiseBlockingResultInfo resultInfo =
                new PointwiseBlockingResultInfo(new IntermediateDataSetID(), 2, 2);
        resultInfo.recordPartitionInfo(0, new ResultPartitionBytes(new long[] {32L, 32L}));
        resultInfo.recordPartitionInfo(1, new ResultPartitionBytes(new long[] {64L, 64L}));

        assertThat(resultInfo.getNumBytesProduced()).isEqualTo(192L);
    }

    @Test
    void testGetNumBytesProducedWithIndexRange() {
        PointwiseBlockingResultInfo resultInfo =
                new PointwiseBlockingResultInfo(new IntermediateDataSetID(), 2, 2);
        resultInfo.recordPartitionInfo(0, new ResultPartitionBytes(new long[] {32L, 64L}));
        resultInfo.recordPartitionInfo(1, new ResultPartitionBytes(new long[] {128L, 256L}));

        IndexRange partitionIndexRange = new IndexRange(0, 0);
        IndexRange subpartitionIndexRange = new IndexRange(0, 1);

        assertThat(resultInfo.getNumBytesProduced(partitionIndexRange, subpartitionIndexRange))
                .isEqualTo(96L);
    }

    @Test
    void testGetBytesWithPartialPartitionInfos() {
        PointwiseBlockingResultInfo resultInfo =
                new PointwiseBlockingResultInfo(new IntermediateDataSetID(), 2, 2);
        resultInfo.recordPartitionInfo(0, new ResultPartitionBytes(new long[] {32L, 64L}));
        assertThatThrownBy(resultInfo::getNumBytesProduced)
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testPartitionFinishedMultiTimes() {
        PointwiseBlockingResultInfo resultInfo =
                new PointwiseBlockingResultInfo(new IntermediateDataSetID(), 2, 2);

        resultInfo.recordPartitionInfo(0, new ResultPartitionBytes(new long[] {32L, 64L}));
        resultInfo.recordPartitionInfo(1, new ResultPartitionBytes(new long[] {64L, 128L}));
        assertThat(resultInfo.getNumOfRecordedPartitions()).isEqualTo(2);
        assertThat(resultInfo.getNumBytesProduced()).isEqualTo(288L);

        // reset partition info
        resultInfo.resetPartitionInfo(0);
        assertThat(resultInfo.getNumOfRecordedPartitions()).isEqualTo(1);

        // record partition info again
        resultInfo.recordPartitionInfo(0, new ResultPartitionBytes(new long[] {64L, 128L}));
        assertThat(resultInfo.getNumBytesProduced()).isEqualTo(384L);
    }
}
