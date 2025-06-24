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

package org.apache.flink.runtime.scheduler.adaptivebatch.util;

import org.apache.flink.runtime.executiongraph.IndexRange;
import org.apache.flink.runtime.scheduler.adaptivebatch.BlockingInputInfo;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.apache.flink.runtime.scheduler.adaptivebatch.util.AllToAllVertexInputInfoComputerTest.createBlockingInputInfos;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SubpartitionSlice}. */
public class SubpartitionSliceTest {
    @Test
    void testCreateSubpartitionSlice() {
        SubpartitionSlice subpartitionSlice =
                SubpartitionSlice.createSubpartitionSlice(
                        new IndexRange(0, 2), new IndexRange(0, 3), 10);
        assertThat(subpartitionSlice.getSubpartitionRange()).isEqualTo(new IndexRange(0, 3));
        assertThat(subpartitionSlice.getPartitionRange(2)).isEqualTo(new IndexRange(0, 1));
        assertThat(subpartitionSlice.getPartitionRange(3)).isEqualTo(new IndexRange(0, 2));
        assertThat(subpartitionSlice.getPartitionRange(4)).isEqualTo(new IndexRange(0, 2));
        assertThat(subpartitionSlice.getDataBytes()).isEqualTo(10);
    }

    @Test
    void testCreateSubpartitionSlices() {
        // 1,10,1 1,10,1 1,10,1
        BlockingInputInfo inputInfos =
                createBlockingInputInfos(1, 1, 3, true, true, List.of(1)).get(0);

        List<SubpartitionSlice> subpartitionSlices =
                SubpartitionSlice.createSubpartitionSlicesByMultiPartitionRanges(
                        List.of(new IndexRange(0, 0), new IndexRange(1, 1), new IndexRange(2, 2)),
                        new IndexRange(0, 0),
                        inputInfos.getSubpartitionBytesByPartitionIndex());
        checkSubpartitionSlices(
                subpartitionSlices,
                List.of(new IndexRange(0, 0), new IndexRange(1, 1), new IndexRange(2, 2)),
                new IndexRange(0, 0),
                new long[] {1L, 1L, 1L},
                3);

        List<SubpartitionSlice> subpartitionSlices2 =
                SubpartitionSlice.createSubpartitionSlicesByMultiPartitionRanges(
                        List.of(new IndexRange(0, 1), new IndexRange(2, 2)),
                        new IndexRange(0, 0),
                        inputInfos.getSubpartitionBytesByPartitionIndex());
        checkSubpartitionSlices(
                subpartitionSlices2,
                List.of(new IndexRange(0, 1), new IndexRange(2, 2)),
                new IndexRange(0, 0),
                new long[] {2L, 1L},
                3);

        List<SubpartitionSlice> subpartitionSlices3 =
                SubpartitionSlice.createSubpartitionSlicesByMultiPartitionRanges(
                        List.of(new IndexRange(0, 0), new IndexRange(1, 1), new IndexRange(2, 2)),
                        new IndexRange(1, 1),
                        inputInfos.getSubpartitionBytesByPartitionIndex());
        checkSubpartitionSlices(
                subpartitionSlices3,
                List.of(new IndexRange(0, 0), new IndexRange(1, 1), new IndexRange(2, 2)),
                new IndexRange(1, 1),
                new long[] {10L, 10L, 10L},
                3);
    }

    private void checkSubpartitionSlices(
            List<SubpartitionSlice> subpartitionSlices,
            List<IndexRange> partitionRanges,
            IndexRange subpartitionRange,
            long[] dataBytes,
            int numPartitions) {
        for (int i = 0; i < subpartitionSlices.size(); ++i) {
            SubpartitionSlice subpartitionSlice = subpartitionSlices.get(i);
            assertThat(subpartitionSlice.getPartitionRange(numPartitions))
                    .isEqualTo(partitionRanges.get(i));
            assertThat(subpartitionSlice.getSubpartitionRange()).isEqualTo(subpartitionRange);
            assertThat(subpartitionSlice.getDataBytes()).isEqualTo(dataBytes[i]);
        }
    }
}
