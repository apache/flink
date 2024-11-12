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

import org.apache.flink.runtime.scheduler.adaptivebatch.BlockingInputInfo;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.flink.runtime.scheduler.adaptivebatch.util.AllToAllVertexInputInfoComputerTest.createBlockingInputInfos;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link AggregatedBlockingInputInfo}. */
public class AggregatedBlockingInputInfoTest {
    @Test
    void testAggregatedInputWithSameNumPartitions() {
        List<BlockingInputInfo> inputInfos =
                createBlockingInputInfos(1, 10, 4, true, true, List.of(1));
        AggregatedBlockingInputInfo aggregatedBlockingInputInfo =
                AggregatedBlockingInputInfo.createAggregatedBlockingInputInfo(
                        40, 4, 20, inputInfos);
        assertThat(aggregatedBlockingInputInfo.getMaxPartitionNum()).isEqualTo(4);
        assertThat(aggregatedBlockingInputInfo.getNumSubpartitions()).isEqualTo(3);
        assertThat(aggregatedBlockingInputInfo.getTargetSize()).isEqualTo(40);
        assertThat(aggregatedBlockingInputInfo.isSplittable()).isFalse();
        assertThat(aggregatedBlockingInputInfo.isSkewedSubpartition(0)).isEqualTo(false);
        assertThat(aggregatedBlockingInputInfo.isSkewedSubpartition(1)).isEqualTo(true);
        assertThat(aggregatedBlockingInputInfo.isSkewedSubpartition(2)).isEqualTo(false);
        assertThat(aggregatedBlockingInputInfo.getAggregatedSubpartitionBytes(0)).isEqualTo(40);
        assertThat(aggregatedBlockingInputInfo.getAggregatedSubpartitionBytes(1)).isEqualTo(400);
        assertThat(aggregatedBlockingInputInfo.getAggregatedSubpartitionBytes(2)).isEqualTo(40);
        assertThat(aggregatedBlockingInputInfo.getSubpartitionBytesByPartition()).isEmpty();

        // all subpartition bytes larger than skewed threshold
        AggregatedBlockingInputInfo aggregatedBlockingInputInfo2 =
                AggregatedBlockingInputInfo.createAggregatedBlockingInputInfo(
                        30, 4, 20, inputInfos);
        assertThat(aggregatedBlockingInputInfo2.getTargetSize()).isEqualTo(40);

        // larger than skewed factor but less than skewed threshold
        AggregatedBlockingInputInfo aggregatedBlockingInputInfo3 =
                AggregatedBlockingInputInfo.createAggregatedBlockingInputInfo(
                        500, 4, 20, inputInfos);
        assertThat(aggregatedBlockingInputInfo3.getTargetSize()).isEqualTo(160);

        // larger than skewed threshold but less than skewed factor
        AggregatedBlockingInputInfo aggregatedBlockingInputInfo4 =
                AggregatedBlockingInputInfo.createAggregatedBlockingInputInfo(
                        100, 20, 20, inputInfos);
        assertThat(aggregatedBlockingInputInfo4.getTargetSize()).isEqualTo(160);

        List<BlockingInputInfo> inputInfosWithoutIntraCorrelation =
                createBlockingInputInfos(2, 10, 4, false, true, List.of(1));
        AggregatedBlockingInputInfo aggregatedBlockingInputInfo5 =
                AggregatedBlockingInputInfo.createAggregatedBlockingInputInfo(
                        40, 4, 20, inputInfosWithoutIntraCorrelation);
        assertThat(aggregatedBlockingInputInfo5.getSubpartitionBytesByPartition())
                .containsExactlyInAnyOrderEntriesOf(
                        Map.of(
                                0,
                                new long[] {10, 100, 10},
                                1,
                                new long[] {10, 100, 10},
                                2,
                                new long[] {10, 100, 10},
                                3,
                                new long[] {10, 100, 10}));
    }

    @Test
    void testAggregatedInputWithDifferentNumPartitions() {
        List<BlockingInputInfo> inputInfos = new ArrayList<>();
        inputInfos.addAll(createBlockingInputInfos(1, 10, 4, false, true, List.of()));
        inputInfos.addAll(createBlockingInputInfos(1, 1, 5, false, true, List.of()));
        AggregatedBlockingInputInfo aggregatedBlockingInputInfo =
                AggregatedBlockingInputInfo.createAggregatedBlockingInputInfo(
                        40, 4, 20, inputInfos);
        assertThat(aggregatedBlockingInputInfo.getMaxPartitionNum()).isEqualTo(5);
        assertThat(aggregatedBlockingInputInfo.getNumSubpartitions()).isEqualTo(3);
        assertThat(aggregatedBlockingInputInfo.getSubpartitionBytesByPartition()).isEmpty();
        assertThat(aggregatedBlockingInputInfo.isSplittable()).isFalse();
    }
}
