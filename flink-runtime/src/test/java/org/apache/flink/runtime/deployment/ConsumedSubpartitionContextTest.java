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

package org.apache.flink.runtime.deployment;

import org.apache.flink.runtime.executiongraph.IndexRange;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.apache.flink.runtime.io.network.partition.ResultPartitionType.BLOCKING;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ConsumedSubpartitionContext}. */
class ConsumedSubpartitionContextTest {
    @Test
    void testBuildConsumedSubpartitionContextWithGroups() {
        Map<IndexRange, IndexRange> consumedSubpartitionGroups =
                Map.of(
                        new IndexRange(0, 1), new IndexRange(0, 2),
                        new IndexRange(2, 3), new IndexRange(3, 5));

        List<IntermediateResultPartitionID> partitions = createPartitions();
        ConsumedPartitionGroup consumedPartitionGroup =
                ConsumedPartitionGroup.fromMultiplePartitions(4, partitions, BLOCKING);

        ConsumedSubpartitionContext context =
                ConsumedSubpartitionContext.buildConsumedSubpartitionContext(
                        consumedSubpartitionGroups, consumedPartitionGroup, partitions::get);

        assertThat(context.getNumConsumedShuffleDescriptors()).isEqualTo(4);

        Collection<IndexRange> shuffleDescriptorRanges =
                context.getConsumedShuffleDescriptorRanges();
        assertThat(shuffleDescriptorRanges).hasSize(1);

        assertThat(context.getConsumedSubpartitionRange(0)).isEqualTo(new IndexRange(0, 2));
        assertThat(context.getConsumedSubpartitionRange(1)).isEqualTo(new IndexRange(0, 2));
        assertThat(context.getConsumedSubpartitionRange(2)).isEqualTo(new IndexRange(3, 5));
        assertThat(context.getConsumedSubpartitionRange(3)).isEqualTo(new IndexRange(3, 5));
    }

    @Test
    void testBuildConsumedSubpartitionContextWithUnorderedGroups() {
        Map<IndexRange, IndexRange> consumedSubpartitionGroups =
                Map.of(
                        new IndexRange(3, 3), new IndexRange(1, 1),
                        new IndexRange(0, 0), new IndexRange(0, 1));

        List<IntermediateResultPartitionID> partitions = createPartitions();
        ConsumedPartitionGroup consumedPartitionGroup =
                ConsumedPartitionGroup.fromMultiplePartitions(4, partitions, BLOCKING);

        ConsumedSubpartitionContext context =
                ConsumedSubpartitionContext.buildConsumedSubpartitionContext(
                        consumedSubpartitionGroups, consumedPartitionGroup, partitions::get);

        assertThat(context.getNumConsumedShuffleDescriptors()).isEqualTo(2);

        Collection<IndexRange> shuffleDescriptorRanges =
                context.getConsumedShuffleDescriptorRanges();
        assertThat(shuffleDescriptorRanges).hasSize(2);

        assertThat(context.getConsumedSubpartitionRange(0)).isEqualTo(new IndexRange(0, 1));
        assertThat(context.getConsumedSubpartitionRange(3)).isEqualTo(new IndexRange(1, 1));
    }

    @Test
    void testBuildConsumedSubpartitionContextWithOverlapGroups() {
        Map<IndexRange, IndexRange> consumedSubpartitionGroups =
                Map.of(
                        new IndexRange(0, 3), new IndexRange(1, 1),
                        new IndexRange(0, 1), new IndexRange(2, 2));

        List<IntermediateResultPartitionID> partitions = createPartitions();
        ConsumedPartitionGroup consumedPartitionGroup =
                ConsumedPartitionGroup.fromMultiplePartitions(4, partitions, BLOCKING);

        ConsumedSubpartitionContext context =
                ConsumedSubpartitionContext.buildConsumedSubpartitionContext(
                        consumedSubpartitionGroups, consumedPartitionGroup, partitions::get);

        assertThat(context.getNumConsumedShuffleDescriptors()).isEqualTo(4);

        Collection<IndexRange> shuffleDescriptorRanges =
                context.getConsumedShuffleDescriptorRanges();
        assertThat(shuffleDescriptorRanges).hasSize(1);

        assertThat(context.getConsumedSubpartitionRange(0)).isEqualTo(new IndexRange(1, 2));
        assertThat(context.getConsumedSubpartitionRange(1)).isEqualTo(new IndexRange(1, 2));
        assertThat(context.getConsumedSubpartitionRange(2)).isEqualTo(new IndexRange(1, 1));
        assertThat(context.getConsumedSubpartitionRange(3)).isEqualTo(new IndexRange(1, 1));
    }

    @Test
    void testBuildConsumedSubpartitionContextWithRange() {
        int numConsumedShuffleDescriptors = 5;
        IndexRange consumedSubpartitionRange = new IndexRange(0, 4);

        ConsumedSubpartitionContext context =
                ConsumedSubpartitionContext.buildConsumedSubpartitionContext(
                        numConsumedShuffleDescriptors, consumedSubpartitionRange);

        assertThat(context.getNumConsumedShuffleDescriptors())
                .isEqualTo(numConsumedShuffleDescriptors);

        Collection<IndexRange> shuffleDescriptorRanges =
                context.getConsumedShuffleDescriptorRanges();
        assertThat(shuffleDescriptorRanges).hasSize(1);
        assertThat(shuffleDescriptorRanges).contains(new IndexRange(0, 4));

        IndexRange subpartitionRange = context.getConsumedSubpartitionRange(2);
        assertThat(subpartitionRange).isEqualTo(consumedSubpartitionRange);
    }

    private static List<IntermediateResultPartitionID> createPartitions() {
        List<IntermediateResultPartitionID> partitions = new ArrayList<>();
        IntermediateDataSetID intermediateDataSetID = new IntermediateDataSetID();
        for (int i = 0; i < 4; i++) {
            partitions.add(new IntermediateResultPartitionID(intermediateDataSetID, i));
        }
        return partitions;
    }
}
