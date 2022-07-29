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

package org.apache.flink.runtime.io.network.partition.hybrid;

import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.TreeMap;

import static org.apache.flink.runtime.io.network.partition.hybrid.HybridShuffleTestUtils.createBufferIndexAndChannelsDeque;
import static org.apache.flink.runtime.io.network.partition.hybrid.HybridShuffleTestUtils.createBufferIndexAndChannelsList;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link HsSpillingStrategyUtils}. */
class HsSpillingStrategyUtilsTest {
    @Test
    void testGetBuffersByConsumptionPriorityInOrderEmptyExpectedSize() {
        TreeMap<Integer, Deque<BufferIndexAndChannel>> subpartitionToAllBuffers = new TreeMap<>();
        subpartitionToAllBuffers.put(0, createBufferIndexAndChannelsDeque(0, 0, 1));
        subpartitionToAllBuffers.put(1, createBufferIndexAndChannelsDeque(1, 2, 4));
        TreeMap<Integer, List<BufferIndexAndChannel>> buffersByConsumptionPriorityInOrder =
                HsSpillingStrategyUtils.getBuffersByConsumptionPriorityInOrder(
                        Arrays.asList(0, 1), subpartitionToAllBuffers, 0);
        assertThat(buffersByConsumptionPriorityInOrder).isEmpty();
    }

    @Test
    void testGetBuffersByConsumptionPriorityInOrder() {
        final int subpartition1 = 0;
        final int subpartition2 = 1;

        final int progress1 = 10;
        final int progress2 = 20;

        TreeMap<Integer, Deque<BufferIndexAndChannel>> subpartitionBuffers = new TreeMap<>();
        List<BufferIndexAndChannel> subpartitionBuffers1 =
                createBufferIndexAndChannelsList(
                        subpartition1, progress1, progress1 + 2, progress1 + 6);
        List<BufferIndexAndChannel> subpartitionBuffers2 =
                createBufferIndexAndChannelsList(
                        subpartition2, progress2 + 1, progress2 + 2, progress2 + 5);
        subpartitionBuffers.put(subpartition1, new ArrayDeque<>(subpartitionBuffers1));
        subpartitionBuffers.put(subpartition2, new ArrayDeque<>(subpartitionBuffers2));

        TreeMap<Integer, List<BufferIndexAndChannel>> buffersByConsumptionPriorityInOrder =
                HsSpillingStrategyUtils.getBuffersByConsumptionPriorityInOrder(
                        Arrays.asList(progress1, progress2), subpartitionBuffers, 5);

        assertThat(buffersByConsumptionPriorityInOrder).hasSize(2);
        assertThat(buffersByConsumptionPriorityInOrder.get(subpartition1))
                .isEqualTo(subpartitionBuffers1.subList(1, 3));
        assertThat(buffersByConsumptionPriorityInOrder.get(subpartition2))
                .isEqualTo(subpartitionBuffers2.subList(0, 3));
    }
}
