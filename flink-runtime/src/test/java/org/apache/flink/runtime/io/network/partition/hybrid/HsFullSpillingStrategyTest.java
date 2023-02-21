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

import org.apache.flink.runtime.io.network.partition.hybrid.HsSpillingStrategy.Decision;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.runtime.io.network.partition.hybrid.HybridShuffleTestUtils.createBufferIndexAndChannelsList;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link HsFullSpillingStrategy}. */
class HsFullSpillingStrategyTest {
    public static final int NUM_SUBPARTITIONS = 2;

    public static final float NUM_BUFFERS_TRIGGER_SPILLING_RATIO = 0.2f;

    public static final float FULL_SPILL_RELEASE_THRESHOLD = 0.8f;

    public static final float FULL_SPILL_RELEASE_RATIO = 0.6f;

    private final HsSpillingStrategy spillStrategy =
            new HsFullSpillingStrategy(
                    HybridShuffleConfiguration.builder(NUM_SUBPARTITIONS, 1)
                            .setFullStrategyNumBuffersTriggerSpillingRatio(
                                    NUM_BUFFERS_TRIGGER_SPILLING_RATIO)
                            .setFullStrategyReleaseThreshold(FULL_SPILL_RELEASE_THRESHOLD)
                            .setFullStrategyReleaseBufferRatio(FULL_SPILL_RELEASE_RATIO)
                            .build());

    @Test
    void testOnBufferFinishedUnSpillBufferBelowThreshold() {
        final int poolSize = 10;
        Optional<Decision> finishedDecision =
                spillStrategy.onBufferFinished(
                        (int) (poolSize * NUM_BUFFERS_TRIGGER_SPILLING_RATIO) - 1, poolSize);
        assertThat(finishedDecision).hasValue(Decision.NO_ACTION);
    }

    @Test
    void testOnBufferFinishedUnSpillBufferEqualToOrGreatThenThreshold() {
        final int poolSize = 10;
        Optional<Decision> finishedDecision =
                spillStrategy.onBufferFinished(
                        (int) (poolSize * NUM_BUFFERS_TRIGGER_SPILLING_RATIO), poolSize);
        assertThat(finishedDecision).isNotPresent();
        finishedDecision =
                spillStrategy.onBufferFinished(
                        (int) (poolSize * NUM_BUFFERS_TRIGGER_SPILLING_RATIO) + 1, poolSize);
        assertThat(finishedDecision).isNotPresent();
    }

    @Test
    void testOnBufferConsumed() {
        BufferIndexAndChannel bufferIndexAndChannel = new BufferIndexAndChannel(0, 0);
        Optional<Decision> bufferConsumedDecision =
                spillStrategy.onBufferConsumed(bufferIndexAndChannel);
        assertThat(bufferConsumedDecision).hasValue(Decision.NO_ACTION);
    }

    @Test
    void testOnUsedMemoryBelowThreshold() {
        Optional<Decision> memoryUsageChangedDecision = spillStrategy.onMemoryUsageChanged(5, 10);
        assertThat(memoryUsageChangedDecision).hasValue(Decision.NO_ACTION);
    }

    @Test
    void testOnUsedMemoryExceedThreshold() {
        final int poolSize = 10;
        final int threshold = (int) (poolSize * FULL_SPILL_RELEASE_THRESHOLD);
        Optional<Decision> memoryUsageChangedDecision =
                spillStrategy.onMemoryUsageChanged(threshold + 1, poolSize);
        assertThat(memoryUsageChangedDecision).isNotPresent();
    }

    @Test
    void testDecideActionWithGlobalInfo() {
        final int subpartition1 = 0;
        final int subpartition2 = 1;

        List<BufferIndexAndChannel> subpartitionBuffers1 =
                createBufferIndexAndChannelsList(subpartition1, 1, 2, 3, 4, 5);
        List<BufferIndexAndChannel> subpartitionBuffers2 =
                createBufferIndexAndChannelsList(subpartition2, 1, 2, 3, 4, 5);

        TestingSpillingInfoProvider spillInfoProvider =
                TestingSpillingInfoProvider.builder()
                        .setGetNumSubpartitionsSupplier(() -> NUM_SUBPARTITIONS)
                        .addSubpartitionBuffers(subpartition1, subpartitionBuffers1)
                        .addSubpartitionBuffers(subpartition2, subpartitionBuffers2)
                        .addSpillBuffers(subpartition1, Arrays.asList(0, 1, 2, 3))
                        .addSpillBuffers(subpartition2, Arrays.asList(1, 2, 3))
                        .setGetNumTotalUnSpillBuffersSupplier(
                                () -> (int) (10 * NUM_BUFFERS_TRIGGER_SPILLING_RATIO))
                        .setGetNumTotalRequestedBuffersSupplier(() -> 10)
                        .setGetPoolSizeSupplier(() -> 10)
                        .build();

        Decision decision = spillStrategy.decideActionWithGlobalInfo(spillInfoProvider);

        // all not spilled buffers need to spill.
        Map<Integer, List<BufferIndexAndChannel>> expectedSpillBuffers = new HashMap<>();
        expectedSpillBuffers.put(subpartition1, subpartitionBuffers1.subList(4, 5));
        expectedSpillBuffers.put(
                subpartition2, new ArrayList<>(subpartitionBuffers2.subList(0, 1)));
        expectedSpillBuffers.get(subpartition2).addAll(subpartitionBuffers2.subList(4, 5));
        assertThat(decision.getBufferToSpill()).isEqualTo(expectedSpillBuffers);

        Map<Integer, List<BufferIndexAndChannel>> expectedReleaseBuffers = new HashMap<>();
        expectedReleaseBuffers.put(
                subpartition1, new ArrayList<>(subpartitionBuffers1.subList(0, 3)));
        expectedReleaseBuffers.put(
                subpartition2, new ArrayList<>(subpartitionBuffers2.subList(1, 4)));
        assertThat(decision.getBufferToRelease()).isEqualTo(expectedReleaseBuffers);
    }

    @Test
    void testOnResultPartitionClosed() {
        final int subpartition1 = 0;
        final int subpartition2 = 1;

        List<BufferIndexAndChannel> subpartitionBuffer1 =
                createBufferIndexAndChannelsList(subpartition1, 0, 1, 2, 3);
        List<BufferIndexAndChannel> subpartitionBuffer2 =
                createBufferIndexAndChannelsList(subpartition2, 0, 1, 2);
        TestingSpillingInfoProvider spillInfoProvider =
                TestingSpillingInfoProvider.builder()
                        .setGetNumSubpartitionsSupplier(() -> 2)
                        .addSubpartitionBuffers(subpartition1, subpartitionBuffer1)
                        .addSubpartitionBuffers(subpartition2, subpartitionBuffer2)
                        .addSpillBuffers(subpartition1, Arrays.asList(2, 3))
                        .addConsumedBuffers(subpartition1, Collections.singletonList(0))
                        .addSpillBuffers(subpartition2, Collections.singletonList(2))
                        .build();

        Decision decision = spillStrategy.onResultPartitionClosed(spillInfoProvider);

        Map<Integer, List<BufferIndexAndChannel>> expectedToSpillBuffers = new HashMap<>();
        expectedToSpillBuffers.put(subpartition1, subpartitionBuffer1.subList(0, 2));
        expectedToSpillBuffers.put(subpartition2, subpartitionBuffer2.subList(0, 2));
        assertThat(decision.getBufferToSpill()).isEqualTo(expectedToSpillBuffers);

        Map<Integer, List<BufferIndexAndChannel>> expectedToReleaseBuffers = new HashMap<>();
        expectedToReleaseBuffers.put(subpartition1, subpartitionBuffer1.subList(0, 4));
        expectedToReleaseBuffers.put(subpartition2, subpartitionBuffer2.subList(0, 3));
        assertThat(decision.getBufferToRelease()).isEqualTo(expectedToReleaseBuffers);
    }
}
