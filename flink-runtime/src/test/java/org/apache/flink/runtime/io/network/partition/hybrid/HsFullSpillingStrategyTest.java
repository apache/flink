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
import java.util.List;
import java.util.Optional;

import static org.apache.flink.runtime.io.network.partition.hybrid.HsSpillingStrategyTestUtils.createBuffer;
import static org.apache.flink.runtime.io.network.partition.hybrid.HsSpillingStrategyTestUtils.createBufferWithIdentitiesList;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link HsFullSpillingStrategy}. */
class HsFullSpillingStrategyTest {
    public static final int NUM_SUBPARTITIONS = 2;

    public static final int NUM_BUFFERS_TRIGGER_SPILLING = 2;

    public static final float FULL_SPILL_RELEASE_THRESHOLD = 0.8f;

    public static final float FULL_SPILL_RELEASE_RATIO = 0.6f;

    private final HsSpillingStrategy spillStrategy =
            new HsFullSpillingStrategy(
                    HybridShuffleConfiguration.builder(NUM_SUBPARTITIONS, 1)
                            .setFullStrategyNumBuffersTriggerSpilling(NUM_BUFFERS_TRIGGER_SPILLING)
                            .setFullStrategyReleaseThreshold(FULL_SPILL_RELEASE_THRESHOLD)
                            .setFullStrategyReleaseBufferRatio(FULL_SPILL_RELEASE_RATIO)
                            .build());

    @Test
    void testOnBufferFinishedUnSpillBufferBelowThreshold() {
        Optional<Decision> finishedDecision =
                spillStrategy.onBufferFinished(NUM_BUFFERS_TRIGGER_SPILLING - 1);
        assertThat(finishedDecision).hasValue(Decision.NO_ACTION);
    }

    @Test
    void testOnBufferFinishedUnSpillBufferEqualToOrGreatThenThreshold() {
        Optional<Decision> finishedDecision =
                spillStrategy.onBufferFinished(NUM_BUFFERS_TRIGGER_SPILLING);
        assertThat(finishedDecision).isNotPresent();
        finishedDecision = spillStrategy.onBufferFinished(NUM_BUFFERS_TRIGGER_SPILLING + 1);
        assertThat(finishedDecision).isNotPresent();
    }

    @Test
    void testOnBufferConsumed() {
        BufferWithIdentity bufferWithIdentity = new BufferWithIdentity(createBuffer(), 0, 0);
        Optional<Decision> bufferConsumedDecision =
                spillStrategy.onBufferConsumed(bufferWithIdentity);
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

        final int progress1 = 10;
        final int progress2 = 20;

        List<BufferWithIdentity> subpartitionBuffers1 =
                createBufferWithIdentitiesList(
                        subpartition1,
                        progress1,
                        progress1 + 2,
                        progress1 + 4,
                        progress1 + 6,
                        progress1 + 8);
        List<BufferWithIdentity> subpartitionBuffers2 =
                createBufferWithIdentitiesList(
                        subpartition2,
                        progress2 + 1,
                        progress2 + 3,
                        progress2 + 5,
                        progress2 + 7,
                        progress2 + 9);

        TestingSpillingInfoProvider spillInfoProvider =
                TestingSpillingInfoProvider.builder()
                        .setGetNumSubpartitionsSupplier(() -> NUM_SUBPARTITIONS)
                        .addSubpartitionBuffers(subpartition1, subpartitionBuffers1)
                        .addSubpartitionBuffers(subpartition2, subpartitionBuffers2)
                        .addSpillBuffers(subpartition1, Arrays.asList(0, 1, 2, 3))
                        .addConsumedBuffers(subpartition1, Arrays.asList(0, 1))
                        .addSpillBuffers(subpartition2, Arrays.asList(1, 2, 3))
                        .addConsumedBuffers(subpartition2, Arrays.asList(0, 1))
                        .setGetNumTotalUnSpillBuffersSupplier(() -> NUM_BUFFERS_TRIGGER_SPILLING)
                        .setGetNumTotalRequestedBuffersSupplier(() -> 10)
                        .setGetPoolSizeSupplier(() -> 10)
                        .setGetNextBufferIndexToConsumeSupplier(
                                () -> Arrays.asList(progress1, progress2))
                        .build();

        Decision decision = spillStrategy.decideActionWithGlobalInfo(spillInfoProvider);

        // all not spilled buffers need to spill.
        ArrayList<BufferWithIdentity> expectedSpillBuffers =
                new ArrayList<>(subpartitionBuffers1.subList(4, 5));
        expectedSpillBuffers.add(subpartitionBuffers2.get(0));
        expectedSpillBuffers.addAll(subpartitionBuffers2.subList(4, 5));
        assertThat(decision.getBufferToSpill()).isEqualTo(expectedSpillBuffers);

        ArrayList<BufferWithIdentity> expectedReleaseBuffers = new ArrayList<>();
        // all consumed spill buffers should release.
        expectedReleaseBuffers.addAll(subpartitionBuffers1.subList(0, 2));
        // priority higher buffers should release.
        expectedReleaseBuffers.addAll(subpartitionBuffers1.subList(3, 4));
        // all consumed spill buffers should release.
        expectedReleaseBuffers.addAll(subpartitionBuffers2.subList(1, 2));
        // priority higher buffers should release.
        expectedReleaseBuffers.addAll(subpartitionBuffers2.subList(2, 4));
        assertThat(decision.getBufferToRelease()).isEqualTo(expectedReleaseBuffers);
    }

    /** All consumed buffers that already spill should release regardless of the release ratio. */
    @Test
    void testDecideActionWithGlobalInfoAllConsumedSpillBufferShouldRelease() {
        final int subpartitionId = 0;
        List<BufferWithIdentity> subpartitionBuffers =
                createBufferWithIdentitiesList(subpartitionId, 0, 1, 2, 3, 4);

        final int poolSize = 5;
        TestingSpillingInfoProvider spillInfoProvider =
                TestingSpillingInfoProvider.builder()
                        .setGetNumSubpartitionsSupplier(() -> 1)
                        .addSubpartitionBuffers(subpartitionId, subpartitionBuffers)
                        .addSpillBuffers(subpartitionId, Arrays.asList(0, 1, 2, 3, 4))
                        .addConsumedBuffers(subpartitionId, Arrays.asList(0, 1, 2, 3))
                        .setGetNumTotalUnSpillBuffersSupplier(() -> 0)
                        .setGetNumTotalRequestedBuffersSupplier(() -> poolSize)
                        .setGetPoolSizeSupplier(() -> poolSize)
                        .build();

        int numReleaseBuffer = (int) (poolSize * FULL_SPILL_RELEASE_RATIO);
        Decision decision = spillStrategy.decideActionWithGlobalInfo(spillInfoProvider);
        assertThat(decision.getBufferToSpill()).isEmpty();
        assertThat(decision.getBufferToRelease())
                .isEqualTo(subpartitionBuffers.subList(0, 4))
                .hasSizeGreaterThan(numReleaseBuffer);
    }
}
