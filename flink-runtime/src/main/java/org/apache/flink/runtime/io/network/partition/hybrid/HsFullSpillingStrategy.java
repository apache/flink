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

import org.apache.flink.runtime.io.network.partition.hybrid.HsSpillingInfoProvider.ConsumeStatus;
import org.apache.flink.runtime.io.network.partition.hybrid.HsSpillingInfoProvider.SpillStatus;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.TreeMap;

import static org.apache.flink.runtime.io.network.partition.hybrid.HsSpillingStrategyUtils.getBuffersByConsumptionPriorityInOrder;

/** A special implementation of {@link HsSpillingStrategy} that spilled all buffers to disk. */
public class HsFullSpillingStrategy implements HsSpillingStrategy {
    private final int numBuffersTriggerSpilling;

    private final float releaseBufferRatio;

    private final float releaseThreshold;

    public HsFullSpillingStrategy(HybridShuffleConfiguration hybridShuffleConfiguration) {
        this.numBuffersTriggerSpilling =
                hybridShuffleConfiguration.getFullStrategyNumBuffersTriggerSpilling();
        this.releaseThreshold = hybridShuffleConfiguration.getFullStrategyReleaseThreshold();
        this.releaseBufferRatio = hybridShuffleConfiguration.getFullStrategyReleaseBufferRatio();
    }

    // For the case of buffer finished, whenever the number of unSpillBuffers reaches
    // numBuffersTriggerSpilling, make a decision based on global information. Otherwise, no need to
    // take action.
    @Override
    public Optional<Decision> onBufferFinished(int numTotalUnSpillBuffers) {
        return numTotalUnSpillBuffers < numBuffersTriggerSpilling
                ? Optional.of(Decision.NO_ACTION)
                : Optional.empty();
    }

    // For the case of buffer consumed, there is no need to take action for HsFullSpillingStrategy.
    @Override
    public Optional<Decision> onBufferConsumed(BufferIndexAndChannel consumedBuffer) {
        return Optional.of(Decision.NO_ACTION);
    }

    // When the amount of memory used exceeds the threshold, decide action based on global
    // information. Otherwise, no need to take action.
    @Override
    public Optional<Decision> onMemoryUsageChanged(
            int numTotalRequestedBuffers, int currentPoolSize) {
        return numTotalRequestedBuffers < currentPoolSize * releaseThreshold
                ? Optional.of(Decision.NO_ACTION)
                : Optional.empty();
    }

    @Override
    public Decision decideActionWithGlobalInfo(HsSpillingInfoProvider spillingInfoProvider) {
        Decision.Builder builder = Decision.builder();
        checkSpill(spillingInfoProvider, builder);
        checkRelease(spillingInfoProvider, builder);
        return builder.build();
    }

    @Override
    public Decision onResultPartitionClosed(HsSpillingInfoProvider spillingInfoProvider) {
        Decision.Builder builder = Decision.builder();
        for (int subpartitionId = 0;
                subpartitionId < spillingInfoProvider.getNumSubpartitions();
                subpartitionId++) {
            builder.addBufferToSpill(
                            subpartitionId,
                            // get all not start spilling buffers.
                            spillingInfoProvider.getBuffersInOrder(
                                    subpartitionId, SpillStatus.NOT_SPILL, ConsumeStatus.ALL))
                    .addBufferToRelease(
                            subpartitionId,
                            // get all not released buffers.
                            spillingInfoProvider.getBuffersInOrder(
                                    subpartitionId, SpillStatus.ALL, ConsumeStatus.ALL));
        }
        return builder.build();
    }

    private void checkSpill(HsSpillingInfoProvider spillingInfoProvider, Decision.Builder builder) {
        if (spillingInfoProvider.getNumTotalUnSpillBuffers() < numBuffersTriggerSpilling) {
            // In case situation changed since onBufferFinished() returns Optional#empty()
            return;
        }
        // Spill all not spill buffers.
        for (int i = 0; i < spillingInfoProvider.getNumSubpartitions(); i++) {
            builder.addBufferToSpill(
                    i,
                    spillingInfoProvider.getBuffersInOrder(
                            i, SpillStatus.NOT_SPILL, ConsumeStatus.ALL));
        }
    }

    private void checkRelease(
            HsSpillingInfoProvider spillingInfoProvider, Decision.Builder builder) {
        if (spillingInfoProvider.getNumTotalRequestedBuffers()
                < spillingInfoProvider.getPoolSize() * releaseThreshold) {
            // In case situation changed since onMemoryUsageChanged() returns Optional#empty()
            return;
        }

        int releaseNum = (int) (spillingInfoProvider.getPoolSize() * releaseBufferRatio);

        // first, release all consumed buffers
        TreeMap<Integer, Deque<BufferIndexAndChannel>> consumedBuffersToRelease = new TreeMap<>();
        int numConsumedBuffers = 0;
        for (int subpartitionId = 0;
                subpartitionId < spillingInfoProvider.getNumSubpartitions();
                subpartitionId++) {

            Deque<BufferIndexAndChannel> consumedSpillSubpartitionBuffers =
                    spillingInfoProvider.getBuffersInOrder(
                            subpartitionId, SpillStatus.SPILL, ConsumeStatus.CONSUMED);
            numConsumedBuffers += consumedSpillSubpartitionBuffers.size();
            consumedBuffersToRelease.put(subpartitionId, consumedSpillSubpartitionBuffers);
        }

        // make up the releaseNum with unconsumed buffers, if needed, w.r.t. the consuming priority
        TreeMap<Integer, List<BufferIndexAndChannel>> unconsumedBufferToRelease = new TreeMap<>();
        if (releaseNum > numConsumedBuffers) {
            TreeMap<Integer, Deque<BufferIndexAndChannel>> unconsumedBuffers = new TreeMap<>();
            for (int subpartitionId = 0;
                    subpartitionId < spillingInfoProvider.getNumSubpartitions();
                    subpartitionId++) {
                unconsumedBuffers.put(
                        subpartitionId,
                        spillingInfoProvider.getBuffersInOrder(
                                subpartitionId, SpillStatus.SPILL, ConsumeStatus.NOT_CONSUMED));
            }
            unconsumedBufferToRelease.putAll(
                    getBuffersByConsumptionPriorityInOrder(
                            spillingInfoProvider.getNextBufferIndexToConsume(),
                            unconsumedBuffers,
                            releaseNum - numConsumedBuffers));
        }

        // collect results in order
        for (int i = 0; i < spillingInfoProvider.getNumSubpartitions(); i++) {
            List<BufferIndexAndChannel> toRelease = new ArrayList<>();
            toRelease.addAll(consumedBuffersToRelease.getOrDefault(i, new ArrayDeque<>()));
            toRelease.addAll(unconsumedBufferToRelease.getOrDefault(i, new ArrayList<>()));
            builder.addBufferToRelease(i, toRelease);
        }
    }
}
