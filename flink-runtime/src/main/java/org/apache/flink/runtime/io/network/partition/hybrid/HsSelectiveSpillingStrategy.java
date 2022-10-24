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
import org.apache.flink.runtime.io.network.partition.hybrid.HsSpillingInfoProvider.ConsumeStatusWithId;
import org.apache.flink.runtime.io.network.partition.hybrid.HsSpillingInfoProvider.SpillStatus;

import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.TreeMap;

import static org.apache.flink.runtime.io.network.partition.hybrid.HsSpillingStrategyUtils.getBuffersByConsumptionPriorityInOrder;

/**
 * A special implementation of {@link HsSpillingStrategy} that reduce disk writes as much as
 * possible.
 */
public class HsSelectiveSpillingStrategy implements HsSpillingStrategy {
    private final float spillBufferRatio;

    private final float spillThreshold;

    public HsSelectiveSpillingStrategy(HybridShuffleConfiguration hybridShuffleConfiguration) {
        spillThreshold = hybridShuffleConfiguration.getSelectiveStrategySpillThreshold();
        spillBufferRatio = hybridShuffleConfiguration.getSelectiveStrategySpillBufferRatio();
    }

    // For the case of buffer finished, there is no need to take action for
    // HsSelectiveSpillingStrategy.
    @Override
    public Optional<Decision> onBufferFinished(int numTotalUnSpillBuffers, int currentPoolSize) {
        return Optional.of(Decision.NO_ACTION);
    }

    // For the case of buffer consumed, this buffer need release. The control of the buffer is taken
    // over by the downstream task.
    @Override
    public Optional<Decision> onBufferConsumed(BufferIndexAndChannel consumedBuffer) {
        return Optional.of(Decision.builder().addBufferToRelease(consumedBuffer).build());
    }

    // When the amount of memory used exceeds the threshold, decide action based on global
    // information. Otherwise, no need to take action.
    @Override
    public Optional<Decision> onMemoryUsageChanged(
            int numTotalRequestedBuffers, int currentPoolSize) {
        return numTotalRequestedBuffers < currentPoolSize * spillThreshold
                ? Optional.of(Decision.NO_ACTION)
                : Optional.empty();
    }

    // Score the buffer of each subpartition and decide the spill and release action. The lower the
    // score, the more likely the buffer will be consumed in the next time, and should be kept in
    // memory as much as possible. Select all buffers that need to be spilled according to the score
    // from high to low.
    @Override
    public Decision decideActionWithGlobalInfo(HsSpillingInfoProvider spillingInfoProvider) {
        if (spillingInfoProvider.getNumTotalRequestedBuffers()
                < spillingInfoProvider.getPoolSize() * spillThreshold) {
            // In case situation changed since onMemoryUsageChanged() returns Optional#empty()
            return Decision.NO_ACTION;
        }

        int spillNum = (int) (spillingInfoProvider.getPoolSize() * spillBufferRatio);

        TreeMap<Integer, Deque<BufferIndexAndChannel>> subpartitionToBuffers = new TreeMap<>();
        for (int channel = 0; channel < spillingInfoProvider.getNumSubpartitions(); channel++) {
            subpartitionToBuffers.put(
                    channel,
                    spillingInfoProvider.getBuffersInOrder(
                            channel,
                            SpillStatus.NOT_SPILL,
                            // selective spilling strategy does not support multiple consumer.
                            ConsumeStatusWithId.fromStatusAndConsumerId(
                                    ConsumeStatus.NOT_CONSUMED, HsConsumerId.DEFAULT)));
        }

        TreeMap<Integer, List<BufferIndexAndChannel>> subpartitionToHighPriorityBuffers =
                getBuffersByConsumptionPriorityInOrder(
                        // selective spilling strategy does not support multiple consumer.
                        spillingInfoProvider.getNextBufferIndexToConsume(HsConsumerId.DEFAULT),
                        subpartitionToBuffers,
                        spillNum);

        Decision.Builder builder = Decision.builder();
        subpartitionToHighPriorityBuffers.forEach(
                (subpartitionId, buffers) -> {
                    builder.addBufferToSpill(subpartitionId, buffers);
                    builder.addBufferToRelease(subpartitionId, buffers);
                });
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
                                    subpartitionId,
                                    SpillStatus.NOT_SPILL,
                                    ConsumeStatusWithId.ALL_ANY))
                    .addBufferToRelease(
                            subpartitionId,
                            // get all not released buffers.
                            spillingInfoProvider.getBuffersInOrder(
                                    subpartitionId, SpillStatus.ALL, ConsumeStatusWithId.ALL_ANY));
        }
        return builder.build();
    }
}
