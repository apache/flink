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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionIndexSet;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageInputChannelId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TieredStorageNettyService;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierConsumerAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierShuffleDescriptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkState;

/** {@link TieredStorageConsumerClient} is used to read buffer from tiered store. */
public class TieredStorageConsumerClient {

    private final List<TierFactory> tierFactories;

    private final TieredStorageNettyService nettyService;

    private final List<TierConsumerAgent> tierConsumerAgents;

    /**
     * This map is used to record the consumer agent being used and the id of segment being read for
     * each data source, which is represented by {@link TieredStoragePartitionId} and {@link
     * TieredStorageSubpartitionId}.
     */
    private final Map<
                    TieredStoragePartitionId,
                    Map<TieredStorageSubpartitionId, Tuple2<TierConsumerAgent, Integer>>>
            currentConsumerAgentAndSegmentIds = new HashMap<>();

    public TieredStorageConsumerClient(
            List<TierFactory> tierFactories,
            List<TieredStorageConsumerSpec> tieredStorageConsumerSpecs,
            List<List<TierShuffleDescriptor>> tierShuffleDescriptors,
            TieredStorageNettyService nettyService) {
        this.tierFactories = tierFactories;
        this.nettyService = nettyService;
        this.tierConsumerAgents =
                createTierConsumerAgents(tieredStorageConsumerSpecs, tierShuffleDescriptors);
    }

    public void setup(BufferPool bufferPool) {
        TieredStorageMemoryManagerImpl memoryManager = new TieredStorageMemoryManagerImpl(0, false);
        memoryManager.setup(bufferPool, Collections.emptyList());
        tierConsumerAgents.forEach(tierConsumerAgent -> tierConsumerAgent.setup(memoryManager));
    }

    public void start() {
        for (TierConsumerAgent tierConsumerAgent : tierConsumerAgents) {
            tierConsumerAgent.start();
        }
    }

    /**
     * Returns the index of the subpartition where the next buffer locates, or -1 if there is no
     * buffer available or the subpartition index does not belong to the specified indexSet.
     *
     * @param partitionId The index of the partition which the returned subpartition should belong
     *     to.
     * @param indexSet The indexes of the subpartitions expected.
     */
    public int peekNextBufferSubpartitionId(
            TieredStoragePartitionId partitionId, ResultSubpartitionIndexSet indexSet)
            throws IOException {
        for (TierConsumerAgent tierConsumerAgent : tierConsumerAgents) {
            int subpartitionId =
                    tierConsumerAgent.peekNextBufferSubpartitionId(partitionId, indexSet);
            if (subpartitionId >= 0) {
                return subpartitionId;
            }
        }
        return -1;
    }

    public Optional<Buffer> getNextBuffer(
            TieredStoragePartitionId partitionId, TieredStorageSubpartitionId subpartitionId)
            throws IOException {
        Tuple2<TierConsumerAgent, Integer> currentConsumerAgentAndSegmentId =
                currentConsumerAgentAndSegmentIds
                        .computeIfAbsent(partitionId, ignore -> new HashMap<>())
                        .getOrDefault(subpartitionId, Tuple2.of(null, 0));
        Optional<Buffer> buffer = Optional.empty();
        if (currentConsumerAgentAndSegmentId.f0 == null) {
            for (TierConsumerAgent tierConsumerAgent : tierConsumerAgents) {
                buffer =
                        tierConsumerAgent.getNextBuffer(
                                partitionId, subpartitionId, currentConsumerAgentAndSegmentId.f1);
                if (buffer.isPresent()) {
                    currentConsumerAgentAndSegmentIds
                            .get(partitionId)
                            .put(
                                    subpartitionId,
                                    Tuple2.of(
                                            tierConsumerAgent,
                                            currentConsumerAgentAndSegmentId.f1));
                    break;
                }
            }
        } else {
            buffer =
                    currentConsumerAgentAndSegmentId.f0.getNextBuffer(
                            partitionId, subpartitionId, currentConsumerAgentAndSegmentId.f1);
        }
        if (!buffer.isPresent()) {
            return Optional.empty();
        }
        Buffer bufferData = buffer.get();
        if (bufferData.getDataType() == Buffer.DataType.END_OF_SEGMENT) {
            currentConsumerAgentAndSegmentIds
                    .get(partitionId)
                    .put(subpartitionId, Tuple2.of(null, currentConsumerAgentAndSegmentId.f1 + 1));
            bufferData.recycleBuffer();
            return getNextBuffer(partitionId, subpartitionId);
        }
        return Optional.of(bufferData);
    }

    public void registerAvailabilityNotifier(AvailabilityNotifier notifier) {
        for (TierConsumerAgent tierConsumerAgent : tierConsumerAgents) {
            tierConsumerAgent.registerAvailabilityNotifier(notifier);
        }
    }

    public void updateTierShuffleDescriptors(
            TieredStoragePartitionId partitionId,
            TieredStorageInputChannelId inputChannelId,
            TieredStorageSubpartitionId subpartitionId,
            List<TierShuffleDescriptor> tierShuffleDescriptors) {
        checkState(tierShuffleDescriptors.size() == tierConsumerAgents.size());
        for (int i = 0; i < tierShuffleDescriptors.size(); i++) {
            tierConsumerAgents
                    .get(i)
                    .updateTierShuffleDescriptor(
                            partitionId,
                            inputChannelId,
                            subpartitionId,
                            tierShuffleDescriptors.get(i));
        }
    }

    public void close() throws IOException {
        for (TierConsumerAgent tierConsumerAgent : tierConsumerAgents) {
            tierConsumerAgent.close();
        }
    }

    // --------------------------------------------------------------------------------------------
    //  Internal methods
    // --------------------------------------------------------------------------------------------

    private List<TierConsumerAgent> createTierConsumerAgents(
            List<TieredStorageConsumerSpec> tieredStorageConsumerSpecs,
            List<List<TierShuffleDescriptor>> shuffleDescriptors) {
        ArrayList<TierConsumerAgent> tierConsumerAgents = new ArrayList<>();

        List<List<TierShuffleDescriptor>> transformedTierShuffleDescriptors =
                transformTierShuffleDescriptors(shuffleDescriptors);
        // Each tier only requires one inner list of transformedTierShuffleDescriptors, so the size
        // of transformedTierShuffleDescriptors and the size of tierFactories are the same.
        checkState(transformedTierShuffleDescriptors.size() == tierFactories.size());
        for (int i = 0; i < tierFactories.size(); i++) {
            tierConsumerAgents.add(
                    tierFactories
                            .get(i)
                            .createConsumerAgent(
                                    tieredStorageConsumerSpecs,
                                    transformedTierShuffleDescriptors.get(i),
                                    nettyService));
        }
        return tierConsumerAgents;
    }

    /**
     * Before transforming the shuffle descriptors, the number of tier shuffle descriptors is
     * numPartitions * numTiers (That means shuffleDescriptors.size() is numPartitions, while the
     * shuffleDescriptors.get(0).size() is numTiers). After transforming, the number of tier shuffle
     * descriptors is numTiers * numPartitions (That means transformedList.size() is numTiers, while
     * transformedList.get(0).size() is numPartitions).
     */
    private static List<List<TierShuffleDescriptor>> transformTierShuffleDescriptors(
            List<List<TierShuffleDescriptor>> shuffleDescriptors) {
        int numTiers = 0;
        int numPartitions = shuffleDescriptors.size();
        for (List<TierShuffleDescriptor> tierShuffleDescriptors : shuffleDescriptors) {
            if (numTiers == 0) {
                numTiers = tierShuffleDescriptors.size();
            }
            checkState(numTiers == tierShuffleDescriptors.size());
        }

        List<List<TierShuffleDescriptor>> transformedList = new ArrayList<>();
        for (int i = 0; i < numTiers; i++) {
            List<TierShuffleDescriptor> innerList = new ArrayList<>();
            for (int j = 0; j < numPartitions; j++) {
                innerList.add(shuffleDescriptors.get(j).get(i));
            }
            transformedList.add(innerList);
        }
        return transformedList;
    }
}
