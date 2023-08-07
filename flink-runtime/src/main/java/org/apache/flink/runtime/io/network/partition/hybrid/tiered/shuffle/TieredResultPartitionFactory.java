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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.shuffle;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.io.disk.BatchShuffleReadBufferPool;
import org.apache.flink.runtime.io.disk.FileChannelManager;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageConfiguration;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageIdMappingUtils;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TieredStorageNettyServiceImpl;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.BufferAccumulator;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.HashBufferAccumulator;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.SortBufferAccumulator;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMemoryManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMemoryManagerImpl;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMemorySpec;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageProducerClient;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageResourceRegistry;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierProducerAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.memory.MemoryTierFactory;
import org.apache.flink.util.function.SupplierWithException;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

/** {@link TieredResultPartitionFactory} contains the components to set up tiered storage. */
public class TieredResultPartitionFactory {

    private final TieredStorageConfiguration tieredStorageConfiguration;
    private final TieredStorageNettyServiceImpl tieredStorageNettyService;
    private final TieredStorageResourceRegistry tieredStorageResourceRegistry;

    public TieredResultPartitionFactory(
            TieredStorageConfiguration tieredStorageConfiguration,
            TieredStorageNettyServiceImpl tieredStorageNettyService,
            TieredStorageResourceRegistry tieredStorageResourceRegistry) {
        this.tieredStorageConfiguration = tieredStorageConfiguration;
        this.tieredStorageNettyService = tieredStorageNettyService;
        this.tieredStorageResourceRegistry = tieredStorageResourceRegistry;
    }

    public TieredStorageConfiguration getTieredStorageConfiguration() {
        return tieredStorageConfiguration;
    }

    public TieredStorageNettyServiceImpl getTieredStorageNettyService() {
        return tieredStorageNettyService;
    }

    public TieredStorageResourceRegistry getTieredStorageResourceRegistry() {
        return tieredStorageResourceRegistry;
    }

    public TieredResultPartition createTieredResultPartition(
            String owningTaskName,
            int partitionIndex,
            ResultPartitionID partitionId,
            ResultPartitionType partitionType,
            int numSubpartitions,
            int maxParallelism,
            Boolean isBroadCastOnly,
            ResultPartitionManager partitionManager,
            @Nullable BufferCompressor bufferCompressor,
            SupplierWithException<BufferPool, IOException> bufferPoolFactory,
            FileChannelManager fileChannelManager,
            BatchShuffleReadBufferPool batchShuffleReadBufferPool,
            ScheduledExecutorService batchShuffleReadIOExecutor) {

        // Create memory manager.
        TieredStorageMemoryManager memoryManager =
                new TieredStorageMemoryManagerImpl(
                        tieredStorageConfiguration.getNumBuffersTriggerFlushRatio(), true);

        // Create buffer accumulator.
        int numAccumulatorExclusiveBuffers =
                tieredStorageConfiguration.getAccumulatorExclusiveBuffers();
        BufferAccumulator bufferAccumulator =
                createBufferAccumulator(
                        numSubpartitions, numAccumulatorExclusiveBuffers, memoryManager);

        // Create producer agents and memory specs.
        Tuple2<List<TierProducerAgent>, List<TieredStorageMemorySpec>>
                producerAgentsAndMemorySpecs =
                        createTierProducerAgentsAndMemorySpecs(
                                numSubpartitions,
                                isBroadCastOnly,
                                TieredStorageIdMappingUtils.convertId(partitionId),
                                memoryManager,
                                bufferAccumulator,
                                partitionType == ResultPartitionType.HYBRID_SELECTIVE,
                                fileChannelManager,
                                batchShuffleReadBufferPool,
                                batchShuffleReadIOExecutor);

        // Create producer client.
        TieredStorageProducerClient tieredStorageProducerClient =
                new TieredStorageProducerClient(
                        numSubpartitions,
                        isBroadCastOnly,
                        bufferAccumulator,
                        bufferCompressor,
                        producerAgentsAndMemorySpecs.f0);

        // Create tiered result partition.
        return new TieredResultPartition(
                owningTaskName,
                partitionIndex,
                partitionId,
                partitionType,
                numSubpartitions,
                maxParallelism,
                partitionManager,
                bufferCompressor,
                bufferPoolFactory,
                tieredStorageProducerClient,
                tieredStorageResourceRegistry,
                tieredStorageNettyService,
                producerAgentsAndMemorySpecs.f1,
                memoryManager);
    }

    private BufferAccumulator createBufferAccumulator(
            int numSubpartitions,
            int numAccumulatorExclusiveBuffers,
            TieredStorageMemoryManager storageMemoryManager) {
        int bufferSize = tieredStorageConfiguration.getTieredStorageBufferSize();
        return (numSubpartitions + 1) > numAccumulatorExclusiveBuffers
                ? new SortBufferAccumulator(
                        numSubpartitions,
                        numAccumulatorExclusiveBuffers,
                        bufferSize,
                        storageMemoryManager)
                : new HashBufferAccumulator(numSubpartitions, bufferSize, storageMemoryManager);
    }

    private Tuple2<List<TierProducerAgent>, List<TieredStorageMemorySpec>>
            createTierProducerAgentsAndMemorySpecs(
                    int numberOfSubpartitions,
                    boolean isBroadcastOnly,
                    TieredStoragePartitionId partitionID,
                    TieredStorageMemoryManager memoryManager,
                    BufferAccumulator bufferAccumulator,
                    boolean isHybridSelective,
                    FileChannelManager fileChannelManager,
                    BatchShuffleReadBufferPool batchShuffleReadBufferPool,
                    ScheduledExecutorService batchShuffleReadIOExecutor) {

        List<TierProducerAgent> tierProducerAgents = new ArrayList<>();
        List<TieredStorageMemorySpec> tieredStorageMemorySpecs = new ArrayList<>();

        tieredStorageMemorySpecs.add(
                new TieredStorageMemorySpec(
                        bufferAccumulator,
                        2
                                * Math.min(
                                        numberOfSubpartitions + 1,
                                        tieredStorageConfiguration
                                                .getAccumulatorExclusiveBuffers())));
        List<Integer> tierExclusiveBuffers =
                tieredStorageConfiguration.getEachTierExclusiveBufferNum();

        List<TierFactory> tierFactories = tieredStorageConfiguration.getTierFactories();
        for (int index = 0; index < tierFactories.size(); ++index) {
            TierFactory tierFactory = tierFactories.get(index);
            if (!isHybridSelective && tierFactory.getClass() == MemoryTierFactory.class) {
                continue;
            }
            TierProducerAgent producerAgent =
                    tierFactory.createProducerAgent(
                            numberOfSubpartitions,
                            partitionID,
                            fileChannelManager.createChannel().getPath(),
                            isBroadcastOnly,
                            memoryManager,
                            tieredStorageNettyService,
                            tieredStorageResourceRegistry,
                            batchShuffleReadBufferPool,
                            batchShuffleReadIOExecutor,
                            Math.max(
                                    2 * batchShuffleReadBufferPool.getNumBuffersPerRequest(),
                                    numberOfSubpartitions),
                            tieredStorageConfiguration.getDiskIOSchedulerBufferRequestTimeout(),
                            tieredStorageConfiguration.getDiskIOSchedulerMaxBuffersReadAhead());
            tierProducerAgents.add(producerAgent);
            tieredStorageMemorySpecs.add(
                    new TieredStorageMemorySpec(producerAgent, tierExclusiveBuffers.get(index)));
        }
        return Tuple2.of(tierProducerAgents, tieredStorageMemorySpecs);
    }
}
