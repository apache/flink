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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.memory;

import org.apache.flink.runtime.io.disk.BatchShuffleReadBufferPool;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TieredStorageNettyService;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageConsumerSpec;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMemoryManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageResourceRegistry;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.NoOpMasterAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierConsumerAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierMasterAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierProducerAgent;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

/** The implementation of {@link TierFactory} for memory tier. */
public class MemoryTierFactory implements TierFactory {

    private final int segmentSizeBytes;

    private final int bufferSizeBytes;

    private final int subpartitionMaxQueuedBuffers;

    public MemoryTierFactory(
            int segmentSizeBytes, int bufferSizeBytes, int subpartitionMaxQueuedBuffers) {
        this.segmentSizeBytes = segmentSizeBytes;
        this.bufferSizeBytes = bufferSizeBytes;
        this.subpartitionMaxQueuedBuffers = subpartitionMaxQueuedBuffers;
    }

    @Override
    public TierMasterAgent createMasterAgent(
            TieredStorageResourceRegistry tieredStorageResourceRegistry) {
        return NoOpMasterAgent.INSTANCE;
    }

    @Override
    public TierProducerAgent createProducerAgent(
            int numSubpartitions,
            TieredStoragePartitionId partitionID,
            String dataFileBasePath,
            boolean isBroadcastOnly,
            TieredStorageMemoryManager memoryManager,
            TieredStorageNettyService nettyService,
            TieredStorageResourceRegistry resourceRegistry,
            BatchShuffleReadBufferPool bufferPool,
            ScheduledExecutorService ioExecutor,
            int maxRequestedBuffers,
            Duration bufferRequestTimeout,
            int maxBufferReadAhead) {
        return new MemoryTierProducerAgent(
                partitionID,
                numSubpartitions,
                bufferSizeBytes,
                segmentSizeBytes,
                subpartitionMaxQueuedBuffers,
                isBroadcastOnly,
                memoryManager,
                nettyService,
                resourceRegistry);
    }

    @Override
    public TierConsumerAgent createConsumerAgent(
            List<TieredStorageConsumerSpec> tieredStorageConsumerSpecs,
            TieredStorageNettyService nettyService) {
        return new MemoryTierConsumerAgent(tieredStorageConsumerSpecs, nettyService);
    }
}
