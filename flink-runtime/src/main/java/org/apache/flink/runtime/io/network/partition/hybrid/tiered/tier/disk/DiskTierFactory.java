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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.disk;

import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.PartitionFileReader;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.PartitionFileWriter;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TieredStorageNettyService;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageConsumerSpec;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMemoryManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageResourceRegistry;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.NoOpMasterAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierConsumerAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierMasterAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierProducerAgent;

import java.util.List;

/** The implementation of {@link TierFactory} for disk tier. */
public class DiskTierFactory implements TierFactory {

    private final int numBytesPerSegment;

    private final int bufferSizeBytes;

    private final float minReservedDiskSpaceFraction;

    public DiskTierFactory(
            int numBytesPerSegment, int bufferSizeBytes, float minReservedDiskSpaceFraction) {
        this.numBytesPerSegment = numBytesPerSegment;
        this.bufferSizeBytes = bufferSizeBytes;
        this.minReservedDiskSpaceFraction = minReservedDiskSpaceFraction;
    }

    @Override
    public TierMasterAgent createMasterAgent(TieredStorageResourceRegistry resourceRegistry) {
        return NoOpMasterAgent.INSTANCE;
    }

    @Override
    public TierProducerAgent createProducerAgent(
            int numSubpartitions,
            TieredStoragePartitionId partitionId,
            String dataFileBasePath,
            boolean isBroadcastOnly,
            PartitionFileWriter partitionFileWriter,
            PartitionFileReader partitionFileReader,
            TieredStorageMemoryManager storageMemoryManager,
            TieredStorageNettyService nettyService,
            TieredStorageResourceRegistry resourceRegistry) {
        return new DiskTierProducerAgent(
                partitionId,
                numSubpartitions,
                numBytesPerSegment,
                bufferSizeBytes,
                dataFileBasePath,
                minReservedDiskSpaceFraction,
                isBroadcastOnly,
                partitionFileWriter,
                storageMemoryManager,
                nettyService,
                resourceRegistry);
    }

    @Override
    public TierConsumerAgent createConsumerAgent(
            List<TieredStorageConsumerSpec> tieredStorageConsumerSpecs,
            TieredStorageNettyService nettyService) {
        // TODO, create the disk tier consumer agent.
        return null;
    }
}
