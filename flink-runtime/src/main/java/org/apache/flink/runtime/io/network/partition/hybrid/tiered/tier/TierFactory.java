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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.disk.BatchShuffleReadBufferPool;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TieredStorageNettyService;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageConsumerSpec;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMemoryManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMemorySpec;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageResourceRegistry;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

/** A factory that creates all the components of a tier. */
public interface TierFactory {

    /** Sets up the tier factory based on the {@link Configuration}. */
    void setup(Configuration configuration);

    /** Get the {@link TieredStorageMemorySpec} of the master-side agent. */
    TieredStorageMemorySpec getMasterAgentMemorySpec();

    /** Get the {@link TieredStorageMemorySpec} of the producer-side agent. */
    TieredStorageMemorySpec getProducerAgentMemorySpec();

    /** Get the {@link TieredStorageMemorySpec} of the consumer-side agent. */
    TieredStorageMemorySpec getConsumerAgentMemorySpec();

    /** Creates the master-side agent of a Tier. */
    TierMasterAgent createMasterAgent(TieredStorageResourceRegistry tieredStorageResourceRegistry);

    /** Creates the producer-side agent of a Tier. */
    TierProducerAgent createProducerAgent(
            int numPartitions,
            int numSubpartitions,
            TieredStoragePartitionId partitionID,
            String dataFileBasePath,
            boolean isBroadcastOnly,
            TieredStorageMemoryManager storageMemoryManager,
            TieredStorageNettyService nettyService,
            TieredStorageResourceRegistry resourceRegistry,
            BatchShuffleReadBufferPool bufferPool,
            ScheduledExecutorService ioExecutor,
            List<TierShuffleDescriptor> shuffleDescriptors,
            int maxRequestedBuffer);

    /** Creates the consumer-side agent of a Tier. */
    TierConsumerAgent createConsumerAgent(
            List<TieredStorageConsumerSpec> tieredStorageConsumerSpecs,
            List<TierShuffleDescriptor> shuffleDescriptors,
            TieredStorageNettyService nettyService);
}
