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

package org.apache.flink.runtime.scheduler.adaptivebatch;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.disk.BatchShuffleReadBufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TieredStorageNettyService;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageConsumerSpec;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMemoryManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMemorySpec;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageResourceRegistry;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierConsumerAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierMasterAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierProducerAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierShuffleDescriptor;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierShuffleHandler;

import javax.annotation.Nullable;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

/** Dummy {@link TierFactory} for testing purpose only. */
public class DummyTierFactory implements TierFactory {
    @Override
    public void setup(Configuration configuration) {}

    @Override
    public TieredStorageMemorySpec getMasterAgentMemorySpec() {
        return null;
    }

    @Override
    public TieredStorageMemorySpec getProducerAgentMemorySpec() {
        return null;
    }

    @Override
    public TieredStorageMemorySpec getConsumerAgentMemorySpec() {
        return null;
    }

    @Override
    public TierMasterAgent createMasterAgent(
            TieredStorageResourceRegistry tieredStorageResourceRegistry) {
        return new DummyTierMasterAgent();
    }

    @Override
    public TierProducerAgent createProducerAgent(
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
            int maxRequestedBuffer,
            @Nullable BufferCompressor bufferCompressor) {
        return null;
    }

    @Override
    public TierConsumerAgent createConsumerAgent(
            List<TieredStorageConsumerSpec> tieredStorageConsumerSpecs,
            List<TierShuffleDescriptor> shuffleDescriptors,
            TieredStorageNettyService nettyService) {
        return null;
    }

    @Override
    public String identifier() {
        return "dummy";
    }

    public static class DummyTierMasterAgent implements TierMasterAgent {

        @Override
        public void registerJob(JobID jobID, TierShuffleHandler tierShuffleHandler) {}

        @Override
        public void unregisterJob(JobID jobID) {}

        @Override
        public TierShuffleDescriptor addPartitionAndGetShuffleDescriptor(
                JobID jobID, int numSubpartitions, ResultPartitionID resultPartitionID) {
            return null;
        }

        @Override
        public void releasePartition(TierShuffleDescriptor shuffleDescriptor) {}

        @Override
        public void close() {}
    }
}
