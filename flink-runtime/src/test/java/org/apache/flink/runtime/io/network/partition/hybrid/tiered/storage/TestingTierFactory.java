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

import org.apache.flink.runtime.io.disk.BatchShuffleReadBufferPool;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TieredStorageNettyService;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierConsumerAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierMasterAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierProducerAgent;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/** Test implementation for {@link TierFactory}. */
public class TestingTierFactory implements TierFactory {

    private final Supplier<TierMasterAgent> tierMasterAgentSupplier;

    private final Supplier<TierProducerAgent> tierProducerAgentSupplier;

    private final BiFunction<
                    List<TieredStorageConsumerSpec>, TieredStorageNettyService, TierConsumerAgent>
            tierConsumerAgentSupplier;

    private TestingTierFactory(
            Supplier<TierMasterAgent> tierMasterAgentSupplier,
            Supplier<TierProducerAgent> tierProducerAgentSupplier,
            BiFunction<
                            List<TieredStorageConsumerSpec>,
                            TieredStorageNettyService,
                            TierConsumerAgent>
                    tierConsumerAgentSupplier) {
        this.tierMasterAgentSupplier = tierMasterAgentSupplier;
        this.tierProducerAgentSupplier = tierProducerAgentSupplier;
        this.tierConsumerAgentSupplier = tierConsumerAgentSupplier;
    }

    @Override
    public TierMasterAgent createMasterAgent(
            TieredStorageResourceRegistry tieredStorageResourceRegistry) {
        return tierMasterAgentSupplier.get();
    }

    @Override
    public TierProducerAgent createProducerAgent(
            int numSubpartitions,
            TieredStoragePartitionId partitionID,
            String dataFileBasePath,
            boolean isBroadcastOnly,
            TieredStorageMemoryManager storageMemoryManager,
            TieredStorageNettyService nettyService,
            TieredStorageResourceRegistry resourceRegistry,
            BatchShuffleReadBufferPool bufferPool,
            ScheduledExecutorService ioExecutor,
            int maxRequestedBuffers,
            Duration bufferRequestTimeout,
            int maxBufferReadAhead) {
        return tierProducerAgentSupplier.get();
    }

    @Override
    public TierConsumerAgent createConsumerAgent(
            List<TieredStorageConsumerSpec> tieredStorageConsumerSpecs,
            TieredStorageNettyService nettyService) {
        return tierConsumerAgentSupplier.apply(tieredStorageConsumerSpecs, nettyService);
    }

    /** Builder for {@link TestingTierFactory}. */
    public static class Builder {

        private Supplier<TierMasterAgent> tierMasterAgentSupplier = () -> null;

        private Supplier<TierProducerAgent> tierProducerAgentSupplier = () -> null;

        private BiFunction<
                        List<TieredStorageConsumerSpec>,
                        TieredStorageNettyService,
                        TierConsumerAgent>
                tierConsumerAgentSupplier = (partitionIdAndSubpartitionId, nettyService) -> null;

        public Builder() {}

        public Builder setTierMasterAgentSupplier(
                Supplier<TierMasterAgent> tierMasterAgentSupplier) {
            this.tierMasterAgentSupplier = tierMasterAgentSupplier;
            return this;
        }

        public Builder setTierProducerAgentSupplier(
                Supplier<TierProducerAgent> tierProducerAgentSupplier) {
            this.tierProducerAgentSupplier = tierProducerAgentSupplier;
            return this;
        }

        public Builder setTierConsumerAgentSupplier(
                BiFunction<
                                List<TieredStorageConsumerSpec>,
                                TieredStorageNettyService,
                                TierConsumerAgent>
                        tierConsumerAgentSupplier) {
            this.tierConsumerAgentSupplier = tierConsumerAgentSupplier;
            return this;
        }

        public TestingTierFactory build() {
            return new TestingTierFactory(
                    tierMasterAgentSupplier, tierProducerAgentSupplier, tierConsumerAgentSupplier);
        }
    }
}
