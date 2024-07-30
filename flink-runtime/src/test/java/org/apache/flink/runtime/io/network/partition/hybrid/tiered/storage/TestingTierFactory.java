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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.disk.BatchShuffleReadBufferPool;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TieredStorageNettyService;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierConsumerAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierMasterAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierProducerAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierShuffleDescriptor;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;

/** Test implementation for {@link TierFactory}. */
public class TestingTierFactory implements TierFactory {

    private Consumer<Configuration> setupConsumer;

    private Supplier<TieredStorageMemorySpec> masterAgentMemorySpecSupplier;

    private Supplier<TieredStorageMemorySpec> producerAgentMemorySpecSupplier;

    private Supplier<TieredStorageMemorySpec> consumerAgentMemorySpecSupplier;

    private Supplier<TierMasterAgent> tierMasterAgentSupplier;

    private Supplier<TierProducerAgent> tierProducerAgentSupplier;

    private BiFunction<
                    List<TieredStorageConsumerSpec>, TieredStorageNettyService, TierConsumerAgent>
            tierConsumerAgentSupplier;

    private TestingTierFactory(
            Consumer<Configuration> setupConsumer,
            Supplier<TieredStorageMemorySpec> masterAgentMemorySpecSupplier,
            Supplier<TieredStorageMemorySpec> producerAgentMemorySpecSupplier,
            Supplier<TieredStorageMemorySpec> consumerAgentMemorySpecSupplier,
            Supplier<TierMasterAgent> tierMasterAgentSupplier,
            Supplier<TierProducerAgent> tierProducerAgentSupplier,
            BiFunction<
                            List<TieredStorageConsumerSpec>,
                            TieredStorageNettyService,
                            TierConsumerAgent>
                    tierConsumerAgentSupplier) {
        this.setupConsumer = setupConsumer;
        this.masterAgentMemorySpecSupplier = masterAgentMemorySpecSupplier;
        this.producerAgentMemorySpecSupplier = producerAgentMemorySpecSupplier;
        this.consumerAgentMemorySpecSupplier = consumerAgentMemorySpecSupplier;
        this.tierMasterAgentSupplier = tierMasterAgentSupplier;
        this.tierProducerAgentSupplier = tierProducerAgentSupplier;
        this.tierConsumerAgentSupplier = tierConsumerAgentSupplier;
    }

    public TestingTierFactory() {}

    @Override
    public void setup(Configuration configuration) {
        setupConsumer.accept(configuration);
    }

    @Override
    public TieredStorageMemorySpec getMasterAgentMemorySpec() {
        return masterAgentMemorySpecSupplier.get();
    }

    @Override
    public TieredStorageMemorySpec getProducerAgentMemorySpec() {
        return producerAgentMemorySpecSupplier.get();
    }

    @Override
    public TieredStorageMemorySpec getConsumerAgentMemorySpec() {
        return consumerAgentMemorySpecSupplier.get();
    }

    @Override
    public TierMasterAgent createMasterAgent(
            TieredStorageResourceRegistry tieredStorageResourceRegistry) {
        return tierMasterAgentSupplier.get();
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
            int maxRequestedBuffers) {
        return tierProducerAgentSupplier.get();
    }

    @Override
    public TierConsumerAgent createConsumerAgent(
            List<TieredStorageConsumerSpec> tieredStorageConsumerSpecs,
            List<TierShuffleDescriptor> shuffleDescriptors,
            TieredStorageNettyService nettyService) {
        return tierConsumerAgentSupplier.apply(tieredStorageConsumerSpecs, nettyService);
    }

    /** Builder for {@link TestingTierFactory}. */
    public static class Builder {

        private Consumer<Configuration> setupConsumer = conf -> {};

        private Supplier<TieredStorageMemorySpec> masterAgentMemorySpecSupplier = () -> null;

        private Supplier<TieredStorageMemorySpec> producerAgentMemorySpecSupplier = () -> null;

        private Supplier<TieredStorageMemorySpec> consumerAgentMemorySpecSupplier = () -> null;

        private Supplier<TierMasterAgent> tierMasterAgentSupplier = () -> null;

        private Supplier<TierProducerAgent> tierProducerAgentSupplier = () -> null;

        private BiFunction<
                        List<TieredStorageConsumerSpec>,
                        TieredStorageNettyService,
                        TierConsumerAgent>
                tierConsumerAgentSupplier = (partitionIdAndSubpartitionId, nettyService) -> null;

        public Builder() {}

        public Builder setSetupConsumer(Consumer<Configuration> setupConsumer) {
            this.setupConsumer = setupConsumer;
            return this;
        }

        public Builder setMasterAgentMemorySpecSupplier(
                Supplier<TieredStorageMemorySpec> masterAgentMemorySpecSupplier) {
            this.masterAgentMemorySpecSupplier = masterAgentMemorySpecSupplier;
            return this;
        }

        public Builder setProducerAgentMemorySpecSupplier(
                Supplier<TieredStorageMemorySpec> producerAgentMemorySpecSupplier) {
            this.producerAgentMemorySpecSupplier = producerAgentMemorySpecSupplier;
            return this;
        }

        public Builder setConsumerAgentMemorySpecSupplier(
                Supplier<TieredStorageMemorySpec> consumerAgentMemorySpecSupplier) {
            this.consumerAgentMemorySpecSupplier = consumerAgentMemorySpecSupplier;
            return this;
        }

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
                    setupConsumer,
                    masterAgentMemorySpecSupplier,
                    producerAgentMemorySpecSupplier,
                    consumerAgentMemorySpecSupplier,
                    tierMasterAgentSupplier,
                    tierProducerAgentSupplier,
                    tierConsumerAgentSupplier);
        }
    }
}
