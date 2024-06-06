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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.common;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.shuffle.TierFactoryInitializer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierFactory;

import java.util.ArrayList;
import java.util.List;

/** Configurations for the Tiered Storage. */
public class TieredStorageConfiguration {

    private static final int DEFAULT_MEMORY_TIER_EXCLUSIVE_BUFFERS = 100;

    private static final int DEFAULT_DISK_TIER_EXCLUSIVE_BUFFERS = 1;

    private static final int DEFAULT_REMOTE_TIER_EXCLUSIVE_BUFFERS = 1;

    private final List<TierFactory> tierFactories;

    private final List<Integer> tierExclusiveBuffers;

    private TieredStorageConfiguration(List<TierFactory> tierFactories) {
        this.tierFactories = tierFactories;
        this.tierExclusiveBuffers = new ArrayList<>();
    }

    /**
     * Get the total exclusive buffer number.
     *
     * @return the total exclusive buffer number.
     */
    public int getTotalExclusiveBufferNum() {
        return tierExclusiveBuffers.stream().mapToInt(Integer::intValue).sum();
    }

    /**
     * Get exclusive buffer number of each tier.
     *
     * @return buffer number of each tier.
     */
    public List<Integer> getEachTierExclusiveBufferNum() {
        return tierExclusiveBuffers;
    }

    public List<TierFactory> getTierFactories() {
        return tierFactories;
    }

    private void setupTierFactoriesAndExclusiveBuffers(Configuration configuration) {
        String remoteStorageBasePath =
                configuration.get(
                        NettyShuffleEnvironmentOptions
                                .NETWORK_HYBRID_SHUFFLE_REMOTE_STORAGE_BASE_PATH);
        tierExclusiveBuffers.add(DEFAULT_MEMORY_TIER_EXCLUSIVE_BUFFERS);
        tierExclusiveBuffers.add(DEFAULT_DISK_TIER_EXCLUSIVE_BUFFERS);
        if (remoteStorageBasePath != null) {
            tierExclusiveBuffers.add(DEFAULT_REMOTE_TIER_EXCLUSIVE_BUFFERS);
        }
    }

    public static TieredStorageConfiguration fromConfiguration(Configuration configuration) {
        TieredStorageConfiguration tieredStorageConfiguration =
                new TieredStorageConfiguration(
                        TierFactoryInitializer.initializeTierFactories(configuration));
        tieredStorageConfiguration.setupTierFactoriesAndExclusiveBuffers(configuration);
        return tieredStorageConfiguration;
    }
}
