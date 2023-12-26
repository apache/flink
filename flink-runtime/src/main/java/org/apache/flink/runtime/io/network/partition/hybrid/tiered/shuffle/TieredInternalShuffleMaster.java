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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageConfiguration;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMasterClient;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageResourceRegistry;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierMasterAgent;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.configuration.NettyShuffleEnvironmentOptions.NETWORK_HYBRID_SHUFFLE_REMOTE_STORAGE_BASE_PATH;
import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageIdMappingUtils.convertId;

/**
 * A wrapper internal shuffle master class for tiered storage. All the tiered storage operations
 * with the shuffle master should be wrapped in this class.
 */
public class TieredInternalShuffleMaster {

    private final TieredStorageMasterClient tieredStorageMasterClient;

    public TieredInternalShuffleMaster(Configuration conf) {
        TieredStorageConfiguration tieredStorageConfiguration =
                TieredStorageConfiguration.builder(
                                conf.getString(NETWORK_HYBRID_SHUFFLE_REMOTE_STORAGE_BASE_PATH))
                        .build();
        TieredStorageResourceRegistry resourceRegistry = new TieredStorageResourceRegistry();
        List<TierMasterAgent> tierFactories =
                tieredStorageConfiguration.getTierFactories().stream()
                        .map(tierFactory -> tierFactory.createMasterAgent(resourceRegistry))
                        .collect(Collectors.toList());
        this.tieredStorageMasterClient = new TieredStorageMasterClient(tierFactories);
    }

    public void addPartition(ResultPartitionID resultPartitionID) {
        tieredStorageMasterClient.addPartition(convertId(resultPartitionID));
    }

    public void releasePartition(ResultPartitionID resultPartitionID) {
        tieredStorageMasterClient.releasePartition(convertId(resultPartitionID));
    }
}
