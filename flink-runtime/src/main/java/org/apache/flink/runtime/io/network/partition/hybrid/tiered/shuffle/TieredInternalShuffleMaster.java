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

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageConfiguration;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageIdMappingUtils;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMasterClient;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageResourceRegistry;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierMasterAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierShuffleDescriptor;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierShuffleHandler;
import org.apache.flink.runtime.shuffle.JobShuffleContext;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleMasterContext;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * A wrapper internal shuffle master class for tiered storage. All the tiered storage operations
 * with the shuffle master should be wrapped in this class.
 */
public class TieredInternalShuffleMaster {

    private final TieredStorageMasterClient tieredStorageMasterClient;

    private final ShuffleMasterContext shuffleMasterContext;

    public TieredInternalShuffleMaster(ShuffleMasterContext shuffleMasterContext) {
        this.shuffleMasterContext = shuffleMasterContext;
        Configuration conf = shuffleMasterContext.getConfiguration();
        TieredStorageConfiguration tieredStorageConfiguration =
                TieredStorageConfiguration.fromConfiguration(conf);
        TieredStorageResourceRegistry resourceRegistry = new TieredStorageResourceRegistry();
        List<TierMasterAgent> tierFactories =
                tieredStorageConfiguration.getTierFactories().stream()
                        .map(tierFactory -> tierFactory.createMasterAgent(resourceRegistry))
                        .collect(Collectors.toList());
        this.tieredStorageMasterClient = new TieredStorageMasterClient(tierFactories);
    }

    /**
     * Registers the target job together with the corresponding {@link JobShuffleContext} to this
     * shuffle master.
     */
    public void registerJob(JobShuffleContext context) {
        tieredStorageMasterClient.registerJob(context.getJobId(), getTierShuffleHandler(context));
    }

    /**
     * Unregisters the target job from this shuffle master, which means the corresponding job has
     * reached a global termination state and all the allocated resources except for the cluster
     * partitions can be cleared.
     *
     * @param jobID ID of the target job to be unregistered.
     */
    public void unregisterJob(JobID jobID) {
        tieredStorageMasterClient.unregisterJob(jobID);
    }

    public List<TierShuffleDescriptor> addPartitionAndGetShuffleDescriptor(
            JobID jobID, ResultPartitionID resultPartitionID) {
        return tieredStorageMasterClient.addPartitionAndGetShuffleDescriptor(
                jobID, resultPartitionID);
    }

    public void releasePartition(ShuffleDescriptor shuffleDescriptor) {
        tieredStorageMasterClient.releasePartition(shuffleDescriptor);
    }

    public void close() {
        tieredStorageMasterClient.close();
    }

    private TierShuffleHandler getTierShuffleHandler(JobShuffleContext context) {
        return new TierShuffleHandler() {
            @Override
            public CompletableFuture<?> onReleasePartitions(
                    Collection<TieredStoragePartitionId> partitionIds) {
                return context.stopTrackingAndReleasePartitions(
                        partitionIds.stream()
                                .map(TieredStorageIdMappingUtils::convertId)
                                .collect(Collectors.toList()));
            }

            @Override
            public void onFatalError(Throwable throwable) {
                shuffleMasterContext.onFatalError(throwable);
            }
        };
    }
}
