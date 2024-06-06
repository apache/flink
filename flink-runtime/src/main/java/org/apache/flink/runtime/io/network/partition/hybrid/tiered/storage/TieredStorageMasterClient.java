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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierMasterAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierShuffleDescriptor;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierShuffleHandler;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkState;

/** Client of the Tiered Storage used by the master. */
public class TieredStorageMasterClient {

    private final List<TierMasterAgent> tiers;

    public TieredStorageMasterClient(List<TierMasterAgent> tiers) {
        this.tiers = tiers;
    }

    public void registerJob(JobID jobID, TierShuffleHandler shuffleHandler) {
        tiers.forEach(tierMasterAgent -> tierMasterAgent.registerJob(jobID, shuffleHandler));
    }

    public void unregisterJob(JobID jobID) {
        tiers.forEach(tierMasterAgent -> tierMasterAgent.unregisterJob(jobID));
    }

    public List<TierShuffleDescriptor> addPartitionAndGetShuffleDescriptor(
            JobID jobID, ResultPartitionID resultPartitionID) {
        return tiers.stream()
                .map(
                        tierMasterAgent ->
                                tierMasterAgent.addPartitionAndGetShuffleDescriptor(
                                        jobID, resultPartitionID))
                .collect(Collectors.toList());
    }

    public void releasePartition(ShuffleDescriptor shuffleDescriptor) {
        checkState(shuffleDescriptor instanceof NettyShuffleDescriptor);
        List<TierShuffleDescriptor> tierShuffleDescriptors =
                ((NettyShuffleDescriptor) shuffleDescriptor).getTierShuffleDescriptors();
        if (tierShuffleDescriptors != null && !tierShuffleDescriptors.isEmpty()) {
            checkState(tierShuffleDescriptors.size() == tiers.size());
            for (int i = 0; i < tierShuffleDescriptors.size(); i++) {
                tiers.get(i).releasePartition(tierShuffleDescriptors.get(i));
            }
        }
    }

    public void close() {
        tiers.forEach(TierMasterAgent::close);
    }
}
