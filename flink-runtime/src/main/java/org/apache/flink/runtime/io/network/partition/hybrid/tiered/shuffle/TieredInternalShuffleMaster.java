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
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMasterClient;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageResourceRegistry;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierMasterAgent;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageIdMappingUtils.convertId;

/**
 * A wrapper internal shuffle master class for tiered storage. All the tiered storage operations
 * with the shuffle master should be wrapped in this class.
 */
public class TieredInternalShuffleMaster {

    private final TieredStorageMasterClient tieredStorageMasterClient;

    private final Map<JobID, List<ResultPartitionID>> jobPartitionIds;

    private final Map<ResultPartitionID, JobID> partitionJobIds;

    public TieredInternalShuffleMaster(Configuration conf) {
        TieredStorageConfiguration tieredStorageConfiguration =
                TieredStorageConfiguration.fromConfiguration(conf);
        TieredStorageResourceRegistry resourceRegistry = new TieredStorageResourceRegistry();
        List<TierMasterAgent> tierFactories =
                tieredStorageConfiguration.getTierFactories().stream()
                        .map(tierFactory -> tierFactory.createMasterAgent(resourceRegistry))
                        .collect(Collectors.toList());
        this.tieredStorageMasterClient = new TieredStorageMasterClient(tierFactories);
        this.jobPartitionIds = new HashMap<>();
        this.partitionJobIds = new HashMap<>();
    }

    public void addPartition(JobID jobID, ResultPartitionID resultPartitionID) {
        jobPartitionIds.computeIfAbsent(jobID, ignore -> new ArrayList<>()).add(resultPartitionID);
        partitionJobIds.put(resultPartitionID, jobID);
        tieredStorageMasterClient.addPartition(convertId(resultPartitionID));
    }

    public void releasePartition(ResultPartitionID resultPartitionID) {
        tieredStorageMasterClient.releasePartition(convertId(resultPartitionID));
        JobID jobID = partitionJobIds.remove(resultPartitionID);
        if (jobID == null) {
            return;
        }

        List<ResultPartitionID> resultPartitionIDs = jobPartitionIds.get(jobID);
        if (resultPartitionIDs == null) {
            return;
        }

        resultPartitionIDs.remove(resultPartitionID);
        // If the result partition id list has been empty, remove the jobID from the map eagerly
        if (resultPartitionIDs.isEmpty()) {
            jobPartitionIds.remove(jobID);
        }
    }

    public void unregisterJob(JobID jobID) {
        List<ResultPartitionID> resultPartitionIDs = jobPartitionIds.remove(jobID);
        if (resultPartitionIDs != null) {
            resultPartitionIDs.forEach(
                    resultPartitionID -> {
                        tieredStorageMasterClient.releasePartition(convertId(resultPartitionID));
                        partitionJobIds.remove(resultPartitionID);
                    });
        }
    }
}
