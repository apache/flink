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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.remote;

import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageResourceRegistry;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierMasterAgent;

import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.SegmentPartitionFile.deletePathQuietly;
import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.SegmentPartitionFile.getPartitionPath;

/** The implementation of {@link TierMasterAgent} for the remote tier. */
public class RemoteTierMasterAgent implements TierMasterAgent {

    private final TieredStorageResourceRegistry resourceRegistry;

    private final String remoteStorageBasePath;

    RemoteTierMasterAgent(
            String remoteStorageBasePath, TieredStorageResourceRegistry resourceRegistry) {
        this.remoteStorageBasePath = remoteStorageBasePath;
        this.resourceRegistry = resourceRegistry;
    }

    @Override
    public void addPartition(TieredStoragePartitionId partitionID) {
        resourceRegistry.registerResource(
                partitionID,
                () -> deletePathQuietly(getPartitionPath(partitionID, remoteStorageBasePath)));
    }

    @Override
    public void releasePartition(TieredStoragePartitionId partitionID) {
        resourceRegistry.clearResourceFor(partitionID);
    }
}
