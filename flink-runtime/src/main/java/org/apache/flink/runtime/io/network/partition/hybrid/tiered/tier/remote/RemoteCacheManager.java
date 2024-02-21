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

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.PartitionFileWriter;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMemoryManager;

import java.util.Arrays;

/**
 * The {@link RemoteCacheManager} is responsible for managing the cached buffers before flush to the
 * remote storage.
 */
class RemoteCacheManager {

    private final SubpartitionRemoteCacheManager[] subpartitionCacheDataManagers;

    private final int[] subpartitionSegmentIds;

    public RemoteCacheManager(
            TieredStoragePartitionId partitionId,
            int numSubpartitions,
            TieredStorageMemoryManager storageMemoryManager,
            PartitionFileWriter partitionFileWriter) {
        this.subpartitionCacheDataManagers = new SubpartitionRemoteCacheManager[numSubpartitions];
        this.subpartitionSegmentIds = new int[numSubpartitions];
        for (int subpartitionId = 0; subpartitionId < numSubpartitions; ++subpartitionId) {
            subpartitionCacheDataManagers[subpartitionId] =
                    new SubpartitionRemoteCacheManager(
                            partitionId, subpartitionId, storageMemoryManager, partitionFileWriter);
        }

        Arrays.fill(subpartitionSegmentIds, -1);
    }

    // ------------------------------------------------------------------------
    //  Called by RemoteTierProducerAgent
    // ------------------------------------------------------------------------

    void startSegment(int subpartitionId, int segmentId) {
        subpartitionCacheDataManagers[subpartitionId].startSegment(segmentId);
        subpartitionSegmentIds[subpartitionId] = segmentId;
    }

    void appendBuffer(Buffer finishedBuffer, int subpartitionId) {
        subpartitionCacheDataManagers[subpartitionId].addBuffer(finishedBuffer);
    }

    void finishSegment(int subpartitionId) {
        subpartitionCacheDataManagers[subpartitionId].finishSegment(
                subpartitionSegmentIds[subpartitionId]);
    }

    void close() {
        Arrays.stream(subpartitionCacheDataManagers).forEach(SubpartitionRemoteCacheManager::close);
    }

    void release() {
        Arrays.stream(subpartitionCacheDataManagers)
                .forEach(SubpartitionRemoteCacheManager::release);
    }
}
