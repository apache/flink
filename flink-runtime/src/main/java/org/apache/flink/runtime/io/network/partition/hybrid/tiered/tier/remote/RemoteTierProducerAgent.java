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
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.PartitionFileWriter;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMemoryManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageResourceRegistry;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierProducerAgent;

import java.util.Arrays;

import static org.apache.flink.util.Preconditions.checkArgument;

/** The implementation of {@link TierProducerAgent} for the remote tier. */
public class RemoteTierProducerAgent implements TierProducerAgent {

    private final int numSubpartitions;

    private final int numBuffersPerSegment;

    private final RemoteCacheManager cacheDataManager;

    private final TieredStorageMemoryManager memoryManager;

    private final int[] currentSubpartitionSegmentWriteBuffers;

    RemoteTierProducerAgent(
            TieredStoragePartitionId partitionId,
            int numSubpartitions,
            int numBytesPerSegment,
            int bufferSizeBytes,
            boolean isBroadcastOnly,
            PartitionFileWriter partitionFileWriter,
            TieredStorageMemoryManager memoryManager,
            TieredStorageResourceRegistry resourceRegistry) {
        checkArgument(
                numBytesPerSegment >= bufferSizeBytes,
                "One segment should contain at least one buffer.");

        this.numSubpartitions = numSubpartitions;
        this.numBuffersPerSegment = numBytesPerSegment / bufferSizeBytes;
        this.memoryManager = memoryManager;
        this.cacheDataManager =
                new RemoteCacheManager(
                        partitionId,
                        isBroadcastOnly ? 1 : numSubpartitions,
                        memoryManager,
                        partitionFileWriter);
        this.currentSubpartitionSegmentWriteBuffers = new int[numSubpartitions];
        Arrays.fill(currentSubpartitionSegmentWriteBuffers, 0);
        resourceRegistry.registerResource(partitionId, this::releaseAllResources);
    }

    @Override
    public boolean tryStartNewSegment(TieredStorageSubpartitionId subpartitionId, int segmentId) {
        cacheDataManager.startSegment(subpartitionId.getSubpartitionId(), segmentId);
        // The remote storage tier should always be able to start a new segment.
        return true;
    }

    @Override
    public boolean tryWrite(
            TieredStorageSubpartitionId subpartitionId, Buffer buffer, Object bufferOwner) {
        int subpartitionIndex = subpartitionId.getSubpartitionId();
        if (currentSubpartitionSegmentWriteBuffers[subpartitionIndex] + 1 > numBuffersPerSegment) {
            cacheDataManager.finishSegment(subpartitionIndex);
            currentSubpartitionSegmentWriteBuffers[subpartitionIndex] = 0;
            return false;
        }
        if (buffer.isBuffer()) {
            memoryManager.transferBufferOwnership(bufferOwner, this, buffer);
        }
        currentSubpartitionSegmentWriteBuffers[subpartitionIndex]++;
        cacheDataManager.appendBuffer(buffer, subpartitionIndex);
        return true;
    }

    @Override
    public void close() {
        for (int subpartitionId = 0; subpartitionId < numSubpartitions; subpartitionId++) {
            cacheDataManager.finishSegment(subpartitionId);
        }
        cacheDataManager.close();
    }

    // ------------------------------------------------------------------------
    //  Internal Methods
    // ------------------------------------------------------------------------

    private void releaseAllResources() {
        cacheDataManager.release();
    }
}
