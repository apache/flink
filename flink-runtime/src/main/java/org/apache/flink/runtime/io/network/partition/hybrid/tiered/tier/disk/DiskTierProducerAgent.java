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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.disk;

import org.apache.flink.runtime.io.disk.BatchShuffleReadBufferPool;
import org.apache.flink.runtime.io.network.api.EndOfSegmentEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageIdMappingUtils;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.PartitionFileReader;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.PartitionFileWriter;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyConnectionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyConnectionWriter;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyServiceProducer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TieredStorageNettyService;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMemoryManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageResourceRegistry;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierProducerAgent;
import org.apache.flink.util.ExceptionUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.util.Preconditions.checkArgument;

/** The disk tier implementation of {@link TierProducerAgent}. */
public class DiskTierProducerAgent implements TierProducerAgent, NettyServiceProducer {

    private final TieredStoragePartitionId partitionId;

    private final int numBuffersPerSegment;

    private final int bufferSizeBytes;

    private final Path dataFilePath;

    private final float minReservedDiskSpaceFraction;

    private final TieredStorageMemoryManager memoryManager;

    private final DiskCacheManager diskCacheManager;

    /**
     * Record the first buffer index in the segment for each subpartition. The index of the list is
     * responding to the subpartition id. The key in the map is the first buffer index and the value
     * in the map is the segment id.
     */
    private final List<Map<Integer, Integer>> firstBufferIndexInSegment;

    /** Record the number of buffers currently written to each subpartition. */
    private final int[] currentSubpartitionWriteBuffers;

    private final DiskIOScheduler diskIOScheduler;

    private volatile boolean isReleased;

    DiskTierProducerAgent(
            TieredStoragePartitionId partitionId,
            int numSubpartitions,
            int numBytesPerSegment,
            int bufferSizeBytes,
            int maxCachedBytesBeforeFlush,
            Path dataFilePath,
            float minReservedDiskSpaceFraction,
            boolean isBroadcastOnly,
            PartitionFileWriter partitionFileWriter,
            PartitionFileReader partitionFileReader,
            TieredStorageMemoryManager memoryManager,
            TieredStorageNettyService nettyService,
            TieredStorageResourceRegistry resourceRegistry,
            BatchShuffleReadBufferPool bufferPool,
            ScheduledExecutorService ioExecutor,
            int maxRequestedBuffers,
            Duration bufferRequestTimeout,
            int maxBufferReadAhead) {
        checkArgument(
                numBytesPerSegment >= bufferSizeBytes,
                "One segment should contain at least one buffer.");

        this.partitionId = partitionId;
        this.numBuffersPerSegment = numBytesPerSegment / bufferSizeBytes;
        this.bufferSizeBytes = bufferSizeBytes;
        this.dataFilePath = dataFilePath;
        this.minReservedDiskSpaceFraction = minReservedDiskSpaceFraction;
        this.memoryManager = memoryManager;
        this.firstBufferIndexInSegment = new ArrayList<>();
        this.currentSubpartitionWriteBuffers = new int[numSubpartitions];

        for (int i = 0; i < numSubpartitions; ++i) {
            // Each map is used to store the segment ids belonging to a subpartition. The map can be
            // accessed by the task thread and the reading IO thread, so the concurrent hashmap is
            // used to ensure the thread safety.
            firstBufferIndexInSegment.add(new ConcurrentHashMap<>());
        }
        this.diskCacheManager =
                new DiskCacheManager(
                        partitionId,
                        isBroadcastOnly ? 1 : numSubpartitions,
                        maxCachedBytesBeforeFlush,
                        memoryManager,
                        partitionFileWriter);

        this.diskIOScheduler =
                new DiskIOScheduler(
                        partitionId,
                        bufferPool,
                        ioExecutor,
                        maxRequestedBuffers,
                        bufferRequestTimeout,
                        maxBufferReadAhead,
                        this::retrieveFirstBufferIndexInSegment,
                        partitionFileReader);

        nettyService.registerProducer(partitionId, this);
        resourceRegistry.registerResource(partitionId, this::releaseResources);
    }

    @Override
    public boolean tryStartNewSegment(TieredStorageSubpartitionId subpartitionId, int segmentId) {
        File filePath = dataFilePath.toFile();
        boolean canStartNewSegment =
                filePath.getUsableSpace() - ((long) numBuffersPerSegment) * bufferSizeBytes
                        > (long) (filePath.getTotalSpace() * minReservedDiskSpaceFraction);
        if (canStartNewSegment) {
            firstBufferIndexInSegment
                    .get(subpartitionId.getSubpartitionId())
                    .put(
                            diskCacheManager.getBufferIndex(subpartitionId.getSubpartitionId()),
                            segmentId);
            diskCacheManager.startSegment(subpartitionId.getSubpartitionId(), segmentId);
        }
        return canStartNewSegment;
    }

    @Override
    public boolean tryWrite(
            TieredStorageSubpartitionId subpartitionId, Buffer finishedBuffer, Object bufferOwner) {
        int subpartitionIndex = subpartitionId.getSubpartitionId();
        if (currentSubpartitionWriteBuffers[subpartitionIndex] != 0
                && currentSubpartitionWriteBuffers[subpartitionIndex] + 1 > numBuffersPerSegment) {
            emitEndOfSegmentEvent(subpartitionIndex);
            currentSubpartitionWriteBuffers[subpartitionIndex] = 0;
            return false;
        }
        if (finishedBuffer.isBuffer()) {
            memoryManager.transferBufferOwnership(bufferOwner, this, finishedBuffer);
        }
        currentSubpartitionWriteBuffers[subpartitionIndex]++;
        emitBuffer(finishedBuffer, subpartitionIndex);
        return true;
    }

    @Override
    public void connectionEstablished(
            TieredStorageSubpartitionId subpartitionId,
            NettyConnectionWriter nettyConnectionWriter) {
        if (!Files.isReadable(dataFilePath)) {
            throw new RuntimeException(
                    new PartitionNotFoundException(
                            TieredStorageIdMappingUtils.convertId(partitionId)));
        }
        diskIOScheduler.connectionEstablished(subpartitionId, nettyConnectionWriter);
    }

    @Override
    public void connectionBroken(NettyConnectionId connectionId) {
        diskIOScheduler.connectionBroken(connectionId);
    }

    @Override
    public void close() {
        diskCacheManager.close();
    }

    // ------------------------------------------------------------------------
    //  Internal Methods
    // ------------------------------------------------------------------------

    private void emitEndOfSegmentEvent(int subpartitionId) {
        try {
            diskCacheManager.appendEndOfSegmentEvent(
                    EventSerializer.toSerializedEvent(EndOfSegmentEvent.INSTANCE), subpartitionId);
        } catch (IOException e) {
            ExceptionUtils.rethrow(e, "Failed to emit end of segment event.");
        }
    }

    private void emitBuffer(Buffer finishedBuffer, int subpartition) {
        diskCacheManager.append(finishedBuffer, subpartition);
    }

    private void releaseResources() {
        if (!isReleased) {
            firstBufferIndexInSegment.clear();
            diskCacheManager.release();
            diskIOScheduler.release();
            isReleased = true;
        }
    }

    private Integer retrieveFirstBufferIndexInSegment(int subpartitionId, int bufferIndex) {
        return firstBufferIndexInSegment.size() > subpartitionId
                ? firstBufferIndexInSegment.get(subpartitionId).get(bufferIndex)
                : null;
    }
}
