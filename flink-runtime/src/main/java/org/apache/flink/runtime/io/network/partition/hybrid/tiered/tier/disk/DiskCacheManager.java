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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.PartitionFileWriter;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMemoryManager;
import org.apache.flink.util.concurrent.FutureUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * The {@link DiskCacheManager} is responsible for managing cached buffers before flushing to files.
 */
class DiskCacheManager {

    private final TieredStoragePartitionId partitionId;

    private final int numSubpartitions;

    private final int maxCachedBytesBeforeFlush;

    private final PartitionFileWriter partitionFileWriter;

    private final SubpartitionDiskCacheManager[] subpartitionCacheManagers;

    /** Whether the current flush process has completed. */
    private CompletableFuture<Void> hasFlushCompleted;

    /**
     * The number of all subpartition's cached bytes in the cache manager. Note that the counter can
     * only be accessed by the task thread and does not require locks.
     */
    private int numCachedBytesCounter;

    DiskCacheManager(
            TieredStoragePartitionId partitionId,
            int numSubpartitions,
            int maxCachedBytesBeforeFlush,
            TieredStorageMemoryManager memoryManager,
            PartitionFileWriter partitionFileWriter) {
        this.partitionId = partitionId;
        this.numSubpartitions = numSubpartitions;
        this.maxCachedBytesBeforeFlush = maxCachedBytesBeforeFlush;
        this.partitionFileWriter = partitionFileWriter;
        this.subpartitionCacheManagers = new SubpartitionDiskCacheManager[numSubpartitions];
        this.hasFlushCompleted = FutureUtils.completedVoidFuture();

        for (int subpartitionId = 0; subpartitionId < numSubpartitions; ++subpartitionId) {
            subpartitionCacheManagers[subpartitionId] = new SubpartitionDiskCacheManager();
        }
        memoryManager.listenBufferReclaimRequest(this::notifyFlushCachedBuffers);
    }

    // ------------------------------------------------------------------------
    //  Called by DiskTierProducerAgent
    // ------------------------------------------------------------------------

    void startSegment(int subpartitionId, int segmentIndex) {
        subpartitionCacheManagers[subpartitionId].startSegment(segmentIndex);
    }

    /**
     * Append buffer to {@link DiskCacheManager}.
     *
     * @param buffer to be managed by this class.
     * @param subpartitionId the subpartition of this record.
     */
    void append(Buffer buffer, int subpartitionId) {
        subpartitionCacheManagers[subpartitionId].append(buffer);
        increaseNumCachedBytesAndCheckFlush(buffer.readableBytes());
    }

    /**
     * Append the end-of-segment event to {@link DiskCacheManager}, which indicates the segment has
     * finished.
     *
     * @param record the end-of-segment event
     * @param subpartitionId target subpartition of this record.
     */
    void appendEndOfSegmentEvent(ByteBuffer record, int subpartitionId) {
        subpartitionCacheManagers[subpartitionId].appendEndOfSegmentEvent(record);
        increaseNumCachedBytesAndCheckFlush(record.remaining());
    }

    /**
     * Return the current buffer index.
     *
     * @param subpartitionId the target subpartition id
     * @return the finished buffer index
     */
    int getBufferIndex(int subpartitionId) {
        return subpartitionCacheManagers[subpartitionId].getBufferIndex();
    }

    /** Close this {@link DiskCacheManager}, it means no data can append to memory. */
    void close() {
        forceFlushCachedBuffers();
    }

    /**
     * Release this {@link DiskCacheManager}, it means all memory taken by this class will recycle.
     */
    void release() {
        Arrays.stream(subpartitionCacheManagers).forEach(SubpartitionDiskCacheManager::release);
        partitionFileWriter.release();
    }

    // ------------------------------------------------------------------------
    //  Internal Methods
    // ------------------------------------------------------------------------

    private void increaseNumCachedBytesAndCheckFlush(int numIncreasedCachedBytes) {
        numCachedBytesCounter += numIncreasedCachedBytes;
        if (numCachedBytesCounter > maxCachedBytesBeforeFlush) {
            forceFlushCachedBuffers();
        }
    }

    private void notifyFlushCachedBuffers() {
        flushBuffers(false);
    }

    private void forceFlushCachedBuffers() {
        flushBuffers(true);
    }

    /**
     * Note that the request of flushing buffers may come from the disk check thread or the task
     * thread, so the method itself should ensure the thread safety.
     */
    private synchronized void flushBuffers(boolean forceFlush) {
        if (!forceFlush && !hasFlushCompleted.isDone()) {
            return;
        }
        List<PartitionFileWriter.SubpartitionBufferContext> buffersToFlush = new ArrayList<>();
        int numToWriteBuffers = getSubpartitionToFlushBuffers(buffersToFlush);

        if (numToWriteBuffers > 0) {
            CompletableFuture<Void> flushCompletableFuture =
                    partitionFileWriter.write(partitionId, buffersToFlush);
            if (!forceFlush) {
                hasFlushCompleted = flushCompletableFuture;
            }
        }
        numCachedBytesCounter = 0;
    }

    private int getSubpartitionToFlushBuffers(
            List<PartitionFileWriter.SubpartitionBufferContext> buffersToFlush) {
        int numToWriteBuffers = 0;
        for (int subpartitionId = 0; subpartitionId < numSubpartitions; subpartitionId++) {
            List<Tuple2<Buffer, Integer>> bufferWithIndexes =
                    subpartitionCacheManagers[subpartitionId].removeAllBuffers();
            buffersToFlush.add(
                    new PartitionFileWriter.SubpartitionBufferContext(
                            subpartitionId,
                            Collections.singletonList(
                                    new PartitionFileWriter.SegmentBufferContext(
                                            subpartitionCacheManagers[subpartitionId]
                                                    .getSegmentId(),
                                            bufferWithIndexes,
                                            false))));
            numToWriteBuffers += bufferWithIndexes.size();
        }
        return numToWriteBuffers;
    }
}
