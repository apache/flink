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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.PartitionFileWriter;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMemoryManager;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.concurrent.FutureUtils;

import net.jcip.annotations.GuardedBy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * This {@link SubpartitionRemoteCacheManager} is responsible for managing the buffers in a single
 * subpartition.
 */
class SubpartitionRemoteCacheManager {

    private static final Logger LOG = LoggerFactory.getLogger(SubpartitionRemoteCacheManager.class);

    private final TieredStoragePartitionId partitionId;

    private final int subpartitionId;

    private final PartitionFileWriter partitionFileWriter;

    /**
     * All the buffers. The first field of the tuple is the buffer, while the second field of the
     * tuple is the buffer index.
     *
     * <p>Note that this field can be accessed by the task thread or the flushing thread, so the
     * thread safety should be ensured.
     */
    @GuardedBy("allBuffers")
    private final Deque<Tuple2<Buffer, Integer>> allBuffers = new LinkedList<>();

    private CompletableFuture<Void> flushCompletableFuture = FutureUtils.completedVoidFuture();

    /**
     * Record the segment id that is writing to.
     *
     * <p>Note that when flushing buffers, this can be touched by task thread or the flushing
     * thread, so the thread safety should be ensured.
     */
    @GuardedBy("allBuffers")
    private int segmentId = -1;

    /**
     * Record the buffer index in the {@link SubpartitionRemoteCacheManager}. Each time a new buffer
     * is added to the {@code allBuffers}, this field is increased by one.
     *
     * <p>Note that the field can only be touched by the task thread, so this field need not be
     * guarded by any lock or synchronizations.
     */
    private int bufferIndex;

    public SubpartitionRemoteCacheManager(
            TieredStoragePartitionId partitionId,
            int subpartitionId,
            TieredStorageMemoryManager storageMemoryManager,
            PartitionFileWriter partitionFileWriter) {
        this.partitionId = partitionId;
        this.subpartitionId = subpartitionId;
        this.partitionFileWriter = partitionFileWriter;
        storageMemoryManager.listenBufferReclaimRequest(this::flushBuffers);
    }

    // ------------------------------------------------------------------------
    //  Called by RemoteCacheManager
    // ------------------------------------------------------------------------

    void startSegment(int segmentId) {
        synchronized (allBuffers) {
            checkState(allBuffers.isEmpty(), "There are un-flushed buffers.");
            this.segmentId = segmentId;
        }
    }

    void addBuffer(Buffer buffer) {
        Tuple2<Buffer, Integer> toAddBuffer = new Tuple2<>(buffer, bufferIndex++);
        synchronized (allBuffers) {
            allBuffers.add(toAddBuffer);
        }
    }

    void finishSegment(int segmentId) {
        // Only task thread can modify the segmentId, and the method can only be called by the task
        // thread, so accessing segmentId is not guarded here.
        //noinspection FieldAccessNotGuarded
        checkState(this.segmentId == segmentId, "Wrong segment id.");
        // Flush the buffers belonging to the current segment
        flushBuffers();

        PartitionFileWriter.SubpartitionBufferContext bufferContext =
                new PartitionFileWriter.SubpartitionBufferContext(
                        subpartitionId,
                        Collections.singletonList(
                                new PartitionFileWriter.SegmentBufferContext(
                                        segmentId, Collections.emptyList(), true)));
        // Notify the partition file writer that the segment is finished through writing the
        // buffer context
        flushCompletableFuture =
                partitionFileWriter.write(partitionId, Collections.singletonList(bufferContext));

        // Only task thread can add buffers, and the method can only be called by the task thread,
        // so accessing allBuffers is not guarded here.
        //noinspection FieldAccessNotGuarded
        checkState(allBuffers.isEmpty(), "Leaking buffers.");
    }

    void close() {
        // Wait the flushing buffers to be completed before closed
        try {
            flushCompletableFuture.get();
        } catch (Exception e) {
            LOG.error("Failed to flush the buffers.", e);
            ExceptionUtils.rethrow(e);
        }
        flushBuffers();
    }

    void release() {
        synchronized (allBuffers) {
            checkState(allBuffers.isEmpty(), "Leaking buffers.");
        }
        partitionFileWriter.release();
    }

    // ------------------------------------------------------------------------
    //  Internal Methods
    // ------------------------------------------------------------------------

    private void flushBuffers() {
        synchronized (allBuffers) {
            List<Tuple2<Buffer, Integer>> allBuffersToFlush = new ArrayList<>(allBuffers);
            allBuffers.clear();
            if (allBuffersToFlush.isEmpty()) {
                return;
            }

            PartitionFileWriter.SubpartitionBufferContext subpartitionBufferContext =
                    new PartitionFileWriter.SubpartitionBufferContext(
                            subpartitionId,
                            Collections.singletonList(
                                    new PartitionFileWriter.SegmentBufferContext(
                                            segmentId, allBuffersToFlush, false)));
            flushCompletableFuture =
                    partitionFileWriter.write(
                            partitionId, Collections.singletonList(subpartitionBufferContext));
        }
    }
}
