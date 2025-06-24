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
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.Buffer.DataType;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;

import net.jcip.annotations.GuardedBy;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * The {@link SubpartitionDiskCacheManager} is responsible to manage the cached buffers in a single
 * subpartition.
 */
class SubpartitionDiskCacheManager {

    /**
     * All the buffers. The first field of the tuple is the buffer, while the second field of the
     * buffer is the buffer index.
     *
     * <p>Note that this field can be accessed by the task thread or the write IO thread, so the
     * thread safety should be ensured.
     */
    private final Deque<Tuple2<Buffer, Integer>> allBuffers = new LinkedList<>();

    /**
     * Record the segment id that is writing to. Each time when the segment is finished, this filed
     * is increased by one.
     *
     * <p>Note that when flushing buffers, this can be touched by task thread or the flushing
     * thread, so the thread safety should be ensured.
     */
    @GuardedBy("allBuffers")
    private int segmentId;

    /**
     * Record the buffer index in the {@link SubpartitionDiskCacheManager}. Each time a new buffer
     * is added to the {@code allBuffers}, this field is increased by one.
     *
     * <p>Note that the field can only be touched by the task thread, so this field need not be
     * guarded by any lock or synchronizations.
     */
    private int bufferIndex;

    // ------------------------------------------------------------------------
    //  Called by DiskCacheManager
    // ------------------------------------------------------------------------

    void startSegment(int segmentId) {
        synchronized (allBuffers) {
            this.segmentId = segmentId;
        }
    }

    void append(Buffer buffer) {
        addBuffer(buffer);
    }

    void appendEndOfSegmentEvent(ByteBuffer record) {
        writeEvent(record, DataType.END_OF_SEGMENT);
    }

    /** Note that allBuffers can be touched by multiple threads. */
    List<Tuple2<Buffer, Integer>> removeAllBuffers() {
        synchronized (allBuffers) {
            List<Tuple2<Buffer, Integer>> targetBuffers = new ArrayList<>(allBuffers);
            allBuffers.clear();
            return targetBuffers;
        }
    }

    int getBufferIndex() {
        return bufferIndex;
    }

    int getSegmentId() {
        synchronized (allBuffers) {
            return segmentId;
        }
    }

    void release() {
        synchronized (allBuffers) {
            while (!allBuffers.isEmpty()) {
                allBuffers.poll().f0.recycleBuffer();
            }
        }
    }

    // ------------------------------------------------------------------------
    //  Internal Methods
    // ------------------------------------------------------------------------

    private void writeEvent(ByteBuffer event, DataType dataType) {
        checkArgument(dataType.isEvent());

        MemorySegment data = MemorySegmentFactory.wrap(event.array());
        addBuffer(new NetworkBuffer(data, FreeingBufferRecycler.INSTANCE, dataType, data.size()));
    }

    /** This method is only called by the task thread. */
    private void addBuffer(Buffer buffer) {
        synchronized (allBuffers) {
            allBuffers.add(new Tuple2<>(buffer, bufferIndex));
        }
        bufferIndex++;
    }
}
