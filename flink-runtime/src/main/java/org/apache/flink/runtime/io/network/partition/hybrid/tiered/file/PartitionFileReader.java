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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.file;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;

import javax.annotation.Nullable;

import java.io.IOException;

/** {@link PartitionFileReader} defines the read logic for different types of shuffle files. */
public interface PartitionFileReader {

    /**
     * Read a buffer from the partition file.
     *
     * @param partitionId the partition id of the buffer
     * @param subpartitionId the subpartition id of the buffer
     * @param segmentId the segment id of the buffer
     * @param bufferIndex the index of buffer
     * @param memorySegment the empty buffer to store the read buffer
     * @param recycler the buffer recycler
     * @return null if there is no data otherwise a buffer.
     */
    @Nullable
    Buffer readBuffer(
            TieredStoragePartitionId partitionId,
            TieredStorageSubpartitionId subpartitionId,
            int segmentId,
            int bufferIndex,
            MemorySegment memorySegment,
            BufferRecycler recycler)
            throws IOException;

    /**
     * Get the priority for reading a particular buffer from the partitioned file. The priority is
     * defined as, it is suggested to read buffers with higher priority (smaller value) in prior to
     * buffers with lower priority (larger value).
     *
     * <p>Depending on the partition file implementation, following the suggestions should typically
     * result in better performance and efficiency. This can be achieved by e.g. choosing preloaded
     * data over others, optimizing the order of disk access to be more sequential, etc.
     *
     * <p>Note: Priorities are suggestions rather than a requirements. The caller can still read
     * data in whichever order it wants.
     *
     * @param partitionId the partition id of the buffer
     * @param subpartitionId the subpartition id of the buffer
     * @param segmentId the segment id of the buffer
     * @param bufferIndex the index of buffer
     * @return the priority of the {@link PartitionFileReader}.
     */
    long getPriority(
            TieredStoragePartitionId partitionId,
            TieredStorageSubpartitionId subpartitionId,
            int segmentId,
            int bufferIndex);

    /** Release the {@link PartitionFileReader}. */
    void release();
}
