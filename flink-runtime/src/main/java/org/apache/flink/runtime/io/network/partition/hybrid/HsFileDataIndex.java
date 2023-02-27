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

package org.apache.flink.runtime.io.network.partition.hybrid;

import java.util.List;
import java.util.Optional;

/**
 * Index of spilled data. For each spilled data buffer, this maintains the subpartition it belongs
 * to, the buffer index within the subpartition, the offset in file it begin with, and its
 * readability.
 *
 * <p>Note: Buffers added to this index should not be read until being explicitly marked READABLE.
 */
public interface HsFileDataIndex {

    /**
     * Get a {@link ReadableRegion} for the given subpartition that starts with the given buffer
     * index, if existed.
     *
     * <p>Note: Depending on the implementation, this method does not guarantee to always return the
     * longest possible readable region.
     *
     * @param subpartitionId that the readable region belongs to
     * @param bufferIndex that the readable region starts with
     * @param consumingOffset of the downstream
     * @return a {@link ReadableRegion} for the given subpartition that starts with the given buffer
     *     index, if exist; otherwise, {@link Optional#empty()}.
     */
    Optional<ReadableRegion> getReadableRegion(
            int subpartitionId, int bufferIndex, int consumingOffset);

    /**
     * Add buffers to the index.
     *
     * <p>Attention: this method only called by spilling thread.
     *
     * @param spilledBuffers to be added. The buffers in the list are expected in the same order as
     *     in the spilled file.
     */
    void addBuffers(List<SpilledBuffer> spilledBuffers);

    /**
     * Mark a buffer as RELEASED.
     *
     * @param subpartitionId that the buffer belongs to
     * @param bufferIndex of the buffer within the subpartition
     */
    void markBufferReleased(int subpartitionId, int bufferIndex);

    /** Close this file data index. */
    void close();

    /**
     * Represents a series of physically continuous buffers in the file, which are readable, from
     * the same subpartition, and has sequential buffer index.
     *
     * <p>For efficiency, the index may not remember offsets of all buffers. Thus, a region is
     * described as: starting from the {@link #offset}, skip the first {@link #numSkip} buffers and
     * include the next {@link #numReadable} buffers.
     */
    class ReadableRegion {
        /** From the {@link #offset}, number of buffers to skip. */
        public final int numSkip;

        /**
         * From the {@link #offset}, number of buffers to include after skipping the first {@link
         * #numSkip} ones.
         */
        public final int numReadable;

        /** The file offset to begin with. */
        public final long offset;

        public ReadableRegion(int numSkip, int numReadable, long offset) {
            this.numSkip = numSkip;
            this.numReadable = numReadable;
            this.offset = offset;
        }
    }

    /** Represents a spilled buffer. */
    class SpilledBuffer {
        /** Id of subpartition that the buffer belongs to. */
        public final int subpartitionId;

        /** Index of the buffer within the subpartition. */
        public final int bufferIndex;

        /** File offset that the buffer begin with. */
        public final long fileOffset;

        public SpilledBuffer(int subpartitionId, int bufferIndex, long fileOffset) {
            this.subpartitionId = subpartitionId;
            this.bufferIndex = bufferIndex;
            this.fileOffset = fileOffset;
        }
    }
}
