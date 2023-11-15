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

package org.apache.flink.runtime.io.network.partition.hybrid.index;

import java.io.IOException;
import java.nio.channels.FileChannel;

/**
 * {@link FileDataIndexRegionHelper} is responsible for writing a {@link Region} to the file or
 * reading a {@link Region} from file.
 */
public interface FileDataIndexRegionHelper<T extends FileDataIndexRegionHelper.Region> {

    /**
     * Write the region to the file.
     *
     * @param channel the file channel to write the region
     * @param region the region to be written to the file
     */
    void writeRegionToFile(FileChannel channel, T region) throws IOException;

    /**
     * Read a region from the file.
     *
     * @param channel the file channel to read the region
     * @param fileOffset the current region data is from this file offset, so start reading the file
     *     from the offset when reading the region
     * @return the region read from the file
     */
    T readRegionFromFile(FileChannel channel, long fileOffset) throws IOException;

    /**
     * A {@link Region} Represents a series of buffers that are:
     *
     * <ul>
     *   <li>From the same subpartition
     *   <li>Logically (i.e. buffer index) consecutive
     *   <li>Physically (i.e. offset in the file) consecutive
     * </ul>
     *
     * <p>The following example illustrates some physically continuous buffers in a file and regions
     * upon them, where `x-y` denotes buffer from subpartition x with buffer index y, and `()`
     * denotes a region.
     *
     * <p>(1-1, 1-2), (2-1), (2-2, 2-3), (1-5, 1-6), (1-4)
     *
     * <p>Note: The file may not contain all the buffers. E.g., 1-3 is missing in the above example.
     *
     * <p>Note: Buffers in file may have different orders than their buffer index. E.g., 1-4 comes
     * after 1-6 in the above example.
     *
     * <p>Note: This index may not always maintain the longest possible regions. E.g., 2-1, 2-2, 2-3
     * are in two separate regions.
     */
    interface Region {

        /** Get the total size in bytes of this region, including the fields and the buffers. */
        int getSize();

        /** Get the first buffer index of this region. */
        int getFirstBufferIndex();

        /** Get the file start offset of this region. */
        long getRegionStartOffset();

        /** Get the file end offset of the region. */
        long getRegionEndOffset();

        /** Get the number of buffers in this region. */
        int getNumBuffers();

        /**
         * Whether the current region contain the buffer.
         *
         * @param bufferIndex the specific buffer index
         */
        boolean containBuffer(int bufferIndex);
    }
}
