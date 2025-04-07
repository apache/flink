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

import org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.ProducerMergedPartitionFileIndex.FixedSizeRegion;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;

/** Utils for read and write {@link FileDataIndexRegionHelper.Region}. */
public class FileRegionWriteReadUtils {

    /**
     * Allocate a buffer with specific size and configure it to native order.
     *
     * @param bufferSize the size of buffer to allocate.
     * @return a native order buffer with expected size.
     */
    public static ByteBuffer allocateAndConfigureBuffer(int bufferSize) {
        ByteBuffer buffer = ByteBuffer.allocateDirect(bufferSize);
        buffer.order(ByteOrder.nativeOrder());
        return buffer;
    }

    /**
     * Write {@link FixedSizeRegion} to {@link FileChannel}.
     *
     * <p>Note that this type of region's length is fixed.
     *
     * @param channel the file's channel to write.
     * @param regionBuffer the buffer to write {@link FixedSizeRegion}'s header.
     * @param region the region to be written to channel.
     */
    public static void writeFixedSizeRegionToFile(
            FileChannel channel, ByteBuffer regionBuffer, FileDataIndexRegionHelper.Region region)
            throws IOException {
        regionBuffer.clear();
        regionBuffer.putInt(region.getFirstBufferIndex());
        regionBuffer.putInt(region.getNumBuffers());
        regionBuffer.putLong(region.getRegionStartOffset());
        regionBuffer.putLong(region.getRegionEndOffset());
        regionBuffer.flip();
        BufferReaderWriterUtil.writeBuffers(channel, regionBuffer.capacity(), regionBuffer);
    }

    /**
     * Read {@link FixedSizeRegion} from {@link FileChannel}.
     *
     * <p>Note that this type of region's length is fixed.
     *
     * @param channel the channel to read.
     * @param regionBuffer the buffer to read {@link FixedSizeRegion}'s header.
     * @param fileOffset the file offset to start read.
     * @return the {@link FixedSizeRegion} that read from this channel.
     */
    public static FixedSizeRegion readFixedSizeRegionFromFile(
            FileChannel channel, ByteBuffer regionBuffer, long fileOffset) throws IOException {
        regionBuffer.clear();
        BufferReaderWriterUtil.readByteBufferFully(channel, regionBuffer, fileOffset);
        regionBuffer.flip();
        int firstBufferIndex = regionBuffer.getInt();
        int numBuffers = regionBuffer.getInt();
        long firstBufferOffset = regionBuffer.getLong();
        long lastBufferEndOffset = regionBuffer.getLong();
        return new FixedSizeRegion(
                firstBufferIndex, firstBufferOffset, lastBufferEndOffset, numBuffers);
    }
}
