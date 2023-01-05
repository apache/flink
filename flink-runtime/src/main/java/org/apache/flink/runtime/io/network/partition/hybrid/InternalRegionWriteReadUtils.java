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

import org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil;
import org.apache.flink.runtime.io.network.partition.hybrid.HsFileDataIndexImpl.InternalRegion;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;

/** Utils for read and write {@link InternalRegion}. */
public class InternalRegionWriteReadUtils {

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
     * Write {@link InternalRegion} to {@link FileChannel}.
     *
     * @param channel the file's channel to write.
     * @param fixedRegionBuffer the buffer to write {@link InternalRegion}'s fixed part.
     * @param region the region to be written to channel.
     */
    public static void writeRegionToFile(
            FileChannel channel, ByteBuffer fixedRegionBuffer, InternalRegion region)
            throws IOException {
        // write fixed buffer.
        fixedRegionBuffer.clear();
        fixedRegionBuffer.putInt(region.getFirstBufferIndex());
        fixedRegionBuffer.putInt(region.getNumBuffers());
        fixedRegionBuffer.putLong(region.getFirstBufferOffset());
        fixedRegionBuffer.flip();

        // write variable buffer.
        ByteBuffer variableBuffer = allocateAndConfigureBuffer(region.getNumBuffers());
        boolean[] released = region.getReleased();
        for (boolean b : released) {
            variableBuffer.put(b ? (byte) 1 : (byte) 0);
        }
        variableBuffer.flip();

        BufferReaderWriterUtil.writeBuffers(
                channel,
                fixedRegionBuffer.capacity() + variableBuffer.capacity(),
                fixedRegionBuffer,
                variableBuffer);
    }

    /**
     * Read {@link InternalRegion} from {@link FileChannel}.
     *
     * @param channel the channel to read.
     * @param fixedRegionBuffer the buffer to read {@link InternalRegion}'s fixed part.
     * @param position position to start read.
     * @return the {@link InternalRegion} that read from this channel.
     */
    public static InternalRegion readRegionFromFile(
            FileChannel channel, ByteBuffer fixedRegionBuffer, long position) throws IOException {
        fixedRegionBuffer.clear();
        BufferReaderWriterUtil.readByteBufferFully(channel, fixedRegionBuffer, position);
        fixedRegionBuffer.flip();
        int firstBufferIndex = fixedRegionBuffer.getInt();
        int numBuffers = fixedRegionBuffer.getInt();
        long firstBufferOffset = fixedRegionBuffer.getLong();
        ByteBuffer variablePart = allocateAndConfigureBuffer(numBuffers);
        BufferReaderWriterUtil.readByteBufferFully(
                channel, variablePart, position + InternalRegion.FIXED_SIZE);
        boolean[] released = new boolean[numBuffers];
        variablePart.flip();
        for (int i = 0; i < numBuffers; i++) {
            released[i] = variablePart.get() != 0;
        }
        return new InternalRegion(firstBufferIndex, firstBufferOffset, numBuffers, released);
    }
}
