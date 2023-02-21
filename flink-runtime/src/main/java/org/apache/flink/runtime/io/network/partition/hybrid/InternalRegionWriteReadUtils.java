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
     * @param headerBuffer the buffer to write {@link InternalRegion}'s header.
     * @param region the region to be written to channel.
     */
    public static void writeRegionToFile(
            FileChannel channel, ByteBuffer headerBuffer, InternalRegion region)
            throws IOException {
        // write header buffer.
        headerBuffer.clear();
        headerBuffer.putInt(region.getFirstBufferIndex());
        headerBuffer.putInt(region.getNumBuffers());
        headerBuffer.putLong(region.getFirstBufferOffset());
        headerBuffer.flip();

        // write payload buffer.
        ByteBuffer payloadBuffer = allocateAndConfigureBuffer(region.getNumBuffers());
        boolean[] released = region.getReleased();
        for (boolean b : released) {
            payloadBuffer.put(b ? (byte) 1 : (byte) 0);
        }
        payloadBuffer.flip();

        BufferReaderWriterUtil.writeBuffers(
                channel,
                headerBuffer.capacity() + payloadBuffer.capacity(),
                headerBuffer,
                payloadBuffer);
    }

    /**
     * Read {@link InternalRegion} from {@link FileChannel}.
     *
     * @param channel the channel to read.
     * @param headerBuffer the buffer to read {@link InternalRegion}'s header.
     * @param position position to start read.
     * @return the {@link InternalRegion} that read from this channel.
     */
    public static InternalRegion readRegionFromFile(
            FileChannel channel, ByteBuffer headerBuffer, long position) throws IOException {
        headerBuffer.clear();
        BufferReaderWriterUtil.readByteBufferFully(channel, headerBuffer, position);
        headerBuffer.flip();
        int firstBufferIndex = headerBuffer.getInt();
        int numBuffers = headerBuffer.getInt();
        long firstBufferOffset = headerBuffer.getLong();
        ByteBuffer payloadBuffer = allocateAndConfigureBuffer(numBuffers);
        BufferReaderWriterUtil.readByteBufferFully(
                channel, payloadBuffer, position + InternalRegion.HEADER_SIZE);
        boolean[] released = new boolean[numBuffers];
        payloadBuffer.flip();
        for (int i = 0; i < numBuffers; i++) {
            released[i] = payloadBuffer.get() != 0;
        }
        return new InternalRegion(firstBufferIndex, firstBufferOffset, numBuffers, released);
    }
}
