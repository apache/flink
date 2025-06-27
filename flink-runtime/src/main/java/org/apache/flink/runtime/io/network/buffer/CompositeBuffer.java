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

package org.apache.flink.runtime.io.network.buffer;

import org.apache.flink.core.memory.MemorySegment;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.CompositeByteBuf;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * An implementation of {@link Buffer} which contains multiple partial buffers for network data
 * communication.
 *
 * <p>This class used for that all partial buffers are derived from the same initial write buffer
 * due to the potential fragmentation during the read process.
 */
public class CompositeBuffer extends AbstractCompositeBuffer {

    public CompositeBuffer(DataType dataType, int length, boolean isCompressed) {
        super(dataType, length, isCompressed);
    }

    public CompositeBuffer(BufferHeader header) {
        this(header.getDataType(), header.getLength(), header.isCompressed());
    }

    @Override
    public ByteBuf asByteBuf() {
        CompositeByteBuf compositeByteBuf = checkNotNull(allocator).compositeDirectBuffer();
        for (Buffer buffer : partialBuffers) {
            compositeByteBuf.addComponent(buffer.asByteBuf());
        }
        compositeByteBuf.writerIndex(currentLength);
        return compositeByteBuf;
    }

    /**
     * Returns the full buffer data in one piece of {@link MemorySegment}. If there is multiple
     * partial buffers, the partial data will be copied to the given target {@link MemorySegment}.
     */
    public Buffer getFullBufferData(MemorySegment segment) {
        checkState(!partialBuffers.isEmpty());
        checkState(currentLength <= segment.size());

        if (partialBuffers.size() == 1) {
            return partialBuffers.get(0);
        }

        int offset = 0;
        for (Buffer buffer : partialBuffers) {
            segment.put(offset, buffer.getNioBufferReadable(), buffer.readableBytes());
            offset += buffer.readableBytes();
        }
        recycleBuffer();
        return new NetworkBuffer(
                segment,
                BufferRecycler.DummyBufferRecycler.INSTANCE,
                dataType,
                isCompressed,
                currentLength);
    }

    @Override
    public void addPartialBuffer(Buffer buffer) {
        buffer.setDataType(dataType);
        buffer.setCompressed(isCompressed);
        partialBuffers.add(buffer);
        currentLength += buffer.readableBytes();
        checkState(currentLength <= length);
    }
}
