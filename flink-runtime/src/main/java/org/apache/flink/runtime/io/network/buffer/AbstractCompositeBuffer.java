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

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufAllocator;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * An implementation of {@link Buffer} which contains multiple partial buffers for network data
 * communication.
 */
public abstract class AbstractCompositeBuffer implements Buffer {

    protected final DataType dataType;

    protected final int length;

    protected final boolean isCompressed;

    protected final List<Buffer> partialBuffers = new ArrayList<>();

    protected int currentLength;

    protected ByteBufAllocator allocator;

    protected AbstractCompositeBuffer(DataType dataType, int length, boolean isCompressed) {
        this.dataType = dataType;
        this.length = length;
        this.isCompressed = isCompressed;
    }

    @Override
    public boolean isBuffer() {
        return dataType.isBuffer();
    }

    @Override
    public void recycleBuffer() {
        for (Buffer partialBuffer : partialBuffers) {
            partialBuffer.recycleBuffer();
        }
    }

    @Override
    public Buffer retainBuffer() {
        for (Buffer partialBuffer : partialBuffers) {
            partialBuffer.retainBuffer();
        }
        return this;
    }

    @Override
    public int getSize() {
        return currentLength;
    }

    @Override
    public int readableBytes() {
        return currentLength;
    }

    @Override
    public void setAllocator(ByteBufAllocator allocator) {
        this.allocator = allocator;
    }

    @Override
    public boolean isCompressed() {
        return isCompressed;
    }

    @Override
    public DataType getDataType() {
        return dataType;
    }

    public abstract void addPartialBuffer(Buffer buffer);

    public List<Buffer> getPartialBuffers() {
        return Collections.unmodifiableList(partialBuffers);
    }

    public int missingLength() {
        return length - currentLength;
    }

    @Override
    public MemorySegment getMemorySegment() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getMemorySegmentOffset() {
        throw new UnsupportedOperationException();
    }

    @Override
    public BufferRecycler getRecycler() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setRecycler(BufferRecycler bufferRecycler) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isRecycled() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Buffer readOnlySlice() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Buffer readOnlySlice(int index, int length) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getMaxCapacity() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getReaderIndex() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setReaderIndex(int readerIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setSize(int writerIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuffer getNioBufferReadable() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuffer getNioBuffer(int index, int length) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setCompressed(boolean isCompressed) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setDataType(DataType dataType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int refCnt() {
        throw new UnsupportedOperationException();
    }
}
