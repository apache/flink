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
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufAllocator;
import org.apache.flink.shaded.netty4.io.netty.buffer.ReadOnlyByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.SlicedByteBuf;

import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Minimal best-effort read-only sliced {@link Buffer} implementation wrapping a {@link
 * NetworkBuffer}'s sub-region based on <tt>io.netty.buffer.SlicedByteBuf</tt> and
 * <tt>io.netty.buffer.ReadOnlyByteBuf</tt>.
 *
 * <p><strong>BEWARE:</strong> We do not guarantee to block every operation that is able to write
 * data but all returned data structures should be handled as if it was!.
 */
public final class ReadOnlySlicedNetworkBuffer extends ReadOnlyByteBuf implements Buffer {

    private final int memorySegmentOffset;

    private boolean isCompressed = false;

    private DataType dataType;

    /**
     * Creates a buffer which shares the memory segment of the given buffer and exposed the given
     * sub-region only.
     *
     * <p>Reader and writer indices as well as markers are not shared. Reference counters are shared
     * but the slice is not {@link #retainBuffer() retained} automatically.
     *
     * @param buffer the buffer to derive from
     * @param index the index to start from
     * @param length the length of the slice
     * @param isCompressed is the buffer compressed
     */
    ReadOnlySlicedNetworkBuffer(NetworkBuffer buffer, int index, int length, boolean isCompressed) {
        super(new SlicedByteBuf(buffer, index, length));
        this.memorySegmentOffset = buffer.getMemorySegmentOffset() + index;
        this.dataType = buffer.getDataType();
        this.isCompressed = isCompressed;
    }

    /**
     * Creates a buffer which shares the memory segment of the given buffer and exposed the given
     * sub-region only.
     *
     * <p>Reader and writer indices as well as markers are not shared. Reference counters are shared
     * but the slice is not {@link #retainBuffer() retained} automatically.
     *
     * @param buffer the buffer to derive from
     * @param index the index to start from
     * @param length the length of the slice
     * @param memorySegmentOffset <tt>buffer</tt>'s absolute offset in the backing {@link
     *     MemorySegment}
     * @param isCompressed whether the buffer is compressed or not
     */
    ReadOnlySlicedNetworkBuffer(
            ByteBuf buffer, int index, int length, int memorySegmentOffset, boolean isCompressed) {
        super(new SlicedByteBuf(buffer, index, length));
        this.memorySegmentOffset = memorySegmentOffset + index;
        this.isCompressed = isCompressed;
        this.dataType = getBuffer().getDataType();
    }

    @Override
    public ByteBuf unwrap() {
        return super.unwrap();
    }

    @Override
    public boolean isBuffer() {
        return dataType.isBuffer();
    }

    /**
     * Returns the underlying memory segment.
     *
     * <p><strong>BEWARE:</strong> Although we cannot set the memory segment read-only it should be
     * handled as if it was!.
     *
     * @return the memory segment backing this buffer
     */
    @Override
    public MemorySegment getMemorySegment() {
        return getBuffer().getMemorySegment();
    }

    @Override
    public int getMemorySegmentOffset() {
        return memorySegmentOffset;
    }

    @Override
    public BufferRecycler getRecycler() {
        return getBuffer().getRecycler();
    }

    @Override
    public void setRecycler(BufferRecycler bufferRecycler) {
        getBuffer().setRecycler(bufferRecycler);
    }

    @Override
    public void recycleBuffer() {
        getBuffer().recycleBuffer();
    }

    @Override
    public boolean isRecycled() {
        return getBuffer().isRecycled();
    }

    @Override
    public ReadOnlySlicedNetworkBuffer retainBuffer() {
        getBuffer().retainBuffer();
        return this;
    }

    @Override
    public ReadOnlySlicedNetworkBuffer readOnlySlice() {
        return readOnlySlice(readerIndex(), readableBytes());
    }

    @Override
    public ReadOnlySlicedNetworkBuffer readOnlySlice(int index, int length) {
        checkState(
                !isCompressed || index + length == writerIndex(),
                "Unable to partially slice a compressed buffer.");
        return new ReadOnlySlicedNetworkBuffer(
                super.unwrap(), index, length, memorySegmentOffset, isCompressed);
    }

    @Override
    public int getMaxCapacity() {
        return maxCapacity();
    }

    @Override
    public int getReaderIndex() {
        return readerIndex();
    }

    @Override
    public void setReaderIndex(int readerIndex) throws IndexOutOfBoundsException {
        readerIndex(readerIndex);
    }

    @Override
    public int getSize() {
        return writerIndex();
    }

    @Override
    public void setSize(int writerIndex) {
        writerIndex(writerIndex);
    }

    @Override
    public ByteBuffer getNioBufferReadable() {
        return nioBuffer();
    }

    @Override
    public ByteBuffer getNioBuffer(int index, int length) throws IndexOutOfBoundsException {
        return nioBuffer(index, length);
    }

    @Override
    public ByteBuffer nioBuffer(int index, int length) {
        return super.nioBuffer(index, length).asReadOnlyBuffer();
    }

    @Override
    public boolean isWritable() {
        return false;
    }

    @Override
    public boolean isWritable(int numBytes) {
        return false;
    }

    @Override
    public ByteBuf ensureWritable(int minWritableBytes) {
        // note: ReadOnlyByteBuf allows this but in most cases this does not make sense
        if (minWritableBytes != 0) {
            throw new ReadOnlyBufferException();
        }
        return this;
    }

    @Override
    public void setAllocator(ByteBufAllocator allocator) {
        getBuffer().setAllocator(allocator);
    }

    @Override
    public ByteBuf asByteBuf() {
        return this;
    }

    @Override
    public boolean isCompressed() {
        return isCompressed;
    }

    @Override
    public void setCompressed(boolean isCompressed) {
        this.isCompressed = isCompressed;
    }

    @Override
    public DataType getDataType() {
        return dataType;
    }

    @Override
    public void setDataType(DataType dataType) {
        this.dataType = dataType;
    }

    private Buffer getBuffer() {
        return ((Buffer) unwrap().unwrap());
    }
}
