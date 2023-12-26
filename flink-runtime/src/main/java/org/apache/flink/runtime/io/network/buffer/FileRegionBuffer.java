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
import org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufAllocator;
import org.apache.flink.shaded.netty4.io.netty.channel.DefaultFileRegion;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class represents a chunk of data in a file channel. Its purpose is to be passed to the netty
 * code and to be written to the socket via the zero-copy direct transfer capabilities of {@link
 * FileChannel#transferTo(long, long, WritableByteChannel)}.
 *
 * <p>This class implements {@link Buffer} mainly for compatible with existing usages. It can be
 * thought of as a "lazy buffer" that does not hold the data directly, although the data can be
 * fetches as a read-only {@code ByteBuffer} when needed, for example in local input channels. See
 * {@link #readInto(MemorySegment)} and {@link #getNioBufferReadable()}. Because this buffer is
 * read-only, the modification methods (and methods that give a writable buffer) throw {@link
 * UnsupportedOperationException}.
 *
 * <p>This class extends from Netty's {@link DefaultFileRegion}, similar as the {@link
 * NetworkBuffer} extends from Netty's {@link ByteBuf}. That way we can pass both of them to Netty
 * in the same way, and Netty will internally treat them appropriately.
 */
public class FileRegionBuffer extends DefaultFileRegion implements Buffer {

    private final FileChannel fileChannel;

    /** The {@link DataType} this buffer represents. */
    private final DataType dataType;

    /** Whether the buffer is compressed or not. */
    private final boolean isCompressed;

    public FileRegionBuffer(
            FileChannel fileChannel,
            long fileChannelPosition,
            int bufferSize,
            DataType dataType,
            boolean isCompressed) {

        super(fileChannel, fileChannelPosition, bufferSize);

        this.fileChannel = checkNotNull(fileChannel);
        this.dataType = checkNotNull(dataType);
        this.isCompressed = isCompressed;
    }

    private int bufferSize() {
        // this is guaranteed to not be lossy, because we initialize this from
        // an int in the constructor.
        return (int) count();
    }

    // ------------------------------------------------------------------------
    // Buffer override methods
    // ------------------------------------------------------------------------

    @Override
    public boolean isBuffer() {
        return dataType.isBuffer();
    }

    @Override
    public MemorySegment getMemorySegment() {
        throw new UnsupportedOperationException("Method should never be called.");
    }

    @Override
    public int getMemorySegmentOffset() {
        throw new UnsupportedOperationException("Method should never be called.");
    }

    @Override
    public ReadOnlySlicedNetworkBuffer readOnlySlice() {
        throw new UnsupportedOperationException("Method should never be called.");
    }

    @Override
    public ReadOnlySlicedNetworkBuffer readOnlySlice(int index, int length) {
        throw new UnsupportedOperationException("Method should never be called.");
    }

    @Override
    public int getMaxCapacity() {
        throw new UnsupportedOperationException("Method should never be called.");
    }

    @Override
    public int getReaderIndex() {
        throw new UnsupportedOperationException("Method should never be called.");
    }

    @Override
    public void setReaderIndex(int readerIndex) throws IndexOutOfBoundsException {
        throw new UnsupportedOperationException("Method should never be called.");
    }

    /**
     * This method is only called by tests and by event-deserialization, like checkpoint barriers.
     * Because such events are not used for bounded intermediate results, this method currently
     * executes only in tests.
     */
    @Override
    public ByteBuffer getNioBufferReadable() {
        try {
            final ByteBuffer buffer = ByteBuffer.allocateDirect(bufferSize());
            BufferReaderWriterUtil.readByteBufferFully(fileChannel, buffer, position());
            buffer.flip();
            return buffer;
        } catch (IOException e) {
            // this is not very pretty, but given that this code runs only in tests
            // the exception wrapping here is simpler than updating the method signature
            // to declare IOExceptions, as would be necessary for a proper "lazy buffer".
            throw new FlinkRuntimeException(e.getMessage(), e);
        }
    }

    @Override
    public ByteBuffer getNioBuffer(int index, int length) throws IndexOutOfBoundsException {
        throw new UnsupportedOperationException("Method should never be called.");
    }

    @Override
    public ByteBuf asByteBuf() {
        throw new UnsupportedOperationException("Method should never be called.");
    }

    @Override
    public void setSize(int writerIndex) {
        throw new UnsupportedOperationException("Method should never be called.");
    }

    @Override
    public int getSize() {
        return bufferSize();
    }

    @Override
    public int readableBytes() {
        return bufferSize();
    }

    @Override
    public void setAllocator(ByteBufAllocator allocator) {
        // nothing to do
    }

    @Override
    public BufferRecycler getRecycler() {
        return null;
    }

    @Override
    public void setRecycler(BufferRecycler bufferRecycler) {
        throw new UnsupportedOperationException("Method should never be called.");
    }

    @Override
    public void recycleBuffer() {
        // nothing to do
    }

    @Override
    public boolean isRecycled() {
        return false;
    }

    @Override
    public FileRegionBuffer retainBuffer() {
        return (FileRegionBuffer) super.retain();
    }

    @Override
    public boolean isCompressed() {
        return isCompressed;
    }

    @Override
    public void setCompressed(boolean isCompressed) {
        throw new UnsupportedOperationException("Method should never be called.");
    }

    @Override
    public DataType getDataType() {
        return dataType;
    }

    @Override
    public void setDataType(DataType dataType) {
        throw new UnsupportedOperationException("Method should never be called.");
    }

    // ------------------------------------------------------------------------
    // File region override methods
    // ------------------------------------------------------------------------

    @Override
    public void deallocate() {
        // nothing to do
    }

    // ------------------------------------------------------------------------
    // Utils
    // ------------------------------------------------------------------------

    public Buffer readInto(MemorySegment segment) throws IOException {
        final ByteBuffer buffer = segment.wrap(0, bufferSize());
        BufferReaderWriterUtil.readByteBufferFully(fileChannel, buffer, position());

        return new NetworkBuffer(
                segment,
                BufferRecycler.DummyBufferRecycler.INSTANCE,
                dataType,
                isCompressed,
                bufferSize());
    }
}
