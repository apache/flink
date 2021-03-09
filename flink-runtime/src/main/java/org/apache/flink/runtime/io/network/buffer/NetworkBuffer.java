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

import org.apache.flink.shaded.netty4.io.netty.buffer.AbstractReferenceCountedByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufAllocator;
import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Wrapper for pooled {@link MemorySegment} instances.
 *
 * <p><strong>NOTE:</strong> before using this buffer in the netty stack, a buffer allocator must be
 * set via {@link #setAllocator(ByteBufAllocator)}!
 */
public class NetworkBuffer extends AbstractReferenceCountedByteBuf implements Buffer {

    /** The backing {@link MemorySegment} instance. */
    private final MemorySegment memorySegment;

    /** The recycler for the backing {@link MemorySegment}. */
    private final BufferRecycler recycler;

    /** The {@link DataType} this buffer represents. */
    private DataType dataType;

    /** Allocator for further byte buffers (needed by netty). */
    private ByteBufAllocator allocator;

    /**
     * The current size of the buffer in the range from 0 (inclusive) to the size of the backing
     * {@link MemorySegment} (inclusive).
     */
    private int currentSize;

    /** Whether the buffer is compressed or not. */
    private boolean isCompressed = false;

    /**
     * Creates a new buffer instance backed by the given <tt>memorySegment</tt> with <tt>0</tt> for
     * the <tt>readerIndex</tt> and <tt>writerIndex</tt>.
     *
     * @param memorySegment backing memory segment (defines {@link #maxCapacity})
     * @param recycler will be called to recycle this buffer once the reference count is <tt>0</tt>
     */
    public NetworkBuffer(MemorySegment memorySegment, BufferRecycler recycler) {
        this(memorySegment, recycler, DataType.DATA_BUFFER);
    }

    /**
     * Creates a new buffer instance backed by the given <tt>memorySegment</tt> with <tt>0</tt> for
     * the <tt>readerIndex</tt> and <tt>writerIndex</tt>.
     *
     * @param memorySegment backing memory segment (defines {@link #maxCapacity})
     * @param recycler will be called to recycle this buffer once the reference count is <tt>0</tt>
     * @param dataType the {@link DataType} this buffer represents
     */
    public NetworkBuffer(MemorySegment memorySegment, BufferRecycler recycler, DataType dataType) {
        this(memorySegment, recycler, dataType, 0);
    }

    /**
     * Creates a new buffer instance backed by the given <tt>memorySegment</tt> with <tt>0</tt> for
     * the <tt>readerIndex</tt> and <tt>size</tt> as <tt>writerIndex</tt>.
     *
     * @param memorySegment backing memory segment (defines {@link #maxCapacity})
     * @param recycler will be called to recycle this buffer once the reference count is <tt>0</tt>
     * @param dataType the {@link DataType} this buffer represents
     * @param size current size of data in the buffer, i.e. the writer index to set
     */
    public NetworkBuffer(
            MemorySegment memorySegment, BufferRecycler recycler, DataType dataType, int size) {
        this(memorySegment, recycler, dataType, false, size);
    }

    /**
     * Creates a new buffer instance backed by the given <tt>memorySegment</tt> with <tt>0</tt> for
     * the <tt>readerIndex</tt> and <tt>size</tt> as <tt>writerIndex</tt>.
     *
     * @param memorySegment backing memory segment (defines {@link #maxCapacity})
     * @param recycler will be called to recycle this buffer once the reference count is <tt>0</tt>
     * @param dataType the {@link DataType} this buffer represents
     * @param size current size of data in the buffer, i.e. the writer index to set
     * @param isCompressed whether the buffer is compressed or not
     */
    public NetworkBuffer(
            MemorySegment memorySegment,
            BufferRecycler recycler,
            DataType dataType,
            boolean isCompressed,
            int size) {
        super(memorySegment.size());
        this.memorySegment = checkNotNull(memorySegment);
        this.recycler = checkNotNull(recycler);
        this.dataType = dataType;
        this.isCompressed = isCompressed;
        this.currentSize = memorySegment.size();
        setSize(size);
    }

    @Override
    public boolean isBuffer() {
        return dataType.isBuffer();
    }

    @Override
    public MemorySegment getMemorySegment() {
        ensureAccessible();

        return memorySegment;
    }

    @Override
    public int getMemorySegmentOffset() {
        return 0;
    }

    @Override
    public BufferRecycler getRecycler() {
        return recycler;
    }

    @Override
    public void recycleBuffer() {
        release();
    }

    @Override
    public boolean isRecycled() {
        return refCnt() == 0;
    }

    @Override
    public NetworkBuffer retainBuffer() {
        return (NetworkBuffer) super.retain();
    }

    @Override
    public ReadOnlySlicedNetworkBuffer readOnlySlice() {
        return readOnlySlice(readerIndex(), readableBytes());
    }

    @Override
    public ReadOnlySlicedNetworkBuffer readOnlySlice(int index, int length) {
        checkState(!isCompressed, "Unable to slice a compressed buffer.");
        return new ReadOnlySlicedNetworkBuffer(this, index, length);
    }

    @Override
    protected void deallocate() {
        recycler.recycle(memorySegment);
    }

    @Override
    protected byte _getByte(int index) {
        return memorySegment.get(index);
    }

    @Override
    protected short _getShort(int index) {
        return memorySegment.getShortBigEndian(index);
    }

    @Override
    protected short _getShortLE(int index) {
        return memorySegment.getShortLittleEndian(index);
    }

    @Override
    protected int _getUnsignedMedium(int index) {
        // from UnpooledDirectByteBuf:
        return (getByte(index) & 0xff) << 16
                | (getByte(index + 1) & 0xff) << 8
                | getByte(index + 2) & 0xff;
    }

    @Override
    protected int _getUnsignedMediumLE(int index) {
        // from UnpooledDirectByteBuf:
        return getByte(index) & 255
                | (getByte(index + 1) & 255) << 8
                | (getByte(index + 2) & 255) << 16;
    }

    @Override
    protected int _getInt(int index) {
        return memorySegment.getIntBigEndian(index);
    }

    @Override
    protected int _getIntLE(int index) {
        return memorySegment.getIntLittleEndian(index);
    }

    @Override
    protected long _getLong(int index) {
        return memorySegment.getLongBigEndian(index);
    }

    @Override
    protected long _getLongLE(int index) {
        return memorySegment.getLongLittleEndian(index);
    }

    @Override
    protected void _setByte(int index, int value) {
        memorySegment.put(index, (byte) value);
    }

    @Override
    protected void _setShort(int index, int value) {
        memorySegment.putShortBigEndian(index, (short) value);
    }

    @Override
    protected void _setShortLE(int index, int value) {
        memorySegment.putShortLittleEndian(index, (short) value);
    }

    @Override
    protected void _setMedium(int index, int value) {
        // from UnpooledDirectByteBuf:
        setByte(index, (byte) (value >>> 16));
        setByte(index + 1, (byte) (value >>> 8));
        setByte(index + 2, (byte) value);
    }

    @Override
    protected void _setMediumLE(int index, int value) {
        // from UnpooledDirectByteBuf:
        setByte(index, (byte) value);
        setByte(index + 1, (byte) (value >>> 8));
        setByte(index + 2, (byte) (value >>> 16));
    }

    @Override
    protected void _setInt(int index, int value) {
        memorySegment.putIntBigEndian(index, value);
    }

    @Override
    protected void _setIntLE(int index, int value) {
        memorySegment.putIntLittleEndian(index, value);
    }

    @Override
    protected void _setLong(int index, long value) {
        memorySegment.putLongBigEndian(index, value);
    }

    @Override
    protected void _setLongLE(int index, long value) {
        memorySegment.putLongLittleEndian(index, value);
    }

    @Override
    public int capacity() {
        return currentSize;
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
    public ByteBuf capacity(int newCapacity) {
        ensureAccessible();

        if (newCapacity < 0 || newCapacity > maxCapacity()) {
            throw new IllegalArgumentException(
                    "Size of buffer must be >= 0 and <= "
                            + memorySegment.size()
                            + ", but was "
                            + newCapacity
                            + ".");
        }

        this.currentSize = newCapacity;
        return this;
    }

    @Override
    public ByteOrder order() {
        return ByteOrder.BIG_ENDIAN;
    }

    @Override
    public ByteBuf unwrap() {
        // not a wrapper of another buffer
        return null;
    }

    @Override
    public boolean isDirect() {
        return memorySegment.isOffHeap();
    }

    @Override
    public ByteBuf getBytes(int index, ByteBuf dst, int dstIndex, int length) {
        // from UnpooledDirectByteBuf:
        checkDstIndex(index, length, dstIndex, dst.capacity());

        if (dst.hasArray()) {
            getBytes(index, dst.array(), dst.arrayOffset() + dstIndex, length);
        } else if (dst.nioBufferCount() > 0) {
            for (ByteBuffer bb : dst.nioBuffers(dstIndex, length)) {
                int bbLen = bb.remaining();
                getBytes(index, bb);
                index += bbLen;
            }
        } else {
            dst.setBytes(dstIndex, this, index, length);
        }
        return this;
    }

    @Override
    public ByteBuf getBytes(int index, byte[] dst, int dstIndex, int length) {
        checkDstIndex(index, length, dstIndex, dst.length);

        memorySegment.get(index, dst, dstIndex, length);
        return this;
    }

    @Override
    public ByteBuf getBytes(int index, ByteBuffer dst) {
        checkIndex(index, dst.remaining());

        memorySegment.get(index, dst, dst.remaining());
        return this;
    }

    @Override
    public ByteBuf getBytes(int index, OutputStream out, int length) throws IOException {
        // adapted from UnpooledDirectByteBuf:
        checkIndex(index, length);
        if (length == 0) {
            return this;
        }

        if (memorySegment.isOffHeap()) {
            byte[] tmp = new byte[length];
            ByteBuffer tmpBuf = memorySegment.wrap(index, length);
            tmpBuf.get(tmp);
            out.write(tmp);
        } else {
            out.write(memorySegment.getArray(), index, length);
        }

        return this;
    }

    @Override
    public int getBytes(int index, GatheringByteChannel out, int length) throws IOException {
        // adapted from UnpooledDirectByteBuf:
        checkIndex(index, length);
        if (length == 0) {
            return 0;
        }

        ByteBuffer tmpBuf = memorySegment.wrap(index, length);
        return out.write(tmpBuf);
    }

    @Override
    public int getBytes(int index, FileChannel out, long position, int length) throws IOException {
        // adapted from UnpooledDirectByteBuf:
        checkIndex(index, length);
        if (length == 0) {
            return 0;
        }

        ByteBuffer tmpBuf = memorySegment.wrap(index, length);
        return out.write(tmpBuf, position);
    }

    @Override
    public ByteBuf setBytes(int index, ByteBuf src, int srcIndex, int length) {
        // from UnpooledDirectByteBuf:
        checkSrcIndex(index, length, srcIndex, src.capacity());
        if (src.nioBufferCount() > 0) {
            for (ByteBuffer bb : src.nioBuffers(srcIndex, length)) {
                int bbLen = bb.remaining();
                setBytes(index, bb);
                index += bbLen;
            }
        } else {
            src.getBytes(srcIndex, this, index, length);
        }
        return this;
    }

    @Override
    public ByteBuf setBytes(int index, byte[] src, int srcIndex, int length) {
        // adapted from UnpooledDirectByteBuf:
        checkSrcIndex(index, length, srcIndex, src.length);

        ByteBuffer tmpBuf = memorySegment.wrap(index, length);
        tmpBuf.put(src, srcIndex, length);
        return this;
    }

    @Override
    public ByteBuf setBytes(int index, ByteBuffer src) {
        // adapted from UnpooledDirectByteBuf:
        checkIndex(index, src.remaining());

        ByteBuffer tmpBuf = memorySegment.wrap(index, src.remaining());
        tmpBuf.put(src);
        return this;
    }

    @Override
    public int setBytes(int index, InputStream in, int length) throws IOException {
        // adapted from UnpooledDirectByteBuf:
        checkIndex(index, length);

        if (memorySegment.isOffHeap()) {
            byte[] tmp = new byte[length];
            int readBytes = in.read(tmp);
            if (readBytes <= 0) {
                return readBytes;
            }
            ByteBuffer tmpBuf = memorySegment.wrap(index, length);
            tmpBuf.put(tmp, 0, readBytes);
            return readBytes;
        } else {
            return in.read(memorySegment.getArray(), index, length);
        }
    }

    @Override
    public int setBytes(int index, ScatteringByteChannel in, int length) throws IOException {
        // adapted from UnpooledDirectByteBuf:
        checkIndex(index, length);

        ByteBuffer tmpBuf = memorySegment.wrap(index, length);
        try {
            return in.read(tmpBuf);
        } catch (ClosedChannelException ignored) {
            return -1;
        }
    }

    @Override
    public int setBytes(int index, FileChannel in, long position, int length) throws IOException {
        // adapted from UnpooledDirectByteBuf:
        checkIndex(index, length);

        ByteBuffer tmpBuf = memorySegment.wrap(index, length);
        try {
            return in.read(tmpBuf, position);
        } catch (ClosedChannelException ignored) {
            return -1;
        }
    }

    @Override
    public ByteBufAllocator alloc() {
        return checkNotNull(allocator);
    }

    @Override
    public void setAllocator(ByteBufAllocator allocator) {
        this.allocator = allocator;
    }

    @Override
    public ByteBuf copy(int index, int length) {
        checkIndex(index, length);

        ByteBuf copy = alloc().buffer(length, maxCapacity());
        copy.writeBytes(this, index, length);
        return copy;
    }

    @Override
    public ByteBuf readBytes(int length) {
        // copied from the one in netty 4.0.50 fixing the wrong allocator being used
        checkReadableBytes(length);
        if (length == 0) {
            return Unpooled.EMPTY_BUFFER;
        }

        ByteBuf buf = alloc().buffer(length, maxCapacity());
        int readerIndex = readerIndex();
        buf.writeBytes(this, readerIndex, length);
        readerIndex(readerIndex + length);
        return buf;
    }

    @Override
    public int nioBufferCount() {
        return 1;
    }

    @Override
    public ByteBuffer getNioBufferReadable() {
        return nioBuffer();
    }

    @Override
    public ByteBuffer getNioBuffer(int index, int length) {
        return nioBuffer(index, length);
    }

    @Override
    public ByteBuffer nioBuffer(int index, int length) {
        checkIndex(index, length);
        return memorySegment.wrap(index, length).slice();
    }

    @Override
    public ByteBuffer internalNioBuffer(int index, int length) {
        return nioBuffer(index, length);
    }

    @Override
    public ByteBuffer[] nioBuffers(int index, int length) {
        return new ByteBuffer[] {nioBuffer(index, length)};
    }

    @Override
    public boolean hasArray() {
        return !memorySegment.isOffHeap();
    }

    @Override
    public byte[] array() {
        ensureAccessible();

        return memorySegment.getArray();
    }

    @Override
    public int arrayOffset() {
        return 0;
    }

    @Override
    public boolean hasMemoryAddress() {
        return memorySegment.isOffHeap();
    }

    @Override
    public long memoryAddress() {
        return memorySegment.getAddress();
    }

    @Override
    public String toString() {
        if (refCnt() == 0) {
            return String.format("Buffer %s (freed)", hashCode());
        }

        StringBuilder buf =
                new StringBuilder()
                        .append("Buffer ")
                        .append(hashCode())
                        .append(" (ridx: ")
                        .append(readerIndex())
                        .append(", widx: ")
                        .append(writerIndex())
                        .append(", cap: ")
                        .append(capacity());
        if (maxCapacity() != Integer.MAX_VALUE) {
            buf.append('/').append(maxCapacity());
        }
        buf.append(", ref count: ").append(refCnt()).append(')');
        return buf.toString();
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
        ensureAccessible();

        this.dataType = dataType;
    }
}
