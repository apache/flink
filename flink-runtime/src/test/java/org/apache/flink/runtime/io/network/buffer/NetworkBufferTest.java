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
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.netty.NettyBufferPool;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link NetworkBuffer} class. */
class NetworkBufferTest extends AbstractByteBufTest {

    /** Upper limit for the max size that is sufficient for all the tests. */
    private static final int MAX_CAPACITY_UPPER_BOUND = 64 * 1024 * 1024;

    private static final NettyBufferPool NETTY_BUFFER_POOL = new NettyBufferPool(1);

    @Override
    protected NetworkBuffer newBuffer(int length, int maxCapacity) {
        return newBuffer(length, maxCapacity, false);
    }

    /**
     * Creates a new buffer for testing.
     *
     * @param length buffer capacity
     * @param maxCapacity buffer maximum capacity (will be used for the underlying {@link
     *     MemorySegment})
     * @param isBuffer whether the buffer should represent data (<tt>true</tt>) or an event
     *     (<tt>false</tt>)
     * @return the buffer
     */
    private static NetworkBuffer newBuffer(int length, int maxCapacity, boolean isBuffer) {
        return newBuffer(length, maxCapacity, isBuffer, FreeingBufferRecycler.INSTANCE);
    }

    /**
     * Creates a new buffer for testing.
     *
     * @param length buffer capacity
     * @param maxCapacity buffer maximum capacity (will be used for the underlying {@link
     *     MemorySegment})
     * @param isBuffer whether the buffer should represent data (<tt>true</tt>) or an event
     *     (<tt>false</tt>)
     * @param recycler the buffer recycler to use
     * @return the buffer
     */
    private static NetworkBuffer newBuffer(
            int length, int maxCapacity, boolean isBuffer, BufferRecycler recycler) {
        final MemorySegment segment =
                MemorySegmentFactory.allocateUnpooledSegment(
                        Math.min(maxCapacity, MAX_CAPACITY_UPPER_BOUND));

        Buffer.DataType dataType =
                isBuffer ? Buffer.DataType.DATA_BUFFER : Buffer.DataType.EVENT_BUFFER;
        NetworkBuffer buffer = new NetworkBuffer(segment, recycler, dataType);
        buffer.capacity(length);
        buffer.setAllocator(NETTY_BUFFER_POOL);

        assertThat(buffer.order()).isSameAs(ByteOrder.BIG_ENDIAN);
        assertThat(buffer.readerIndex()).isZero();
        assertThat(buffer.writerIndex()).isZero();
        return buffer;
    }

    @Test
    void testDataBufferIsBuffer() {
        assertThat(newBuffer(1024, 1024, true).isBuffer()).isTrue();
    }

    @Test
    void testEventBufferIsBuffer() {
        assertThat(newBuffer(1024, 1024, false).isBuffer()).isFalse();
    }

    @Test
    void testDataBufferTagAsEvent() {
        testTagAsEvent(true);
    }

    @Test
    void testEventBufferTagAsEvent() {
        testTagAsEvent(false);
    }

    private static void testTagAsEvent(boolean isBuffer) {
        NetworkBuffer buffer = newBuffer(1024, 1024, isBuffer);
        buffer.setDataType(Buffer.DataType.EVENT_BUFFER);
        assertThat(buffer.isBuffer()).isFalse();
    }

    @Test
    void testDataBufferGetMemorySegment() {
        testGetMemorySegment(true);
    }

    @Test
    void testEventBufferGetMemorySegment() {
        testGetMemorySegment(false);
    }

    private static void testGetMemorySegment(boolean isBuffer) {
        final MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(1024);
        Buffer.DataType dataType =
                isBuffer ? Buffer.DataType.DATA_BUFFER : Buffer.DataType.EVENT_BUFFER;
        NetworkBuffer buffer = new NetworkBuffer(segment, FreeingBufferRecycler.INSTANCE, dataType);
        assertThat(buffer.getMemorySegment()).isSameAs(segment);
    }

    @Test
    void testDataBufferGetRecycler() {
        testGetRecycler(true);
    }

    @Test
    void testEventBufferGetRecycler() {
        testGetRecycler(false);
    }

    private static void testGetRecycler(boolean isBuffer) {
        BufferRecycler recycler = MemorySegment::free;

        NetworkBuffer dataBuffer = newBuffer(1024, 1024, isBuffer, recycler);
        assertThat(dataBuffer.getRecycler()).isSameAs(recycler);
    }

    @Test
    void testDataBufferRecycleBuffer() {
        testRecycleBuffer(true);
    }

    @Test
    void testEventBufferRecycleBuffer() {
        testRecycleBuffer(false);
    }

    /**
     * Tests that {@link NetworkBuffer#recycleBuffer()} and {@link NetworkBuffer#isRecycled()} are
     * coupled and are also consistent with {@link NetworkBuffer#refCnt()}.
     */
    private static void testRecycleBuffer(boolean isBuffer) {
        NetworkBuffer buffer = newBuffer(1024, 1024, isBuffer);
        assertThat(buffer.isRecycled()).isFalse();
        buffer.recycleBuffer();
        assertThat(buffer.isRecycled()).isTrue();
        assertThat(buffer.refCnt()).isZero();
    }

    @Test
    void testDataBufferRetainBuffer() {
        testRetainBuffer(true);
    }

    @Test
    void testEventBufferRetainBuffer() {
        testRetainBuffer(false);
    }

    /**
     * Tests that {@link NetworkBuffer#retainBuffer()} and {@link NetworkBuffer#isRecycled()} are
     * coupled and are also consistent with {@link NetworkBuffer#refCnt()}.
     */
    private static void testRetainBuffer(boolean isBuffer) {
        NetworkBuffer buffer = newBuffer(1024, 1024, isBuffer);
        assertThat(buffer.isRecycled()).isFalse();
        buffer.retainBuffer();
        assertThat(buffer.isRecycled()).isFalse();
        assertThat(buffer.refCnt()).isEqualTo(2);
    }

    @Test
    void testDataBufferCreateSlice1() {
        testCreateSlice1(true);
    }

    @Test
    void testEventBufferCreateSlice1() {
        testCreateSlice1(false);
    }

    private static void testCreateSlice1(boolean isBuffer) {
        NetworkBuffer buffer = newBuffer(1024, 1024, isBuffer);
        buffer.setSize(10); // fake some data
        ReadOnlySlicedNetworkBuffer slice = buffer.readOnlySlice();

        assertThat(slice.getReaderIndex()).isZero();
        assertThat(slice.getSize()).isEqualTo(10);
        assertThat(slice.unwrap().unwrap()).isSameAs(buffer);

        // slice indices should be independent:
        buffer.setSize(8);
        buffer.setReaderIndex(2);
        assertThat(slice.getReaderIndex()).isZero();
        assertThat(slice.getSize()).isEqualTo(10);
    }

    @Test
    void testDataBufferCreateSlice2() {
        testCreateSlice2(true);
    }

    @Test
    void testEventBufferCreateSlice2() {
        testCreateSlice2(false);
    }

    private static void testCreateSlice2(boolean isBuffer) {
        NetworkBuffer buffer = newBuffer(1024, 1024, isBuffer);
        buffer.setSize(2); // fake some data
        ReadOnlySlicedNetworkBuffer slice = buffer.readOnlySlice(1, 10);

        assertThat(slice.getReaderIndex()).isZero();
        assertThat(slice.getSize()).isEqualTo(10);
        assertThat(slice.unwrap().unwrap()).isSameAs(buffer);

        // slice indices should be independent:
        buffer.setSize(8);
        buffer.setReaderIndex(2);
        assertThat(slice.getReaderIndex()).isZero();
        assertThat(slice.getSize()).isEqualTo(10);
    }

    @Test
    void testDataBufferGetMaxCapacity() {
        testGetMaxCapacity(true);
    }

    @Test
    void testEventBufferGetMaxCapacity() {
        testGetMaxCapacity(false);
    }

    private static void testGetMaxCapacity(boolean isBuffer) {
        NetworkBuffer buffer = newBuffer(100, 1024, isBuffer);
        assertThat(buffer.getMaxCapacity()).isEqualTo(1024);
        MemorySegment segment = buffer.getMemorySegment();
        assertThat(segment.size())
                .isEqualTo(buffer.getMaxCapacity())
                .isEqualTo(buffer.maxCapacity());
    }

    @Test
    void testDataBufferGetSetReaderIndex() {
        testGetSetReaderIndex(true);
    }

    @Test
    void testEventBufferGetSetReaderIndex() {
        testGetSetReaderIndex(false);
    }

    /**
     * Tests that {@link NetworkBuffer#setReaderIndex(int)} and {@link
     * NetworkBuffer#getReaderIndex()} are consistent.
     */
    private static void testGetSetReaderIndex(boolean isBuffer) {
        NetworkBuffer buffer = newBuffer(100, 1024, isBuffer);
        assertThat(buffer.getReaderIndex()).isZero();

        // fake some data
        buffer.setSize(100);
        assertThat(buffer.getReaderIndex()).isZero();
        buffer.setReaderIndex(1);
        assertThat(buffer.getReaderIndex()).isOne();
    }

    @Test
    void testDataBufferSetGetSize() {
        testSetGetSize(true);
    }

    @Test
    void testEventBufferSetGetSize() {
        testSetGetSize(false);
    }

    private static void testSetGetSize(boolean isBuffer) {
        NetworkBuffer buffer = newBuffer(1024, 1024, isBuffer);

        assertThat(buffer.getSize()).isZero(); // initially 0
        assertThat(buffer.writerIndex()).isEqualTo(buffer.getSize());
        assertThat(buffer.readerIndex()).isZero(); // initially 0

        buffer.setSize(10);
        assertThat(buffer.getSize()).isEqualTo(10);
        assertThat(buffer.writerIndex()).isEqualTo(buffer.getSize());
        assertThat(buffer.readerIndex()).isZero(); // independent
    }

    @Test
    void testDataBufferReadableBytes() {
        testReadableBytes(true);
    }

    @Test
    void testEventBufferReadableBytes() {
        testReadableBytes(false);
    }

    private static void testReadableBytes(boolean isBuffer) {
        NetworkBuffer buffer = newBuffer(1024, 1024, isBuffer);

        assertThat(buffer.readableBytes()).isZero();
        buffer.setSize(10);
        assertThat(buffer.readableBytes()).isEqualTo(10);
        buffer.setReaderIndex(2);
        assertThat(buffer.readableBytes()).isEqualTo(8);
        buffer.setReaderIndex(10);
        assertThat(buffer.readableBytes()).isZero();
    }

    @Test
    void testDataBufferGetNioBufferReadable() {
        testGetNioBufferReadable(true);
    }

    @Test
    void testEventBufferGetNioBufferReadable() {
        testGetNioBufferReadable(false);
    }

    private void testGetNioBufferReadable(boolean isBuffer) {
        NetworkBuffer buffer = newBuffer(1024, 1024, isBuffer);

        ByteBuffer byteBuffer = buffer.getNioBufferReadable();
        assertThat(byteBuffer.isReadOnly()).isFalse();
        assertThat(byteBuffer.remaining()).isZero();
        assertThat(byteBuffer.limit()).isZero();
        assertThat(byteBuffer.capacity()).isZero();

        // add some data
        buffer.setSize(10);
        // nothing changes in the byteBuffer
        assertThat(byteBuffer.remaining()).isZero();
        assertThat(byteBuffer.limit()).isZero();
        assertThat(byteBuffer.capacity()).isZero();
        // get a new byteBuffer (should have updated indices)
        byteBuffer = buffer.getNioBufferReadable();
        assertThat(byteBuffer.isReadOnly()).isFalse();
        assertThat(byteBuffer.remaining()).isEqualTo(10);
        assertThat(byteBuffer.limit()).isEqualTo(10);
        assertThat(byteBuffer.capacity()).isEqualTo(10);

        // modify byteBuffer position and verify nothing has changed in the original buffer
        byteBuffer.position(1);
        assertThat(buffer.getReaderIndex()).isZero();
        assertThat(buffer.getSize()).isEqualTo(10);
    }

    @Test
    void testGetNioBufferReadableThreadSafe() {
        NetworkBuffer buffer = newBuffer(1024, 1024);
        testGetNioBufferReadableThreadSafe(buffer);
    }

    static void testGetNioBufferReadableThreadSafe(Buffer buffer) {
        ByteBuffer buf1 = buffer.getNioBufferReadable();
        ByteBuffer buf2 = buffer.getNioBufferReadable();

        assertThat(buf1).isNotNull();
        assertThat(buf2).isNotNull();

        assertThat(buf1)
                .withFailMessage("Repeated call to getNioBuffer() returns the same nio buffer")
                .isNotSameAs(buf2);
    }

    @Test
    void testDataBufferGetNioBuffer() {
        testGetNioBuffer(true);
    }

    @Test
    void testEventBufferGetNioBuffer() {
        testGetNioBuffer(false);
    }

    private void testGetNioBuffer(boolean isBuffer) {
        NetworkBuffer buffer = newBuffer(1024, 1024, isBuffer);

        ByteBuffer byteBuffer = buffer.getNioBuffer(1, 1);
        assertThat(byteBuffer.isReadOnly()).isFalse();
        assertThat(byteBuffer.remaining()).isOne();
        assertThat(byteBuffer.limit()).isOne();
        assertThat(byteBuffer.capacity()).isOne();

        // add some data
        buffer.setSize(10);
        // nothing changes in the byteBuffer
        assertThat(byteBuffer.remaining()).isOne();
        assertThat(byteBuffer.limit()).isOne();
        assertThat(byteBuffer.capacity()).isOne();
        // get a new byteBuffer (should have updated indices)
        byteBuffer = buffer.getNioBuffer(1, 2);
        assertThat(byteBuffer.isReadOnly()).isFalse();
        assertThat(byteBuffer.remaining()).isEqualTo(2);
        assertThat(byteBuffer.limit()).isEqualTo(2);
        assertThat(byteBuffer.capacity()).isEqualTo(2);

        // modify byteBuffer position and verify nothing has changed in the original buffer
        byteBuffer.position(1);
        assertThat(buffer.getReaderIndex()).isZero();
        assertThat(buffer.getSize()).isEqualTo(10);
    }

    @Test
    void testGetNioBufferThreadSafe() {
        NetworkBuffer buffer = newBuffer(1024, 1024);
        testGetNioBufferThreadSafe(buffer, 10);
    }

    static void testGetNioBufferThreadSafe(Buffer buffer, int length) {
        ByteBuffer buf1 = buffer.getNioBuffer(0, length);
        ByteBuffer buf2 = buffer.getNioBuffer(0, length);

        assertThat(buf1).isNotNull();
        assertThat(buf2).isNotNull();

        assertThat(buf1)
                .withFailMessage(
                        "Repeated call to getNioBuffer(int, int) returns the same nio buffer")
                .isNotSameAs(buf2);
    }

    @Test
    void testDataBufferSetAllocator() {
        testSetAllocator(true);
    }

    @Test
    void testEventBufferSetAllocator() {
        testSetAllocator(false);
    }

    private void testSetAllocator(boolean isBuffer) {
        NetworkBuffer buffer = newBuffer(1024, 1024, isBuffer);
        NettyBufferPool allocator = new NettyBufferPool(1);

        buffer.setAllocator(allocator);
        assertThat(buffer.alloc()).isSameAs(allocator);
    }
}
