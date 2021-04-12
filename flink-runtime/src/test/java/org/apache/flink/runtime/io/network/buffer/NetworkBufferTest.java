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

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/** Tests for the {@link NetworkBuffer} class. */
public class NetworkBufferTest extends AbstractByteBufTest {

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

        assertSame(ByteOrder.BIG_ENDIAN, buffer.order());
        assertEquals(0, buffer.readerIndex());
        assertEquals(0, buffer.writerIndex());
        return buffer;
    }

    @Test
    public void testDataBufferIsBuffer() {
        assertFalse(newBuffer(1024, 1024, false).isBuffer());
    }

    @Test
    public void testEventBufferIsBuffer() {
        assertFalse(newBuffer(1024, 1024, false).isBuffer());
    }

    @Test
    public void testDataBufferTagAsEvent() {
        testTagAsEvent(true);
    }

    @Test
    public void testEventBufferTagAsEvent() {
        testTagAsEvent(false);
    }

    private static void testTagAsEvent(boolean isBuffer) {
        NetworkBuffer buffer = newBuffer(1024, 1024, isBuffer);
        buffer.setDataType(Buffer.DataType.EVENT_BUFFER);
        assertFalse(buffer.isBuffer());
    }

    @Test
    public void testDataBufferGetMemorySegment() {
        testGetMemorySegment(true);
    }

    @Test
    public void testEventBufferGetMemorySegment() {
        testGetMemorySegment(false);
    }

    private static void testGetMemorySegment(boolean isBuffer) {
        final MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(1024);
        Buffer.DataType dataType =
                isBuffer ? Buffer.DataType.DATA_BUFFER : Buffer.DataType.EVENT_BUFFER;
        NetworkBuffer buffer = new NetworkBuffer(segment, FreeingBufferRecycler.INSTANCE, dataType);
        assertSame(segment, buffer.getMemorySegment());
    }

    @Test
    public void testDataBufferGetRecycler() {
        testGetRecycler(true);
    }

    @Test
    public void testEventBufferGetRecycler() {
        testGetRecycler(false);
    }

    private static void testGetRecycler(boolean isBuffer) {
        BufferRecycler recycler = MemorySegment::free;

        NetworkBuffer dataBuffer = newBuffer(1024, 1024, isBuffer, recycler);
        assertSame(recycler, dataBuffer.getRecycler());
    }

    @Test
    public void testDataBufferRecycleBuffer() {
        testRecycleBuffer(true);
    }

    @Test
    public void testEventBufferRecycleBuffer() {
        testRecycleBuffer(false);
    }

    /**
     * Tests that {@link NetworkBuffer#recycleBuffer()} and {@link NetworkBuffer#isRecycled()} are
     * coupled and are also consistent with {@link NetworkBuffer#refCnt()}.
     */
    private static void testRecycleBuffer(boolean isBuffer) {
        NetworkBuffer buffer = newBuffer(1024, 1024, isBuffer);
        assertFalse(buffer.isRecycled());
        buffer.recycleBuffer();
        assertTrue(buffer.isRecycled());
        assertEquals(0, buffer.refCnt());
    }

    @Test
    public void testDataBufferRetainBuffer() {
        testRetainBuffer(true);
    }

    @Test
    public void testEventBufferRetainBuffer() {
        testRetainBuffer(false);
    }

    /**
     * Tests that {@link NetworkBuffer#retainBuffer()} and {@link NetworkBuffer#isRecycled()} are
     * coupled and are also consistent with {@link NetworkBuffer#refCnt()}.
     */
    private static void testRetainBuffer(boolean isBuffer) {
        NetworkBuffer buffer = newBuffer(1024, 1024, isBuffer);
        assertFalse(buffer.isRecycled());
        buffer.retainBuffer();
        assertFalse(buffer.isRecycled());
        assertEquals(2, buffer.refCnt());
    }

    @Test
    public void testDataBufferCreateSlice1() {
        testCreateSlice1(true);
    }

    @Test
    public void testEventBufferCreateSlice1() {
        testCreateSlice1(false);
    }

    private static void testCreateSlice1(boolean isBuffer) {
        NetworkBuffer buffer = newBuffer(1024, 1024, isBuffer);
        buffer.setSize(10); // fake some data
        ReadOnlySlicedNetworkBuffer slice = buffer.readOnlySlice();

        assertEquals(0, slice.getReaderIndex());
        assertEquals(10, slice.getSize());
        assertSame(buffer, slice.unwrap().unwrap());

        // slice indices should be independent:
        buffer.setSize(8);
        buffer.setReaderIndex(2);
        assertEquals(0, slice.getReaderIndex());
        assertEquals(10, slice.getSize());
    }

    @Test
    public void testDataBufferCreateSlice2() {
        testCreateSlice2(true);
    }

    @Test
    public void testEventBufferCreateSlice2() {
        testCreateSlice2(false);
    }

    private static void testCreateSlice2(boolean isBuffer) {
        NetworkBuffer buffer = newBuffer(1024, 1024, isBuffer);
        buffer.setSize(2); // fake some data
        ReadOnlySlicedNetworkBuffer slice = buffer.readOnlySlice(1, 10);

        assertEquals(0, slice.getReaderIndex());
        assertEquals(10, slice.getSize());
        assertSame(buffer, slice.unwrap().unwrap());

        // slice indices should be independent:
        buffer.setSize(8);
        buffer.setReaderIndex(2);
        assertEquals(0, slice.getReaderIndex());
        assertEquals(10, slice.getSize());
    }

    @Test
    public void testDataBufferGetMaxCapacity() {
        testGetMaxCapacity(true);
    }

    @Test
    public void testEventBufferGetMaxCapacity() {
        testGetMaxCapacity(false);
    }

    private static void testGetMaxCapacity(boolean isBuffer) {
        NetworkBuffer buffer = newBuffer(100, 1024, isBuffer);
        assertEquals(1024, buffer.getMaxCapacity());
        MemorySegment segment = buffer.getMemorySegment();
        Assert.assertEquals(segment.size(), buffer.getMaxCapacity());
        Assert.assertEquals(segment.size(), buffer.maxCapacity());
    }

    @Test
    public void testDataBufferGetSetReaderIndex() {
        testGetSetReaderIndex(true);
    }

    @Test
    public void testEventBufferGetSetReaderIndex() {
        testGetSetReaderIndex(false);
    }

    /**
     * Tests that {@link NetworkBuffer#setReaderIndex(int)} and {@link
     * NetworkBuffer#getReaderIndex()} are consistent.
     */
    private static void testGetSetReaderIndex(boolean isBuffer) {
        NetworkBuffer buffer = newBuffer(100, 1024, isBuffer);
        assertEquals(0, buffer.getReaderIndex());

        // fake some data
        buffer.setSize(100);
        assertEquals(0, buffer.getReaderIndex());
        buffer.setReaderIndex(1);
        assertEquals(1, buffer.getReaderIndex());
    }

    @Test
    public void testDataBufferSetGetSize() {
        testSetGetSize(true);
    }

    @Test
    public void testEventBufferSetGetSize() {
        testSetGetSize(false);
    }

    private static void testSetGetSize(boolean isBuffer) {
        NetworkBuffer buffer = newBuffer(1024, 1024, isBuffer);

        assertEquals(0, buffer.getSize()); // initially 0
        assertEquals(buffer.writerIndex(), buffer.getSize());
        assertEquals(0, buffer.readerIndex()); // initially 0

        buffer.setSize(10);
        assertEquals(10, buffer.getSize());
        assertEquals(buffer.writerIndex(), buffer.getSize());
        assertEquals(0, buffer.readerIndex()); // independent
    }

    @Test
    public void testDataBufferReadableBytes() {
        testReadableBytes(true);
    }

    @Test
    public void testEventBufferReadableBytes() {
        testReadableBytes(false);
    }

    private static void testReadableBytes(boolean isBuffer) {
        NetworkBuffer buffer = newBuffer(1024, 1024, isBuffer);

        assertEquals(0, buffer.readableBytes());
        buffer.setSize(10);
        assertEquals(10, buffer.readableBytes());
        buffer.setReaderIndex(2);
        assertEquals(8, buffer.readableBytes());
        buffer.setReaderIndex(10);
        assertEquals(0, buffer.readableBytes());
    }

    @Test
    public void testDataBufferGetNioBufferReadable() {
        testGetNioBufferReadable(true);
    }

    @Test
    public void testEventBufferGetNioBufferReadable() {
        testGetNioBufferReadable(false);
    }

    private void testGetNioBufferReadable(boolean isBuffer) {
        NetworkBuffer buffer = newBuffer(1024, 1024, isBuffer);

        ByteBuffer byteBuffer = buffer.getNioBufferReadable();
        assertFalse(byteBuffer.isReadOnly());
        assertEquals(0, byteBuffer.remaining());
        assertEquals(0, byteBuffer.limit());
        assertEquals(0, byteBuffer.capacity());

        // add some data
        buffer.setSize(10);
        // nothing changes in the byteBuffer
        assertEquals(0, byteBuffer.remaining());
        assertEquals(0, byteBuffer.limit());
        assertEquals(0, byteBuffer.capacity());
        // get a new byteBuffer (should have updated indices)
        byteBuffer = buffer.getNioBufferReadable();
        assertFalse(byteBuffer.isReadOnly());
        assertEquals(10, byteBuffer.remaining());
        assertEquals(10, byteBuffer.limit());
        assertEquals(10, byteBuffer.capacity());

        // modify byteBuffer position and verify nothing has changed in the original buffer
        byteBuffer.position(1);
        assertEquals(0, buffer.getReaderIndex());
        assertEquals(10, buffer.getSize());
    }

    @Test
    public void testGetNioBufferReadableThreadSafe() {
        NetworkBuffer buffer = newBuffer(1024, 1024);
        testGetNioBufferReadableThreadSafe(buffer);
    }

    static void testGetNioBufferReadableThreadSafe(Buffer buffer) {
        ByteBuffer buf1 = buffer.getNioBufferReadable();
        ByteBuffer buf2 = buffer.getNioBufferReadable();

        assertNotNull(buf1);
        assertNotNull(buf2);

        assertTrue("Repeated call to getNioBuffer() returns the same nio buffer", buf1 != buf2);
    }

    @Test
    public void testDataBufferGetNioBuffer() {
        testGetNioBuffer(true);
    }

    @Test
    public void testEventBufferGetNioBuffer() {
        testGetNioBuffer(false);
    }

    private void testGetNioBuffer(boolean isBuffer) {
        NetworkBuffer buffer = newBuffer(1024, 1024, isBuffer);

        ByteBuffer byteBuffer = buffer.getNioBuffer(1, 1);
        assertFalse(byteBuffer.isReadOnly());
        assertEquals(1, byteBuffer.remaining());
        assertEquals(1, byteBuffer.limit());
        assertEquals(1, byteBuffer.capacity());

        // add some data
        buffer.setSize(10);
        // nothing changes in the byteBuffer
        assertEquals(1, byteBuffer.remaining());
        assertEquals(1, byteBuffer.limit());
        assertEquals(1, byteBuffer.capacity());
        // get a new byteBuffer (should have updated indices)
        byteBuffer = buffer.getNioBuffer(1, 2);
        assertFalse(byteBuffer.isReadOnly());
        assertEquals(2, byteBuffer.remaining());
        assertEquals(2, byteBuffer.limit());
        assertEquals(2, byteBuffer.capacity());

        // modify byteBuffer position and verify nothing has changed in the original buffer
        byteBuffer.position(1);
        assertEquals(0, buffer.getReaderIndex());
        assertEquals(10, buffer.getSize());
    }

    @Test
    public void testGetNioBufferThreadSafe() {
        NetworkBuffer buffer = newBuffer(1024, 1024);
        testGetNioBufferThreadSafe(buffer, 10);
    }

    static void testGetNioBufferThreadSafe(Buffer buffer, int length) {
        ByteBuffer buf1 = buffer.getNioBuffer(0, length);
        ByteBuffer buf2 = buffer.getNioBuffer(0, length);

        assertNotNull(buf1);
        assertNotNull(buf2);

        assertTrue(
                "Repeated call to getNioBuffer(int, int) returns the same nio buffer",
                buf1 != buf2);
    }

    @Test
    public void testDataBufferSetAllocator() {
        testSetAllocator(true);
    }

    @Test
    public void testEventBufferSetAllocator() {
        testSetAllocator(false);
    }

    private void testSetAllocator(boolean isBuffer) {
        NetworkBuffer buffer = newBuffer(1024, 1024, isBuffer);
        NettyBufferPool allocator = new NettyBufferPool(1);

        buffer.setAllocator(allocator);
        assertSame(allocator, buffer.alloc());
    }
}
