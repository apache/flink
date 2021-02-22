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
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.netty.NettyBufferPool;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/** Tests for {@link ReadOnlySlicedNetworkBuffer}. */
public class ReadOnlySlicedBufferTest {
    private static final int BUFFER_SIZE = 1024;
    private static final int DATA_SIZE = 10;

    private NetworkBuffer buffer;

    @Before
    public void setUp() throws Exception {
        final MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(BUFFER_SIZE);
        buffer =
                new NetworkBuffer(
                        segment, FreeingBufferRecycler.INSTANCE, Buffer.DataType.DATA_BUFFER, 0);
        for (int i = 0; i < DATA_SIZE; ++i) {
            buffer.writeByte(i);
        }
    }

    @Test
    public void testForwardsIsBuffer() throws IOException {
        assertEquals(buffer.isBuffer(), buffer.readOnlySlice().isBuffer());
        assertEquals(buffer.isBuffer(), buffer.readOnlySlice(1, 2).isBuffer());
        Buffer eventBuffer = EventSerializer.toBuffer(EndOfPartitionEvent.INSTANCE, false);
        assertEquals(eventBuffer.isBuffer(), eventBuffer.readOnlySlice().isBuffer());
        assertEquals(eventBuffer.isBuffer(), eventBuffer.readOnlySlice(1, 2).isBuffer());
    }

    @Test(expected = ReadOnlyBufferException.class)
    public void testSetDataTypeThrows1() {
        buffer.readOnlySlice().setDataType(Buffer.DataType.EVENT_BUFFER);
    }

    @Test(expected = ReadOnlyBufferException.class)
    public void testSetDataTypeThrows2() {
        buffer.readOnlySlice(1, 2).setDataType(Buffer.DataType.EVENT_BUFFER);
    }

    @Test
    public void testForwardsGetMemorySegment() {
        assertSame(buffer.getMemorySegment(), buffer.readOnlySlice().getMemorySegment());
        assertSame(buffer.getMemorySegment(), buffer.readOnlySlice(1, 2).getMemorySegment());
    }

    @Test
    public void testForwardsGetRecycler() {
        assertSame(buffer.getRecycler(), buffer.readOnlySlice().getRecycler());
        assertSame(buffer.getRecycler(), buffer.readOnlySlice(1, 2).getRecycler());
    }

    /**
     * Tests forwarding of both {@link ReadOnlySlicedNetworkBuffer#recycleBuffer()} and {@link
     * ReadOnlySlicedNetworkBuffer#isRecycled()}.
     */
    @Test
    public void testForwardsRecycleBuffer1() {
        ReadOnlySlicedNetworkBuffer slice = buffer.readOnlySlice();
        assertFalse(slice.isRecycled());
        slice.recycleBuffer();
        assertTrue(slice.isRecycled());
        assertTrue(buffer.isRecycled());
    }

    /**
     * Tests forwarding of both {@link ReadOnlySlicedNetworkBuffer#recycleBuffer()} and {@link
     * ReadOnlySlicedNetworkBuffer#isRecycled()}.
     */
    @Test
    public void testForwardsRecycleBuffer2() {
        ReadOnlySlicedNetworkBuffer slice = buffer.readOnlySlice(1, 2);
        assertFalse(slice.isRecycled());
        slice.recycleBuffer();
        assertTrue(slice.isRecycled());
        assertTrue(buffer.isRecycled());
    }

    /**
     * Tests forwarding of both {@link ReadOnlySlicedNetworkBuffer#recycleBuffer()} and {@link
     * ReadOnlySlicedNetworkBuffer#isRecycled()}.
     */
    @Test
    public void testForwardsRetainBuffer1() {
        ReadOnlySlicedNetworkBuffer slice = buffer.readOnlySlice();
        assertEquals(buffer.refCnt(), slice.refCnt());
        slice.retainBuffer();
        assertEquals(buffer.refCnt(), slice.refCnt());
    }

    /**
     * Tests forwarding of both {@link ReadOnlySlicedNetworkBuffer#retainBuffer()} and {@link
     * ReadOnlySlicedNetworkBuffer#isRecycled()}.
     */
    @Test
    public void testForwardsRetainBuffer2() {
        ReadOnlySlicedNetworkBuffer slice = buffer.readOnlySlice(1, 2);
        assertEquals(buffer.refCnt(), slice.refCnt());
        slice.retainBuffer();
        assertEquals(buffer.refCnt(), slice.refCnt());
    }

    @Test
    public void testCreateSlice1() {
        buffer.readByte(); // so that we do not start at position 0
        ReadOnlySlicedNetworkBuffer slice1 = buffer.readOnlySlice();
        buffer.readByte(); // should not influence the second slice at all
        ReadOnlySlicedNetworkBuffer slice2 = slice1.readOnlySlice();
        assertSame(buffer, slice2.unwrap().unwrap());
        assertSame(slice1.getMemorySegment(), slice2.getMemorySegment());
        assertEquals(1, slice1.getMemorySegmentOffset());
        assertEquals(slice1.getMemorySegmentOffset(), slice2.getMemorySegmentOffset());

        assertReadableBytes(slice1, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        assertReadableBytes(slice2, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    }

    @Test
    public void testCreateSlice2() {
        buffer.readByte(); // so that we do not start at position 0
        ReadOnlySlicedNetworkBuffer slice1 = buffer.readOnlySlice();
        buffer.readByte(); // should not influence the second slice at all
        ReadOnlySlicedNetworkBuffer slice2 = slice1.readOnlySlice(1, 2);
        assertSame(buffer, slice2.unwrap().unwrap());
        assertSame(slice1.getMemorySegment(), slice2.getMemorySegment());
        assertEquals(1, slice1.getMemorySegmentOffset());
        assertEquals(2, slice2.getMemorySegmentOffset());

        assertReadableBytes(slice1, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        assertReadableBytes(slice2, 2, 3);
    }

    @Test
    public void testCreateSlice3() {
        ReadOnlySlicedNetworkBuffer slice1 = buffer.readOnlySlice(1, 2);
        buffer.readByte(); // should not influence the second slice at all
        ReadOnlySlicedNetworkBuffer slice2 = slice1.readOnlySlice();
        assertSame(buffer, slice2.unwrap().unwrap());
        assertSame(slice1.getMemorySegment(), slice2.getMemorySegment());
        assertEquals(1, slice1.getMemorySegmentOffset());
        assertEquals(1, slice2.getMemorySegmentOffset());

        assertReadableBytes(slice1, 1, 2);
        assertReadableBytes(slice2, 1, 2);
    }

    @Test
    public void testCreateSlice4() {
        ReadOnlySlicedNetworkBuffer slice1 = buffer.readOnlySlice(1, 5);
        buffer.readByte(); // should not influence the second slice at all
        ReadOnlySlicedNetworkBuffer slice2 = slice1.readOnlySlice(1, 2);
        assertSame(buffer, slice2.unwrap().unwrap());
        assertSame(slice1.getMemorySegment(), slice2.getMemorySegment());
        assertEquals(1, slice1.getMemorySegmentOffset());
        assertEquals(2, slice2.getMemorySegmentOffset());

        assertReadableBytes(slice1, 1, 2, 3, 4, 5);
        assertReadableBytes(slice2, 2, 3);
    }

    @Test
    public void testGetMaxCapacity() {
        assertEquals(DATA_SIZE, buffer.readOnlySlice().getMaxCapacity());
        assertEquals(2, buffer.readOnlySlice(1, 2).getMaxCapacity());
    }

    /**
     * Tests the independence of the reader index via {@link
     * ReadOnlySlicedNetworkBuffer#setReaderIndex(int)} and {@link
     * ReadOnlySlicedNetworkBuffer#getReaderIndex()}.
     */
    @Test
    public void testGetSetReaderIndex1() {
        testGetSetReaderIndex(buffer.readOnlySlice());
    }

    /**
     * Tests the independence of the reader index via {@link
     * ReadOnlySlicedNetworkBuffer#setReaderIndex(int)} and {@link
     * ReadOnlySlicedNetworkBuffer#getReaderIndex()}.
     */
    @Test
    public void testGetSetReaderIndex2() {
        testGetSetReaderIndex(buffer.readOnlySlice(1, 2));
    }

    private void testGetSetReaderIndex(ReadOnlySlicedNetworkBuffer slice) {
        assertEquals(0, buffer.getReaderIndex());
        assertEquals(0, slice.getReaderIndex());
        slice.setReaderIndex(1);
        assertEquals(0, buffer.getReaderIndex());
        assertEquals(1, slice.getReaderIndex());
    }

    /**
     * Tests the independence of the writer index via {@link
     * ReadOnlySlicedNetworkBuffer#setSize(int)}, {@link ReadOnlySlicedNetworkBuffer#getSize()}.
     */
    @Test
    public void testGetSetSize1() {
        testGetSetSize(buffer.readOnlySlice(), DATA_SIZE);
    }

    /**
     * Tests the independence of the writer index via {@link
     * ReadOnlySlicedNetworkBuffer#setSize(int)}, {@link ReadOnlySlicedNetworkBuffer#getSize()}.
     */
    @Test
    public void testGetSetSize2() {
        testGetSetSize(buffer.readOnlySlice(1, 2), 2);
    }

    private void testGetSetSize(ReadOnlySlicedNetworkBuffer slice, int sliceSize) {
        assertEquals(DATA_SIZE, buffer.getSize());
        assertEquals(sliceSize, slice.getSize());
        buffer.setSize(DATA_SIZE + 1);
        assertEquals(DATA_SIZE + 1, buffer.getSize());
        assertEquals(sliceSize, slice.getSize());
    }

    @Test
    public void testReadableBytes() {
        assertEquals(buffer.readableBytes(), buffer.readOnlySlice().readableBytes());
        assertEquals(2, buffer.readOnlySlice(1, 2).readableBytes());
    }

    @Test
    public void testGetNioBufferReadable1() {
        testGetNioBufferReadable(buffer.readOnlySlice(), DATA_SIZE);
    }

    @Test
    public void testGetNioBufferReadable2() {
        testGetNioBufferReadable(buffer.readOnlySlice(1, 2), 2);
    }

    private void testGetNioBufferReadable(ReadOnlySlicedNetworkBuffer slice, int sliceSize) {
        ByteBuffer sliceByteBuffer = slice.getNioBufferReadable();
        assertTrue(sliceByteBuffer.isReadOnly());
        assertEquals(sliceSize, sliceByteBuffer.remaining());
        assertEquals(sliceSize, sliceByteBuffer.limit());
        assertEquals(sliceSize, sliceByteBuffer.capacity());

        // modify sliceByteBuffer position and verify nothing has changed in the original buffer
        sliceByteBuffer.position(1);
        assertEquals(0, buffer.getReaderIndex());
        assertEquals(0, slice.getReaderIndex());
        assertEquals(DATA_SIZE, buffer.getSize());
        assertEquals(sliceSize, slice.getSize());
    }

    @Test
    public void testGetNioBuffer1() {
        testGetNioBuffer(buffer.readOnlySlice(), DATA_SIZE);
    }

    @Test
    public void testGetNioBuffer2() {
        testGetNioBuffer(buffer.readOnlySlice(1, 2), 2);
    }

    private void testGetNioBuffer(ReadOnlySlicedNetworkBuffer slice, int sliceSize) {
        ByteBuffer sliceByteBuffer = slice.getNioBuffer(1, 1);
        assertTrue(sliceByteBuffer.isReadOnly());
        assertEquals(1, sliceByteBuffer.remaining());
        assertEquals(1, sliceByteBuffer.limit());
        assertEquals(1, sliceByteBuffer.capacity());

        // modify sliceByteBuffer position and verify nothing has changed in the original buffer
        sliceByteBuffer.position(1);
        assertEquals(0, buffer.getReaderIndex());
        assertEquals(0, slice.getReaderIndex());
        assertEquals(DATA_SIZE, buffer.getSize());
        assertEquals(sliceSize, slice.getSize());
    }

    @Test
    public void testGetNioBufferReadableThreadSafe1() {
        NetworkBufferTest.testGetNioBufferReadableThreadSafe(buffer.readOnlySlice());
    }

    @Test
    public void testGetNioBufferReadableThreadSafe2() {
        NetworkBufferTest.testGetNioBufferReadableThreadSafe(buffer.readOnlySlice(1, 2));
    }

    @Test
    public void testGetNioBufferThreadSafe1() {
        NetworkBufferTest.testGetNioBufferThreadSafe(buffer.readOnlySlice(), DATA_SIZE);
    }

    @Test
    public void testGetNioBufferThreadSafe2() {
        NetworkBufferTest.testGetNioBufferThreadSafe(buffer.readOnlySlice(1, 2), 2);
    }

    @Test
    public void testForwardsSetAllocator() {
        testForwardsSetAllocator(buffer.readOnlySlice());
        testForwardsSetAllocator(buffer.readOnlySlice(1, 2));
    }

    private void testForwardsSetAllocator(ReadOnlySlicedNetworkBuffer slice) {
        NettyBufferPool allocator = new NettyBufferPool(1);
        slice.setAllocator(allocator);
        assertSame(buffer.alloc(), slice.alloc());
        assertSame(allocator, slice.alloc());
    }

    private static void assertReadableBytes(Buffer actualBuffer, int... expectedBytes) {
        ByteBuffer actualBytesBuffer = actualBuffer.getNioBufferReadable();
        int[] actual = new int[actualBytesBuffer.limit()];
        for (int i = 0; i < actual.length; ++i) {
            actual[i] = actualBytesBuffer.get();
        }
        assertArrayEquals(expectedBytes, actual);

        // verify absolutely positioned read method:
        ByteBuf buffer = (ByteBuf) actualBuffer;
        for (int i = 0; i < buffer.readableBytes(); ++i) {
            actual[i] = buffer.getByte(buffer.readerIndex() + i);
        }
        assertArrayEquals(expectedBytes, actual);

        // verify relatively positioned read method:
        for (int i = 0; i < buffer.readableBytes(); ++i) {
            actual[i] = buffer.readByte();
        }
        assertArrayEquals(expectedBytes, actual);
    }
}
