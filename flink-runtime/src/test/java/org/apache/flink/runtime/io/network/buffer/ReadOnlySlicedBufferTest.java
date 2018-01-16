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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link ReadOnlySlicedNetworkBuffer}.
 */
public class ReadOnlySlicedBufferTest {
	private static final int BUFFER_SIZE = 1024;
	private static final int DATA_SIZE = 10;

	private NetworkBuffer buffer;

	@Before
	public void setUp() throws Exception {
		final MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(BUFFER_SIZE);
		buffer = new NetworkBuffer(segment, FreeingBufferRecycler.INSTANCE, true, DATA_SIZE);
		buffer.setSize(DATA_SIZE);
	}

	@Test
	public void testForwardsIsBuffer() throws IOException {
		assertEquals(buffer.isBuffer(), buffer.readOnlySlice().isBuffer());
		assertEquals(buffer.isBuffer(), buffer.readOnlySlice(1, 2).isBuffer());
		Buffer eventBuffer = EventSerializer.toBuffer(EndOfPartitionEvent.INSTANCE);
		assertEquals(eventBuffer.isBuffer(), eventBuffer.readOnlySlice().isBuffer());
		assertEquals(eventBuffer.isBuffer(), eventBuffer.readOnlySlice(1, 2).isBuffer());
	}

	@Test(expected = ReadOnlyBufferException.class)
	public void testTagAsEventThrows1() {
		buffer.readOnlySlice().tagAsEvent();
	}

	@Test(expected = ReadOnlyBufferException.class)
	public void testTagAsEventThrows2() {
		buffer.readOnlySlice(1, 2).tagAsEvent();
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
	 * Tests forwarding of both {@link ReadOnlySlicedNetworkBuffer#recycleBuffer()} and
	 * {@link ReadOnlySlicedNetworkBuffer#isRecycled()}.
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
	 * Tests forwarding of both {@link ReadOnlySlicedNetworkBuffer#recycleBuffer()} and
	 * {@link ReadOnlySlicedNetworkBuffer#isRecycled()}.
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
	 * Tests forwarding of both {@link ReadOnlySlicedNetworkBuffer#recycleBuffer()} and
	 * {@link ReadOnlySlicedNetworkBuffer#isRecycled()}.
	 */
	@Test
	public void testForwardsRetainBuffer1() {
		ReadOnlySlicedNetworkBuffer slice = buffer.readOnlySlice();
		assertEquals(buffer.refCnt(), slice.refCnt());
		slice.retainBuffer();
		assertEquals(buffer.refCnt(), slice.refCnt());
	}

	/**
	 * Tests forwarding of both {@link ReadOnlySlicedNetworkBuffer#retainBuffer()} and
	 * {@link ReadOnlySlicedNetworkBuffer#isRecycled()}.
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
		ReadOnlySlicedNetworkBuffer slice1 = buffer.readOnlySlice();
		ReadOnlySlicedNetworkBuffer slice2 = slice1.readOnlySlice();
		ByteBuf unwrap = slice2.unwrap();
		assertSame(buffer, unwrap);
	}

	@Test
	public void testCreateSlice2() {
		ReadOnlySlicedNetworkBuffer slice1 = buffer.readOnlySlice();
		ReadOnlySlicedNetworkBuffer slice2 = slice1.readOnlySlice(1, 2);
		ByteBuf unwrap = slice2.unwrap();
		assertSame(buffer, unwrap);
	}

	@Test
	public void testCreateSlice3() {
		ReadOnlySlicedNetworkBuffer slice1 = buffer.readOnlySlice(1, 2);
		ReadOnlySlicedNetworkBuffer slice2 = slice1.readOnlySlice();
		ByteBuf unwrap = slice2.unwrap();
		assertSame(buffer, unwrap);
	}

	@Test
	public void testCreateSlice4() {
		ReadOnlySlicedNetworkBuffer slice1 = buffer.readOnlySlice(1, 5);
		ReadOnlySlicedNetworkBuffer slice2 = slice1.readOnlySlice(1, 2);
		ByteBuf unwrap = slice2.unwrap();
		assertSame(buffer, unwrap);
	}

	@Test
	public void testGetMaxCapacity() {
		assertEquals(DATA_SIZE, buffer.readOnlySlice().getMaxCapacity());
		assertEquals(2, buffer.readOnlySlice(1, 2).getMaxCapacity());
	}

	/**
	 * Tests the independence of the reader index via
	 * {@link ReadOnlySlicedNetworkBuffer#setReaderIndex(int)} and
	 * {@link ReadOnlySlicedNetworkBuffer#getReaderIndex()}.
	 */
	@Test
	public void testGetSetReaderIndex1() {
		testGetSetReaderIndex(buffer.readOnlySlice());
	}

	/**
	 * Tests the independence of the reader index via
	 * {@link ReadOnlySlicedNetworkBuffer#setReaderIndex(int)} and
	 * {@link ReadOnlySlicedNetworkBuffer#getReaderIndex()}.
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
	 * Tests the independence of the writer index via
	 * {@link ReadOnlySlicedNetworkBuffer#setSize(int)},
	 * {@link ReadOnlySlicedNetworkBuffer#getSize()}, and
	 * {@link ReadOnlySlicedNetworkBuffer#getSizeUnsafe()}.
	 */
	@Test
	public void testGetSetSize1() {
		testGetSetSize(buffer.readOnlySlice(), DATA_SIZE);
	}

	/**
	 * Tests the independence of the writer index via
	 * {@link ReadOnlySlicedNetworkBuffer#setSize(int)},
	 * {@link ReadOnlySlicedNetworkBuffer#getSize()}, and
	 * {@link ReadOnlySlicedNetworkBuffer#getSizeUnsafe()}.
	 */
	@Test
	public void testGetSetSize2() {
		testGetSetSize(buffer.readOnlySlice(1, 2), 2);
	}

	private void testGetSetSize(ReadOnlySlicedNetworkBuffer slice, int sliceSize) {
		assertEquals(DATA_SIZE, buffer.getSize());
		assertEquals(DATA_SIZE, buffer.getSizeUnsafe());
		assertEquals(sliceSize, slice.getSize());
		assertEquals(sliceSize, slice.getSizeUnsafe());
		buffer.setSize(DATA_SIZE + 1);
		assertEquals(DATA_SIZE + 1, buffer.getSize());
		assertEquals(DATA_SIZE + 1, buffer.getSizeUnsafe());
		assertEquals(sliceSize, slice.getSize());
		assertEquals(sliceSize, slice.getSizeUnsafe());
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
		BufferTest.testGetNioBufferReadableThreadSafe(buffer.readOnlySlice());
	}

	@Test
	public void testGetNioBufferReadableThreadSafe2() {
		BufferTest.testGetNioBufferReadableThreadSafe(buffer.readOnlySlice(1, 2));
	}

	@Test
	public void testGetNioBufferThreadSafe1() {
		BufferTest.testGetNioBufferThreadSafe(buffer.readOnlySlice(), DATA_SIZE);
	}

	@Test
	public void testGetNioBufferThreadSafe2() {
		BufferTest.testGetNioBufferThreadSafe(buffer.readOnlySlice(1, 2), 2);
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
}
