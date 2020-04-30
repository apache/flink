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

package org.apache.flink.core.memory;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.function.BiConsumer;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Various tests with freed memory segments for {@link HeapMemorySegment} and {@link
 * HybridMemorySegment} (in both heap and off-heap modes).
 */
public class OperationsOnFreedSegmentTest {

	private static final int PAGE_SIZE = (int) ((Math.random() * 10000) + 1000);

	@Test
	public void testSingleSegmentOperationsHeapSegment() throws Exception {
		for (MemorySegment segment : createTestSegments()) {
			testOpsOnFreedSegment(segment);
		}
	}

	@Test
	public void testCompare() {
		MemorySegment aliveHeap = new HeapMemorySegment(new byte[PAGE_SIZE]);
		MemorySegment aliveHybridHeap = MemorySegmentFactory.wrap(new byte[PAGE_SIZE]);
		MemorySegment aliveHybridOffHeap = MemorySegmentFactory.allocateUnpooledOffHeapMemory(PAGE_SIZE);

		MemorySegment freedHeap = new HeapMemorySegment(new byte[PAGE_SIZE]);
		MemorySegment freedHybridHeap = MemorySegmentFactory.wrap(new byte[PAGE_SIZE]);
		MemorySegment freedHybridOffHeap = MemorySegmentFactory.allocateUnpooledOffHeapMemory(PAGE_SIZE);
		freedHeap.free();
		freedHybridHeap.free();
		freedHybridOffHeap.free();

		MemorySegment[] alive = { aliveHeap, aliveHybridHeap, aliveHybridOffHeap };
		MemorySegment[] free = { freedHeap, freedHybridHeap, freedHybridOffHeap };

		// alive with free
		for (MemorySegment seg1 : alive) {
			for (MemorySegment seg2 : free) {
				testCompare(seg1, seg2);
			}
		}

		// free with alive
		for (MemorySegment seg1 : free) {
			for (MemorySegment seg2 : alive) {
				testCompare(seg1, seg2);
			}
		}

		// free with free
		for (MemorySegment seg1 : free) {
			for (MemorySegment seg2 : free) {
				testCompare(seg1, seg2);
			}
		}
	}

	@Test
	public void testCopyTo() {
		testAliveVsFree(this::testCopy);
	}

	@Test
	public void testSwap() {
		testAliveVsFree(this::testSwap);
	}

	private static void testAliveVsFree(BiConsumer<MemorySegment, MemorySegment> testOperation) {
		MemorySegment[] alive = createTestSegments();
		MemorySegment[] free = createTestSegments();
		for (MemorySegment segment : free) {
			segment.free();
		}

		// alive with free
		for (MemorySegment seg1 : alive) {
			for (MemorySegment seg2 : free) {
				testOperation.accept(seg1, seg2);
			}
		}

		// free with alive
		for (MemorySegment seg1 : free) {
			for (MemorySegment seg2 : alive) {
				testOperation.accept(seg1, seg2);
			}
		}

		// free with free
		for (MemorySegment seg1 : free) {
			for (MemorySegment seg2 : free) {
				testOperation.accept(seg1, seg2);
			}
		}
	}

	private static MemorySegment[] createTestSegments() {
		MemorySegment heap = new HeapMemorySegment(new byte[PAGE_SIZE]);
		MemorySegment hybridHeap = MemorySegmentFactory.wrap(new byte[PAGE_SIZE]);
		MemorySegment hybridOffHeap = MemorySegmentFactory.allocateUnpooledOffHeapMemory(PAGE_SIZE);
		MemorySegment hybridOffHeapUnsafe = MemorySegmentFactory.allocateOffHeapUnsafeMemory(PAGE_SIZE, null);

		MemorySegment[] segments = { heap, hybridHeap, hybridOffHeap, hybridOffHeapUnsafe };

		return segments;
	}

	private void testOpsOnFreedSegment(MemorySegment segment) throws Exception {
		segment.free();
		assertTrue(segment.isFreed());

		// --------- bytes -----------

		try {
			segment.get(0);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | NullPointerException ignored) {}

		try {
			segment.get(-1);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | NullPointerException | IndexOutOfBoundsException ignored) {}

		try {
			segment.get(1);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | NullPointerException ignored) {}

		try {
			segment.get(segment.size());
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | NullPointerException ignored) {}

		try {
			segment.get(-segment.size());
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | NullPointerException | IndexOutOfBoundsException ignored) {}

		try {
			segment.get(Integer.MAX_VALUE);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | NullPointerException ignored) {}

		try {
			segment.get(Integer.MIN_VALUE);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | NullPointerException ignored) {}

		try {
			segment.put(0, (byte) 0);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | NullPointerException ignored) {}

		try {
			segment.put(-1, (byte) 0);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | NullPointerException | IndexOutOfBoundsException ignored) {}

		try {
			segment.put(1, (byte) 0);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | NullPointerException ignored) {}

		try {
			segment.put(segment.size(), (byte) 0);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | NullPointerException ignored) {}

		try {
			segment.put(-segment.size(), (byte) 0);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | NullPointerException | IndexOutOfBoundsException ignored) {}

		try {
			segment.put(Integer.MAX_VALUE, (byte) 0);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | NullPointerException ignored) {}

		try {
			segment.put(Integer.MIN_VALUE, (byte) 0);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | NullPointerException ignored) {}

		// --------- booleans -----------

		try {
			segment.getBoolean(0);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | NullPointerException ignored) {}

		try {
			segment.getBoolean(-1);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | NullPointerException | IndexOutOfBoundsException ignored) {}

		try {
			segment.getBoolean(1);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | NullPointerException ignored) {}

		try {
			segment.getBoolean(segment.size());
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | NullPointerException ignored) {}

		try {
			segment.getBoolean(-segment.size());
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | NullPointerException | IndexOutOfBoundsException ignored) {}

		try {
			segment.getBoolean(Integer.MAX_VALUE);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | NullPointerException ignored) {}

		try {
			segment.getBoolean(Integer.MIN_VALUE);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | NullPointerException ignored) {}

		try {
			segment.putBoolean(0, true);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | NullPointerException ignored) {}

		try {
			segment.putBoolean(-1, true);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | NullPointerException | IndexOutOfBoundsException ignored) {}

		try {
			segment.putBoolean(1, true);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | NullPointerException ignored) {}

		try {
			segment.putBoolean(segment.size(), true);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | NullPointerException ignored) {}

		try {
			segment.putBoolean(-segment.size(), true);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | NullPointerException | IndexOutOfBoundsException ignored) {}

		try {
			segment.putBoolean(Integer.MAX_VALUE, true);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | NullPointerException ignored) {}

		try {
			segment.putBoolean(Integer.MIN_VALUE, true);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | NullPointerException ignored) {}

		// --------- char -----------

		try {
			segment.getChar(0);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException ignored) {}

		try {
			segment.getChar(-1);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | IndexOutOfBoundsException ignored) {}

		try {
			segment.getChar(1);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException ignored) {}

		try {
			segment.getChar(segment.size());
			fail("Should fail with an exception");
		}
		catch (IllegalStateException ignored) {}

		try {
			segment.getChar(-segment.size());
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | IndexOutOfBoundsException ignored) {}

		try {
			segment.getChar(Integer.MAX_VALUE);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException ignored) {}

		try {
			segment.getChar(Integer.MIN_VALUE);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException ignored) {}

		try {
			segment.putChar(0, 'a');
			fail("Should fail with an exception");
		}
		catch (IllegalStateException ignored) {}

		try {
			segment.putChar(-1, 'a');
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | IndexOutOfBoundsException ignored) {}

		try {
			segment.putChar(1, 'a');
			fail("Should fail with an exception");
		}
		catch (IllegalStateException ignored) {}

		try {
			segment.putChar(segment.size(), 'a');
			fail("Should fail with an exception");
		}
		catch (IllegalStateException ignored) {}

		try {
			segment.putChar(-segment.size(), 'a');
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | IndexOutOfBoundsException ignored) {}

		try {
			segment.putChar(Integer.MAX_VALUE, 'a');
			fail("Should fail with an exception");
		}
		catch (IllegalStateException ignored) {}

		try {
			segment.putChar(Integer.MIN_VALUE, 'a');
			fail("Should fail with an exception");
		}
		catch (IllegalStateException ignored) {}

		// --------- short -----------

		try {
			segment.getShort(0);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException ignored) {}

		try {
			segment.getShort(-1);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | IndexOutOfBoundsException ignored) {}

		try {
			segment.getShort(1);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException ignored) {}

		try {
			segment.getShort(segment.size());
			fail("Should fail with an exception");
		}
		catch (IllegalStateException ignored) {}

		try {
			segment.getShort(-segment.size());
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | IndexOutOfBoundsException ignored) {}

		try {
			segment.getShort(Integer.MAX_VALUE);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException ignored) {}

		try {
			segment.getShort(Integer.MIN_VALUE);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException ignored) {}

		try {
			segment.putShort(0, (short) 42);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException ignored) {}

		try {
			segment.putShort(-1, (short) 42);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | IndexOutOfBoundsException ignored) {}

		try {
			segment.putShort(1, (short) 42);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException ignored) {}

		try {
			segment.putShort(segment.size(), (short) 42);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException ignored) {}

		try {
			segment.putShort(-segment.size(), (short) 42);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | IndexOutOfBoundsException ignored) {}

		try {
			segment.putShort(Integer.MAX_VALUE, (short) 42);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException ignored) {}

		try {
			segment.putShort(Integer.MIN_VALUE, (short) 42);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException ignored) {}

		// --------- integer -----------

		try {
			segment.getInt(0);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException ignored) {}

		try {
			segment.getInt(-1);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | IndexOutOfBoundsException ignored) {}

		try {
			segment.getInt(1);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException ignored) {}

		try {
			segment.getInt(segment.size());
			fail("Should fail with an exception");
		}
		catch (IllegalStateException ignored) {}

		try {
			segment.getInt(-segment.size());
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | IndexOutOfBoundsException ignored) {}

		try {
			segment.getInt(Integer.MAX_VALUE);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException ignored) {}

		try {
			segment.getInt(Integer.MIN_VALUE);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException ignored) {}

		try {
			segment.putInt(0, 42);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException ignored) {}

		try {
			segment.putInt(-1, 42);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | IndexOutOfBoundsException ignored) {}

		try {
			segment.putInt(1, 42);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException ignored) {}

		try {
			segment.putInt(segment.size(), 42);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException ignored) {}

		try {
			segment.putInt(-segment.size(), 42);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | IndexOutOfBoundsException ignored) {}

		try {
			segment.putInt(Integer.MAX_VALUE, 42);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException ignored) {}

		try {
			segment.putInt(Integer.MIN_VALUE, 42);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException ignored) {}

		// --------- longs -----------

		try {
			segment.getLong(0);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException ignored) {}

		try {
			segment.getLong(-1);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | IndexOutOfBoundsException ignored) {}

		try {
			segment.getLong(1);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException ignored) {}

		try {
			segment.getLong(segment.size());
			fail("Should fail with an exception");
		}
		catch (IllegalStateException ignored) {}

		try {
			segment.getLong(-segment.size());
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | IndexOutOfBoundsException ignored) {}

		try {
			segment.getLong(Integer.MAX_VALUE);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException ignored) {}

		try {
			segment.getLong(Integer.MIN_VALUE);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException ignored) {}

		try {
			segment.putLong(0, 42);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException ignored) {}

		try {
			segment.putLong(-1, 42);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | IndexOutOfBoundsException ignored) {}

		try {
			segment.putLong(1, 42);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException ignored) {}

		try {
			segment.putLong(segment.size(), 42);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException ignored) {}

		try {
			segment.putLong(-segment.size(), 42);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | IndexOutOfBoundsException ignored) {}

		try {
			segment.putLong(Integer.MAX_VALUE, 42);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException ignored) {}

		try {
			segment.putLong(Integer.MIN_VALUE, 42);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException ignored) {}

		// --------- floats -----------

		try {
			segment.getFloat(0);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException ignored) {}

		try {
			segment.getFloat(-1);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | IndexOutOfBoundsException ignored) {}

		try {
			segment.getFloat(1);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException ignored) {}

		try {
			segment.getFloat(segment.size());
			fail("Should fail with an exception");
		}
		catch (IllegalStateException ignored) {}

		try {
			segment.getFloat(-segment.size());
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | IndexOutOfBoundsException ignored) {}

		try {
			segment.getFloat(Integer.MAX_VALUE);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException ignored) {}

		try {
			segment.getFloat(Integer.MIN_VALUE);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException ignored) {}

		try {
			segment.putFloat(0, 42);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException ignored) {}

		try {
			segment.putFloat(-1, 42);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | IndexOutOfBoundsException ignored) {}

		try {
			segment.putFloat(1, 42);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException ignored) {}

		try {
			segment.putFloat(segment.size(), 42);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException ignored) {}

		try {
			segment.putFloat(-segment.size(), 42);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | IndexOutOfBoundsException ignored) {}

		try {
			segment.putFloat(Integer.MAX_VALUE, 42);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException ignored) {}

		try {
			segment.putFloat(Integer.MIN_VALUE, 42);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException ignored) {}

		// --------- doubles -----------

		try {
			segment.getDouble(0);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException ignored) {}

		try {
			segment.getDouble(-1);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | IndexOutOfBoundsException ignored) {}

		try {
			segment.getDouble(1);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException ignored) {}

		try {
			segment.getDouble(segment.size());
			fail("Should fail with an exception");
		}
		catch (IllegalStateException ignored) {}

		try {
			segment.getDouble(-segment.size());
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | IndexOutOfBoundsException ignored) {}

		try {
			segment.getDouble(Integer.MAX_VALUE);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException ignored) {}

		try {
			segment.getDouble(Integer.MIN_VALUE);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException ignored) {}

		try {
			segment.putDouble(0, 42);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException ignored) {}

		try {
			segment.putDouble(-1, 42);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | IndexOutOfBoundsException ignored) {}

		try {
			segment.putDouble(1, 42);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException ignored) {}

		try {
			segment.putDouble(segment.size(), 42);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException ignored) {}

		try {
			segment.putDouble(-segment.size(), 42);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | IndexOutOfBoundsException ignored) {}

		try {
			segment.putDouble(Integer.MAX_VALUE, 42);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException ignored) {}

		try {
			segment.putDouble(Integer.MIN_VALUE, 42);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException ignored) {}

		// --------- byte[] -----------

		final byte[] array = new byte[55];

		try {
			segment.get(0, array, 3, 17);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | NullPointerException ignored) {}

		try {
			segment.get(-1, array, 3, 17);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | NullPointerException | IndexOutOfBoundsException ignored) {}

		try {
			segment.get(1, array, 3, 17);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | NullPointerException ignored) {}

		try {
			segment.get(segment.size(), array, 3, 17);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | NullPointerException ignored) {}

		try {
			segment.get(-segment.size(), array, 3, 17);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | NullPointerException | IndexOutOfBoundsException ignored) {}

		try {
			segment.get(Integer.MAX_VALUE, array, 3, 17);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | NullPointerException ignored) {}

		try {
			segment.get(Integer.MIN_VALUE, array, 3, 17);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | NullPointerException ignored) {}

		try {
			segment.put(0, array, 3, 17);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | NullPointerException ignored) {}

		try {
			segment.put(-1, array, 3, 17);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | NullPointerException | IndexOutOfBoundsException ignored) {}

		try {
			segment.put(1, array, 3, 17);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | NullPointerException ignored) {}

		try {
			segment.put(segment.size(), array, 3, 17);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | NullPointerException ignored) {}

		try {
			segment.put(-segment.size(), array, 3, 17);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | NullPointerException | IndexOutOfBoundsException ignored) {}

		try {
			segment.put(Integer.MAX_VALUE, array, 3, 17);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | NullPointerException ignored) {}

		try {
			segment.put(Integer.MIN_VALUE, array, 3, 17);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | NullPointerException ignored) {}

		// --------- ByteBuffer -----------

		for (ByteBuffer bbuf : new ByteBuffer[] {
			ByteBuffer.allocate(55),
			ByteBuffer.allocateDirect(55) }) {

			try {
				segment.get(0, bbuf, 17);
				fail("Should fail with an exception");
			}
			catch (IllegalStateException | NullPointerException ignored) {}

			try {
				segment.get(-1, bbuf, 17);
				fail("Should fail with an exception");
			}
			catch (IllegalStateException | NullPointerException | IndexOutOfBoundsException ignored) {}

			try {
				segment.get(1, bbuf, 17);
				fail("Should fail with an exception");
			}
			catch (IllegalStateException | NullPointerException ignored) {}

			try {
				segment.get(segment.size(), bbuf, 17);
				fail("Should fail with an exception");
			}
			catch (IllegalStateException | NullPointerException ignored) {}

			try {
				segment.get(-segment.size(), bbuf, 17);
				fail("Should fail with an exception");
			}
			catch (IllegalStateException | NullPointerException | IndexOutOfBoundsException ignored) {}

			try {
				segment.get(Integer.MAX_VALUE, bbuf, 17);
				fail("Should fail with an exception");
			}
			catch (IllegalStateException | NullPointerException | IndexOutOfBoundsException ignored) {}

			try {
				segment.get(Integer.MIN_VALUE, bbuf, 17);
				fail("Should fail with an exception");
			}
			catch (IllegalStateException | NullPointerException | IndexOutOfBoundsException ignored) {}

			try {
				segment.put(0, bbuf, 17);
				fail("Should fail with an exception");
			}
			catch (IllegalStateException | NullPointerException | IndexOutOfBoundsException ignored) {}

			try {
				segment.put(-1, bbuf, 17);
				fail("Should fail with an exception");
			}
			catch (IllegalStateException | NullPointerException | IndexOutOfBoundsException ignored) {}

			try {
				segment.put(1, bbuf, 17);
				fail("Should fail with an exception");
			}
			catch (IllegalStateException | NullPointerException ignored) {}

			try {
				segment.put(segment.size(), bbuf, 17);
				fail("Should fail with an exception");
			}
			catch (IllegalStateException | NullPointerException ignored) {}

			try {
				segment.put(-segment.size(), bbuf, 17);
				fail("Should fail with an exception");
			}
			catch (IllegalStateException | NullPointerException | IndexOutOfBoundsException ignored) {}

			try {
				segment.put(Integer.MAX_VALUE, bbuf, 17);
				fail("Should fail with an exception");
			}
			catch (IllegalStateException | NullPointerException | IndexOutOfBoundsException ignored) {}

			try {
				segment.put(Integer.MIN_VALUE, bbuf, 17);
				fail("Should fail with an exception");
			}
			catch (IllegalStateException | NullPointerException | IndexOutOfBoundsException ignored) {}
		}

		// --------- Data Input / Output -----------

		final DataInput din = new DataInputStream(new ByteArrayInputStream(new byte[100]));
		final DataOutput dout = new DataOutputStream(new ByteArrayOutputStream());

		try {
			segment.get(dout, 0, 17);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | NullPointerException ignored) {}

		try {
			segment.get(dout, -1, 17);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | NullPointerException | IndexOutOfBoundsException ignored) {}

		try {
			segment.get(dout, 1, 17);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | NullPointerException ignored) {}

		try {
			segment.get(dout, segment.size(), 17);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | NullPointerException ignored) {}

		try {
			segment.get(dout, -segment.size(), 17);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | NullPointerException | IndexOutOfBoundsException ignored) {}

		try {
			segment.get(dout, Integer.MAX_VALUE, 17);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | NullPointerException | IndexOutOfBoundsException ignored) {}

		try {
			segment.get(dout, Integer.MIN_VALUE, 17);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | NullPointerException | IndexOutOfBoundsException ignored) {}

		try {
			segment.put(din, 0, 17);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | NullPointerException | IndexOutOfBoundsException ignored) {}

		try {
			segment.put(din, -1, 17);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | NullPointerException | IndexOutOfBoundsException ignored) {}

		try {
			segment.put(din, 1, 17);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | NullPointerException ignored) {}

		try {
			segment.put(din, segment.size(), 17);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | NullPointerException ignored) {}

		try {
			segment.put(din, -segment.size(), 17);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | NullPointerException | IndexOutOfBoundsException ignored) {}

		try {
			segment.put(din, Integer.MAX_VALUE, 17);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | NullPointerException | IndexOutOfBoundsException ignored) {}

		try {
			segment.put(din, Integer.MIN_VALUE, 17);
			fail("Should fail with an exception");
		}
		catch (IllegalStateException | NullPointerException | IndexOutOfBoundsException ignored) {}
	}

	private void testCompare(MemorySegment seg1, MemorySegment seg2) {
		int[] offsetsToTest = { 0, 1, -1, seg1.size(), -seg1.size(), Integer.MAX_VALUE, Integer.MIN_VALUE };
		int[] lengthsToTest = { 1, seg1.size(), Integer.MAX_VALUE };

		for (int off1 : offsetsToTest) {
			for (int off2 : offsetsToTest) {
				for (int len : lengthsToTest) {
					try {
						seg1.compare(seg2, off1, off2, len);
						fail("Should fail with an exception");
					}
					catch (IllegalStateException | IndexOutOfBoundsException | NullPointerException ignored) {}
				}
			}
		}
	}

	private void testCopy(MemorySegment seg1, MemorySegment seg2) {
		int[] offsetsToTest = { 0, 1, -1, seg1.size(), -seg1.size(), Integer.MAX_VALUE, Integer.MIN_VALUE };
		int[] lengthsToTest = { 0, 1, -1, seg1.size(), -seg1.size(), Integer.MAX_VALUE, Integer.MIN_VALUE };

		for (int off1 : offsetsToTest) {
			for (int off2 : offsetsToTest) {
				for (int len : lengthsToTest) {
					try {
						seg1.copyTo(off1, seg2, off2, len);
						fail("Should fail with an exception");
					}
					catch (IllegalStateException | IndexOutOfBoundsException | NullPointerException ignored) {}
				}
			}
		}
	}

	private void testSwap(MemorySegment seg1, MemorySegment seg2) {
		int[] offsetsToTest = { 0, 1, -1, seg1.size(), -seg1.size(), Integer.MAX_VALUE, Integer.MIN_VALUE };
		int[] lengthsToTest = { 0, 1, -1, seg1.size(), -seg1.size(), Integer.MAX_VALUE, Integer.MIN_VALUE };
		byte[] swapBuffer = new byte[seg1.size()];

		for (int off1 : offsetsToTest) {
			for (int off2 : offsetsToTest) {
				for (int len : lengthsToTest) {
					try {
						seg1.swapBytes(swapBuffer, seg2, off1, off2, len);
						fail("Should fail with an exception");
					}
					catch (IllegalStateException | IndexOutOfBoundsException | NullPointerException ignored) {}
				}
			}
		}
	}
}
