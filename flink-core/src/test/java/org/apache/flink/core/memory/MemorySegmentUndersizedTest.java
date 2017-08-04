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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for undersized {@link HeapMemorySegment} and {@link HybridMemorySegment} (in both heap and
 * off-heap modes).
 */
public class MemorySegmentUndersizedTest {

	@Test
	public void testZeroSizeHeapSegment() {
		MemorySegment segment = new HeapMemorySegment(new byte[0]);

		testZeroSizeBuffer(segment);
		testSegmentWithSizeLargerZero(segment);
	}

	@Test
	public void testZeroSizeHeapHybridSegment() {
		MemorySegment segment = new HybridMemorySegment(new byte[0]);

		testZeroSizeBuffer(segment);
		testSegmentWithSizeLargerZero(segment);
	}

	@Test
	public void testZeroSizeOffHeapHybridSegment() {
		MemorySegment segment = new HybridMemorySegment(ByteBuffer.allocateDirect(0));

		testZeroSizeBuffer(segment);
		testSegmentWithSizeLargerZero(segment);
	}

	@Test
	public void testSizeOneHeapSegment() {
		testSegmentWithSizeLargerZero(new HeapMemorySegment(new byte[1]));
	}

	@Test
	public void testSizeOneHeapHybridSegment() {
		testSegmentWithSizeLargerZero(new HybridMemorySegment(new byte[1]));
	}

	@Test
	public void testSizeOneOffHeapHybridSegment() {
		testSegmentWithSizeLargerZero(new HybridMemorySegment(ByteBuffer.allocateDirect(1)));
	}

	private static void testZeroSizeBuffer(MemorySegment segment) {
		// ------ bytes ------

		try {
			segment.put(0, (byte) 0);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.get(0);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		// ------ booleans ------

		try {
			segment.putBoolean(0, true);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.getBoolean(0);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}
	}

	private static void testSegmentWithSizeLargerZero(MemorySegment segment) {

		// ------ bytes ------

		try {
			segment.put(1, (byte) 0);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.put(-1, (byte) 0);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.put(8, (byte) 0);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.put(-8, (byte) 0);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.put(Integer.MAX_VALUE, (byte) 0);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.put(Integer.MIN_VALUE, (byte) 0);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.get(1);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.get(-1);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.get(8);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.get(-8);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.get(Integer.MAX_VALUE);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.get(Integer.MIN_VALUE);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		// ------ booleans ------

		try {
			segment.putBoolean(1, true);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.putBoolean(-1, true);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.putBoolean(8, true);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.putBoolean(-8, true);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.putBoolean(Integer.MAX_VALUE, true);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.putBoolean(Integer.MIN_VALUE, true);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.getBoolean(1);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.getBoolean(-1);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.getBoolean(8);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.getBoolean(-8);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.getBoolean(Integer.MAX_VALUE);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.getBoolean(Integer.MIN_VALUE);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		// ------ chars ------

		try {
			segment.putChar(0, 'a');
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.putChar(1, 'a');
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.putChar(-1, 'a');
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.putChar(8, 'a');
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.putChar(-8, 'a');
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.putChar(Integer.MAX_VALUE, 'a');
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.putChar(Integer.MIN_VALUE, 'a');
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.getChar(0);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.getChar(1);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.getChar(-1);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.getChar(8);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.getChar(-8);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.getChar(Integer.MAX_VALUE);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.getChar(Integer.MIN_VALUE);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		// ------ shorts ------

		try {
			segment.putShort(0, (short) 0);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.putShort(1, (short) 0);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.putShort(-1, (short) 0);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.putShort(8, (short) 0);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.putShort(-8, (short) 0);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.putShort(Integer.MAX_VALUE, (short) 0);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.putShort(Integer.MIN_VALUE, (short) 0);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.getShort(0);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.getShort(1);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.getShort(-1);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.getShort(8);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.getShort(-8);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.getShort(Integer.MAX_VALUE);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.getShort(Integer.MIN_VALUE);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		// ------ ints ------

		try {
			segment.putInt(0, 0);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.putInt(1, 0);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.putInt(-1, 0);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.putInt(8, 0);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.putInt(-8, 0);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.putInt(Integer.MAX_VALUE, 0);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.putInt(Integer.MIN_VALUE, 0);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.getInt(0);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}
		try {
			segment.getInt(1);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.getInt(-1);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.getInt(8);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.getInt(-8);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.getInt(Integer.MAX_VALUE);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.getInt(Integer.MIN_VALUE);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		// ------ longs ------

		try {
			segment.putLong(0, 0L);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.putLong(1, 0L);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.putLong(-1, 0L);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.putLong(8, 0L);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.putLong(-8, 0L);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.putLong(Integer.MAX_VALUE, 0L);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.putLong(Integer.MIN_VALUE, 0L);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.getLong(0);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}
		try {
			segment.getLong(1);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.getLong(-1);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.getLong(8);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.getLong(-8);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.getLong(Integer.MAX_VALUE);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.getLong(Integer.MIN_VALUE);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		// ------ floats ------

		try {
			segment.putFloat(0, 0.0f);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.putFloat(1, 0.0f);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.putFloat(-1, 0.0f);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.putFloat(8, 0.0f);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.putFloat(-8, 0.0f);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.putFloat(Integer.MAX_VALUE, 0.0f);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.putFloat(Integer.MIN_VALUE, 0.0f);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.getFloat(0);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}
		try {
			segment.getFloat(1);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.getFloat(-1);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.getFloat(8);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.getFloat(-8);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.getFloat(Integer.MAX_VALUE);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.getFloat(Integer.MIN_VALUE);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		// ------ doubles ------

		try {
			segment.putDouble(0, 0.0);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.putDouble(1, 0.0);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.putDouble(-1, 0.0);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.putDouble(8, 0.0);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.putDouble(Integer.MAX_VALUE, 0.0);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.putDouble(Integer.MIN_VALUE, 0.0);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.getDouble(0);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}
		try {
			segment.getDouble(1);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.getDouble(-1);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.getDouble(8);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.getDouble(-8);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.getDouble(Integer.MAX_VALUE);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.getDouble(Integer.MIN_VALUE);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		// ------ byte[] ------

		try {
			segment.put(0, new byte[7]);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.put(1, new byte[7]);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.put(-1, new byte[7]);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.put(8, new byte[7]);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.put(-8, new byte[7]);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.put(Integer.MAX_VALUE, new byte[7]);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.put(Integer.MIN_VALUE, new byte[7]);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.get(0, new byte[7]);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.get(1, new byte[7]);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.get(-1, new byte[7]);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.get(8, new byte[7]);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.get(-8, new byte[7]);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.get(Integer.MAX_VALUE, new byte[7]);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.get(Integer.MIN_VALUE, new byte[7]);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		// ------ ByteBuffer ------

		final ByteBuffer buf = ByteBuffer.allocate(7);
		final int numBytes = 3;

		try {
			segment.put(0, buf, numBytes);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.put(1, buf, numBytes);
					fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.put(-1, buf, numBytes);
					fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.put(8, buf, numBytes);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.put(-8, buf, numBytes);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.put(Integer.MAX_VALUE, buf, numBytes);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.put(Integer.MIN_VALUE, buf, numBytes);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.get(0, buf, numBytes);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}
		try {
			segment.get(1, buf, numBytes);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.get(-1, buf, numBytes);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.get(8, buf, numBytes);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.get(-8, buf, numBytes);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.get(Integer.MAX_VALUE, buf, numBytes);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.get(Integer.MIN_VALUE, buf, numBytes);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		// ------ DataInput / DataOutput ------

		final DataInput dataInput = new DataInputStream(new ByteArrayInputStream(new byte[20]));
		final DataOutput dataOutput = new DataOutputStream(new ByteArrayOutputStream());

		try {
			segment.put(dataInput, 0, numBytes);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.put(dataInput, 1, numBytes);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.put(dataInput, -1, numBytes);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.put(dataInput, 8, numBytes);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.put(dataInput, -8, numBytes);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.put(dataInput, Integer.MAX_VALUE, numBytes);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.put(dataInput, Integer.MIN_VALUE, numBytes);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.get(dataOutput, 0, numBytes);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}
		try {
			segment.get(dataOutput, 1, numBytes);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.get(dataOutput, -1, numBytes);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.get(dataOutput, 8, numBytes);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.get(dataOutput, -8, numBytes);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.get(dataOutput, Integer.MAX_VALUE, numBytes);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}

		try {
			segment.get(dataOutput, Integer.MIN_VALUE, numBytes);
			fail("IndexOutOfBoundsException expected");
		}
		catch (Exception e) {
			assertTrue(e instanceof IndexOutOfBoundsException);
		}
	}
}
