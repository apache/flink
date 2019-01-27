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

package org.apache.flink.table.dataformat;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.dataformat.util.MultiSegUtil;

import org.junit.Before;
import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test for MultiSegUtil.
 */
public class MultiSegUtilTest {

	private MemorySegment[] segments;
	private MemorySegment[] oneSegment;
	private Random rnd;

	@Before
	public void before() {
		segments = new MemorySegment[2];
		oneSegment = new MemorySegment[1];
		segments[0] = MemorySegmentFactory.wrap(new byte[16]);
		segments[1] = MemorySegmentFactory.wrap(new byte[16]);
		oneSegment[0] = MemorySegmentFactory.wrap(new byte[32]);
		rnd = new Random();
	}

	private void copyToOneSeg() {
		segments[0].copyTo(0, oneSegment[0], 0, 16);
		segments[1].copyTo(0, oneSegment[0], 16, 16);
	}

	private void clear() {
		segments[0].putLong(0, 0);
		segments[0].putLong(8, 0);
		segments[1].putLong(0, 0);
		segments[1].putLong(8, 0);

		oneSegment[0].putLong(0, 0);
		oneSegment[0].putLong(8, 0);
		oneSegment[0].putLong(16, 0);
		oneSegment[0].putLong(24, 0);
	}

	@Test
	public void testBitSet() {
		for (int i = 0; i < 192; i++) {
			clear();
			MultiSegUtil.bitSet(segments, 8, i);
			copyToOneSeg();
			assertTrue(MultiSegUtil.bitGet(segments, 8, i));
			assertTrue(MultiSegUtil.bitGet(oneSegment, 8, i));
			assertFalse(MultiSegUtil.bitGet(oneSegment, 7, i));

			MultiSegUtil.bitUnSet(segments, 8, i);
			copyToOneSeg();
			assertFalse(MultiSegUtil.bitGet(segments, 8, i));
			assertFalse(MultiSegUtil.bitGet(oneSegment, 8, i));

			MultiSegUtil.bitSet(oneSegment, 8, i);
			assertTrue(MultiSegUtil.bitGet(oneSegment, 8, i));
			MultiSegUtil.bitUnSet(oneSegment, 8, i);
			assertFalse(MultiSegUtil.bitGet(oneSegment, 8, i));
		}
	}

	@Test
	public void testBoolean() {
		boolean value = rnd.nextBoolean();
		for (int i = 0; i < 32; i++) {
			MultiSegUtil.setBoolean(segments, i, value);
			copyToOneSeg();
			assertEquals(value, MultiSegUtil.getBoolean(segments, i));
			assertEquals(value, MultiSegUtil.getBoolean(oneSegment, i));

			MultiSegUtil.setBoolean(oneSegment, i, value);
			assertEquals(value, MultiSegUtil.getBoolean(oneSegment, i));
		}
	}

	@Test
	public void testByte() {
		byte value = (byte) rnd.nextInt();
		for (int i = 0; i < 32; i++) {
			MultiSegUtil.setByte(segments, i, value);
			copyToOneSeg();
			assertEquals(value, MultiSegUtil.getByte(segments, i));
			assertEquals(value, MultiSegUtil.getByte(oneSegment, i));

			MultiSegUtil.setByte(oneSegment, i, value);
			assertEquals(value, MultiSegUtil.getByte(oneSegment, i));
		}
	}

	@Test
	public void testInt() {
		int value = rnd.nextInt();
		for (int i = 0; i < 29; i++) {
			MultiSegUtil.setInt(segments, i, value);
			copyToOneSeg();
			assertEquals(value, MultiSegUtil.getInt(segments, i));
			assertEquals(value, MultiSegUtil.getInt(oneSegment, i));

			MultiSegUtil.setInt(oneSegment, i, value);
			assertEquals(value, MultiSegUtil.getInt(oneSegment, i));
		}
	}

	@Test
	public void testLong() {
		long value = rnd.nextLong();
		for (int i = 0; i < 25; i++) {
			MultiSegUtil.setLong(segments, i, value);
			copyToOneSeg();
			assertEquals(value, MultiSegUtil.getLong(segments, i));
			assertEquals(value, MultiSegUtil.getLong(oneSegment, i));

			MultiSegUtil.setLong(oneSegment, i, value);
			assertEquals(value, MultiSegUtil.getLong(oneSegment, i));
		}
	}

	@Test
	public void testShort() {
		short value = (short) rnd.nextInt();
		for (int i = 0; i < 31; i++) {
			MultiSegUtil.setShort(segments, i, value);
			copyToOneSeg();
			assertEquals(value, MultiSegUtil.getShort(segments, i));
			assertEquals(value, MultiSegUtil.getShort(oneSegment, i));

			MultiSegUtil.setShort(oneSegment, i, value);
			assertEquals(value, MultiSegUtil.getShort(oneSegment, i));
		}
	}

	@Test
	public void testFloat() {
		float value = rnd.nextFloat();
		for (int i = 0; i < 29; i++) {
			MultiSegUtil.setFloat(segments, i, value);
			copyToOneSeg();
			assertTrue(value == MultiSegUtil.getFloat(segments, i));
			assertTrue(value == MultiSegUtil.getFloat(oneSegment, i));

			MultiSegUtil.setFloat(oneSegment, i, value);
			assertTrue(value == MultiSegUtil.getFloat(oneSegment, i));
		}
	}

	@Test
	public void testDouble() {
		double value = rnd.nextDouble();
		for (int i = 0; i < 25; i++) {
			MultiSegUtil.setDouble(segments, i, value);
			copyToOneSeg();
			assertTrue(value == MultiSegUtil.getDouble(segments, i));
			assertTrue(value == MultiSegUtil.getDouble(oneSegment, i));

			MultiSegUtil.setDouble(oneSegment, i, value);
			assertTrue(value == MultiSegUtil.getDouble(oneSegment, i));
		}
	}

	@Test
	public void testChar() {
		char value = (char) rnd.nextInt();
		for (int i = 0; i < 31; i++) {
			MultiSegUtil.setChar(segments, i, value);
			copyToOneSeg();
			assertEquals(value, MultiSegUtil.getChar(segments, i));
			assertEquals(value, MultiSegUtil.getChar(oneSegment, i));

			MultiSegUtil.setChar(oneSegment, i, value);
			assertEquals(value, MultiSegUtil.getChar(oneSegment, i));
		}
	}
}
