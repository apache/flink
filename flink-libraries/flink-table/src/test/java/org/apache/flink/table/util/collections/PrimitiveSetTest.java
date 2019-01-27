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

package org.apache.flink.table.util.collections;

import org.apache.flink.table.dataformat.BinaryString;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test to primitive sets.
 */
public class PrimitiveSetTest {

	@Test
	public void testByte() {
		ByteSet set = new ByteSet();
		assertFalse(set.contains((byte) 0));
		set.add((byte) 0);
		assertTrue(set.contains((byte) 0));

		for (int i = 5; i < 89; i++) {
			set.add((byte) i);
		}
		assertFalse(set.contains((byte) 2));
		assertTrue(set.contains((byte) 45));
		assertTrue(set.contains((byte) 78));
	}

	@Test
	public void testDenseByte() {
		ByteSet set = new ByteSet();
		set.add((byte) 0);
		set.add((byte) 1);
		set.add((byte) 5);
		set.add((byte) 10);
		set.optimize();

		assertTrue(set.contains((byte) 0));
		assertTrue(set.contains((byte) 1));
		assertFalse(set.contains((byte) 2));
		assertTrue(set.contains((byte) 5));
		assertTrue(set.contains((byte) 10));
		assertFalse(set.contains((byte) 200));
	}

	@Test
	public void testShort() {
		ShortSet set = new ShortSet();
		assertFalse(set.contains((short) 0));
		set.add((short) 0);
		assertTrue(set.contains((short) 0));

		for (int i = 5; i < 89; i++) {
			set.add((short) i);
		}
		assertFalse(set.contains((short) 2));
		assertTrue(set.contains((short) 45));
		assertTrue(set.contains((short) 78));
	}

	@Test
	public void testDenseShort() {
		ShortSet set = new ShortSet();
		set.add((short) 0);
		set.add((short) 1);
		set.add((short) 5);
		set.add((short) 10);
		set.optimize();

		assertTrue(set.contains((short) 0));
		assertTrue(set.contains((short) 1));
		assertFalse(set.contains((short) 2));
		assertTrue(set.contains((short) 5));
		assertTrue(set.contains((short) 10));
		assertFalse(set.contains((short) 200));
	}

	@Test
	public void testInt() {
		IntSet set = new IntSet();
		assertFalse(set.contains(0));
		set.add(0);
		assertTrue(set.contains(0));

		for (int i = 5; i < 89; i++) {
			set.add(i);
		}
		assertFalse(set.contains(2));
		assertTrue(set.contains(45));
		assertTrue(set.contains(78));
	}

	@Test
	public void testDenseInt() {
		IntSet set = new IntSet();
		set.add(0);
		set.add(1);
		set.add(5);
		set.add(10);
		set.optimize();

		assertTrue(set.contains(0));
		assertTrue(set.contains(1));
		assertFalse(set.contains(2));
		assertTrue(set.contains(5));
		assertTrue(set.contains(10));
		assertFalse(set.contains(200));
	}

	@Test
	public void testLong() {
		LongSet set = new LongSet();
		assertFalse(set.contains(0));
		set.add(0);
		assertTrue(set.contains(0));

		for (int i = 5; i < 89; i++) {
			set.add(i);
		}
		assertFalse(set.contains(2));
		assertTrue(set.contains(45));
		assertTrue(set.contains(78));
	}

	@Test
	public void testDenseLong() {
		LongSet set = new LongSet();
		set.add(0);
		set.add(1);
		set.add(5);
		set.add(10);
		set.optimize();

		assertTrue(set.contains(0));
		assertTrue(set.contains(1));
		assertFalse(set.contains(2));
		assertTrue(set.contains(5));
		assertTrue(set.contains(10));
		assertFalse(set.contains(200));
	}

	@Test
	public void testFloat() {
		FloatSet set = new FloatSet();
		assertFalse(set.contains(0));
		set.add(0);
		assertTrue(set.contains(0));

		for (int i = 5; i < 89; i++) {
			set.add(i);
		}
		assertFalse(set.contains(2));
		assertTrue(set.contains(45));
		assertTrue(set.contains(78));
	}

	@Test
	public void testDouble() {
		DoubleSet set = new DoubleSet();
		assertFalse(set.contains(0));
		set.add(0);
		assertTrue(set.contains(0));

		for (int i = 5; i < 89; i++) {
			set.add(i);
		}
		assertFalse(set.contains(2));
		assertTrue(set.contains(45));
		assertTrue(set.contains(78));
	}

	@Test
	public void testObject() {
		ObjectHashSet<BinaryString> set = new ObjectHashSet<>();
		for (int i = 5; i < 89; i++) {
			set.add(BinaryString.fromString(String.valueOf(i)));
		}
		assertFalse(set.contains(BinaryString.fromString("2")));
		assertTrue(set.contains(BinaryString.fromString("45")));
		assertTrue(set.contains(BinaryString.fromString("78")));
	}

}
