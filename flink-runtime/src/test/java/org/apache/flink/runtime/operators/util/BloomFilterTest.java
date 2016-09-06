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

package org.apache.flink.runtime.operators.util;

import org.apache.flink.core.memory.HeapMemorySegment;
import org.apache.flink.core.memory.MemorySegment;

import org.apache.flink.core.memory.MemorySegmentFactory;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BloomFilterTest {
	
	private static BloomFilter bloomFilter;
	private static final int INPUT_SIZE = 1024;
	private static final double FALSE_POSITIVE_PROBABILITY = 0.05;
	
	@BeforeClass
	public static void init() {
		int bitsSize = BloomFilter.optimalNumOfBits(INPUT_SIZE, FALSE_POSITIVE_PROBABILITY);
		bitsSize = bitsSize + (Long.SIZE - (bitsSize % Long.SIZE));
		int byteSize = bitsSize >>> 3;
		MemorySegment memorySegment = MemorySegmentFactory.allocateUnpooledSegment(byteSize);
		bloomFilter = new BloomFilter(INPUT_SIZE, byteSize);
		bloomFilter.setBitsLocation(memorySegment, 0);
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void testBloomFilterArguments1() {
		new BloomFilter(-1, 128);
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void testBloomFilterArguments2() {
		new BloomFilter(0, 128);
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void testBloomFilterArguments3() {
		new BloomFilter(1024, -1);
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void testBloomFilterArguments4() {
		new BloomFilter(1024, 0);
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void testBloomFilterArguments5() {
		new BloomFilter(1024, 21);
	}
	
	@Test
	public void testBloomNumBits() {
		assertEquals(0, BloomFilter.optimalNumOfBits(0, 0));
		assertEquals(0, BloomFilter.optimalNumOfBits(0, 1));
		assertEquals(0, BloomFilter.optimalNumOfBits(1, 1));
		assertEquals(7, BloomFilter.optimalNumOfBits(1, 0.03));
		assertEquals(72, BloomFilter.optimalNumOfBits(10, 0.03));
		assertEquals(729, BloomFilter.optimalNumOfBits(100, 0.03));
		assertEquals(7298, BloomFilter.optimalNumOfBits(1000, 0.03));
		assertEquals(72984, BloomFilter.optimalNumOfBits(10000, 0.03));
		assertEquals(729844, BloomFilter.optimalNumOfBits(100000, 0.03));
		assertEquals(7298440, BloomFilter.optimalNumOfBits(1000000, 0.03));
		assertEquals(6235224, BloomFilter.optimalNumOfBits(1000000, 0.05));
	}
	
	@Test
	public void testBloomFilterNumHashFunctions() {
		assertEquals(1, BloomFilter.optimalNumOfHashFunctions(-1, -1));
		assertEquals(1, BloomFilter.optimalNumOfHashFunctions(0, 0));
		assertEquals(1, BloomFilter.optimalNumOfHashFunctions(10, 0));
		assertEquals(1, BloomFilter.optimalNumOfHashFunctions(10, 10));
		assertEquals(7, BloomFilter.optimalNumOfHashFunctions(10, 100));
		assertEquals(1, BloomFilter.optimalNumOfHashFunctions(100, 100));
		assertEquals(1, BloomFilter.optimalNumOfHashFunctions(1000, 100));
		assertEquals(1, BloomFilter.optimalNumOfHashFunctions(10000, 100));
		assertEquals(1, BloomFilter.optimalNumOfHashFunctions(100000, 100));
		assertEquals(1, BloomFilter.optimalNumOfHashFunctions(1000000, 100));
	}
	
	@Test
	public void testBloomFilterFalsePositiveProbability() {
		assertEquals(7298440, BloomFilter.optimalNumOfBits(1000000, 0.03));
		assertEquals(6235224, BloomFilter.optimalNumOfBits(1000000, 0.05));
		assertEquals(4792529, BloomFilter.optimalNumOfBits(1000000, 0.1));
		assertEquals(3349834, BloomFilter.optimalNumOfBits(1000000, 0.2));
		assertEquals(2505911, BloomFilter.optimalNumOfBits(1000000, 0.3));
		assertEquals(1907139, BloomFilter.optimalNumOfBits(1000000, 0.4));
		
		// Make sure the estimated fpp error is less than 1%.
		assertTrue(Math.abs(BloomFilter.estimateFalsePositiveProbability(1000000, 7298440) - 0.03) < 0.01);
		assertTrue(Math.abs(BloomFilter.estimateFalsePositiveProbability(1000000, 6235224) - 0.05) < 0.01);
		assertTrue(Math.abs(BloomFilter.estimateFalsePositiveProbability(1000000, 4792529) - 0.1) < 0.01);
		assertTrue(Math.abs(BloomFilter.estimateFalsePositiveProbability(1000000, 3349834) - 0.2) < 0.01);
		assertTrue(Math.abs(BloomFilter.estimateFalsePositiveProbability(1000000, 2505911) - 0.3) < 0.01);
		assertTrue(Math.abs(BloomFilter.estimateFalsePositiveProbability(1000000, 1907139) - 0.4) < 0.01);
	}
	
	@Test
	public void testHashcodeInput() {
		bloomFilter.reset();
		int val1 = "val1".hashCode();
		int val2 = "val2".hashCode();
		int val3 = "val3".hashCode();
		int val4 = "val4".hashCode();
		int val5 = "val5".hashCode();
		
		assertFalse(bloomFilter.testHash(val1));
		assertFalse(bloomFilter.testHash(val2));
		assertFalse(bloomFilter.testHash(val3));
		assertFalse(bloomFilter.testHash(val4));
		assertFalse(bloomFilter.testHash(val5));
		bloomFilter.addHash(val1);
		assertTrue(bloomFilter.testHash(val1));
		assertFalse(bloomFilter.testHash(val2));
		assertFalse(bloomFilter.testHash(val3));
		assertFalse(bloomFilter.testHash(val4));
		assertFalse(bloomFilter.testHash(val5));
		bloomFilter.addHash(val2);
		assertTrue(bloomFilter.testHash(val1));
		assertTrue(bloomFilter.testHash(val2));
		assertFalse(bloomFilter.testHash(val3));
		assertFalse(bloomFilter.testHash(val4));
		assertFalse(bloomFilter.testHash(val5));
		bloomFilter.addHash(val3);
		assertTrue(bloomFilter.testHash(val1));
		assertTrue(bloomFilter.testHash(val2));
		assertTrue(bloomFilter.testHash(val3));
		assertFalse(bloomFilter.testHash(val4));
		assertFalse(bloomFilter.testHash(val5));
		bloomFilter.addHash(val4);
		assertTrue(bloomFilter.testHash(val1));
		assertTrue(bloomFilter.testHash(val2));
		assertTrue(bloomFilter.testHash(val3));
		assertTrue(bloomFilter.testHash(val4));
		assertFalse(bloomFilter.testHash(val5));
		bloomFilter.addHash(val5);
		assertTrue(bloomFilter.testHash(val1));
		assertTrue(bloomFilter.testHash(val2));
		assertTrue(bloomFilter.testHash(val3));
		assertTrue(bloomFilter.testHash(val4));
		assertTrue(bloomFilter.testHash(val5));
	}
}