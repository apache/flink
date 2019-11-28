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

package org.apache.flink.runtime.operators.hash;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypePairComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.operators.hash.MutableHashTable.HashBucketIterator;
import org.apache.flink.runtime.operators.testutils.UniformIntPairGenerator;
import org.apache.flink.runtime.operators.testutils.types.IntPair;
import org.apache.flink.runtime.operators.testutils.types.IntPairComparator;
import org.apache.flink.runtime.operators.testutils.types.IntPairPairComparator;
import org.apache.flink.runtime.operators.testutils.types.IntPairSerializer;
import org.apache.flink.util.MutableObjectIterator;

import org.junit.Test;

import static org.junit.Assert.*;

public class HashTablePerformanceComparison {
		
	private static final int PAGE_SIZE = 16 * 1024;
	
	private final int NUM_PAIRS = 20000000;
	
	private final int SIZE = 36;
		
	private final TypeSerializer<IntPair> serializer = new IntPairSerializer();
	
	private final TypeComparator<IntPair> comparator = new IntPairComparator();
	
	private final TypePairComparator<IntPair, IntPair> pairComparator = new IntPairPairComparator();
	
	@Test
	public void testCompactingHashMapPerformance() {
		
		try {
			final int NUM_MEM_PAGES = SIZE * NUM_PAIRS / PAGE_SIZE;
			
			MutableObjectIterator<IntPair> buildInput = new UniformIntPairGenerator(NUM_PAIRS, 1, false);
			
			MutableObjectIterator<IntPair> probeTester = new UniformIntPairGenerator(NUM_PAIRS, 1, false);
			
			MutableObjectIterator<IntPair> updater = new UniformIntPairGenerator(NUM_PAIRS, 1, false);

			MutableObjectIterator<IntPair> updateTester = new UniformIntPairGenerator(NUM_PAIRS, 1, false);
			
			long start;
			long end;
			
			long first = System.currentTimeMillis();
			
			System.out.println("Creating and filling CompactingHashMap...");
			start = System.currentTimeMillis();
			AbstractMutableHashTable<IntPair> table = new CompactingHashTable<IntPair>(serializer, comparator, getMemory(NUM_MEM_PAGES, PAGE_SIZE));
			table.open();
			
			IntPair target = new IntPair();
			while(buildInput.next(target) != null) {
				table.insert(target);
			}
			end = System.currentTimeMillis();
			System.out.println("HashMap ready. Time: " + (end-start) + " ms");
			
			System.out.println("Starting first probing run...");
			start = System.currentTimeMillis();
			
			AbstractHashTableProber<IntPair, IntPair> prober = table.getProber(comparator, pairComparator);
			IntPair temp = new IntPair();
			while(probeTester.next(target) != null) {
				assertNotNull(prober.getMatchFor(target, temp));
				assertEquals(temp.getValue(), target.getValue());
			}
			end = System.currentTimeMillis();
			System.out.println("Probing done. Time: " + (end-start) + " ms");
			
			System.out.println("Starting update...");
			start = System.currentTimeMillis();
			while(updater.next(target) != null) {
				target.setValue(target.getValue() + 1);
				table.insertOrReplaceRecord(target);
			}
			end = System.currentTimeMillis();
			System.out.println("Update done. Time: " + (end-start) + " ms");
			
			System.out.println("Starting second probing run...");
			start = System.currentTimeMillis();
			while (updateTester.next(target) != null) {
				assertNotNull(prober.getMatchFor(target, temp));
				assertEquals(target.getValue() + 1, temp.getValue());
			}
			end = System.currentTimeMillis();
			System.out.println("Probing done. Time: " + (end-start) + " ms");
			
			table.close();
			
			end = System.currentTimeMillis();
			System.out.println("Overall time: " + (end-first) + " ms");
			
			assertEquals("Memory lost", NUM_MEM_PAGES, table.getFreeMemory().size());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail("Error: " + e.getMessage());
		}
	}
	
	@Test
	public void testMutableHashMapPerformance() {
		try (IOManager ioManager = new IOManagerAsync()) {
			final int NUM_MEM_PAGES = SIZE * NUM_PAIRS / PAGE_SIZE;
			
			MutableObjectIterator<IntPair> buildInput = new UniformIntPairGenerator(NUM_PAIRS, 1, false);

			MutableObjectIterator<IntPair> probeInput = new UniformIntPairGenerator(0, 1, false);
			
			MutableObjectIterator<IntPair> probeTester = new UniformIntPairGenerator(NUM_PAIRS, 1, false);
			
			MutableObjectIterator<IntPair> updater = new UniformIntPairGenerator(NUM_PAIRS, 1, false);

			MutableObjectIterator<IntPair> updateTester = new UniformIntPairGenerator(NUM_PAIRS, 1, false);
			
			long start;
			long end;
			
			long first = System.currentTimeMillis();
			
			System.out.println("Creating and filling MutableHashMap...");
			start = System.currentTimeMillis();
			MutableHashTable<IntPair, IntPair> table = new MutableHashTable<IntPair, IntPair>(serializer, serializer, comparator, comparator, pairComparator, getMemory(NUM_MEM_PAGES, PAGE_SIZE), ioManager);				
			table.open(buildInput, probeInput);
			end = System.currentTimeMillis();
			System.out.println("HashMap ready. Time: " + (end-start) + " ms");
			
			System.out.println("Starting first probing run...");
			start = System.currentTimeMillis();
			IntPair compare = new IntPair();
			HashBucketIterator<IntPair, IntPair> iter;
			IntPair target = new IntPair(); 
			while(probeTester.next(compare) != null) {
				iter = table.getMatchesFor(compare);
				iter.next(target);
				assertEquals(target.getKey(), compare.getKey());
				assertEquals(target.getValue(), compare.getValue());
				assertTrue(iter.next(target) == null);
			}
			end = System.currentTimeMillis();
			System.out.println("Probing done. Time: " + (end-start) + " ms");
	
			System.out.println("Starting update...");
			start = System.currentTimeMillis();
			while(updater.next(compare) != null) {
				compare.setValue(compare.getValue() + 1);
				iter = table.getMatchesFor(compare);
				iter.next(target);
				iter.writeBack(compare);
				//assertFalse(iter.next(target));
			}
			end = System.currentTimeMillis();
			System.out.println("Update done. Time: " + (end-start) + " ms");
			
			System.out.println("Starting second probing run...");
			start = System.currentTimeMillis();
			while(updateTester.next(compare) != null) {
				compare.setValue(compare.getValue() + 1);
				iter = table.getMatchesFor(compare);
				iter.next(target);
				assertEquals(target.getKey(), compare.getKey());
				assertEquals(target.getValue(), compare.getValue());
				assertTrue(iter.next(target) == null);
			}
			end = System.currentTimeMillis();
			System.out.println("Probing done. Time: " + (end-start) + " ms");
			
			table.close();
			
			end = System.currentTimeMillis();
			System.out.println("Overall time: " + (end-first) + " ms");
			
			assertEquals("Memory lost", NUM_MEM_PAGES, table.getFreedMemory().size());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail("Error: " + e.getMessage());
		}
	}

	@Test
	public void testInPlaceMutableHashTablePerformance() {
		try {
			final int NUM_MEM_PAGES = SIZE * NUM_PAIRS / PAGE_SIZE;

			MutableObjectIterator<IntPair> buildInput = new UniformIntPairGenerator(NUM_PAIRS, 1, false);

			MutableObjectIterator<IntPair> probeTester = new UniformIntPairGenerator(NUM_PAIRS, 1, false);

			MutableObjectIterator<IntPair> updater = new UniformIntPairGenerator(NUM_PAIRS, 1, false);

			MutableObjectIterator<IntPair> updateTester = new UniformIntPairGenerator(NUM_PAIRS, 1, false);

			long start;
			long end;

			long first = System.currentTimeMillis();

			System.out.println("Creating and filling InPlaceMutableHashTable...");
			start = System.currentTimeMillis();
			InPlaceMutableHashTable<IntPair> table = new InPlaceMutableHashTable<>(serializer, comparator, getMemory(NUM_MEM_PAGES, PAGE_SIZE));
			table.open();

			IntPair target = new IntPair();
			while(buildInput.next(target) != null) {
				table.insert(target);
			}
			end = System.currentTimeMillis();
			System.out.println("HashMap ready. Time: " + (end-start) + " ms");

			System.out.println("Starting first probing run...");
			start = System.currentTimeMillis();

			AbstractHashTableProber<IntPair, IntPair> prober = table.getProber(comparator, pairComparator);
			IntPair temp = new IntPair();
			while(probeTester.next(target) != null) {
				assertNotNull(prober.getMatchFor(target, temp));
				assertEquals(temp.getValue(), target.getValue());
			}
			end = System.currentTimeMillis();
			System.out.println("Probing done. Time: " + (end-start) + " ms");

			System.out.println("Starting update...");
			start = System.currentTimeMillis();
			while(updater.next(target) != null) {
				target.setValue(target.getValue() + 1);
				table.insertOrReplaceRecord(target);
			}
			end = System.currentTimeMillis();
			System.out.println("Update done. Time: " + (end-start) + " ms");

			System.out.println("Starting second probing run...");
			start = System.currentTimeMillis();
			while (updateTester.next(target) != null) {
				assertNotNull(prober.getMatchFor(target, temp));
				assertEquals(target.getValue() + 1, temp.getValue());
			}
			end = System.currentTimeMillis();
			System.out.println("Probing done. Time: " + (end-start) + " ms");

			table.close();

			end = System.currentTimeMillis();
			System.out.println("Overall time: " + (end-first) + " ms");

			assertEquals("Memory lost", NUM_MEM_PAGES, table.getFreeMemory().size());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail("Error: " + e.getMessage());
		}
	}

	
	private static List<MemorySegment> getMemory(int numPages, int pageSize) {
		List<MemorySegment> memory = new ArrayList<MemorySegment>();
		
		for (int i = 0; i < numPages; i++) {
			memory.add(MemorySegmentFactory.allocateUnpooledSegment(pageSize));
		}
		
		return memory;
	}

}
