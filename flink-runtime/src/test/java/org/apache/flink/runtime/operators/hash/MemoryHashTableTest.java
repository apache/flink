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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypePairComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.operators.testutils.UniformStringPairGenerator;
import org.apache.flink.runtime.operators.testutils.types.IntList;
import org.apache.flink.runtime.operators.testutils.types.IntListComparator;
import org.apache.flink.runtime.operators.testutils.types.IntListPairComparator;
import org.apache.flink.runtime.operators.testutils.types.IntListSerializer;
import org.apache.flink.runtime.operators.testutils.types.IntPair;
import org.apache.flink.runtime.operators.testutils.types.IntPairComparator;
import org.apache.flink.runtime.operators.testutils.types.IntPairListPairComparator;
import org.apache.flink.runtime.operators.testutils.types.IntPairPairComparator;
import org.apache.flink.runtime.operators.testutils.types.IntPairSerializer;
import org.apache.flink.runtime.operators.testutils.types.StringPair;
import org.apache.flink.runtime.operators.testutils.types.StringPairComparator;
import org.apache.flink.runtime.operators.testutils.types.StringPairPairComparator;
import org.apache.flink.runtime.operators.testutils.types.StringPairSerializer;
import org.apache.flink.util.MutableObjectIterator;

import org.junit.Test;

import org.powermock.reflect.Whitebox;

import static org.junit.Assert.*;


public class MemoryHashTableTest {
	
	private static final long RANDOM_SEED = 76518743207143L;
	
	private static final int KEY_VALUE_DIFF = 1021;
	
	private static final int PAGE_SIZE = 16 * 1024;
	
	
	private final Random rnd = new Random(RANDOM_SEED);
		
	private final TypeSerializer<IntPair> serializer = new IntPairSerializer();
	
	private final TypeComparator<IntPair> comparator = new IntPairComparator();
	
	private final TypePairComparator<IntPair, IntPair> pairComparator = new IntPairPairComparator();
	
	
	private static final int MAX_LIST_SIZE = 8;
	
	private final TypeSerializer<IntList> serializerV = new IntListSerializer();
	
	private final TypeComparator<IntList> comparatorV = new IntListComparator();
	
	private final TypePairComparator<IntList, IntList> pairComparatorV = new IntListPairComparator();
	
	private final TypePairComparator<IntPair, IntList> pairComparatorPL =new IntPairListPairComparator();
	
	private final int SIZE = 75;
	
	private final int NUM_PAIRS = 100000;

	private final int NUM_LISTS = 100000;
	
	private final int ADDITIONAL_MEM = 100;
	
	private final int NUM_REWRITES = 10;
	

	private final TypeSerializer<StringPair> serializerS = new StringPairSerializer();
	
	private final TypeComparator<StringPair> comparatorS = new StringPairComparator();
	
	private final TypePairComparator<StringPair, StringPair> pairComparatorS = new StringPairPairComparator();
	
	
	@Test
	public void testDifferentProbers() {
		final int NUM_MEM_PAGES = 32 * NUM_PAIRS / PAGE_SIZE;
		
		AbstractMutableHashTable<IntPair> table = new CompactingHashTable<IntPair>(serializer, comparator, getMemory(NUM_MEM_PAGES, PAGE_SIZE));
		
		AbstractHashTableProber<IntPair, IntPair> prober1 = table.getProber(comparator, pairComparator);
		AbstractHashTableProber<IntPair, IntPair> prober2 = table.getProber(comparator, pairComparator);
		
		assertFalse(prober1 == prober2);
		
		table.close();
		assertEquals("Memory lost", NUM_MEM_PAGES, table.getFreeMemory().size());
	}
	
	@Test
	public void testBuildAndRetrieve() {
		try {
			final int NUM_MEM_PAGES = 32 * NUM_PAIRS / PAGE_SIZE;
			
			final IntPair[] pairs = getRandomizedIntPairs(NUM_PAIRS, rnd);
			
			AbstractMutableHashTable<IntPair> table = new CompactingHashTable<IntPair>(serializer, comparator, getMemory(NUM_MEM_PAGES, PAGE_SIZE));
			table.open();
			
			for (int i = 0; i < NUM_PAIRS; i++) {
				table.insert(pairs[i]);
			}
	
			AbstractHashTableProber<IntPair, IntPair> prober = table.getProber(comparator, pairComparator);
			IntPair target = new IntPair();
			
			for (int i = 0; i < NUM_PAIRS; i++) {
				assertNotNull(prober.getMatchFor(pairs[i], target));
				assertEquals(pairs[i].getValue(), target.getValue());
			}
			
			
			table.close();
			assertEquals("Memory lost", NUM_MEM_PAGES, table.getFreeMemory().size());
		} catch (Exception e) {
			e.printStackTrace();
			fail("Error: " + e.getMessage());
		}
	}
	
	@Test
	public void testEntryIterator() {
		try {
			final int NUM_MEM_PAGES = SIZE * NUM_LISTS / PAGE_SIZE;
			final IntList[] lists = getRandomizedIntLists(NUM_LISTS, rnd);
			
			AbstractMutableHashTable<IntList> table = new CompactingHashTable<IntList>(serializerV, comparatorV, getMemory(NUM_MEM_PAGES, PAGE_SIZE));
			table.open();
			int result = 0;
			for (int i = 0; i < NUM_LISTS; i++) {
				table.insert(lists[i]);
				result += lists[i].getKey();
			}
	
			MutableObjectIterator<IntList> iter = table.getEntryIterator();
			IntList target = new IntList();
			
			int sum = 0;
			while((target = iter.next(target)) != null) {
				sum += target.getKey();
			}
			table.close();
			
			assertTrue(sum == result);
			assertEquals("Memory lost", NUM_MEM_PAGES, table.getFreeMemory().size());
		} catch (Exception e) {
			e.printStackTrace();
			fail("Error: " + e.getMessage());
		}
	}
	
	@Test
	public void testMultipleProbers() {
		try {
			final int NUM_MEM_PAGES = SIZE * NUM_LISTS / PAGE_SIZE;
			final IntList[] lists = getRandomizedIntLists(NUM_LISTS, rnd);
			final IntPair[] pairs = getRandomizedIntPairs(NUM_LISTS, rnd);
			
			AbstractMutableHashTable<IntList> table = new CompactingHashTable<IntList>(serializerV, comparatorV, getMemory(NUM_MEM_PAGES, PAGE_SIZE));
			table.open();
			for (int i = 0; i < NUM_LISTS; i++) {
				table.insert(lists[i]);
			}
			
			AbstractHashTableProber<IntList, IntList> listProber = table.getProber(comparatorV, pairComparatorV);
			
			AbstractHashTableProber<IntPair, IntList> pairProber = table.getProber(comparator, pairComparatorPL);
			
			IntList target = new IntList();
			for (int i = 0; i < NUM_LISTS; i++) {
				assertNotNull(pairProber.getMatchFor(pairs[i], target));
				assertNotNull(listProber.getMatchFor(lists[i], target));
				assertArrayEquals(lists[i].getValue(), target.getValue());
			}
			table.close();
			assertEquals("Memory lost", NUM_MEM_PAGES, table.getFreeMemory().size());
		} catch (Exception e) {
			e.printStackTrace();
			fail("Error: " + e.getMessage());
		}
	}
	
	@Test
	public void testVariableLengthBuildAndRetrieve() {
		try {
			final int NUM_MEM_PAGES = SIZE * NUM_LISTS / PAGE_SIZE;
			
			final IntList[] lists = getRandomizedIntLists(NUM_LISTS, rnd);
			
			AbstractMutableHashTable<IntList> table = new CompactingHashTable<IntList>(serializerV, comparatorV, getMemory(NUM_MEM_PAGES, PAGE_SIZE));
			table.open();
			
			for (int i = 0; i < NUM_LISTS; i++) {
				try {
					table.insert(lists[i]);
				} catch (Exception e) {
					throw e;
				}
			}


			AbstractHashTableProber<IntList, IntList> prober = table.getProber(comparatorV, pairComparatorV);
			IntList target = new IntList();
			
			for (int i = 0; i < NUM_LISTS; i++) {
				assertNotNull(prober.getMatchFor(lists[i], target));
				assertArrayEquals(lists[i].getValue(), target.getValue());
			}
			
			final IntList[] overwriteLists = getRandomizedIntLists(NUM_LISTS, rnd);
			
			// test replacing
			for (int i = 0; i < NUM_LISTS; i++) {
				table.insertOrReplaceRecord(overwriteLists[i]);
			}
			
			for (int i = 0; i < NUM_LISTS; i++) {
				assertNotNull("" + i, prober.getMatchFor(overwriteLists[i], target));
				assertArrayEquals(overwriteLists[i].getValue(), target.getValue());
			}
			
			table.close();
			assertEquals("Memory lost", NUM_MEM_PAGES, table.getFreeMemory().size());
		} catch (Exception e) {
			e.printStackTrace();
			fail("Error: " + e.getMessage());
		}
	}
	
	@Test
	public void testVariableLengthBuildAndRetrieveMajorityUpdated() {
		try {
			final int NUM_MEM_PAGES = SIZE * NUM_LISTS / PAGE_SIZE;
						
			final IntList[] lists = getRandomizedIntLists(NUM_LISTS, rnd);
			
			AbstractMutableHashTable<IntList> table = new CompactingHashTable<IntList>(serializerV, comparatorV, getMemory(NUM_MEM_PAGES, PAGE_SIZE));
			table.open();
			
			for (int i = 0; i < NUM_LISTS; i++) {
				table.insert(lists[i]);
			}

			AbstractHashTableProber<IntList, IntList> prober = table.getProber(comparatorV, pairComparatorV);
			IntList target = new IntList();
			
			for (int i = 0; i < NUM_LISTS; i++) {
				assertNotNull(prober.getMatchFor(lists[i], target));
				assertArrayEquals(lists[i].getValue(), target.getValue());
			}
			
			final IntList[] overwriteLists = getRandomizedIntLists(NUM_LISTS, rnd);
			
			// test replacing
			for (int i = 0; i < NUM_LISTS; i++) {
				if( i % 100 != 0) {
					table.insertOrReplaceRecord(overwriteLists[i]);
					lists[i] = overwriteLists[i];
				}
			}
			
			for (int i = 0; i < NUM_LISTS; i++) {
				assertNotNull("" + i, prober.getMatchFor(lists[i], target));
				assertArrayEquals(lists[i].getValue(), target.getValue());
			}
			
			table.close();
			assertEquals("Memory lost", NUM_MEM_PAGES, table.getFreeMemory().size());
		} catch (Exception e) {
			e.printStackTrace();
			fail("Error: " + e.getMessage());
		}
	}
	
	@Test
	public void testVariableLengthBuildAndRetrieveMinorityUpdated() {
		try {
			final int NUM_LISTS = 20000;
			final int NUM_MEM_PAGES = SIZE * NUM_LISTS / PAGE_SIZE;
			
			final int STEP_SIZE = 100;
			
			final IntList[] lists = getRandomizedIntLists(NUM_LISTS, rnd);
			
			AbstractMutableHashTable<IntList> table = new CompactingHashTable<IntList>(serializerV, comparatorV, getMemory(NUM_MEM_PAGES, PAGE_SIZE));
			table.open();
			
			for (int i = 0; i < NUM_LISTS; i++) {
				table.insert(lists[i]);
			}
			
			AbstractHashTableProber<IntList, IntList> prober = table.getProber(comparatorV, pairComparatorV);
			IntList target = new IntList();
			
			for (int i = 0; i < NUM_LISTS; i++) {
				assertNotNull(prober.getMatchFor(lists[i], target));
				assertArrayEquals(lists[i].getValue(), target.getValue());
			}
			
			final IntList[] overwriteLists = getRandomizedIntLists(NUM_LISTS/STEP_SIZE, rnd);
			
			// test replacing
			for (int i = 0; i < NUM_LISTS; i += STEP_SIZE) {
				overwriteLists[i/STEP_SIZE].setKey(overwriteLists[i/STEP_SIZE].getKey()*STEP_SIZE);
				table.insertOrReplaceRecord(overwriteLists[i/STEP_SIZE]);
				lists[i] = overwriteLists[i/STEP_SIZE];
			}
			
			for (int i = 0; i < NUM_LISTS; i++) {
				assertNotNull(prober.getMatchFor(lists[i], target));
				assertArrayEquals(lists[i].getValue(), target.getValue());
			}
			
			table.close();
			assertEquals("Memory lost", NUM_MEM_PAGES, table.getFreeMemory().size());
		} catch (Exception e) {
			e.printStackTrace();
			fail("Error: " + e.getMessage());
		}
	}
	
	@Test
	public void testRepeatedBuildAndRetrieve() {
		try {
			final int NUM_MEM_PAGES = SIZE * NUM_LISTS / PAGE_SIZE;
			
			final IntList[] lists = getRandomizedIntLists(NUM_LISTS, rnd);
			
			AbstractMutableHashTable<IntList> table = new CompactingHashTable<IntList>(serializerV, comparatorV, getMemory(NUM_MEM_PAGES, PAGE_SIZE));
			table.open();
			
			for (int i = 0; i < NUM_LISTS; i++) {
				try {
					table.insert(lists[i]);
				} catch (Exception e) {
					throw e;
				}
			}


			AbstractHashTableProber<IntList, IntList> prober = table.getProber(comparatorV, pairComparatorV);
			IntList target = new IntList();
			
			for (int i = 0; i < NUM_LISTS; i++) {
				assertNotNull(prober.getMatchFor(lists[i], target));
				assertArrayEquals(lists[i].getValue(), target.getValue());
			}
			
			IntList[] overwriteLists;
			
			for(int k = 0; k < NUM_REWRITES; k++) {
				overwriteLists = getRandomizedIntLists(NUM_LISTS, rnd);
				// test replacing
				for (int i = 0; i < NUM_LISTS; i++) {
					table.insertOrReplaceRecord(overwriteLists[i]);
				}
			
				for (int i = 0; i < NUM_LISTS; i++) {
					assertNotNull("" + i, prober.getMatchFor(overwriteLists[i], target));
					assertArrayEquals(overwriteLists[i].getValue(), target.getValue());
				}
			}
			
			table.close();
			assertEquals("Memory lost", NUM_MEM_PAGES, table.getFreeMemory().size());
		} catch (Exception e) {
			e.printStackTrace();
			fail("Error: " + e.getMessage());
		}
	}
	
	@Test
	public void testProberUpdate() {
		try {
			final int NUM_MEM_PAGES = SIZE * NUM_LISTS / PAGE_SIZE;
			
			final IntList[] lists = getRandomizedIntLists(NUM_LISTS, rnd);
			
			AbstractMutableHashTable<IntList> table = new CompactingHashTable<IntList>(serializerV, comparatorV, getMemory(NUM_MEM_PAGES, PAGE_SIZE));
			table.open();
			
			for (int i = 0; i < NUM_LISTS; i++) {
				table.insert(lists[i]);
			}

			final IntList[] overwriteLists = getRandomizedIntLists(NUM_LISTS, rnd);

			AbstractHashTableProber<IntList, IntList> prober = table.getProber(comparatorV, pairComparatorV);
			IntList target = new IntList();
			
			for (int i = 0; i < NUM_LISTS; i++) {
				assertNotNull(""+i,prober.getMatchFor(lists[i], target));
				assertArrayEquals(lists[i].getValue(), target.getValue());
				prober.updateMatch(overwriteLists[i]);
			}
			
			for (int i = 0; i < NUM_LISTS; i++) {
				assertNotNull("" + i, prober.getMatchFor(overwriteLists[i], target));
				assertArrayEquals(overwriteLists[i].getValue(), target.getValue());
			}
			
			table.close();
			assertEquals("Memory lost", NUM_MEM_PAGES, table.getFreeMemory().size());
		} catch (Exception e) {
			e.printStackTrace();
			fail("Error: " + e.getMessage());
		}
	}
	
	@Test
	public void testResize() {
		try {
			final int NUM_MEM_PAGES = 30 * NUM_PAIRS / PAGE_SIZE;
			final IntPair[] pairs = getRandomizedIntPairs(NUM_PAIRS, rnd);
			
			List<MemorySegment> memory = getMemory(NUM_MEM_PAGES, PAGE_SIZE);
			CompactingHashTable<IntPair> table = new CompactingHashTable<IntPair>(serializer, comparator, memory);
			table.open();
			
			for (int i = 0; i < NUM_PAIRS; i++) {
				table.insert(pairs[i]);
			}
	
			AbstractHashTableProber<IntPair, IntPair> prober = table.getProber(comparator, pairComparator);
			IntPair target = new IntPair();
			
			for (int i = 0; i < NUM_PAIRS; i++) {
				assertNotNull(prober.getMatchFor(pairs[i], target));
				assertEquals(pairs[i].getValue(), target.getValue());
			}
			
			// make sure there is enough memory for resize
			memory.addAll(getMemory(ADDITIONAL_MEM, PAGE_SIZE));
			Boolean b = Whitebox.<Boolean>invokeMethod(table, "resizeHashTable");
			assertTrue(b.booleanValue());
			
			for (int i = 0; i < NUM_PAIRS; i++) {
				assertNotNull(pairs[i].getKey() + " " + pairs[i].getValue(), prober.getMatchFor(pairs[i], target));
				assertEquals(pairs[i].getValue(), target.getValue());
			}
			
			table.close();
			assertEquals("Memory lost", NUM_MEM_PAGES + ADDITIONAL_MEM, table.getFreeMemory().size());
		} catch (Exception e) {
			e.printStackTrace();
			fail("Error: " + e.getMessage());
		}
	}
	
	@Test
	public void testDoubleResize() {
		try {
			final int NUM_MEM_PAGES = 30 * NUM_PAIRS / PAGE_SIZE;
			final IntPair[] pairs = getRandomizedIntPairs(NUM_PAIRS, rnd);
			
			List<MemorySegment> memory = getMemory(NUM_MEM_PAGES, PAGE_SIZE);
			CompactingHashTable<IntPair> table = new CompactingHashTable<IntPair>(serializer, comparator, memory);
			table.open();
			
			for (int i = 0; i < NUM_PAIRS; i++) {
				table.insert(pairs[i]);
			}
	
			AbstractHashTableProber<IntPair, IntPair> prober = table.getProber(comparator, pairComparator);
			IntPair target = new IntPair();
			
			for (int i = 0; i < NUM_PAIRS; i++) {
				assertNotNull(prober.getMatchFor(pairs[i], target));
				assertEquals(pairs[i].getValue(), target.getValue());
			}
			
			// make sure there is enough memory for resize
			memory.addAll(getMemory(ADDITIONAL_MEM, PAGE_SIZE));
			Boolean b = Whitebox.<Boolean>invokeMethod(table, "resizeHashTable");
			assertTrue(b.booleanValue());
			
			for (int i = 0; i < NUM_PAIRS; i++) {
				assertNotNull(pairs[i].getKey() + " " + pairs[i].getValue(), prober.getMatchFor(pairs[i], target));
				assertEquals(pairs[i].getValue(), target.getValue());
			}
			
			// make sure there is enough memory for resize
			memory.addAll(getMemory(ADDITIONAL_MEM, PAGE_SIZE));
			b = Whitebox.<Boolean>invokeMethod(table, "resizeHashTable");
			assertTrue(b.booleanValue());
						
			for (int i = 0; i < NUM_PAIRS; i++) {
				assertNotNull(pairs[i].getKey() + " " + pairs[i].getValue(), prober.getMatchFor(pairs[i], target));
				assertEquals(pairs[i].getValue(), target.getValue());
			}
						
			table.close();
			assertEquals("Memory lost", NUM_MEM_PAGES + ADDITIONAL_MEM + ADDITIONAL_MEM, table.getFreeMemory().size());
		} catch (Exception e) {
			e.printStackTrace();
			fail("Error: " + e.getMessage());
		}
	}
	
	@Test
	public void testTripleResize() {
		try {
			final int NUM_MEM_PAGES = 30 * NUM_PAIRS / PAGE_SIZE;
			final IntPair[] pairs = getRandomizedIntPairs(NUM_PAIRS, rnd);
			
			List<MemorySegment> memory = getMemory(NUM_MEM_PAGES, PAGE_SIZE);
			CompactingHashTable<IntPair> table = new CompactingHashTable<IntPair>(serializer, comparator, memory);
			table.open();
			
			for (int i = 0; i < NUM_PAIRS; i++) {
				table.insert(pairs[i]);
			}
	
			AbstractHashTableProber<IntPair, IntPair> prober = table.getProber(comparator, pairComparator);
			IntPair target = new IntPair();
			
			for (int i = 0; i < NUM_PAIRS; i++) {
				assertNotNull(prober.getMatchFor(pairs[i], target));
				assertEquals(pairs[i].getValue(), target.getValue());
			}
			
			// make sure there is enough memory for resize
			memory.addAll(getMemory(ADDITIONAL_MEM, PAGE_SIZE));
			Boolean b = Whitebox.<Boolean>invokeMethod(table, "resizeHashTable");
			assertTrue(b.booleanValue());
			
			for (int i = 0; i < NUM_PAIRS; i++) {
				assertNotNull(pairs[i].getKey() + " " + pairs[i].getValue(), prober.getMatchFor(pairs[i], target));
				assertEquals(pairs[i].getValue(), target.getValue());
			}
			
			// make sure there is enough memory for resize
			memory.addAll(getMemory(ADDITIONAL_MEM, PAGE_SIZE));
			b = Whitebox.<Boolean>invokeMethod(table, "resizeHashTable");
			assertTrue(b.booleanValue());
						
			for (int i = 0; i < NUM_PAIRS; i++) {
				assertNotNull(pairs[i].getKey() + " " + pairs[i].getValue(), prober.getMatchFor(pairs[i], target));
				assertEquals(pairs[i].getValue(), target.getValue());
			}
			
			// make sure there is enough memory for resize
			memory.addAll(getMemory(2*ADDITIONAL_MEM, PAGE_SIZE));
			b = Whitebox.<Boolean>invokeMethod(table, "resizeHashTable");
			assertTrue(b.booleanValue());
									
			for (int i = 0; i < NUM_PAIRS; i++) {
				assertNotNull(pairs[i].getKey() + " " + pairs[i].getValue(), prober.getMatchFor(pairs[i], target));
				assertEquals(pairs[i].getValue(), target.getValue());
			}
						
			table.close();
			assertEquals("Memory lost", NUM_MEM_PAGES + 4*ADDITIONAL_MEM, table.getFreeMemory().size());
		} catch (Exception e) {
			e.printStackTrace();
			fail("Error: " + e.getMessage());
		}
	}
	
	@Test
	public void testResizeWithCompaction(){
		try {
			final int NUM_MEM_PAGES = (SIZE * NUM_LISTS / PAGE_SIZE);
			
			final IntList[] lists = getRandomizedIntLists(NUM_LISTS, rnd);
			
			List<MemorySegment> memory = getMemory(NUM_MEM_PAGES, PAGE_SIZE);
			CompactingHashTable<IntList> table = new CompactingHashTable<IntList>(serializerV, comparatorV, memory);
			table.open();
			
			for (int i = 0; i < NUM_LISTS; i++) {
				try {
					table.insert(lists[i]);
				} catch (Exception e) {
					throw e;
				}
			}

			AbstractHashTableProber<IntList, IntList> prober = table.getProber(comparatorV, pairComparatorV);
			IntList target = new IntList();
			
			for (int i = 0; i < NUM_LISTS; i++) {
				assertNotNull(prober.getMatchFor(lists[i], target));
				assertArrayEquals(lists[i].getValue(), target.getValue());
			}
			
			// make sure there is enough memory for resize
			memory.addAll(getMemory(ADDITIONAL_MEM, PAGE_SIZE));
			Boolean b = Whitebox.<Boolean>invokeMethod(table, "resizeHashTable");
			assertTrue(b.booleanValue());
						
			for (int i = 0; i < NUM_LISTS; i++) {
				assertNotNull(prober.getMatchFor(lists[i], target));
				assertArrayEquals(lists[i].getValue(), target.getValue());
			}
			
			final IntList[] overwriteLists = getRandomizedIntLists(NUM_LISTS, rnd);
			
			// test replacing
			for (int i = 0; i < NUM_LISTS; i++) {
				table.insertOrReplaceRecord(overwriteLists[i]);
			}
			
			Field list = Whitebox.getField(CompactingHashTable.class, "partitions");
			@SuppressWarnings("unchecked")
			ArrayList<InMemoryPartition<IntList>> partitions = (ArrayList<InMemoryPartition<IntList>>) list.get(table);
			int numPartitions = partitions.size();
			for(int i = 0; i < numPartitions; i++) {
				Whitebox.invokeMethod(table, "compactPartition", i);
			}
			
			// make sure there is enough memory for resize
			memory.addAll(getMemory(2*ADDITIONAL_MEM, PAGE_SIZE));
			b = Whitebox.<Boolean>invokeMethod(table, "resizeHashTable");
			assertTrue(b.booleanValue());									
			
			for (int i = 0; i < NUM_LISTS; i++) {
				assertNotNull("" + i, prober.getMatchFor(overwriteLists[i], target));
				assertArrayEquals(overwriteLists[i].getValue(), target.getValue());
			}
			
			table.close();
			assertEquals("Memory lost", NUM_MEM_PAGES + 3*ADDITIONAL_MEM, table.getFreeMemory().size());
		} catch (Exception e) {
			e.printStackTrace();
			fail("Error: " + e.getMessage());
		}
	}
	
	@Test
	public void testVariableLengthStringBuildAndRetrieve() {
		try {
			final int NUM_MEM_PAGES = 40 * NUM_PAIRS / PAGE_SIZE;
			
			MutableObjectIterator<StringPair> buildInput = new UniformStringPairGenerator(NUM_PAIRS, 1, false);
			
			MutableObjectIterator<StringPair> probeTester = new UniformStringPairGenerator(NUM_PAIRS, 1, false);
			
			MutableObjectIterator<StringPair> updater = new UniformStringPairGenerator(NUM_PAIRS, 1, false);

			MutableObjectIterator<StringPair> updateTester = new UniformStringPairGenerator(NUM_PAIRS, 1, false);
			
			AbstractMutableHashTable<StringPair> table = new CompactingHashTable<StringPair>(serializerS, comparatorS, getMemory(NUM_MEM_PAGES, PAGE_SIZE));
			table.open();
			
			StringPair target = new StringPair();
			while(buildInput.next(target) != null) {
				table.insert(target);
			}

			AbstractHashTableProber<StringPair, StringPair> prober = table.getProber(comparatorS, pairComparatorS);
			StringPair temp = new StringPair();
			while(probeTester.next(target) != null) {
				assertNotNull("" + target.getKey(), prober.getMatchFor(target, temp));
				assertEquals(temp.getValue(), target.getValue());
			}
			
			while(updater.next(target) != null) {
				target.setValue(target.getValue());
				table.insertOrReplaceRecord(target);
			}
			
			while (updateTester.next(target) != null) {
				assertNotNull(prober.getMatchFor(target, temp));
				assertEquals(target.getValue(), temp.getValue());
			}
			
			table.close();
			assertEquals("Memory lost", NUM_MEM_PAGES, table.getFreeMemory().size());
		} catch (Exception e) {
			e.printStackTrace();
			fail("Error: " + e.getMessage());
		}
	}
	
	
	private static IntPair[] getRandomizedIntPairs(int num, Random rnd) {
		IntPair[] pairs = new IntPair[num];
		
		// create all the pairs, dense
		for (int i = 0; i < num; i++) {
			pairs[i] = new IntPair(i, i + KEY_VALUE_DIFF);
		}
		
		// randomly swap them
		for (int i = 0; i < 2 * num; i++) {
			int pos1 = rnd.nextInt(num);
			int pos2 = rnd.nextInt(num);
			
			IntPair tmp = pairs[pos1];
			pairs[pos1] = pairs[pos2];
			pairs[pos2] = tmp;
		}
		
		return pairs;
	}
	
	private static IntList[] getRandomizedIntLists(int num, Random rnd) {
		IntList[] lists = new IntList[num];
		for (int i = 0; i < num; i++) {
			int[] value = new int[rnd.nextInt(MAX_LIST_SIZE)+1];
			//int[] value = new int[MAX_LIST_SIZE-1];
			for (int j = 0; j < value.length; j++) {
				value[j] = -rnd.nextInt(Integer.MAX_VALUE);
			}
			lists[i] = new IntList(i, value);
		}
		
		return lists;
	}
	
	private static List<MemorySegment> getMemory(int numPages, int pageSize) {
		List<MemorySegment> memory = new ArrayList<MemorySegment>();
		
		for (int i = 0; i < numPages; i++) {
			memory.add(new MemorySegment(new byte[pageSize]));
		}
		
		return memory;
	}
}
