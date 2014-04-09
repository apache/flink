/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.pact.runtime.hash;


import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.Test;

import eu.stratosphere.api.common.typeutils.TypeComparator;
import eu.stratosphere.api.common.typeutils.TypePairComparator;
import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.core.memory.MemorySegment;
import eu.stratosphere.pact.runtime.test.util.UniformStringPairGenerator;
import eu.stratosphere.pact.runtime.test.util.types.IntList;
import eu.stratosphere.pact.runtime.test.util.types.IntListComparator;
import eu.stratosphere.pact.runtime.test.util.types.IntListPairComparator;
import eu.stratosphere.pact.runtime.test.util.types.IntListSerializer;
import eu.stratosphere.pact.runtime.test.util.types.IntPair;
import eu.stratosphere.pact.runtime.test.util.types.IntPairComparator;
import eu.stratosphere.pact.runtime.test.util.types.IntPairPairComparator;
import eu.stratosphere.pact.runtime.test.util.types.IntPairSerializer;
import eu.stratosphere.pact.runtime.test.util.types.StringPair;
import eu.stratosphere.pact.runtime.test.util.types.StringPairComparator;
import eu.stratosphere.pact.runtime.test.util.types.StringPairPairComparator;
import eu.stratosphere.pact.runtime.test.util.types.StringPairSerializer;
import eu.stratosphere.util.MutableObjectIterator;
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
	
	private final int SIZE = 80; //FIXME 75 triggers serialization bug in testVariableLengthBuildAndRetrieve
	
	private final int NUM_PAIRS = 100000;

	private final int NUM_LISTS = 100000;
	

	private final TypeSerializer<StringPair> serializerS = new StringPairSerializer();
	
	private final TypeComparator<StringPair> comparatorS = new StringPairComparator();
	
	private final TypePairComparator<StringPair, StringPair> pairComparatorS = new StringPairPairComparator();
	
	
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
	
			@SuppressWarnings("unchecked")
			AbstractHashTableProber<IntPair, IntPair> prober = (CompactingHashTable<IntPair>.HashTableProber<IntPair>) table.getProber(comparator, pairComparator);
			IntPair target = new IntPair();
			
			for (int i = 0; i < NUM_PAIRS; i++) {
				assertTrue(prober.getMatchFor(pairs[i], target));
				assertEquals(pairs[i].getValue(), target.getValue());
			}
			
			
			table.close();
			assertEquals("Memory lost", NUM_MEM_PAGES, table.getFreeMemory().size());
		}
		catch (Exception e) {
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
		}
		catch (Exception e) {
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
					System.out.println("index: " + i + " ");
					throw e;
				}
			}
						
			@SuppressWarnings("unchecked")
			AbstractHashTableProber<IntList, IntList> prober = (CompactingHashTable<IntList>.HashTableProber<IntList>) table.getProber(comparatorV, pairComparatorV);
			IntList target = new IntList();
			
			for (int i = 0; i < NUM_LISTS; i++) {
				assertTrue(prober.getMatchFor(lists[i], target));
				assertArrayEquals(lists[i].getValue(), target.getValue());
			}
			
			final IntList[] overwriteLists = getRandomizedIntLists(NUM_LISTS, rnd);
			
			// test replacing
			IntList tempHolder = new IntList();
			for (int i = 0; i < NUM_LISTS; i++) {
				table.insertOrReplaceRecord(overwriteLists[i], tempHolder);
			}
			
			for (int i = 0; i < NUM_LISTS; i++) {
				assertTrue(prober.getMatchFor(overwriteLists[i], target));
				assertArrayEquals(overwriteLists[i].getValue(), target.getValue());
			}
			
			table.close();
			assertEquals("Memory lost", NUM_MEM_PAGES, table.getFreeMemory().size());
		}
		catch (Exception e) {
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
						
			@SuppressWarnings("unchecked")
			AbstractHashTableProber<IntList, IntList> prober = (CompactingHashTable<IntList>.HashTableProber<IntList>) table.getProber(comparatorV, pairComparatorV);
			IntList target = new IntList();
			
			for (int i = 0; i < NUM_LISTS; i++) {
				assertTrue(prober.getMatchFor(lists[i], target));
				assertArrayEquals(lists[i].getValue(), target.getValue());
			}
			
			final IntList[] overwriteLists = getRandomizedIntLists(NUM_LISTS, rnd);
			
			// test replacing
			IntList tempHolder = new IntList();
			for (int i = 0; i < NUM_LISTS; i++) {
				if( i % 100 != 0) {
					table.insertOrReplaceRecord(overwriteLists[i], tempHolder);
					lists[i] = overwriteLists[i];
				}
			}
			
			for (int i = 0; i < NUM_LISTS; i++) {
				assertTrue(prober.getMatchFor(lists[i], target));
				assertArrayEquals(lists[i].getValue(), target.getValue());
			}
			
			table.close();
			assertEquals("Memory lost", NUM_MEM_PAGES, table.getFreeMemory().size());
		}
		catch (Exception e) {
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
						
			@SuppressWarnings("unchecked")
			AbstractHashTableProber<IntList, IntList> prober = (CompactingHashTable<IntList>.HashTableProber<IntList>) table.getProber(comparatorV, pairComparatorV);
			IntList target = new IntList();
			
			for (int i = 0; i < NUM_LISTS; i++) {
				assertTrue(prober.getMatchFor(lists[i], target));
				assertArrayEquals(lists[i].getValue(), target.getValue());
			}
			
			final IntList[] overwriteLists = getRandomizedIntLists(NUM_LISTS/STEP_SIZE, rnd);
			
			// test replacing
			IntList tempHolder = new IntList();
			for (int i = 0; i < NUM_LISTS; i += STEP_SIZE) {
				overwriteLists[i/STEP_SIZE].setKey(overwriteLists[i/STEP_SIZE].getKey()*STEP_SIZE);
				table.insertOrReplaceRecord(overwriteLists[i/STEP_SIZE], tempHolder);
				lists[i] = overwriteLists[i/STEP_SIZE];
			}
			
			for (int i = 0; i < NUM_LISTS; i++) {
				assertTrue(prober.getMatchFor(lists[i], target));
				assertArrayEquals(lists[i].getValue(), target.getValue());
			}
			
			table.close();
			assertEquals("Memory lost", NUM_MEM_PAGES, table.getFreeMemory().size());
		}
		catch (Exception e) {
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
			
			long start = 0L;
			long end = 0L;
			
			long first = System.currentTimeMillis();
			
			System.out.println("Creating and filling CompactingHashMap...");
			start = System.currentTimeMillis();
			AbstractMutableHashTable<StringPair> table = new CompactingHashTable<StringPair>(serializerS, comparatorS, getMemory(NUM_MEM_PAGES, PAGE_SIZE));
			table.open();
			
			StringPair target = new StringPair();
			while(buildInput.next(target) != null) {
				table.insert(target);
			}
			end = System.currentTimeMillis();
			System.out.println("HashMap ready. Time: " + (end-start) + " ms");
			
			System.out.println("Starting first probing run...");
			start = System.currentTimeMillis();
			@SuppressWarnings("unchecked")
			AbstractHashTableProber<StringPair, StringPair> prober = (CompactingHashTable<StringPair>.HashTableProber<StringPair>)table.getProber(comparatorS, pairComparatorS);
			StringPair temp = new StringPair();
			while(probeTester.next(target) != null) {
				assertTrue(prober.getMatchFor(target, temp));
				assertEquals(temp.getValue(), target.getValue());
			}
			end = System.currentTimeMillis();
			System.out.println("Probing done. Time: " + (end-start) + " ms");
			
			System.out.println("Starting update...");
			start = System.currentTimeMillis();
			while(updater.next(target) != null) {
				target.setValue(target.getValue());
				table.insertOrReplaceRecord(target, temp);
			}
			end = System.currentTimeMillis();
			System.out.println("Update done. Time: " + (end-start) + " ms");
			
			System.out.println("Starting second probing run...");
			start = System.currentTimeMillis();
			while (updateTester.next(target) != null) {
				assertTrue(prober.getMatchFor(target, temp));
				assertEquals(target.getValue(), temp.getValue());
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
