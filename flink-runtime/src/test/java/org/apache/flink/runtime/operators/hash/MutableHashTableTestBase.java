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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypePairComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongComparator;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.TupleComparator;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
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

import static org.junit.Assert.*;


public abstract class MutableHashTableTestBase {
	
	protected static final long RANDOM_SEED = 76518743207143L;
	
	private static final int KEY_VALUE_DIFF = 1021;
	
	public final int PAGE_SIZE = 16 * 1024;

		
	public final TypeSerializer<IntPair> intPairSerializer = new IntPairSerializer();
	
	public final TypeComparator<IntPair> intPairComparator = new IntPairComparator();
	
	public final TypePairComparator<IntPair, IntPair> pairComparator = new IntPairPairComparator();
	
	
	private static final int MAX_LIST_SIZE = 8;

	public final TypeSerializer<IntList> serializerV = new IntListSerializer();

	public final TypeComparator<IntList> comparatorV = new IntListComparator();

	public final TypePairComparator<IntList, IntList> pairComparatorV = new IntListPairComparator();

	public final TypePairComparator<IntPair, IntList> pairComparatorPL = new IntPairListPairComparator();

	@SuppressWarnings("unchecked")
	protected TupleSerializer<Tuple2<Long, String>> tuple2LongStringSerializer =
		new TupleSerializer<>(
			(Class<Tuple2<Long, String>>) (Class<?>) Tuple2.class,
			new TypeSerializer<?>[] { LongSerializer.INSTANCE, StringSerializer.INSTANCE });

	protected TupleComparator<Tuple2<Long, String>> tuple2LongStringComparator =
		new TupleComparator<>(
			new int[] {0},
			new TypeComparator<?>[] { new LongComparator(true) },
			new TypeSerializer<?>[] { LongSerializer.INSTANCE });
	
	public final int SIZE = 75;

	public final int NUM_PAIRS = 100000;

	public final int NUM_LISTS = 100000;
	
	protected final int ADDITIONAL_MEM = 100;
	
	private final int NUM_REWRITES = 10;


	public final TypeSerializer<StringPair> serializerS = new StringPairSerializer();
	
	public final TypeComparator<StringPair> comparatorS = new StringPairComparator();
	
	private final TypePairComparator<StringPair, StringPair> pairComparatorS = new StringPairPairComparator();


	abstract protected <T> AbstractMutableHashTable<T> getHashTable(
		TypeSerializer<T> serializer, TypeComparator<T> comparator, List<MemorySegment> memory);

	@Test
	public void testDifferentProbers() {
		final int NUM_MEM_PAGES = 32 * NUM_PAIRS / PAGE_SIZE;
		AbstractMutableHashTable<IntPair> table = getHashTable(intPairSerializer, intPairComparator, getMemory(NUM_MEM_PAGES));

		AbstractHashTableProber<IntPair, IntPair> prober1 = table.getProber(intPairComparator, pairComparator);
		AbstractHashTableProber<IntPair, IntPair> prober2 = table.getProber(intPairComparator, pairComparator);

		assertFalse(prober1 == prober2);

		table.close(); // (This also tests calling close without calling open first.)
		assertEquals("Memory lost", NUM_MEM_PAGES, table.getFreeMemory().size());
	}

	@Test
	public void testBuildAndRetrieve() throws Exception {
		final int NUM_MEM_PAGES = 32 * NUM_PAIRS / PAGE_SIZE;
		AbstractMutableHashTable<IntPair> table = getHashTable(intPairSerializer, intPairComparator, getMemory(NUM_MEM_PAGES));

		final Random rnd = new Random(RANDOM_SEED);
		final IntPair[] pairs = getRandomizedIntPairs(NUM_PAIRS, rnd);

		table.open();

		for (int i = 0; i < NUM_PAIRS; i++) {
			table.insert(pairs[i]);
		}

		AbstractHashTableProber<IntPair, IntPair> prober = table.getProber(intPairComparator, pairComparator);
		IntPair target = new IntPair();

		for (int i = 0; i < NUM_PAIRS; i++) {
			assertNotNull(prober.getMatchFor(pairs[i], target));
			assertEquals(pairs[i].getValue(), target.getValue());
		}


		table.close();
		assertEquals("Memory lost", NUM_MEM_PAGES, table.getFreeMemory().size());
	}

	@Test
	public void testEntryIterator() throws Exception {
		final int NUM_MEM_PAGES = SIZE * NUM_LISTS / PAGE_SIZE;
		AbstractMutableHashTable<IntList> table = getHashTable(serializerV, comparatorV, getMemory(NUM_MEM_PAGES));

		final Random rnd = new Random(RANDOM_SEED);
		final IntList[] lists = getRandomizedIntLists(NUM_LISTS, rnd);

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

	@Test
	public void testMultipleProbers() throws Exception {
		final int NUM_MEM_PAGES = SIZE * NUM_LISTS / PAGE_SIZE;
		AbstractMutableHashTable<IntList> table = getHashTable(serializerV, comparatorV, getMemory(NUM_MEM_PAGES));

		final Random rnd = new Random(RANDOM_SEED);
		final IntList[] lists = getRandomizedIntLists(NUM_LISTS, rnd);
		final IntPair[] pairs = getRandomizedIntPairs(NUM_LISTS, rnd);

		table.open();
		for (int i = 0; i < NUM_LISTS; i++) {
			table.insert(lists[i]);
		}

		AbstractHashTableProber<IntList, IntList> listProber = table.getProber(comparatorV, pairComparatorV);

		AbstractHashTableProber<IntPair, IntList> pairProber = table.getProber(intPairComparator, pairComparatorPL);

		IntList target = new IntList();
		for (int i = 0; i < NUM_LISTS; i++) {
			assertNotNull(pairProber.getMatchFor(pairs[i], target));
			assertNotNull(listProber.getMatchFor(lists[i], target));
			assertArrayEquals(lists[i].getValue(), target.getValue());
		}
		table.close();
		assertEquals("Memory lost", NUM_MEM_PAGES, table.getFreeMemory().size());
	}

	@Test
	public void testVariableLengthBuildAndRetrieve() throws Exception {
		final int NUM_MEM_PAGES = SIZE * NUM_LISTS / PAGE_SIZE;
		AbstractMutableHashTable<IntList> table = getHashTable(serializerV, comparatorV, getMemory(NUM_MEM_PAGES));

		final Random rnd = new Random(RANDOM_SEED);
		final IntList[] lists = getRandomizedIntLists(NUM_LISTS, rnd);

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
	}

	@Test
	public void testVariableLengthBuildAndRetrieveMajorityUpdated() throws Exception {
		final int NUM_MEM_PAGES = SIZE * NUM_LISTS / PAGE_SIZE;
		AbstractMutableHashTable<IntList> table = getHashTable(serializerV, comparatorV, getMemory(NUM_MEM_PAGES));

		final Random rnd = new Random(RANDOM_SEED);
		final IntList[] lists = getRandomizedIntLists(NUM_LISTS, rnd);

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
	}

	@Test
	public void testVariableLengthBuildAndRetrieveMinorityUpdated() throws Exception {
		final int NUM_LISTS = 20000;
		final int NUM_MEM_PAGES = SIZE * NUM_LISTS / PAGE_SIZE;
		AbstractMutableHashTable<IntList> table =
			getHashTable(serializerV, comparatorV, getMemory(NUM_MEM_PAGES));

		final int STEP_SIZE = 100;

		final Random rnd = new Random(RANDOM_SEED);
		final IntList[] lists = getRandomizedIntLists(NUM_LISTS, rnd);

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
	}

	@Test
	public void testRepeatedBuildAndRetrieve() throws Exception {
		final int NUM_MEM_PAGES = SIZE * NUM_LISTS / PAGE_SIZE;
		AbstractMutableHashTable<IntList> table = getHashTable(serializerV, comparatorV, getMemory(NUM_MEM_PAGES));

		final Random rnd = new Random(RANDOM_SEED);
		final IntList[] lists = getRandomizedIntLists(NUM_LISTS, rnd);

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
	}

	@Test
	public void testProberUpdate() throws Exception {
		final int NUM_MEM_PAGES = SIZE * NUM_LISTS / PAGE_SIZE;
		AbstractMutableHashTable<IntList> table = getHashTable(serializerV, comparatorV, getMemory(NUM_MEM_PAGES));

		final Random rnd = new Random(RANDOM_SEED);
		final IntList[] lists = getRandomizedIntLists(NUM_LISTS, rnd);

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
	}

	@Test
	public void testVariableLengthStringBuildAndRetrieve() throws IOException {
		final int NUM_MEM_PAGES = 40 * NUM_PAIRS / PAGE_SIZE;
		AbstractMutableHashTable<StringPair> table = getHashTable(serializerS, comparatorS, getMemory(NUM_MEM_PAGES));

		MutableObjectIterator<StringPair> buildInput = new UniformStringPairGenerator(NUM_PAIRS, 1, false);

		MutableObjectIterator<StringPair> probeTester = new UniformStringPairGenerator(NUM_PAIRS, 1, false);

		MutableObjectIterator<StringPair> updater = new UniformStringPairGenerator(NUM_PAIRS, 1, false);

		MutableObjectIterator<StringPair> updateTester = new UniformStringPairGenerator(NUM_PAIRS, 1, false);

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
	}


	protected static IntPair[] getRandomizedIntPairs(int num, Random rnd) {
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
	
	protected static IntList[] getRandomizedIntLists(int num, Random rnd) {
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
	
	public List<MemorySegment> getMemory(int numPages) {
		return getMemory(numPages, PAGE_SIZE);
	}

	private static List<MemorySegment> getMemory(int numSegments, int segmentSize) {
		ArrayList<MemorySegment> list = new ArrayList<MemorySegment>(numSegments);
		for (int i = 0; i < numSegments; i++) {
			list.add(MemorySegmentFactory.allocateUnpooledSegment(segmentSize));
		}
		return list;
	}
}
