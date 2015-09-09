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

import static org.junit.Assert.*;

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
import org.apache.flink.util.MutableObjectIterator;

import org.junit.Test;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

public class CompactingHashTableTest {
	
	private final TypeSerializer<Tuple2<Long, String>> serializer;
	private final TypeComparator<Tuple2<Long, String>> comparator;
	
	private final TypeComparator<Long> probeComparator;

	private final TypePairComparator<Long, Tuple2<Long, String>> pairComparator;
	
	
	public CompactingHashTableTest() {
		TypeSerializer<?>[] fieldSerializers = { LongSerializer.INSTANCE, StringSerializer.INSTANCE };
		@SuppressWarnings("unchecked")
		Class<Tuple2<Long, String>> clazz = (Class<Tuple2<Long, String>>) (Class<?>) Tuple2.class;
		this.serializer = new TupleSerializer<Tuple2<Long, String>>(clazz, fieldSerializers);
		
		TypeComparator<?>[] comparators = { new LongComparator(true) };
		TypeSerializer<?>[] comparatorSerializers = { LongSerializer.INSTANCE };
		
		this.comparator = new TupleComparator<Tuple2<Long, String>>(new int[] {0}, comparators, comparatorSerializers);
		
		this.probeComparator = new LongComparator(true);
		
		this.pairComparator = new TypePairComparator<Long, Tuple2<Long, String>>() {
			
			private long ref;
			
			@Override
			public void setReference(Long reference) {
				ref = reference;
			}

			@Override
			public boolean equalToReference(Tuple2<Long, String> candidate) {
				//noinspection UnnecessaryUnboxing
				return candidate.f0.longValue() == ref;
			}

			@Override
			public int compareToReference(Tuple2<Long, String> candidate) {
				long x = ref;
				long y = candidate.f0;
				return (x < y) ? -1 : ((x == y) ? 0 : 1);
			}
		};
	}
	
	// ------------------------------------------------------------------------
	//  tests
	// ------------------------------------------------------------------------
	
	@Test
	public void testHashTableGrowthWithInsert() {
		try {
			final int numElements = 1000000;
			
			List<MemorySegment> memory = getMemory(10000, 32 * 1024);
			
			// we create a hash table that thinks the records are super large. that makes it choose initially
			// a lot of memory for the partition buffers, and start with a smaller hash table. that way
			// we trigger a hash table growth early.
			CompactingHashTable<Tuple2<Long, String>> table = new CompactingHashTable<Tuple2<Long, String>>(
					serializer, comparator, memory, 10000);
			table.open();
			
			for (long i = 0; i < numElements; i++) {
				table.insert(new Tuple2<Long, String>(i, String.valueOf(i)));
			}
			
			// make sure that all elements are contained via the entry iterator
			{
				BitSet bitSet = new BitSet(numElements);
				MutableObjectIterator<Tuple2<Long, String>> iter = table.getEntryIterator();
				Tuple2<Long, String> next;
				while ((next = iter.next()) != null) {
					assertNotNull(next.f0);
					assertNotNull(next.f1);
					assertEquals(next.f0.longValue(), Long.parseLong(next.f1));
					
					bitSet.set(next.f0.intValue());
				}
				
				assertEquals(numElements, bitSet.cardinality());
			}
			
			// make sure all entries are contained via the prober
			{
				CompactingHashTable<Tuple2<Long, String>>.HashTableProber<Long> proper = 
						table.getProber(probeComparator, pairComparator);
				
				for (long i = 0; i < numElements; i++) {
					assertNotNull(proper.getMatchFor(i));
					assertNull(proper.getMatchFor(i + numElements));
				}
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	/**
	 * This test validates that records are not lost via "insertOrReplace()" as in bug [FLINK-2361]
	 */
	@Test
	public void testHashTableGrowthWithInsertOrReplace() {
		try {
			final int numElements = 1000000;

			List<MemorySegment> memory = getMemory(10000, 32 * 1024);

			// we create a hash table that thinks the records are super large. that makes it choose initially
			// a lot of memory for the partition buffers, and start with a smaller hash table. that way
			// we trigger a hash table growth early.
			CompactingHashTable<Tuple2<Long, String>> table = new CompactingHashTable<Tuple2<Long, String>>(
					serializer, comparator, memory, 10000);
			table.open();
			
			for (long i = 0; i < numElements; i++) {
				table.insertOrReplaceRecord(new Tuple2<Long, String>(i, String.valueOf(i)));
			}

			// make sure that all elements are contained via the entry iterator
			{
				BitSet bitSet = new BitSet(numElements);
				MutableObjectIterator<Tuple2<Long, String>> iter = table.getEntryIterator();
				Tuple2<Long, String> next;
				while ((next = iter.next()) != null) {
					assertNotNull(next.f0);
					assertNotNull(next.f1);
					assertEquals(next.f0.longValue(), Long.parseLong(next.f1));

					bitSet.set(next.f0.intValue());
				}

				assertEquals(numElements, bitSet.cardinality());
			}

			// make sure all entries are contained via the prober
			{
				CompactingHashTable<Tuple2<Long, String>>.HashTableProber<Long> proper =
						table.getProber(probeComparator, pairComparator);

				for (long i = 0; i < numElements; i++) {
					assertNotNull(proper.getMatchFor(i));
					assertNull(proper.getMatchFor(i + numElements));
				}
			}

		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	/**
	 * This test validates that new inserts (rather than updates) in "insertOrReplace()" properly
	 * react to out of memory conditions.
	 */
	@Test
	public void testInsertsWithInsertOrReplace() {
		try {
			final int numElements = 1000;

			final String longString = getLongString(10000);
			List<MemorySegment> memory = getMemory(1000, 32 * 1024);

			// we create a hash table that thinks the records are super large. that makes it choose initially
			// a lot of memory for the partition buffers, and start with a smaller hash table. that way
			// we trigger a hash table growth early.
			CompactingHashTable<Tuple2<Long, String>> table = new CompactingHashTable<Tuple2<Long, String>>(
					serializer, comparator, memory, 100);
			table.open();

			// first, we insert some elements
			for (long i = 0; i < numElements; i++) {
				table.insertOrReplaceRecord(new Tuple2<Long, String>(i, longString));
			}

			// now, we replace the same elements, causing fragmentation
			for (long i = 0; i < numElements; i++) {
				table.insertOrReplaceRecord(new Tuple2<Long, String>(i, longString));
			}
			
			// now we insert an additional set of elements. without compaction during this insertion,
			// the memory will run out
			for (long i = 0; i < numElements; i++) {
				table.insertOrReplaceRecord(new Tuple2<Long, String>(i + numElements, longString));
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	private static List<MemorySegment> getMemory(int numSegments, int segmentSize) {
		ArrayList<MemorySegment> list = new ArrayList<MemorySegment>(numSegments);
		for (int i = 0; i < numSegments; i++) {
			list.add(new MemorySegment(new byte[segmentSize]));
		}
		return list;
	}

	private static String getLongString(int length) {
		StringBuilder bld = new StringBuilder(length);
		for (int i = 0; i < length; i++) {
			bld.append('a');
		}
		return bld.toString();
	}
}
