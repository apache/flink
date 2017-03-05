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

package org.apache.flink.runtime.codegeneration;

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.TupleComparator;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.codegeneration.utils.CodeGenerationSorterBaseTest;
import org.apache.flink.runtime.codegeneration.utils.SorterTestDataGenerator;
import org.apache.flink.runtime.operators.sort.InMemorySorter;
import org.apache.flink.runtime.operators.testutils.TestData;
import org.apache.flink.runtime.operators.testutils.TestData.TupleGenerator.KeyMode;
import org.apache.flink.runtime.operators.testutils.TestData.TupleGenerator.ValueMode;
import org.junit.Test;

import java.util.List;
import java.util.Random;


public class SortingTest extends CodeGenerationSorterBaseTest {

	@Test
	public void testSortIntKeys() throws Exception {
		// Tuple<Int,String> and sort by int
		int keyPos = 0;

		@SuppressWarnings("unchecked")
		List<MemorySegment> memory = createMemory();

		TypeSerializer serializer  = TestData.getIntStringTupleSerializer();
		TypeComparator comparator  = TestData.getIntStringTupleComparator();

		InMemorySorter<Tuple2<Integer, String>> sorter = createSorter(serializer, comparator, memory);

		final TestData.TupleGenerator generator = new TestData.TupleGenerator(SEED, KEY_MAX, VALUE_LENGTH, KeyMode.RANDOM,
			ValueMode.RANDOM_LENGTH);

		testSorting( new SorterTestDataGenerator<Tuple2<Integer,String>>() {
			@Override
			public Tuple2<Integer, String> generate(Tuple2<Integer, String> record) {
				return generator.next(record);
			}
		}, sorter, comparator.getFlatComparators()[0], keyPos );

		// release the memory occupied by the buffers
		sorter.dispose();
		this.memoryManager.release(memory);
	}

	@Test
	public void testSortString5BytesKeys() throws Exception {
		// Tuple<Int,String> and sort by string
		int keyPos = 1;

		@SuppressWarnings("unchecked")
		List<MemorySegment> memory = createMemory();

		TypeSerializer serializer  = TestData.getIntStringTupleSerializer();
		TypeComparator comparator  = TestData.getIntStringTupleTypeInfo().createComparator(new int[]{keyPos}, new boolean[]{true}, 0, null);

		InMemorySorter<Tuple2<Integer, String>> sorter = createSorter(serializer, comparator, memory);

		final TestData.TupleGenerator generator = new TestData.TupleGenerator(SEED, KEY_MAX, 5, KeyMode.RANDOM,
			ValueMode.FIX_LENGTH);

		testSorting( new SorterTestDataGenerator<Tuple2<Integer,String>>() {
			@Override
			public Tuple2<Integer, String> generate(Tuple2<Integer, String> record) {
				return generator.next(record);
			}
		}, sorter, comparator.getFlatComparators()[0], keyPos );

		// release the memory occupied by the buffers
		sorter.dispose();
		this.memoryManager.release(memory);
	}

	@Test
	public void testSortStringVariableLengthKeys() throws Exception {
		// Tuple<Int,String> and sort by string
		int keyPos = 1;

		@SuppressWarnings("unchecked")
		List<MemorySegment> memory = createMemory();

		TypeSerializer serializer  = TestData.getIntStringTupleSerializer();
		TypeComparator comparator  = TestData.getIntStringTupleTypeInfo().createComparator(new int[]{keyPos}, new boolean[]{true}, 0, null);

		InMemorySorter<Tuple2<Integer, String>> sorter = createSorter(serializer, comparator, memory);

		final TestData.TupleGenerator generator = new TestData.TupleGenerator(SEED, KEY_MAX, 20, KeyMode.RANDOM,
			ValueMode.RANDOM_LENGTH);

		testSorting( new SorterTestDataGenerator<Tuple2<Integer,String>>() {
			@Override
			public Tuple2<Integer, String> generate(Tuple2<Integer, String> record) {
				return generator.next(record);
			}
		}, sorter, comparator.getFlatComparators()[0], keyPos );

		// release the memory occupied by the buffers
		sorter.dispose();
		this.memoryManager.release(memory);
	}

	@Test
	public void testSortLongKeys() throws Exception {
		// Tuple<Long, Int>
		int keyPos = 0;

		@SuppressWarnings("unchecked")
		List<MemorySegment> memory = createMemory();

		TypeSerializer[] insideSerializers = {
			LongSerializer.INSTANCE, IntSerializer.INSTANCE
		};

		TupleSerializer<Tuple2<Long,Integer>> serializer = new TupleSerializer<>(
			(Class<Tuple2<Long, Integer>>) (Class<?>) Tuple2.class, insideSerializers
		);

		TupleComparator<Tuple2<Long, Integer>> comparator = new TupleComparator<>(
			new int[]{keyPos}, new TypeComparator[]{ new LongComparator(true) }, insideSerializers
		);

		InMemorySorter<Tuple2<Long, Integer>> sorter = createSorter( serializer, comparator, memory);

		final Random randomGenerator = new Random(SEED);

		testSorting( new SorterTestDataGenerator<Tuple2<Long, Integer>>() {
			@Override
			public Tuple2<Long, Integer> generate(Tuple2<Long, Integer> record) {
				record.setFields(randomGenerator.nextLong(), randomGenerator.nextInt());
				return record;
			}
		}, sorter, comparator.getFlatComparators()[0], keyPos );

		// release the memory occupied by the buffers
		sorter.dispose();
		this.memoryManager.release(memory);
	}

	@Test
	public void testSort12BytesIntegerLongKeys() throws Exception {
		// Tuple<<Int,Long>, Int>
		int keyPos = 0;

		@SuppressWarnings("unchecked")
		List<MemorySegment> memory = createMemory();

		TypeSerializer keySerializer = new TupleSerializer(Tuple2.class, new TypeSerializer[]{
			IntSerializer.INSTANCE,
			LongSerializer.INSTANCE
		});

		TypeComparator keyComparator = new TupleComparator(
			new int[]{0,1},
			new TypeComparator[]{
				new IntComparator(true),
				new LongComparator(true)
			},
			new TypeSerializer[] { keySerializer }
		);

		TypeSerializer[] insideSerializers = {
			keySerializer,
			IntSerializer.INSTANCE
		};

		TupleSerializer<Tuple2<Tuple2<Integer,Long>,Integer>> serializer = new TupleSerializer<>(
			(Class<Tuple2<Tuple2<Integer,Long>, Integer>>) (Class<?>) Tuple2.class, insideSerializers
		);

		TupleComparator<Tuple2<Tuple2<Integer,Long>, Integer>> comparators = new TupleComparator<>(
			new int[]{keyPos}, new TypeComparator[]{ keyComparator },
			insideSerializers
		);

		InMemorySorter<Tuple2<Tuple2<Integer,Long>, Integer>> sorter = createSorter( serializer, comparators, memory);

		final Random randomGenerator = new Random(SEED);

		testSorting( new SorterTestDataGenerator<Tuple2<Tuple2<Integer,Long>, Integer>>() {
			@Override
			public Tuple2<Tuple2<Integer, Long>, Integer> generate(Tuple2<Tuple2 <Integer, Long>, Integer> record) {
				Tuple2<Integer, Long> insideTp = new Tuple2<>();
				insideTp.setFields(randomGenerator.nextInt(), randomGenerator.nextLong());
				record.setFields(insideTp, randomGenerator.nextInt());
				return record;
			}
		}, sorter, keyComparator, keyPos );

		// release the memory occupied by the buffers
		sorter.dispose();
		this.memoryManager.release(memory);
	}

	@Test
	public void testSort12BytesLongIntKeys() throws Exception {
		// Tuple<<Long,Int>, Int>
		int keyPos = 0;

		@SuppressWarnings("unchecked")
		List<MemorySegment> memory = createMemory();

		TypeSerializer keySerializer = new TupleSerializer(Tuple2.class, new TypeSerializer[]{
			LongSerializer.INSTANCE,
			IntSerializer.INSTANCE
		});

		TypeComparator keyComparator = new TupleComparator(
			new int[]{0,1},
			new TypeComparator[]{
				new LongComparator(true),
				new IntComparator(true)
			},
			new TypeSerializer[] { keySerializer }
		);

		TypeSerializer[] insideSerializers = {
			keySerializer,
			IntSerializer.INSTANCE
		};

		TupleSerializer<Tuple2<Tuple2<Long,Integer>,Integer>> serializer = new TupleSerializer<>(
			(Class<Tuple2<Tuple2<Long,Integer>, Integer>>) (Class<?>) Tuple2.class, insideSerializers
		);

		TupleComparator<Tuple2<Tuple2<Long,Integer>, Integer>> comparators = new TupleComparator<>(
			new int[]{keyPos}, new TypeComparator[]{ keyComparator },
			insideSerializers
		);

		InMemorySorter<Tuple2<Tuple2<Long, Integer>, Integer>> sorter = createSorter( serializer, comparators, memory);

		final Random randomGenerator = new Random(SEED);

		testSorting( new SorterTestDataGenerator<Tuple2<Tuple2<Long, Integer>, Integer>>() {
			@Override
			public Tuple2<Tuple2<Long, Integer>, Integer> generate(Tuple2<Tuple2<Long, Integer>, Integer> record) {
				Tuple2<Long, Integer> insideTp = new Tuple2<>();
				insideTp.setFields(randomGenerator.nextLong(), randomGenerator.nextInt());
				record.setFields(insideTp, randomGenerator.nextInt());
				return record;
			}
		}, sorter, keyComparator, keyPos );

		// release the memory occupied by the buffers
		sorter.dispose();
		this.memoryManager.release(memory);
	}

	@Test
	public void testSort6BytesIntShortKeys() throws Exception {
		// Tuple<<Int, Short>, Int>
		int keyPos = 0;

		@SuppressWarnings("unchecked")
		List<MemorySegment> memory = createMemory();

		TypeSerializer keySerializer = new TupleSerializer(Tuple2.class, new TypeSerializer[]{
			IntSerializer.INSTANCE,
			ShortSerializer.INSTANCE
		});

		TypeComparator keyComparator = new TupleComparator(
			new int[]{0,1},
			new TypeComparator[]{
				new IntComparator(true),
				new ShortComparator(true)
			},
			new TypeSerializer[] { keySerializer }
		);

		TypeSerializer[] insideSerializers = {
			keySerializer,
			IntSerializer.INSTANCE
		};

		TupleSerializer<Tuple2<Tuple2<Integer,Short>, Integer>> serializer = new TupleSerializer<>(
			(Class<Tuple2<Tuple2<Integer,Short>, Integer>>) (Class<?>) Tuple2.class, insideSerializers
		);

		TupleComparator<Tuple2<Tuple2<Integer,Short>, Integer>> comparators = new TupleComparator<>(
			new int[]{0}, new TypeComparator[]{ keyComparator },
			insideSerializers
		);

		InMemorySorter<Tuple2<Tuple2<Integer,Short>, Integer>> sorter = createSorter( serializer, comparators, memory);

		final Random randomGenerator = new Random(SEED);

		testSorting( new SorterTestDataGenerator<Tuple2<Tuple2<Integer, Short>, Integer>>() {
			@Override
			public Tuple2<Tuple2<Integer, Short>, Integer> generate(Tuple2 <Tuple2<Integer, Short>, Integer> record) {
				Tuple2<Integer, Short> insideTp = new Tuple2<>();
				insideTp.setFields(randomGenerator.nextInt(), (short) randomGenerator.nextInt());
				record.setFields(insideTp, randomGenerator.nextInt());
				return record;
			}
		}, sorter, keyComparator, keyPos );

		// release the memory occupied by the buffers
		sorter.dispose();
		this.memoryManager.release(memory);
	}

	@Test
	public void testSort6BytesShortIntKeys() throws Exception {
		// Tuple<<Short, Int>, Int>
		int keyPos = 0;

		@SuppressWarnings("unchecked")
		List<MemorySegment> memory = createMemory();

		TypeSerializer keySerializer = new TupleSerializer(Tuple2.class, new TypeSerializer[]{
			ShortSerializer.INSTANCE,
			IntSerializer.INSTANCE
		});

		TypeComparator keyComparator = new TupleComparator(
			new int[]{0,1},
			new TypeComparator[]{
				new ShortComparator(true),
				new IntComparator(true)
			},
			new TypeSerializer[] { keySerializer }
		);

		TypeSerializer[] insideSerializers = {
			keySerializer,
			IntSerializer.INSTANCE
		};

		TupleSerializer<Tuple2<Tuple2<Short,Integer>, Integer>> serializer = new TupleSerializer<>(
			(Class<Tuple2<Tuple2<Short,Integer>, Integer>>) (Class<?>) Tuple2.class, insideSerializers
		);

		TupleComparator<Tuple2<Tuple2<Short,Integer>, Integer>> comparators = new TupleComparator<>(
			new int[]{keyPos}, new TypeComparator[]{ keyComparator },
			insideSerializers
		);

		InMemorySorter<Tuple2<Tuple2<Short,Integer>, Integer>> sorter = createSorter( serializer, comparators, memory);

		final Random randomGenerator = new Random(SEED);

		testSorting( new SorterTestDataGenerator<Tuple2<Tuple2<Short,Integer>, Integer>>() {
			@Override
			public Tuple2<Tuple2<Short,Integer>, Integer> generate(Tuple2<Tuple2<Short,Integer>, Integer> record) {
				Tuple2<Short, Integer> insideTp = new Tuple2<>();
				insideTp.setFields((short)randomGenerator.nextInt(),randomGenerator.nextInt());
				record.setFields(insideTp, randomGenerator.nextInt());
				return record;
			}
		}, sorter, keyComparator, keyPos );

		// release the memory occupied by the buffers
		sorter.dispose();
		this.memoryManager.release(memory);
	}

	@Test
	public void testSort5BytesIntByteKeys() throws Exception {
		// Tuple<<Int, Byte>, Int>
		int keyPos = 0;

		@SuppressWarnings("unchecked")
		List<MemorySegment> memory = createMemory();

		TypeSerializer keySerializer = new TupleSerializer(Tuple2.class, new TypeSerializer[]{
			IntSerializer.INSTANCE,
			ByteSerializer.INSTANCE
		});

		TypeComparator keyComparator = new TupleComparator(
			new int[]{0,1},
			new TypeComparator[]{
				new IntComparator(true),
				new ByteComparator(true)
			},
			new TypeSerializer[] { keySerializer }
		);

		TypeSerializer[] insideSerializers = {
			keySerializer,
			IntSerializer.INSTANCE
		};

		TupleSerializer<Tuple2<Tuple2<Integer,Byte>, Integer>> serializer = new TupleSerializer<>(
			(Class<Tuple2<Tuple2<Integer,Byte>, Integer>>) (Class<?>) Tuple2.class, insideSerializers
		);

		TupleComparator<Tuple2<Tuple2<Integer,Byte>, Integer>> comparators = new TupleComparator<>(
			new int[]{keyPos}, new TypeComparator[]{ keyComparator },
			insideSerializers
		);

		InMemorySorter<Tuple2<Tuple2<Integer,Byte>, Integer>> sorter = createSorter( serializer, comparators, memory);

		final Random randomGenerator = new Random(SEED);
		final byte[] temporyBytes = new byte[1];

		testSorting( new SorterTestDataGenerator<Tuple2<Tuple2<Integer, Byte>, Integer>>() {
			@Override
			public Tuple2<Tuple2<Integer, Byte>, Integer> generate(Tuple2<Tuple2<Integer, Byte>, Integer> record) {
				Tuple2<Integer, Byte> insideTp = new Tuple2<>();
				randomGenerator.nextBytes(temporyBytes);
				insideTp.setFields(randomGenerator.nextInt(), temporyBytes[0] );
				record.setFields(insideTp, randomGenerator.nextInt());
				return record;
			}
		}, sorter, keyComparator, keyPos );

		// release the memory occupied by the buffers
		sorter.dispose();
		this.memoryManager.release(memory);
	}
}
