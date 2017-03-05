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

import freemarker.template.TemplateException;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.TupleComparator;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemoryType;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.sort.InMemorySorter;
import org.apache.flink.runtime.operators.sort.QuickSort;
import org.apache.flink.runtime.operators.testutils.DummyInvokable;
import org.apache.flink.runtime.operators.testutils.TestData;
import org.apache.flink.runtime.operators.testutils.TestData.TupleGenerator.KeyMode;
import org.apache.flink.runtime.operators.testutils.TestData.TupleGenerator.ValueMode;
import org.apache.flink.util.MutableObjectIterator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Random;


public class SortingTest {

	private static final long SEED = 649180756312423613L;

	private static final long SEED2 = 97652436586326573L;

	private static final int KEY_MAX = Integer.MAX_VALUE;

	private static final int VALUE_LENGTH = 118;

	private static final int MEMORY_SIZE = 1024 * 1024 * 64;

	private static final int MEMORY_PAGE_SIZE = 32 * 1024;

	private MemoryManager memoryManager;
	private SorterFactory sorterFactory;

	private static final ExecutionConfig executionConfig = new ExecutionConfig(){
		{
			setCodeGenerationForSorterEnabled(true);
		}
	};


	@Before
	public void beforeTest() throws IOException {
		this.memoryManager = new MemoryManager(MEMORY_SIZE, 1, MEMORY_PAGE_SIZE, MemoryType.HEAP, true);
		this.sorterFactory = SorterFactory.getInstance();

		Assert.assertTrue("Code generation for sorter is enabled", executionConfig.isCodeGenerationForSorterEnabled());
	}

	@After
	public void afterTest() {
		if (!this.memoryManager.verifyEmpty()) {
			Assert.fail("Memory Leak: Some memory has not been returned to the memory manager.");
		}

		if (this.memoryManager != null) {
			this.memoryManager.shutdown();
			this.memoryManager = null;
		}
	}

	private InMemorySorter<Tuple2<Integer, String>> newSortBuffer(List<MemorySegment> memory) throws Exception {
		return this.sorterFactory.createSorter(executionConfig, TestData.getIntStringTupleSerializer(), TestData.getIntStringTupleComparator(), memory);
	}

	@Test
	public void testSortIntKeys() throws Exception {
		final int NUM_RECORDS = 559273;

		final int numSegments = MEMORY_SIZE / MEMORY_PAGE_SIZE;
		final List<MemorySegment> memory = this.memoryManager.allocatePages(new DummyInvokable(), numSegments);

		InMemorySorter<Tuple2<Integer, String>> sorter = newSortBuffer(memory);
		TestData.TupleGenerator generator = new TestData.TupleGenerator(SEED, KEY_MAX, VALUE_LENGTH, KeyMode.RANDOM,
			ValueMode.RANDOM_LENGTH);

		// write the records
		Tuple2<Integer, String> record = new Tuple2<>();
		int num = 0;
		do {
			generator.next(record);
			num++;
		}
		while (sorter.write(record) && num < NUM_RECORDS);

		QuickSort qs = new QuickSort();
		qs.sort(sorter);

		MutableObjectIterator<Tuple2<Integer, String>> iter = sorter.getIterator();
		Tuple2<Integer, String> readTarget = new Tuple2<>();

		iter.next(readTarget);
		int last = readTarget.f0;

		while ((readTarget = iter.next(readTarget)) != null) {
			int current = readTarget.f0;

			final int cmp = last - current;
			if (cmp > 0) {
				Assert.fail("Next key is not larger or equal to previous key.");
			}

			last = current;
		}

		// release the memory occupied by the buffers
		sorter.dispose();
		this.memoryManager.release(memory);
	}

	@Test
	public void testSortString5BytesKeys() throws Exception {

		@SuppressWarnings("unchecked")
		List<MemorySegment> memory = createMemory();
		TypeComparator<Tuple2<Integer, String>> accessors = TestData.getIntStringTupleTypeInfo().createComparator(new int[]{1}, new boolean[]{true}, 0, null);
		InMemorySorter<Tuple2<Integer, String>> sorter = createSorter(TestData.getIntStringTupleSerializer(), accessors, memory);

		TestData.TupleGenerator generator = new TestData.TupleGenerator(SEED, KEY_MAX, 5, KeyMode.RANDOM,
			ValueMode.FIX_LENGTH);

		Tuple2<Integer, String> record = new Tuple2<>();
		do {
			generator.next(record);
		}
		while (sorter.write(record));

		QuickSort qs = new QuickSort();
		qs.sort(sorter);

		MutableObjectIterator<Tuple2<Integer, String>> iter = sorter.getIterator();
		Tuple2<Integer, String> readTarget = new Tuple2<>();

		iter.next(readTarget);
		String last = readTarget.f1;

		while ((readTarget = iter.next(readTarget)) != null) {
			String current = readTarget.f1;

			final int cmp = last.compareTo(current);
			if (cmp > 0) {
				Assert.fail("Next value is not larger or equal to previous value.");
			}

			last = current;
		}

		// release the memory occupied by the buffers
		sorter.dispose();
		this.memoryManager.release(memory);
	}
	@Test
	public void testSortStringVariableLengthKeys() throws Exception {

		@SuppressWarnings("unchecked")
		List<MemorySegment> memory = createMemory();
		TypeComparator<Tuple2<Integer, String>> accessors = TestData.getIntStringTupleTypeInfo().createComparator(new int[]{1}, new boolean[]{true}, 0, null);
		InMemorySorter<Tuple2<Integer, String>> sorter = createSorter(TestData.getIntStringTupleSerializer(), accessors, memory);

		TestData.TupleGenerator generator = new TestData.TupleGenerator(SEED, KEY_MAX, 5, KeyMode.RANDOM,
			ValueMode.RANDOM_LENGTH);

		Tuple2<Integer, String> record = new Tuple2<>();
		do {
			generator.next(record);
		}
		while (sorter.write(record));

		QuickSort qs = new QuickSort();
		qs.sort(sorter);

		MutableObjectIterator<Tuple2<Integer, String>> iter = sorter.getIterator();
		Tuple2<Integer, String> readTarget = new Tuple2<>();

		iter.next(readTarget);
		String last = readTarget.f1;

		while ((readTarget = iter.next(readTarget)) != null) {
			String current = readTarget.f1;

			final int cmp = last.compareTo(current);
			if (cmp > 0) {
				Assert.fail("Next value is not larger or equal to previous value.");
			}

			last = current;
		}

		// release the memory occupied by the buffers
		sorter.dispose();
		this.memoryManager.release(memory);
	}

	@Test
	public void testSortLongKeys() throws Exception {

		@SuppressWarnings("unchecked")
		List<MemorySegment> memory = createMemory();

		TypeSerializer[] insideSerializers = {
			LongSerializer.INSTANCE, IntSerializer.INSTANCE
		};

		TupleSerializer<Tuple2<Long,Integer>> serializer = new TupleSerializer<Tuple2<Long, Integer>>(
			(Class<Tuple2<Long, Integer>>) (Class<?>) Tuple2.class, insideSerializers
		);

		TupleComparator<Tuple2<Long, Integer>> comparators = new TupleComparator<Tuple2<Long, Integer>>(
			new int[]{0}, new TypeComparator[]{ new LongComparator(true) }, insideSerializers
		);

		InMemorySorter<Tuple2<Long, Integer>> sorter = createSorter( serializer, comparators, memory);
//		InMemorySorter<Tuple2<Long, Integer>> sorter = new NormalizedKeySorter<>(serializer, comparators, memory);

		Random randomGenerator = new Random(SEED);

		Tuple2<Long, Integer> record = new Tuple2<>();
		do {
			record.setFields(randomGenerator.nextLong(), randomGenerator.nextInt());
		}
		while (sorter.write(record));

		QuickSort qs = new QuickSort();
		qs.sort(sorter);

		MutableObjectIterator<Tuple2<Long, Integer>> iter = sorter.getIterator();
		Tuple2<Long, Integer> readTarget = new Tuple2<>();

		iter.next(readTarget);
		Long last = readTarget.f0;

		while ((readTarget = iter.next(readTarget)) != null) {
			Long current = readTarget.f0;

			final int cmp = last.compareTo(current);
			if (cmp > 0) {
				Assert.fail("Next value is not larger or equal to previous value.");
			}

			last = current;
		}

		// release the memory occupied by the buffers
		sorter.dispose();
		this.memoryManager.release(memory);
	}

	@Test
	public void testSort12BytesIntegerLongKeys() throws Exception {
		// Tuple( (Int,Long), Int )

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
			new int[]{0}, new TypeComparator[]{ keyComparator },
			insideSerializers
		);

		InMemorySorter<Tuple2<Tuple2<Integer,Long>, Integer>> sorter = createSorter( serializer, comparators, memory);

		Random randomGenerator = new Random(SEED);

		Tuple2<Tuple2<Integer,Long>, Integer> record = new Tuple2<>();
		do {

			Tuple2<Integer,Long> insideTp = new Tuple2<>();
			insideTp.setFields(randomGenerator.nextInt(), randomGenerator.nextLong());
			record.setFields(insideTp, randomGenerator.nextInt());
		}
		while (sorter.write(record));

		QuickSort qs = new QuickSort();
		qs.sort(sorter);

		MutableObjectIterator<Tuple2<Tuple2<Integer,Long>, Integer>> iter = sorter.getIterator();
		Tuple2<Tuple2<Integer,Long>, Integer> readTarget = iter.next();

		Tuple2<Integer,Long> last = readTarget.f0;

		while ((readTarget = iter.next()) != null) {
			Tuple2<Integer,Long> current = readTarget.f0;

			final int cmp = keyComparator.compare(last, current);
			if (cmp > 0) {
				Assert.fail("Next value is not larger or equal to previous value.");
			}

			last = current;
		}

		// release the memory occupied by the buffers
		sorter.dispose();
		this.memoryManager.release(memory);
	}

	@Test
	public void testSort12BytesLongIntKeys() throws Exception {
		// Tuple( (Long,Int), Int )

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
			new int[]{0}, new TypeComparator[]{ keyComparator },
			insideSerializers
		);

		InMemorySorter<Tuple2<Tuple2<Long, Integer>, Integer>> sorter = createSorter( serializer, comparators, memory);

		Random randomGenerator = new Random(SEED);

		Tuple2<Tuple2<Long, Integer>, Integer> record = new Tuple2<>();
		do {

			Tuple2<Long,Integer> insideTp = new Tuple2<>();
			insideTp.setFields(randomGenerator.nextLong(), randomGenerator.nextInt());
			record.setFields(insideTp, randomGenerator.nextInt());
		}
		while (sorter.write(record));

		QuickSort qs = new QuickSort();
		qs.sort(sorter);

		MutableObjectIterator<Tuple2<Tuple2<Long, Integer>, Integer>> iter = sorter.getIterator();
		Tuple2<Tuple2<Long, Integer>, Integer> readTarget = iter.next();

		Tuple2<Long,Integer> last = readTarget.f0;

		while ((readTarget = iter.next()) != null) {
			Tuple2<Long,Integer> current = readTarget.f0;

			final int cmp = keyComparator.compare(last, current);
			if (cmp > 0) {
				Assert.fail("Next value is not larger or equal to previous value.");
			}

			last = current;
		}

		// release the memory occupied by the buffers
		sorter.dispose();
		this.memoryManager.release(memory);
	}

	@Test
	public void testSort6BytesIntShortKeys() throws Exception {
		// Tuple( (Int,Short), Int )

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

		Random randomGenerator = new Random(SEED);

		Tuple2<Tuple2<Integer,Short>, Integer> record = new Tuple2<>();
		int count = 20;
		do {

			Tuple2<Integer,Short> insideTp = new Tuple2<>();
			insideTp.setFields(randomGenerator.nextInt(), (short)randomGenerator.nextInt());
			record.setFields(insideTp, randomGenerator.nextInt());
//			System.out.println(record);
		}
		while (sorter.write(record));

		QuickSort qs = new QuickSort();
		qs.sort(sorter);

		MutableObjectIterator<Tuple2<Tuple2<Integer,Short>, Integer>> iter = sorter.getIterator();
		Tuple2<Tuple2<Integer,Short>, Integer> readTarget = iter.next();

		Tuple2<Integer,Short> last = readTarget.f0;

		System.out.println("------");
		while ((readTarget = iter.next()) != null) {
			Tuple2<Integer,Short> current = readTarget.f0;

			final int cmp = keyComparator.compare(last, current);
			if (cmp > 0) {
				System.out.println(last);
				System.out.println(current);
				Assert.fail("Next value is not larger or equal to previous value.");

			}

			last = current;
		}

		// release the memory occupied by the buffers
		sorter.dispose();
		this.memoryManager.release(memory);
	}

	@Test
	public void testSort6BytesShortIntKeys() throws Exception {
		// Tuple( (Short,Int), Int )

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
			new int[]{0}, new TypeComparator[]{ keyComparator },
			insideSerializers
		);

		InMemorySorter<Tuple2<Tuple2<Short,Integer>, Integer>> sorter = createSorter( serializer, comparators, memory);

		Random randomGenerator = new Random(SEED);

		Tuple2<Tuple2<Short,Integer>, Integer> record = new Tuple2<>();

		int count = 10;
		do {

			Tuple2<Short,Integer> insideTp = new Tuple2<>();
			insideTp.setFields( (short)randomGenerator.nextInt(), randomGenerator.nextInt());
			record.setFields(insideTp, randomGenerator.nextInt());
		}
		while (sorter.write(record) && count-- >0);

		QuickSort qs = new QuickSort();
		qs.sort(sorter);

		MutableObjectIterator<Tuple2<Tuple2<Short,Integer>, Integer>> iter = sorter.getIterator();
		Tuple2<Tuple2<Short,Integer>, Integer> readTarget = iter.next();

		Tuple2<Short,Integer> last = readTarget.f0;

		while ((readTarget = iter.next()) != null) {
			Tuple2<Short,Integer> current = readTarget.f0;

			final int cmp = keyComparator.compare(last, current);
			if (cmp > 0) {
				Assert.fail("Next value is not larger or equal to previous value.");
			}

			last = current;
		}

		// release the memory occupied by the buffers
		sorter.dispose();
		this.memoryManager.release(memory);
	}

	@Test
	public void testSort5BytesIntByteKeys() throws Exception {
		// Tuple( (Int,Byte), Int )

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
			new int[]{0}, new TypeComparator[]{ keyComparator },
			insideSerializers
		);

		InMemorySorter<Tuple2<Tuple2<Integer,Byte>, Integer>> sorter = createSorter( serializer, comparators, memory);

		Random randomGenerator = new Random(SEED);

		byte[] temporyBytes = new byte[1];
		Tuple2<Tuple2<Integer,Byte>, Integer> record = new Tuple2<>();
		do {

			Tuple2<Integer,Byte> insideTp = new Tuple2<>();
			randomGenerator.nextBytes(temporyBytes);
			byte randomByte = temporyBytes[0];
			insideTp.setFields(randomGenerator.nextInt(), randomByte);
			record.setFields(insideTp, randomGenerator.nextInt());
		}
		while (sorter.write(record));

		QuickSort qs = new QuickSort();
		qs.sort(sorter);

		MutableObjectIterator<Tuple2<Tuple2<Integer,Byte>, Integer>> iter = sorter.getIterator();
		Tuple2<Tuple2<Integer,Byte>, Integer> readTarget = iter.next();

		Tuple2<Integer,Byte> last = readTarget.f0;

		while ((readTarget = iter.next()) != null) {
			Tuple2<Integer,Byte> current = readTarget.f0;

			final int cmp = keyComparator.compare(last, current);
			if (cmp > 0) {
				Assert.fail("Next value is not larger or equal to previous value.");
			}

			last = current;
		}

		// release the memory occupied by the buffers
		sorter.dispose();
		this.memoryManager.release(memory);
	}
	private InMemorySorter createSorter(TypeSerializer serializer, TypeComparator comparator, List<MemorySegment> memory ) throws IllegalAccessException, TemplateException, IOException, InstantiationException, InvocationTargetException, NoSuchMethodException, ClassNotFoundException {
		return this.sorterFactory.createSorter(
			executionConfig,
			serializer,
			comparator,
			memory
		);
	}

	private List<MemorySegment> createMemory() throws MemoryAllocationException {
		final int numSegments = MEMORY_SIZE / MEMORY_PAGE_SIZE;
		final List<MemorySegment> memory = this.memoryManager.allocatePages(new DummyInvokable(), numSegments);

		return memory;
	}

}
