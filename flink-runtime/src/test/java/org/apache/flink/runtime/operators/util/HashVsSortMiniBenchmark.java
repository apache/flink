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

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.typeutils.GenericPairComparator;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypePairComparator;
import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.MemoryType;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.hash.ReusingBuildFirstHashJoinIterator;
import org.apache.flink.runtime.operators.hash.ReusingBuildSecondHashJoinIterator;
import org.apache.flink.runtime.operators.sort.ReusingMergeInnerJoinIterator;
import org.apache.flink.runtime.operators.sort.UnilateralSortMerger;
import org.apache.flink.runtime.operators.testutils.DiscardingOutputCollector;
import org.apache.flink.runtime.operators.testutils.DummyInvokable;
import org.apache.flink.runtime.operators.testutils.TestData;
import org.apache.flink.runtime.operators.testutils.TestData.TupleGenerator.KeyMode;
import org.apache.flink.runtime.operators.testutils.TestData.TupleGenerator.ValueMode;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("deprecation")
public class HashVsSortMiniBenchmark {
	
	// total memory
	private static final int MEMORY_SIZE = 1024 * 1024 * 32;
	
	private static final int PAGE_SIZE = 32 * 1024;
	
	private static final int MEMORY_PAGES_FOR_MERGE = 10;
	
	private static final int MEMORY_FOR_SORTER = (MEMORY_SIZE - PAGE_SIZE * MEMORY_PAGES_FOR_MERGE) / 2;

	// the size of the left and right inputs
	private static final int INPUT_1_SIZE = 2000000;

	private static final int INPUT_2_SIZE = 100000;

	// random seeds for the left and right input data generators
	private static final long SEED1 = 561349061987311L;

	private static final long SEED2 = 231434613412342L;

	
	// dummy abstract task
	private final AbstractInvokable parentTask = new DummyInvokable();

	// memory and io manager
	private IOManager ioManager;
	private MemoryManager memoryManager;
	
	private TypeSerializerFactory<Tuple2<Integer, String>> serializer1;
	private TypeSerializerFactory<Tuple2<Integer, String>> serializer2;
	private TypeComparator<Tuple2<Integer, String>> comparator1;
	private TypeComparator<Tuple2<Integer, String>> comparator2;
	private TypePairComparator<Tuple2<Integer, String>, Tuple2<Integer, String>> pairComparator11;


	@SuppressWarnings("unchecked")
	@Before
	public void beforeTest() {
		this.serializer1 = TestData.getIntStringTupleSerializerFactory();
		this.serializer2 = TestData.getIntStringTupleSerializerFactory();
		this.comparator1 = TestData.getIntStringTupleComparator();
		this.comparator2 = TestData.getIntStringTupleComparator();
		this.pairComparator11 = new GenericPairComparator(this.comparator1, this.comparator2);
		
		this.memoryManager = new MemoryManager(MEMORY_SIZE, 1, PAGE_SIZE, MemoryType.HEAP, true);
		this.ioManager = new IOManagerAsync();
	}

	@After
	public void afterTest() {
		if (this.memoryManager != null) {
			Assert.assertTrue("Memory Leak: Not all memory has been returned to the memory manager.",
				this.memoryManager.verifyEmpty());
			this.memoryManager.shutdown();
			this.memoryManager = null;
		}
		
		if (this.ioManager != null) {
			this.ioManager.shutdown();
			if (!this.ioManager.isProperlyShutDown()) {
				Assert.fail("I/O manager failed to properly shut down.");
			}
			this.ioManager = null;
		}
	}
	
	@Test
	public void testSortBothMerge() {
		try {
			
			TestData.TupleGenerator generator1 = new TestData.TupleGenerator(SEED1, INPUT_1_SIZE / 10, 100, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);
			TestData.TupleGenerator generator2 = new TestData.TupleGenerator(SEED2, INPUT_2_SIZE, 100, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);

			final TestData.TupleGeneratorIterator input1 = new TestData.TupleGeneratorIterator(generator1, INPUT_1_SIZE);
			final TestData.TupleGeneratorIterator input2 = new TestData.TupleGeneratorIterator(generator2, INPUT_2_SIZE);
			
			final FlatJoinFunction matcher = new NoOpMatcher();
			final Collector<Tuple2<Integer, String>> collector = new DiscardingOutputCollector<>();
			
			long start = System.nanoTime();
			
			final UnilateralSortMerger<Tuple2<Integer, String>> sorter1 = new UnilateralSortMerger<>(
					this.memoryManager, this.ioManager, input1, this.parentTask, this.serializer1, 
					this.comparator1.duplicate(), (double)MEMORY_FOR_SORTER/MEMORY_SIZE, 128, 0.8f,
					true /*use large record handler*/, true);
			
			final UnilateralSortMerger<Tuple2<Integer, String>> sorter2 = new UnilateralSortMerger<>(
					this.memoryManager, this.ioManager, input2, this.parentTask, this.serializer2, 
					this.comparator2.duplicate(), (double)MEMORY_FOR_SORTER/MEMORY_SIZE, 128, 0.8f,
					true /*use large record handler*/, true);
			
			final MutableObjectIterator<Tuple2<Integer, String>> sortedInput1 = sorter1.getIterator();
			final MutableObjectIterator<Tuple2<Integer, String>> sortedInput2 = sorter2.getIterator();
			
			// compare with iterator values
			ReusingMergeInnerJoinIterator<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple2<Integer, String>> iterator =
				new ReusingMergeInnerJoinIterator<>(sortedInput1, sortedInput2,
						this.serializer1.getSerializer(), this.comparator1, this.serializer2.getSerializer(), this.comparator2, this.pairComparator11,
						this.memoryManager, this.ioManager, MEMORY_PAGES_FOR_MERGE, this.parentTask);
			
			iterator.open();
			
			while (iterator.callWithNextKey(matcher, collector));
			
			iterator.close();
			sorter1.close();
			sorter2.close();
			
			long elapsed = System.nanoTime() - start;
			double msecs = elapsed / (1000 * 1000);
			
			System.out.println("Sort-Merge Took " + msecs + " msecs.");
		}
		catch (Exception e) {
			e.printStackTrace();
			Assert.fail("An exception occurred during the test: " + e.getMessage());
		}
	}
	
	@Test
	public void testBuildFirst() {
		try {
			TestData.TupleGenerator generator1 = new TestData.TupleGenerator(SEED1, INPUT_1_SIZE / 10, 100, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);
			TestData.TupleGenerator generator2 = new TestData.TupleGenerator(SEED2, INPUT_2_SIZE, 100, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);
			
			final TestData.TupleGeneratorIterator input1 = new TestData.TupleGeneratorIterator(generator1, INPUT_1_SIZE);
			final TestData.TupleGeneratorIterator input2 = new TestData.TupleGeneratorIterator(generator2, INPUT_2_SIZE);
			
			final FlatJoinFunction matcher = new NoOpMatcher();
			
			final Collector<Tuple2<Integer, String>> collector = new DiscardingOutputCollector<>();
			
			long start = System.nanoTime();
			
			// compare with iterator values
			final ReusingBuildFirstHashJoinIterator<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple2<Integer, String>> iterator =
					new ReusingBuildFirstHashJoinIterator<>(
						input1, input2, this.serializer1.getSerializer(), this.comparator1, 
							this.serializer2.getSerializer(), this.comparator2, this.pairComparator11,
							this.memoryManager, this.ioManager, this.parentTask, 1, false, false, true);
			
			iterator.open();
			
			while (iterator.callWithNextKey(matcher, collector));
			
			iterator.close();
			
			long elapsed = System.nanoTime() - start;
			double msecs = elapsed / (1000 * 1000);
			
			System.out.println("Hash Build First Took " + msecs + " msecs.");
		}
		catch (Exception e) {
			e.printStackTrace();
			Assert.fail("An exception occurred during the test: " + e.getMessage());
		}
	}
	
	@Test
	public void testBuildSecond() {
		try {
			TestData.TupleGenerator generator1 = new TestData.TupleGenerator(SEED1, INPUT_1_SIZE / 10, 100, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);
			TestData.TupleGenerator generator2 = new TestData.TupleGenerator(SEED2, INPUT_2_SIZE, 100, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);
			
			final TestData.TupleGeneratorIterator input1 = new TestData.TupleGeneratorIterator(generator1, INPUT_1_SIZE);
			final TestData.TupleGeneratorIterator input2 = new TestData.TupleGeneratorIterator(generator2, INPUT_2_SIZE);
			
			final FlatJoinFunction matcher = new NoOpMatcher();
			
			final Collector<Tuple2<Integer, String>> collector = new DiscardingOutputCollector<>();
			
			long start = System.nanoTime();
			
			// compare with iterator values
			ReusingBuildSecondHashJoinIterator<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple2<Integer, String>> iterator =
					new ReusingBuildSecondHashJoinIterator<>(
						input1, input2, this.serializer1.getSerializer(), this.comparator1, 
						this.serializer2.getSerializer(), this.comparator2, this.pairComparator11,
						this.memoryManager, this.ioManager, this.parentTask, 1, false, false, true);
			
			iterator.open();
			
			while (iterator.callWithNextKey(matcher, collector));
			
			iterator.close();
			
			long elapsed = System.nanoTime() - start;
			double msecs = elapsed / (1000 * 1000);
			
			System.out.println("Hash Build Second took " + msecs + " msecs.");
		}
		catch (Exception e) {
			e.printStackTrace();
			Assert.fail("An exception occurred during the test: " + e.getMessage());
		}
	}
	
	@Test
	public void testSortOnly() throws Exception {
		TestData.TupleGenerator generator1 = new TestData.TupleGenerator(SEED1, INPUT_1_SIZE / 10, 100, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);

		final TestData.TupleGeneratorIterator input1 = new TestData.TupleGeneratorIterator(generator1, INPUT_1_SIZE);

		long start = System.nanoTime();

		final UnilateralSortMerger<Tuple2<Integer, String>> sorter = new UnilateralSortMerger<>(
				this.memoryManager, this.ioManager, input1, this.parentTask, this.serializer1,
				this.comparator1.duplicate(), (double)MEMORY_FOR_SORTER/MEMORY_SIZE, 128, 0.8f,
				true /*use large record handler*/, true);

		MutableObjectIterator<Tuple2<Integer, String>> iter = sorter.getIterator();

		long stop1 = System.nanoTime();

		Tuple2<Integer, String> t = new Tuple2<>();
		while (iter.next() != null);

		long stop2 = System.nanoTime();

		long sortMsecs = (stop1 - start) / 1_000_000;
		long allMsecs = (stop2 - start) / 1_000_000;

		System.out.printf("Sort only took %d / %d msecs\n", sortMsecs, allMsecs);

		sorter.close();
	}
	
	private static final class NoOpMatcher implements FlatJoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple2<Integer, String>> {
		private static final long serialVersionUID = 1L;
		
		@Override
		public void join(Tuple2<Integer, String> rec1, Tuple2<Integer, String> rec2, Collector<Tuple2<Integer, String>> out) throws Exception {
		}
	}
}
