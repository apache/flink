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

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypePairComparator;
import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.api.common.typeutils.record.RecordComparator;
import org.apache.flink.api.common.typeutils.record.RecordPairComparator;
import org.apache.flink.api.common.typeutils.record.RecordSerializerFactory;
import org.apache.flink.api.java.record.functions.JoinFunction;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memorymanager.DefaultMemoryManager;
import org.apache.flink.runtime.memorymanager.MemoryManager;
import org.apache.flink.runtime.operators.hash.ReusingBuildFirstHashMatchIterator;
import org.apache.flink.runtime.operators.hash.ReusingBuildSecondHashMatchIterator;
import org.apache.flink.runtime.operators.sort.ReusingMergeInnerJoinIterator;
import org.apache.flink.runtime.operators.sort.UnilateralSortMerger;
import org.apache.flink.runtime.operators.testutils.DiscardingOutputCollector;
import org.apache.flink.runtime.operators.testutils.DummyInvokable;
import org.apache.flink.runtime.operators.testutils.TestData;
import org.apache.flink.runtime.operators.testutils.TestData.Generator;
import org.apache.flink.runtime.operators.testutils.TestData.Generator.KeyMode;
import org.apache.flink.runtime.operators.testutils.TestData.Generator.ValueMode;
import org.apache.flink.types.Record;
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
	
	private TypeSerializerFactory<Record> serializer1;
	private TypeSerializerFactory<Record> serializer2;
	private TypeComparator<Record> comparator1;
	private TypeComparator<Record> comparator2;
	private TypePairComparator<Record, Record> pairComparator11;


	@SuppressWarnings("unchecked")
	@Before
	public void beforeTest() {
		this.serializer1 = RecordSerializerFactory.get();
		this.serializer2 = RecordSerializerFactory.get();
		this.comparator1 = new RecordComparator(new int[] {0}, new Class[] {TestData.Key.class});
		this.comparator2 = new RecordComparator(new int[] {0}, new Class[] {TestData.Key.class});
		this.pairComparator11 = new RecordPairComparator(new int[] {0}, new int[] {0}, new Class[] {TestData.Key.class});
		
		this.memoryManager = new DefaultMemoryManager(MEMORY_SIZE, PAGE_SIZE);
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
			
			Generator generator1 = new Generator(SEED1, INPUT_1_SIZE / 10, 100, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);
			Generator generator2 = new Generator(SEED2, INPUT_2_SIZE, 100, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);

			final TestData.GeneratorIterator input1 = new TestData.GeneratorIterator(generator1, INPUT_1_SIZE);
			final TestData.GeneratorIterator input2 = new TestData.GeneratorIterator(generator2, INPUT_2_SIZE);
			
			final JoinFunction matcher = new NoOpMatcher();
			final Collector<Record> collector = new DiscardingOutputCollector<Record>();
			
			long start = System.nanoTime();
			
			final UnilateralSortMerger<Record> sorter1 = new UnilateralSortMerger<Record>(
					this.memoryManager, this.ioManager, input1, this.parentTask, this.serializer1, 
					this.comparator1.duplicate(), MEMORY_FOR_SORTER, 128, 0.8f);
			
			final UnilateralSortMerger<Record> sorter2 = new UnilateralSortMerger<Record>(
					this.memoryManager, this.ioManager, input2, this.parentTask, this.serializer2, 
					this.comparator2.duplicate(), MEMORY_FOR_SORTER, 128, 0.8f);
			
			final MutableObjectIterator<Record> sortedInput1 = sorter1.getIterator();
			final MutableObjectIterator<Record> sortedInput2 = sorter2.getIterator();
			
			// compare with iterator values
			ReusingMergeInnerJoinIterator<Record, Record, Record> iterator =
				new ReusingMergeInnerJoinIterator<Record, Record, Record>(sortedInput1, sortedInput2,
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
			Generator generator1 = new Generator(SEED1, INPUT_1_SIZE / 10, 100, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);
			Generator generator2 = new Generator(SEED2, INPUT_2_SIZE, 100, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);
			
			final TestData.GeneratorIterator input1 = new TestData.GeneratorIterator(generator1, INPUT_1_SIZE);
			final TestData.GeneratorIterator input2 = new TestData.GeneratorIterator(generator2, INPUT_2_SIZE);
			
			final JoinFunction matcher = new NoOpMatcher();
			
			final Collector<Record> collector = new DiscardingOutputCollector<Record>();
			
			long start = System.nanoTime();
			
			// compare with iterator values
			final ReusingBuildFirstHashMatchIterator<Record, Record, Record> iterator =
					new ReusingBuildFirstHashMatchIterator<Record, Record, Record>(
						input1, input2, this.serializer1.getSerializer(), this.comparator1, 
							this.serializer2.getSerializer(), this.comparator2, this.pairComparator11,
							this.memoryManager, this.ioManager, this.parentTask, MEMORY_SIZE);
			
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
			Generator generator1 = new Generator(SEED1, INPUT_1_SIZE / 10, 100, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);
			Generator generator2 = new Generator(SEED2, INPUT_2_SIZE, 100, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);
			
			final TestData.GeneratorIterator input1 = new TestData.GeneratorIterator(generator1, INPUT_1_SIZE);
			final TestData.GeneratorIterator input2 = new TestData.GeneratorIterator(generator2, INPUT_2_SIZE);
			
			final JoinFunction matcher = new NoOpMatcher();
			
			final Collector<Record> collector = new DiscardingOutputCollector<Record>();
			
			long start = System.nanoTime();
			
			// compare with iterator values
			ReusingBuildSecondHashMatchIterator<Record, Record, Record> iterator =
					new ReusingBuildSecondHashMatchIterator<Record, Record, Record>(
						input1, input2, this.serializer1.getSerializer(), this.comparator1, 
						this.serializer2.getSerializer(), this.comparator2, this.pairComparator11,
						this.memoryManager, this.ioManager, this.parentTask, MEMORY_SIZE);
			
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
	
	
	private static final class NoOpMatcher extends JoinFunction {
		private static final long serialVersionUID = 1L;
		
		@Override
		public void join(Record rec1, Record rec2, Collector<Record> out) throws Exception {
		}
	}
}
