/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.pact.runtime.task.util;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.services.memorymanager.spi.DefaultMemoryManager;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.pact.common.generic.types.TypeComparator;
import eu.stratosphere.pact.common.generic.types.TypePairComparator;
import eu.stratosphere.pact.common.generic.types.TypeSerializer;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.runtime.hash.BuildFirstHashMatchIterator;
import eu.stratosphere.pact.runtime.hash.BuildSecondHashMatchIterator;
import eu.stratosphere.pact.runtime.plugable.PactRecordComparator;
import eu.stratosphere.pact.runtime.plugable.PactRecordPairComparator;
import eu.stratosphere.pact.runtime.plugable.PactRecordSerializer;
import eu.stratosphere.pact.runtime.sort.SortMergeMatchIterator;
import eu.stratosphere.pact.runtime.test.util.DiscardingOutputCollector;
import eu.stratosphere.pact.runtime.test.util.DummyInvokable;
import eu.stratosphere.pact.runtime.test.util.TestData;
import eu.stratosphere.pact.runtime.test.util.TestData.Generator;
import eu.stratosphere.pact.runtime.test.util.TestData.Generator.KeyMode;
import eu.stratosphere.pact.runtime.test.util.TestData.Generator.ValueMode;


public class HashVsSortTest {
	// total memory
	private static final int MEMORY_SIZE = 1024 * 1024 * 32;

	// the size of the left and right inputs
	private static final int INPUT_1_SIZE = 2000000;

	private static final int INPUT_2_SIZE = 100000;

	// random seeds for the left and right input data generators
	private static final long SEED1 = 561349061987311L;

	private static final long SEED2 = 231434613412342L;

	
	// dummy abstract task
	private final AbstractTask parentTask = new DummyInvokable();

	// memory and io manager
	private IOManager ioManager;
	private MemoryManager memoryManager;
	
	private TypeSerializer<PactRecord> serializer1;
	private TypeSerializer<PactRecord> serializer2;
	private TypeComparator<PactRecord> comparator1;
	private TypeComparator<PactRecord> comparator2;
	private TypePairComparator<PactRecord, PactRecord> pairComparator11;


	@SuppressWarnings("unchecked")
	@Before
	public void beforeTest()
	{
		this.serializer1 = PactRecordSerializer.get();
		this.serializer2 = PactRecordSerializer.get();
		this.comparator1 = new PactRecordComparator(new int[] {0}, new Class[] {TestData.Key.class});
		this.comparator2 = new PactRecordComparator(new int[] {0}, new Class[] {TestData.Key.class});
		this.pairComparator11 = new PactRecordPairComparator(new int[] {0}, new int[] {0}, new Class[] {TestData.Key.class});
		
		this.memoryManager = new DefaultMemoryManager(MEMORY_SIZE);
		this.ioManager = new IOManager();
	}

	@After
	public void afterTest()
	{
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
			
			final MatchStub matcher = new NoOpMatcher();
			
			final Collector<PactRecord> collector = new DiscardingOutputCollector();
	
			// reset the generators
			generator1.reset();
			generator2.reset();
			input1.reset();
			input2.reset();
	
			// compare with iterator values
			SortMergeMatchIterator<PactRecord, PactRecord, PactRecord> iterator = 
				new SortMergeMatchIterator<PactRecord, PactRecord, PactRecord>(input1, input2, 
						this.serializer1, this.comparator1, this.serializer2, this.comparator2, this.pairComparator11,
						this.memoryManager, this.ioManager, 
						MEMORY_SIZE, 64, 0.7f, LocalStrategy.SORT_BOTH_MERGE, this.parentTask);
	
			long start = System.nanoTime();
			
			iterator.open();
			
			while (iterator.callWithNextKey(matcher, collector));
			
			iterator.close();
			
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
			
			final MatchStub matcher =new NoOpMatcher();
			
			final Collector<PactRecord> collector = new DiscardingOutputCollector();
	
			// reset the generators
			generator1.reset();
			generator2.reset();
			input1.reset();
			input2.reset();
	
			// compare with iterator values
			final BuildFirstHashMatchIterator<PactRecord, PactRecord, PactRecord> iterator = 
					new BuildFirstHashMatchIterator<PactRecord, PactRecord, PactRecord>(
						input1, input2, this.serializer1, this.comparator1, 
							this.serializer2, this.comparator2, this.pairComparator11,
							this.memoryManager, this.ioManager, this.parentTask, MEMORY_SIZE);
	
			long start = System.nanoTime();
			
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
			
			final MatchStub matcher = new NoOpMatcher();
			
			final Collector<PactRecord> collector = new DiscardingOutputCollector();
	
			// reset the generators
			generator1.reset();
			generator2.reset();
			input1.reset();
			input2.reset();
	
			// compare with iterator values
			BuildSecondHashMatchIterator<PactRecord, PactRecord, PactRecord> iterator = 
					new BuildSecondHashMatchIterator<PactRecord, PactRecord, PactRecord>(
						input1, input2, this.serializer1, this.comparator1, 
						this.serializer2, this.comparator2, this.pairComparator11,
						this.memoryManager, this.ioManager, this.parentTask, MEMORY_SIZE);
	
			long start = System.nanoTime();
			
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
	
	
	private static final class NoOpMatcher extends MatchStub
	{
		@Override
		public void match(PactRecord rec1, PactRecord rec2, Collector<PactRecord> out) {}
		
	}
}
