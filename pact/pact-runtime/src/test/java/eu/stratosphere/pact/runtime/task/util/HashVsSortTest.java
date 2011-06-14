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
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import eu.stratosphere.nephele.io.Reader;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.services.memorymanager.spi.DefaultMemoryManager;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MatchStub;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.runtime.hash.BuildFirstHashMatchIterator;
import eu.stratosphere.pact.runtime.hash.BuildSecondHashMatchIterator;
import eu.stratosphere.pact.runtime.sort.SortMergeMatchIterator;
import eu.stratosphere.pact.runtime.task.util.TaskConfig.LocalStrategy;
import eu.stratosphere.pact.runtime.test.util.DiscardingOutputCollector;
import eu.stratosphere.pact.runtime.test.util.DummyInvokable;
import eu.stratosphere.pact.runtime.test.util.TestData;
import eu.stratosphere.pact.runtime.test.util.TestData.Generator;
import eu.stratosphere.pact.runtime.test.util.TestData.RecordReaderMock;
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
	private static IOManager ioManager;

	private MemoryManager memoryManager;


	@BeforeClass
	public static void beforeClass() {
		ioManager = new IOManager();
	}

	@AfterClass
	public static void afterClass() {
		if (ioManager != null) {
			ioManager.shutdown();
			if (!ioManager.isProperlyShutDown()) {
				Assert.fail("I/O manager failed to properly shut down.");
			}
			ioManager = null;
		}
		
	}

	@Before
	public void beforeTest() {
		memoryManager = new DefaultMemoryManager(MEMORY_SIZE);
	}

	@After
	public void afterTest() {
		if (memoryManager != null) {
			Assert.assertTrue("Memory Leak: Not all memory has been returned to the memory manager.",
				memoryManager.verifyEmpty());
			memoryManager.shutdown();
			memoryManager = null;
		}
	}
	@Test
	public void testSortBothMerge() {
		try {
			
			Generator generator1 = new Generator(SEED1, INPUT_1_SIZE / 10, 100, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);
			Generator generator2 = new Generator(SEED2, INPUT_2_SIZE, 100, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);

			Reader<KeyValuePair<TestData.Key, TestData.Value>> reader1 = new RecordReaderMock(generator1, INPUT_1_SIZE);
			Reader<KeyValuePair<TestData.Key, TestData.Value>> reader2 = new RecordReaderMock(generator2, INPUT_2_SIZE);
			
			final MatchStub<TestData.Key, TestData.Value, TestData.Value, TestData.Key, TestData.Value> matcher =
				new NoOpMatcher();
			
			final Collector<TestData.Key, TestData.Value> collector = new DiscardingOutputCollector<TestData.Key, TestData.Value>();
	
			// reset the generators
			generator1.reset();
			generator2.reset();
	
			// compare with iterator values
			SortMergeMatchIterator<TestData.Key, TestData.Value, TestData.Value> iterator = 
				new SortMergeMatchIterator<TestData.Key, TestData.Value, TestData.Value>(
						memoryManager, ioManager, reader1, reader2, TestData.Key.class,
						TestData.Value.class, TestData.Value.class,
						MEMORY_SIZE, 64, 0.7f, LocalStrategy.SORT_BOTH_MERGE, parentTask);
	
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
			
			final MatchStub<TestData.Key, TestData.Value, TestData.Value, TestData.Key, TestData.Value> matcher =
				new NoOpMatcher();
			
			final Collector<TestData.Key, TestData.Value> collector = new DiscardingOutputCollector<TestData.Key, TestData.Value>();
	
			// reset the generators
			generator1.reset();
			generator2.reset();
			input1.reset();
			input2.reset();
	
			// compare with iterator values
			BuildFirstHashMatchIterator<TestData.Key, TestData.Value, TestData.Value> iterator = 
				new BuildFirstHashMatchIterator<TestData.Key, TestData.Value, TestData.Value>(input1, input2,
						TestData.Key.class, TestData.Value.class, TestData.Value.class, this.memoryManager, ioManager,
						this.parentTask, MEMORY_SIZE);
	
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
			
			final MatchStub<TestData.Key, TestData.Value, TestData.Value, TestData.Key, TestData.Value> matcher =
				new NoOpMatcher();
			
			final Collector<TestData.Key, TestData.Value> collector = new DiscardingOutputCollector<TestData.Key, TestData.Value>();
	
			// reset the generators
			generator1.reset();
			generator2.reset();
			input1.reset();
			input2.reset();
	
			// compare with iterator values
			BuildSecondHashMatchIterator<TestData.Key, TestData.Value, TestData.Value> iterator = 
				new BuildSecondHashMatchIterator<TestData.Key, TestData.Value, TestData.Value>(input1, input2,
						TestData.Key.class, TestData.Value.class, TestData.Value.class, this.memoryManager, ioManager,
						this.parentTask, MEMORY_SIZE);
	
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
	
	
	private static final class NoOpMatcher extends MatchStub<TestData.Key, TestData.Value, TestData.Value, TestData.Key, TestData.Value>
	{
		@Override
		public void match(TestData.Key key, TestData.Value value1, TestData.Value value2, Collector<TestData.Key, TestData.Value> out) {}
		
	}
}
