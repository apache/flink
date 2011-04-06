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

package eu.stratosphere.pact.runtime.sort;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.runtime.task.util.TaskConfig.LocalStrategy;
import eu.stratosphere.pact.runtime.test.util.DummyInvokable;
import eu.stratosphere.pact.runtime.test.util.TestData;
import eu.stratosphere.pact.runtime.test.util.TestData.Generator;
import eu.stratosphere.pact.runtime.test.util.TestData.RecordReaderMock;
import eu.stratosphere.pact.runtime.test.util.TestData.Generator.KeyMode;
import eu.stratosphere.pact.runtime.test.util.TestData.Generator.ValueMode;

/**
 * @author Erik Nijkamp
 */
public class SortMergeMatchIteratorITCase {
	// total memory
	private static final int MEMORY_SIZE = 1024 * 1024 * 128;

	// the size of the left and right inputs
	private static final int INPUT_1_SIZE = 20000;

	private static final int INPUT_2_SIZE = 1000;

	// random seeds for the left and right input data generators
	private static final long SEED1 = 561349061987311L;

	private static final long SEED2 = 231434613412342L;

	// left and right input data generators
	private Generator generator1;

	private Generator generator2;

	// left and right input RecordReader mocks
	private Reader<KeyValuePair<TestData.Key, TestData.Value>> reader1;

	private Reader<KeyValuePair<TestData.Key, TestData.Value>> reader2;
	
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
			
			generator1 = new Generator(SEED1, 500, 4096, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);
			generator2 = new Generator(SEED2, 500, 2048, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);

			reader1 = new RecordReaderMock(generator1, INPUT_1_SIZE);
			reader2 = new RecordReaderMock(generator2, INPUT_2_SIZE);
			
			// collect expected data
			Map<Key, Collection<Value>> expectedValuesMap1 = collectData(generator1, INPUT_1_SIZE);
			Map<Key, Collection<Value>> expectedValuesMap2 = collectData(generator2, INPUT_2_SIZE);
			Map<Key, Collection<Match>> expectedMatchesMap = matchValues(expectedValuesMap1, expectedValuesMap2);
	
			// reset the generators
			generator1.reset();
			generator2.reset();
	
			// compare with iterator values
			SortMergeMatchIterator<TestData.Key, TestData.Value, TestData.Value> iterator = 
				new SortMergeMatchIterator<TestData.Key, TestData.Value, TestData.Value>(
						memoryManager, ioManager, reader1, reader2, TestData.Key.class,
						TestData.Value.class, TestData.Value.class,
						MEMORY_SIZE, 64, LocalStrategy.SORT_BOTH_MERGE, parentTask);
	
			iterator.open();
			while (iterator.next()) {
				TestData.Key key = new TestData.Key(iterator.getKey().getKey());
	
				// assert that matches for this key exist
				Assert.assertTrue("No matches for key " + key + " are expected", expectedMatchesMap.containsKey(key));
	
				// assert that each map is expected
				Iterator<TestData.Value> iter1 = iterator.getValues1();
				Iterator<TestData.Value> iter2 = iterator.getValues2();
	
				// clone add memorize
				List<TestData.Value> values1 = new ArrayList<TestData.Value>();
				while (iter1.hasNext()) {
					values1.add(new TestData.Value(iter1.next().getValue()));
				}
	
				List<TestData.Value> values2 = new ArrayList<TestData.Value>();
				while (iter2.hasNext()) {
					values2.add(new TestData.Value(iter2.next().getValue()));
				}
	
				// compare
				for (Value value1 : values1) {
					for (Value value2 : values2) {
						Collection<Match> expectedValues = expectedMatchesMap.get(key);
						Match match = new Match(value1, value2);
						Assert.assertTrue("Unexpected match " + match + " for key " + key, expectedValues.contains(match));
						expectedValues.remove(match);
					}
				}
	
			}
			iterator.close();
	
			// assert that each expected match was seen
			for (Entry<Key, Collection<Match>> entry : expectedMatchesMap.entrySet()) {
				Assert.assertTrue("Collection for key " + entry.getKey() + " is not empty", entry.getValue().isEmpty());
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			Assert.fail("An exception occurred during the test: " + e.getMessage());
		}
	}
	
	@Test
	public void testSortFirstMerge() {
		try {
			
			generator1 = new Generator(SEED1, 500, 4096, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);
			generator2 = new Generator(SEED2, 500, 2048, KeyMode.SORTED, ValueMode.RANDOM_LENGTH);

			reader1 = new RecordReaderMock(generator1, INPUT_1_SIZE);
			reader2 = new RecordReaderMock(generator2, INPUT_2_SIZE);

			// collect expected data
			Map<Key, Collection<Value>> expectedValuesMap1 = collectData(generator1, INPUT_1_SIZE);
			Map<Key, Collection<Value>> expectedValuesMap2 = collectData(generator2, INPUT_2_SIZE);
			Map<Key, Collection<Match>> expectedMatchesMap = matchValues(expectedValuesMap1, expectedValuesMap2);
	
			// reset the generators
			generator1.reset();
			generator2.reset();
	
			// compare with iterator values
			SortMergeMatchIterator<TestData.Key, TestData.Value, TestData.Value> iterator = 
				new SortMergeMatchIterator<TestData.Key, TestData.Value, TestData.Value>(
						memoryManager, ioManager, reader1, reader2, TestData.Key.class,
						TestData.Value.class, TestData.Value.class,
						MEMORY_SIZE, 64, LocalStrategy.SORT_FIRST_MERGE, parentTask);
	
			iterator.open();
			while (iterator.next()) {
				TestData.Key key = new TestData.Key(iterator.getKey().getKey());
	
				// assert that matches for this key exist
				Assert.assertTrue("No matches for key " + key + " are expected", expectedMatchesMap.containsKey(key));
	
				// assert that each map is expected
				Iterator<TestData.Value> iter1 = iterator.getValues1();
				Iterator<TestData.Value> iter2 = iterator.getValues2();
	
				// clone add memorize
				List<TestData.Value> values1 = new ArrayList<TestData.Value>();
				while (iter1.hasNext()) {
					values1.add(new TestData.Value(iter1.next().getValue()));
				}
	
				List<TestData.Value> values2 = new ArrayList<TestData.Value>();
				while (iter2.hasNext()) {
					values2.add(new TestData.Value(iter2.next().getValue()));
				}
	
				// compare
				for (Value value1 : values1) {
					for (Value value2 : values2) {
						Collection<Match> expectedValues = expectedMatchesMap.get(key);
						Match match = new Match(value1, value2);
						Assert.assertTrue("Unexpected match " + match + " for key " + key, expectedValues.contains(match));
						expectedValues.remove(match);
					}
				}
	
			}
			iterator.close();
	
			// assert that each expected match was seen
			for (Entry<Key, Collection<Match>> entry : expectedMatchesMap.entrySet()) {
				Assert.assertTrue("Collection for key " + entry.getKey() + " is not empty", entry.getValue().isEmpty());
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			Assert.fail("An exception occurred during the test: " + e.getMessage());
		}
	}
	
	@Test
	public void testSortSecondMerge() {
		try {
			
			generator1 = new Generator(SEED1, 500, 4096, KeyMode.SORTED, ValueMode.RANDOM_LENGTH);
			generator2 = new Generator(SEED2, 500, 2048, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);

			reader1 = new RecordReaderMock(generator1, INPUT_1_SIZE);
			reader2 = new RecordReaderMock(generator2, INPUT_2_SIZE);

			// collect expected data
			Map<Key, Collection<Value>> expectedValuesMap1 = collectData(generator1, INPUT_1_SIZE);
			Map<Key, Collection<Value>> expectedValuesMap2 = collectData(generator2, INPUT_2_SIZE);
			Map<Key, Collection<Match>> expectedMatchesMap = matchValues(expectedValuesMap1, expectedValuesMap2);
	
			// reset the generators
			generator1.reset();
			generator2.reset();
	
			// compare with iterator values
			SortMergeMatchIterator<TestData.Key, TestData.Value, TestData.Value> iterator = 
				new SortMergeMatchIterator<TestData.Key, TestData.Value, TestData.Value>(
						memoryManager, ioManager, reader1, reader2, TestData.Key.class,
						TestData.Value.class, TestData.Value.class,
						MEMORY_SIZE, 64, LocalStrategy.SORT_SECOND_MERGE, parentTask);
	
			iterator.open();
			while (iterator.next()) {
				TestData.Key key = new TestData.Key(iterator.getKey().getKey());
	
				// assert that matches for this key exist
				Assert.assertTrue("No matches for key " + key + " are expected", expectedMatchesMap.containsKey(key));
	
				// assert that each map is expected
				Iterator<TestData.Value> iter1 = iterator.getValues1();
				Iterator<TestData.Value> iter2 = iterator.getValues2();
	
				// clone add memorize
				List<TestData.Value> values1 = new ArrayList<TestData.Value>();
				while (iter1.hasNext()) {
					values1.add(new TestData.Value(iter1.next().getValue()));
				}
	
				List<TestData.Value> values2 = new ArrayList<TestData.Value>();
				while (iter2.hasNext()) {
					values2.add(new TestData.Value(iter2.next().getValue()));
				}
	
				// compare
				for (Value value1 : values1) {
					for (Value value2 : values2) {
						Collection<Match> expectedValues = expectedMatchesMap.get(key);
						Match match = new Match(value1, value2);
						Assert.assertTrue("Unexpected match " + match + " for key " + key, expectedValues.contains(match));
						expectedValues.remove(match);
					}
				}
	
			}
			iterator.close();
	
			// assert that each expected match was seen
			for (Entry<Key, Collection<Match>> entry : expectedMatchesMap.entrySet()) {
				Assert.assertTrue("Collection for key " + entry.getKey() + " is not empty", entry.getValue().isEmpty());
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			Assert.fail("An exception occurred during the test: " + e.getMessage());
		}
	}
	
	@Test
	public void testMerge() {
		try {
			
			generator1 = new Generator(SEED1, 500, 4096, KeyMode.SORTED, ValueMode.RANDOM_LENGTH);
			generator2 = new Generator(SEED2, 500, 2048, KeyMode.SORTED, ValueMode.RANDOM_LENGTH);

			reader1 = new RecordReaderMock(generator1, INPUT_1_SIZE);
			reader2 = new RecordReaderMock(generator2, INPUT_2_SIZE);

			// collect expected data
			Map<Key, Collection<Value>> expectedValuesMap1 = collectData(generator1, INPUT_1_SIZE);
			Map<Key, Collection<Value>> expectedValuesMap2 = collectData(generator2, INPUT_2_SIZE);
			Map<Key, Collection<Match>> expectedMatchesMap = matchValues(expectedValuesMap1, expectedValuesMap2);
	
			// reset the generators
			generator1.reset();
			generator2.reset();
	
			// compare with iterator values
			SortMergeMatchIterator<TestData.Key, TestData.Value, TestData.Value> iterator = 
				new SortMergeMatchIterator<TestData.Key, TestData.Value, TestData.Value>(
						memoryManager, ioManager, reader1, reader2, TestData.Key.class,
						TestData.Value.class, TestData.Value.class,
						MEMORY_SIZE, 64, LocalStrategy.MERGE, parentTask);
	
			iterator.open();
			while (iterator.next()) {
				TestData.Key key = new TestData.Key(iterator.getKey().getKey());
	
				// assert that matches for this key exist
				Assert.assertTrue("No matches for key " + key + " are expected", expectedMatchesMap.containsKey(key));
	
				// assert that each map is expected
				Iterator<TestData.Value> iter1 = iterator.getValues1();
				Iterator<TestData.Value> iter2 = iterator.getValues2();
	
				// clone add memorize
				List<TestData.Value> values1 = new ArrayList<TestData.Value>();
				while (iter1.hasNext()) {
					values1.add(new TestData.Value(iter1.next().getValue()));
				}
	
				List<TestData.Value> values2 = new ArrayList<TestData.Value>();
				while (iter2.hasNext()) {
					values2.add(new TestData.Value(iter2.next().getValue()));
				}
	
				// compare
				for (Value value1 : values1) {
					for (Value value2 : values2) {
						Collection<Match> expectedValues = expectedMatchesMap.get(key);
						Match match = new Match(value1, value2);
						Assert.assertTrue("Unexpected match " + match + " for key " + key, expectedValues.contains(match));
						expectedValues.remove(match);
					}
				}
	
			}
			iterator.close();
	
			// assert that each expected match was seen
			for (Entry<Key, Collection<Match>> entry : expectedMatchesMap.entrySet()) {
				Assert.assertTrue("Collection for key " + entry.getKey() + " is not empty", entry.getValue().isEmpty());
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			Assert.fail("An exception occurred during the test: " + e.getMessage());
		}
	}

	private Map<Key, Collection<Match>> matchValues(Map<Key, Collection<Value>> leftMap,
			Map<Key, Collection<Value>> rightMap) {
		Map<Key, Collection<Match>> map = new HashMap<Key, Collection<Match>>();

		for (Key key : leftMap.keySet()) {
			Collection<Value> leftValues = leftMap.get(key);
			Collection<Value> rightValues = rightMap.get(key);

			if (rightValues == null) {
				continue;
			}

			if (!map.containsKey(key)) {
				map.put(key, new ArrayList<Match>());
			}

			Collection<Match> matchedValues = map.get(key);

			for (Value leftValue : leftValues) {
				for (Value rightValue : rightValues) {
					matchedValues.add(new Match(leftValue, rightValue));
				}
			}
		}

		return map;
	}

	private Map<Key, Collection<Value>> collectData(Generator generator, int size) {
		Map<Key, Collection<Value>> map = new HashMap<Key, Collection<Value>>();

		for (int i = 0; i < size; i++) {
			KeyValuePair<TestData.Key, TestData.Value> pair = generator.next();

			if (!map.containsKey(pair.getKey())) {
				map.put(pair.getKey(), new ArrayList<Value>());
			}

			Collection<Value> values = map.get(pair.getKey());
			values.add(pair.getValue());
		}

		return map;
	}

	/**
	 * Private class used for storage of the expected matches in a hashmap.
	 */
	private static class Match {
		private final Value left;

		private final Value right;

		public Match(Value left, Value right) {
			this.left = left;
			this.right = right;
		}

		@Override
		public boolean equals(Object obj) {
			Match o = (Match) obj;
			return this.left.equals(o.left) && this.right.equals(o.right);
		}

		@Override
		public String toString() {
			return left + ", " + right;
		}
	}
}
