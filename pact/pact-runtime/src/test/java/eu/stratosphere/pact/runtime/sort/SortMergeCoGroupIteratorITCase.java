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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
 */
public class SortMergeCoGroupIteratorITCase {
	
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
			Map<Key, List<Collection<Value>>> expectedCoGroupsMap = coGroupValues(expectedValuesMap1, expectedValuesMap2);
	
			// reset the generators
			generator1.reset();
			generator2.reset();
	
			// compare with iterator values
			SortMergeCoGroupIterator<TestData.Key, TestData.Value, TestData.Value> iterator = 
				new SortMergeCoGroupIterator<TestData.Key, TestData.Value, TestData.Value>(
						memoryManager, ioManager, reader1, reader2, TestData.Key.class,
						TestData.Value.class, TestData.Value.class,
						MEMORY_SIZE, 64, LocalStrategy.SORT_BOTH_MERGE, parentTask);
	
			iterator.open();
			while (iterator.next()) {
				TestData.Key key = new TestData.Key(iterator.getKey().getKey());
	
				// assert that matches for this key exist
				Assert.assertTrue("No matches for key " + key + " are expected", expectedCoGroupsMap.containsKey(key));
	
				// assert that each map is expected
				Iterator<TestData.Value> iter1 = iterator.getValues1();
				Iterator<TestData.Value> iter2 = iterator.getValues2();
	
				Collection<Value> expValues1 = expectedCoGroupsMap.get(key).get(0);
				Collection<Value> expValues2 = expectedCoGroupsMap.get(key).get(1);
				
				while(iter1.hasNext()) {
					Assert.assertTrue("Value not in expected set of first input", expValues1.remove(iter1.next()));
				}
				Assert.assertTrue("Expected set of first input not empty", expValues1.isEmpty());
				
				while(iter2.hasNext()) {
					Assert.assertTrue("Value not in expected set of second input", expValues2.remove(iter2.next()));
				}
				Assert.assertTrue("Expected set of second input not empty", expValues2.isEmpty());
	
				expectedCoGroupsMap.remove(key);
			}
			iterator.close();
	
			Assert.assertTrue("Expected key set not empty", expectedCoGroupsMap.isEmpty());

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
			Map<Key, List<Collection<Value>>> expectedCoGroupsMap = coGroupValues(expectedValuesMap1, expectedValuesMap2);
	
			// reset the generators
			generator1.reset();
			generator2.reset();
	
			// compare with iterator values
			SortMergeCoGroupIterator<TestData.Key, TestData.Value, TestData.Value> iterator = 
				new SortMergeCoGroupIterator<TestData.Key, TestData.Value, TestData.Value>(
						memoryManager, ioManager, reader1, reader2, TestData.Key.class,
						TestData.Value.class, TestData.Value.class,
						MEMORY_SIZE, 64, LocalStrategy.SORT_FIRST_MERGE, parentTask);
	
			iterator.open();
			while (iterator.next()) {
				TestData.Key key = new TestData.Key(iterator.getKey().getKey());
	
				// assert that matches for this key exist
				Assert.assertTrue("No matches for key " + key + " are expected", expectedCoGroupsMap.containsKey(key));
	
				// assert that each map is expected
				Iterator<TestData.Value> iter1 = iterator.getValues1();
				Iterator<TestData.Value> iter2 = iterator.getValues2();
	
				Collection<Value> expValues1 = expectedCoGroupsMap.get(key).get(0);
				Collection<Value> expValues2 = expectedCoGroupsMap.get(key).get(1);
				
				while(iter1.hasNext()) {
					Assert.assertTrue("Value not in expected set of first input", expValues1.remove(iter1.next()));
				}
				Assert.assertTrue("Expected set of first input not empty", expValues1.isEmpty());
				
				while(iter2.hasNext()) {
					Assert.assertTrue("Value not in expected set of second input", expValues2.remove(iter2.next()));
				}
				Assert.assertTrue("Expected set of second input not empty", expValues2.isEmpty());
	
				expectedCoGroupsMap.remove(key);
			}
			iterator.close();
	
			Assert.assertTrue("Expected key set not empty", expectedCoGroupsMap.isEmpty());
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
			Map<Key, List<Collection<Value>>> expectedCoGroupsMap = coGroupValues(expectedValuesMap1, expectedValuesMap2);
	
			// reset the generators
			generator1.reset();
			generator2.reset();
	
			// compare with iterator values
			SortMergeCoGroupIterator<TestData.Key, TestData.Value, TestData.Value> iterator = 
				new SortMergeCoGroupIterator<TestData.Key, TestData.Value, TestData.Value>(
						memoryManager, ioManager, reader1, reader2, TestData.Key.class,
						TestData.Value.class, TestData.Value.class,
						MEMORY_SIZE, 64, LocalStrategy.SORT_SECOND_MERGE, parentTask);
	
			iterator.open();
			while (iterator.next()) {
				TestData.Key key = new TestData.Key(iterator.getKey().getKey());
	
				// assert that matches for this key exist
				Assert.assertTrue("No matches for key " + key + " are expected", expectedCoGroupsMap.containsKey(key));
	
				// assert that each map is expected
				Iterator<TestData.Value> iter1 = iterator.getValues1();
				Iterator<TestData.Value> iter2 = iterator.getValues2();
	
				Collection<Value> expValues1 = expectedCoGroupsMap.get(key).get(0);
				Collection<Value> expValues2 = expectedCoGroupsMap.get(key).get(1);
				
				while(iter1.hasNext()) {
					Assert.assertTrue("Value not in expected set of first input", expValues1.remove(iter1.next()));
				}
				Assert.assertTrue("Expected set of first input not empty", expValues1.isEmpty());
				
				while(iter2.hasNext()) {
					Assert.assertTrue("Value not in expected set of second input", expValues2.remove(iter2.next()));
				}
				Assert.assertTrue("Expected set of second input not empty", expValues2.isEmpty());
	
				expectedCoGroupsMap.remove(key);
			}
			iterator.close();
	
			Assert.assertTrue("Expected key set not empty", expectedCoGroupsMap.isEmpty());
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
			Map<Key, List<Collection<Value>>> expectedCoGroupsMap = coGroupValues(expectedValuesMap1, expectedValuesMap2);
	
			// reset the generators
			generator1.reset();
			generator2.reset();
	
			// compare with iterator values
			SortMergeCoGroupIterator<TestData.Key, TestData.Value, TestData.Value> iterator = 
				new SortMergeCoGroupIterator<TestData.Key, TestData.Value, TestData.Value>(
						memoryManager, ioManager, reader1, reader2, TestData.Key.class,
						TestData.Value.class, TestData.Value.class,
						MEMORY_SIZE, 64, LocalStrategy.MERGE, parentTask);
	
			iterator.open();
			while (iterator.next()) {
				TestData.Key key = new TestData.Key(iterator.getKey().getKey());
	
				// assert that matches for this key exist
				Assert.assertTrue("No matches for key " + key + " are expected", expectedCoGroupsMap.containsKey(key));
	
				// assert that each map is expected
				Iterator<TestData.Value> iter1 = iterator.getValues1();
				Iterator<TestData.Value> iter2 = iterator.getValues2();
	
				Collection<Value> expValues1 = expectedCoGroupsMap.get(key).get(0);
				Collection<Value> expValues2 = expectedCoGroupsMap.get(key).get(1);
				
				while(iter1.hasNext()) {
					Assert.assertTrue("Value not in expected set of first input", expValues1.remove(iter1.next()));
				}
				Assert.assertTrue("Expected set of first input not empty", expValues1.isEmpty());
				
				while(iter2.hasNext()) {
					Assert.assertTrue("Value not in expected set of second input", expValues2.remove(iter2.next()));
				}
				Assert.assertTrue("Expected set of second input not empty", expValues2.isEmpty());
	
				expectedCoGroupsMap.remove(key);
			}
			iterator.close();
	
			Assert.assertTrue("Expected key set not empty", expectedCoGroupsMap.isEmpty());
		}
		catch (Exception e) {
			e.printStackTrace();
			Assert.fail("An exception occurred during the test: " + e.getMessage());
		}
	}

	private Map<Key, List<Collection<Value>>> coGroupValues(Map<Key, Collection<Value>> leftMap,
			Map<Key, Collection<Value>> rightMap) {
		Map<Key, List<Collection<Value>>> map = new HashMap<Key, List<Collection<Value>>>();

		Set<Key> keySet = new HashSet<Key>(leftMap.keySet());
		keySet.addAll(rightMap.keySet());
		
		for (Key key : keySet) {
			Collection<Value> leftValues = leftMap.get(key);
			Collection<Value> rightValues = rightMap.get(key);

			map.put(key,new ArrayList<Collection<Value>>(2));
			
			if (leftValues == null) {
				map.get(key).add(new ArrayList<Value>(0));
			} else {
				map.get(key).add(leftValues);
			}
			
			if (rightValues == null) {
				map.get(key).add(new ArrayList<Value>(0));
			} else {
				map.get(key).add(rightValues);
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
	
}
