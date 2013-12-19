/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.api.typeutils.TypeComparator;
import eu.stratosphere.api.typeutils.TypePairComparator;
import eu.stratosphere.api.typeutils.TypeSerializer;
import eu.stratosphere.pact.runtime.plugable.pactrecord.RecordComparator;
import eu.stratosphere.pact.runtime.plugable.pactrecord.RecordPairComparator;
import eu.stratosphere.pact.runtime.plugable.pactrecord.RecordSerializer;
import eu.stratosphere.pact.runtime.test.util.TestData;
import eu.stratosphere.pact.runtime.test.util.TestData.Generator;
import eu.stratosphere.pact.runtime.test.util.TestData.Generator.KeyMode;
import eu.stratosphere.pact.runtime.test.util.TestData.Generator.ValueMode;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.MutableObjectIterator;

/**
 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
 */
public class SortMergeCoGroupIteratorITCase
{
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
	private MutableObjectIterator<Record> reader1;

	private MutableObjectIterator<Record> reader2;
	
	
	private TypeSerializer<Record> serializer1;
	private TypeSerializer<Record> serializer2;
	private TypeComparator<Record> comparator1;
	private TypeComparator<Record> comparator2;
	private TypePairComparator<Record, Record> pairComparator;


	@SuppressWarnings("unchecked")
	@Before
	public void beforeTest() {
		this.serializer1 = RecordSerializer.get();
		this.serializer2 = RecordSerializer.get();
		this.comparator1 = new RecordComparator(new int[] {0}, new Class[]{TestData.Key.class});
		this.comparator2 = new RecordComparator(new int[] {0}, new Class[]{TestData.Key.class});
		this.pairComparator = new RecordPairComparator(new int[] {0}, new int[] {0}, new Class[]{TestData.Key.class});
	}
	
	@Test
	public void testMerge() {
		try {
			
			generator1 = new Generator(SEED1, 500, 4096, KeyMode.SORTED, ValueMode.RANDOM_LENGTH);
			generator2 = new Generator(SEED2, 500, 2048, KeyMode.SORTED, ValueMode.RANDOM_LENGTH);

			reader1 = new TestData.GeneratorIterator(generator1, INPUT_1_SIZE);
			reader2 = new TestData.GeneratorIterator(generator2, INPUT_2_SIZE);

			// collect expected data
			Map<TestData.Key, Collection<TestData.Value>> expectedValuesMap1 = collectData(generator1, INPUT_1_SIZE);
			Map<TestData.Key, Collection<TestData.Value>> expectedValuesMap2 = collectData(generator2, INPUT_2_SIZE);
			Map<TestData.Key, List<Collection<TestData.Value>>> expectedCoGroupsMap = coGroupValues(expectedValuesMap1, expectedValuesMap2);
	
			// reset the generators
			generator1.reset();
			generator2.reset();
	
			// compare with iterator values
			SortMergeCoGroupIterator<Record, Record> iterator =	new SortMergeCoGroupIterator<Record, Record>(
					this.reader1, this.reader2, this.serializer1, this.comparator1, this.serializer2, this.comparator2,
					this.pairComparator);
	
			iterator.open();
			
			final TestData.Key key = new TestData.Key();
			while (iterator.next())
			{
				Iterator<Record> iter1 = iterator.getValues1();
				Iterator<Record> iter2 = iterator.getValues2();
				
				TestData.Value v1 = null;
				TestData.Value v2 = null;
				
				if (iter1.hasNext()) {
					Record rec = iter1.next();
					rec.getFieldInto(0, key);
					v1 = rec.getField(1, TestData.Value.class);
				}
				else if (iter2.hasNext()) {
					Record rec = iter2.next();
					rec.getFieldInto(0, key);
					v2 = rec.getField(1, TestData.Value.class);
				}
				else {
					Assert.fail("No input on both sides.");
				}
	
				// assert that matches for this key exist
				Assert.assertTrue("No matches for key " + key, expectedCoGroupsMap.containsKey(key));
				
				Collection<TestData.Value> expValues1 = expectedCoGroupsMap.get(key).get(0);
				Collection<TestData.Value> expValues2 = expectedCoGroupsMap.get(key).get(1);
				
				if (v1 != null) {
					expValues1.remove(v1);
				}
				else {
					expValues2.remove(v2);
				}
				
				while(iter1.hasNext()) {
					Record rec = iter1.next();
					Assert.assertTrue("Value not in expected set of first input", expValues1.remove(rec.getField(1, TestData.Value.class)));
				}
				Assert.assertTrue("Expected set of first input not empty", expValues1.isEmpty());
				
				while(iter2.hasNext()) {
					Record rec = iter2.next();
					Assert.assertTrue("Value not in expected set of second input", expValues2.remove(rec.getField(1, TestData.Value.class)));
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

	// --------------------------------------------------------------------------------------------
	
	private Map<TestData.Key, List<Collection<TestData.Value>>> coGroupValues(
			Map<TestData.Key, Collection<TestData.Value>> leftMap,
			Map<TestData.Key, Collection<TestData.Value>> rightMap)
	{
		Map<TestData.Key, List<Collection<TestData.Value>>> map = new HashMap<TestData.Key, List<Collection<TestData.Value>>>(1000);

		Set<TestData.Key> keySet = new HashSet<TestData.Key>(leftMap.keySet());
		keySet.addAll(rightMap.keySet());
		
		for (TestData.Key key : keySet) {
			Collection<TestData.Value> leftValues = leftMap.get(key);
			Collection<TestData.Value> rightValues = rightMap.get(key);
			ArrayList<Collection<TestData.Value>> list = new ArrayList<Collection<TestData.Value>>(2);
			
			if (leftValues == null) {
				list.add(new ArrayList<TestData.Value>(0));
			} else {
				list.add(leftValues);
			}
			
			if (rightValues == null) {
				list.add(new ArrayList<TestData.Value>(0));
			} else {
				list.add(rightValues);
			}
			
			map.put(key, list);
		}
		return map;
	}

	private Map<TestData.Key, Collection<TestData.Value>> collectData(Generator iter, int num)
	throws Exception
	{
		Map<TestData.Key, Collection<TestData.Value>> map = new HashMap<TestData.Key, Collection<TestData.Value>>();
		Record pair = new Record();
		
		for (int i = 0; i < num; i++) {
			iter.next(pair);
			TestData.Key key = pair.getField(0, TestData.Key.class);
			
			if (!map.containsKey(key)) {
				map.put(new TestData.Key(key.getKey()), new ArrayList<TestData.Value>());
			}

			Collection<TestData.Value> values = map.get(key);
			values.add(new TestData.Value(pair.getField(1, TestData.Value.class).getValue()));
		}
		return map;
	}
}
