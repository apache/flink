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


package org.apache.flink.runtime.operators.sort;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.flink.api.common.typeutils.GenericPairComparator;

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypePairComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.operators.testutils.TestData;
import org.apache.flink.runtime.operators.testutils.TestData.TupleGenerator;
import org.apache.flink.runtime.operators.testutils.TestData.TupleGenerator.KeyMode;
import org.apache.flink.runtime.operators.testutils.TestData.TupleGenerator.ValueMode;
import org.apache.flink.util.MutableObjectIterator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 */
public class ReusingSortMergeCoGroupIteratorITCase
{
	// the size of the left and right inputs
	private static final int INPUT_1_SIZE = 20000;

	private static final int INPUT_2_SIZE = 1000;

	// random seeds for the left and right input data generators
	private static final long SEED1 = 561349061987311L;

	private static final long SEED2 = 231434613412342L;

	// left and right input data generators
	private TupleGenerator generator1;

	private TupleGenerator generator2;

	// left and right input Tuple2<Integer, String>Reader mocks
	private MutableObjectIterator<Tuple2<Integer, String>> reader1;

	private MutableObjectIterator<Tuple2<Integer, String>> reader2;
	
	private TypeSerializer<Tuple2<Integer, String>> serializer1;
	private TypeSerializer<Tuple2<Integer, String>> serializer2;
	private TypeComparator<Tuple2<Integer, String>> comparator1;
	private TypeComparator<Tuple2<Integer, String>> comparator2;
	private TypePairComparator<Tuple2<Integer, String>, Tuple2<Integer, String>> pairComparator;


	@SuppressWarnings("unchecked")
	@Before
	public void beforeTest() {
		this.serializer1 = TestData.getIntStringTupleSerializer();
		this.serializer2 = TestData.getIntStringTupleSerializer();
		this.comparator1 = TestData.getIntStringTupleComparator();
		this.comparator2 = TestData.getIntStringTupleComparator();
		this.pairComparator = new GenericPairComparator(comparator1, comparator2);
	}
	
	@Test
	public void testMerge() {
		try {
			
			generator1 = new TupleGenerator(SEED1, 500, 4096, KeyMode.SORTED, ValueMode.RANDOM_LENGTH);
			generator2 = new TupleGenerator(SEED2, 500, 2048, KeyMode.SORTED, ValueMode.RANDOM_LENGTH);

			reader1 = new TestData.TupleGeneratorIterator(generator1, INPUT_1_SIZE);
			reader2 = new TestData.TupleGeneratorIterator(generator2, INPUT_2_SIZE);

			// collect expected data
			Map<Integer, Collection<String>> expectedStringsMap1 = collectData(generator1, INPUT_1_SIZE);
			Map<Integer, Collection<String>> expectedStringsMap2 = collectData(generator2, INPUT_2_SIZE);
			Map<Integer, List<Collection<String>>> expectedCoGroupsMap = coGroupValues(expectedStringsMap1, expectedStringsMap2);
	
			// reset the generators
			generator1.reset();
			generator2.reset();
	
			// compare with iterator values
			ReusingSortMergeCoGroupIterator<Tuple2<Integer, String>, Tuple2<Integer, String>> iterator =	new ReusingSortMergeCoGroupIterator<>(
					this.reader1, this.reader2, this.serializer1, this.comparator1, this.serializer2, this.comparator2,
					this.pairComparator);
	
			iterator.open();
			
			int key = 0;
			while (iterator.next())
			{
				Iterator<Tuple2<Integer, String>> iter1 = iterator.getValues1().iterator();
				Iterator<Tuple2<Integer, String>> iter2 = iterator.getValues2().iterator();
				
				String v1 = null;
				String v2 = null;
				
				if (iter1.hasNext()) {
					Tuple2<Integer, String> rec = iter1.next();
					key = rec.f0;
					v1 = rec.f1;
				}
				else if (iter2.hasNext()) {
					Tuple2<Integer, String> rec = iter2.next();
					key = rec.f0;
					v2 = rec.f1;
				}
				else {
					Assert.fail("No input on both sides.");
				}
	
				// assert that matches for this key exist
				Assert.assertTrue("No matches for key " + key, expectedCoGroupsMap.containsKey(key));
				
				Collection<String> expValues1 = expectedCoGroupsMap.get(key).get(0);
				Collection<String> expValues2 = expectedCoGroupsMap.get(key).get(1);
				
				if (v1 != null) {
					expValues1.remove(v1);
				}
				else {
					expValues2.remove(v2);
				}
				
				while(iter1.hasNext()) {
					Tuple2<Integer, String> rec = iter1.next();
					Assert.assertTrue("String not in expected set of first input", expValues1.remove(rec.f1));
				}
				Assert.assertTrue("Expected set of first input not empty", expValues1.isEmpty());
				
				while(iter2.hasNext()) {
					Tuple2<Integer, String> rec = iter2.next();
					Assert.assertTrue("String not in expected set of second input", expValues2.remove(rec.f1));
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
	
	private Map<Integer, List<Collection<String>>> coGroupValues(
			Map<Integer, Collection<String>> leftMap,
			Map<Integer, Collection<String>> rightMap)
	{
		Map<Integer, List<Collection<String>>> map = new HashMap<>(1000);

		Set<Integer> keySet = new HashSet<>(leftMap.keySet());
		keySet.addAll(rightMap.keySet());
		
		for (Integer key : keySet) {
			Collection<String> leftValues = leftMap.get(key);
			Collection<String> rightValues = rightMap.get(key);
			ArrayList<Collection<String>> list = new ArrayList<>(2);
			
			if (leftValues == null) {
				list.add(new ArrayList<String>(0));
			} else {
				list.add(leftValues);
			}
			
			if (rightValues == null) {
				list.add(new ArrayList<String>(0));
			} else {
				list.add(rightValues);
			}
			
			map.put(key, list);
		}
		return map;
	}

	private Map<Integer, Collection<String>> collectData(TupleGenerator iter, int num)
	throws Exception
	{
		Map<Integer, Collection<String>> map = new HashMap<>();
		Tuple2<Integer, String> pair = new Tuple2<>();
		
		for (int i = 0; i < num; i++) {
			iter.next(pair);
			int key = pair.f0;
			
			if (!map.containsKey(key)) {
				map.put(key, new ArrayList<String>());
			}

			Collection<String> values = map.get(key);
			values.add(pair.f1);
		}
		return map;
	}
}
