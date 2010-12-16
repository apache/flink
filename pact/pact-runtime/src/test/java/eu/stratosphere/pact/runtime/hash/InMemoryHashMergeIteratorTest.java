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

package eu.stratosphere.pact.runtime.hash;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import junit.framework.TestCase;
import eu.stratosphere.nephele.io.Reader;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.runtime.hash.InMemoryHashMatchIterator;
import eu.stratosphere.pact.runtime.test.util.TestData;
import eu.stratosphere.pact.runtime.test.util.TestData.Generator;
import eu.stratosphere.pact.runtime.test.util.TestData.RecordReaderMock;

// TODO beautify (see TestSortMergeIterator) (en)
public class InMemoryHashMergeIteratorTest extends TestCase {
	// the size of the left and right inputs
	private static int input1Size = 50000;

	private static int input2Size = 1000;

	// random seeds for the left and right input data generators
	private static long seed1 = 561349061987311L;

	private static long seed2 = 231434613412342L;

	// left and right input data generators
	private Generator generator1;

	private Generator generator2;

	// left and right input RecordReader mocks
	@SuppressWarnings("rawtypes")
	private Reader<? extends KeyValuePair> reader1;

	@SuppressWarnings("rawtypes")
	private Reader<? extends KeyValuePair> reader2;

	@Override
	public void setUp() {
		generator1 = new Generator(seed1, 500, 16);
		generator2 = new Generator(seed2, 500, 16);

		reader1 = new RecordReaderMock(generator1, input1Size);
		reader2 = new RecordReaderMock(generator2, input2Size);
	}

	@Override
	public void tearDown() {
		generator1 = null;
		generator2 = null;

		reader1 = null;
		reader2 = null;
	}

	public void testIterator() throws IOException, InterruptedException {
		// collect expected data
		Map<Key, Collection<Value>> expectedValuesMap1 = collectData(generator1, input1Size);
		Map<Key, Collection<Value>> expectedValuesMap2 = collectData(generator2, input2Size);
		Map<Key, Collection<Match>> expectedMatchesMap = matchValues(expectedValuesMap1, expectedValuesMap2);

		// reset the generators
		generator1.reset();
		generator2.reset();

		// compare with iterator values
		InMemoryHashMatchIterator iterator = new InMemoryHashMatchIterator(reader1, reader2);

		iterator.open();
		while (iterator.next()) {
			Key key = (Key) iterator.getKey();

			// assert that matches for this key exist
			assertTrue("No matches for key " + key + " are expected", expectedMatchesMap.containsKey(key));

			// assert that each map is expected
			Iterator<Value> iter1 = iterator.getValues1();
			Iterator<Value> iter2 = iterator.getValues2();

			List<Value> values1 = new ArrayList<Value>();
			while (iter1.hasNext()) {
				values1.add(iter1.next());
			}

			List<Value> values2 = new ArrayList<Value>();
			while (iter2.hasNext()) {
				values2.add(iter2.next());
			}

			for (Value value1 : values1) {
				for (Value value2 : values2) {
					Collection<Match> expectedValues = expectedMatchesMap.get(key);
					Match match = new Match((Value) value1, (Value) value2);
					assertTrue("Unexpected match " + match + " for key " + key, expectedValues.contains(match));
					expectedValues.remove(match);
				}
			}

		}
		iterator.close();

		// assert that each expected match was seen
		for (Entry<Key, Collection<Match>> entry : expectedMatchesMap.entrySet()) {
			assertTrue("Collection for key " + entry.getKey() + " is not empty", entry.getValue().isEmpty());
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

		System.out.println();

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
