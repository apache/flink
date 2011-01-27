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
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.runtime.test.util.TestData;

/**
 * @author Erik Nijkamp
 */
public class MergeIteratorTest {

	@BeforeClass
	public static void beforeClass() {
	}

	@AfterClass
	public static void afterClass() {
	}

	private Iterator<KeyValuePair<TestData.Key, TestData.Value>> newIterator(final int[] keys, final String[] values) {
		return new Iterator<KeyValuePair<TestData.Key, TestData.Value>>() {
			int current = 0;

			@Override
			public boolean hasNext() {
				return current < keys.length;
			}

			@Override
			public KeyValuePair<TestData.Key, TestData.Value> next() {
				KeyValuePair<TestData.Key, TestData.Value> pair = new KeyValuePair<TestData.Key, TestData.Value>(
					new TestData.Key(keys[current]), new TestData.Value(values[current]));
				current++;
				return pair;
			}

			@Override
			public void remove() {

			}
		};
	}

	@Test
	public void testMerge() {
		// iterarors
		List<Iterator<KeyValuePair<TestData.Key, TestData.Value>>> iterators = new ArrayList<Iterator<KeyValuePair<TestData.Key, TestData.Value>>>();
		iterators.add(newIterator(new int[] { 1, 2, 4, 5, 10 }, new String[] { "1", "2", "4", "5", "10" }));
		iterators.add(newIterator(new int[] { 3, 6, 7, 10, 12 }, new String[] { "3", "6", "7", "10", "12" }));

		// comparator
		Comparator<TestData.Key> comparator = new TestData.KeyComparator();

		// merge iterator
		Iterator<KeyValuePair<TestData.Key, TestData.Value>> iterator = new MergeIterator<TestData.Key, TestData.Value>(
			iterators, comparator);

		// check order
		KeyValuePair<TestData.Key, TestData.Value> pair1 = iterator.next();
		while (iterator.hasNext()) {
			KeyValuePair<TestData.Key, TestData.Value> pair2 = iterator.next();
			Logger.getRootLogger().debug("1 -> " + pair1.getKey() + " | 2 -> " + pair2.getKey());
			Assert.assertTrue(comparator.compare(pair1.getKey(), pair2.getKey()) <= 0);
			pair1 = pair2;
		}
	}
}
