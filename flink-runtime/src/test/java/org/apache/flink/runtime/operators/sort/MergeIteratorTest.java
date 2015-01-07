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
import java.util.Comparator;
import java.util.List;

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.record.RecordComparator;
import org.apache.flink.runtime.operators.sort.MergeIterator;
import org.apache.flink.runtime.operators.testutils.TestData;
import org.apache.flink.runtime.operators.testutils.TestData.Key;
import org.apache.flink.runtime.operators.testutils.TestData.Value;
import org.apache.flink.types.Record;
import org.apache.flink.util.MutableObjectIterator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MergeIteratorTest {
	
	private TypeComparator<Record> comparator;
	
	
	@SuppressWarnings("unchecked")
	@Before
	public void setup() {
		this.comparator = new RecordComparator(new int[] {0}, new Class[] { TestData.Key.class});
	}
	
	
	private MutableObjectIterator<Record> newIterator(final int[] keys, final String[] values) {
		
		return new MutableObjectIterator<Record>() {
			
			private Key key = new Key();
			private Value value = new Value();
			
			private int current = 0;

			@Override
			public Record next(Record reuse) {
				if (current < keys.length) {
					key.setKey(keys[current]);
					value.setValue(values[current]);
					current++;
					reuse.setField(0, key);
					reuse.setField(1, value);
					return reuse;
				}
				else {
					return null;
				}
			}

			@Override
			public Record next() {
				if (current < keys.length) {
					Record result = new Record(new Key(keys[current]), new Value(values[current]));
					current++;
					return result;
				}
				else {
					return null;
				}
			}
		};
	}

	@Test
	public void testMergeOfTwoStreams() throws Exception
	{
		// iterators
		List<MutableObjectIterator<Record>> iterators = new ArrayList<MutableObjectIterator<Record>>();
		iterators.add(newIterator(new int[] { 1, 2, 4, 5, 10 }, new String[] { "1", "2", "4", "5", "10" }));
		iterators.add(newIterator(new int[] { 3, 6, 7, 10, 12 }, new String[] { "3", "6", "7", "10", "12" }));
		
		final int[] expected = new int[] {1, 2, 3, 4, 5, 6, 7, 10, 10, 12};

		// comparator
		Comparator<TestData.Key> comparator = new TestData.KeyComparator();

		// merge iterator
		MutableObjectIterator<Record> iterator = new MergeIterator<Record>(iterators, this.comparator);

		// check expected order
		Record rec1 = new Record();
		Record rec2 = new Record();
		final Key k1 = new Key();
		final Key k2 = new Key();
		
		int pos = 1;
		
		Assert.assertTrue((rec1 = iterator.next(rec1)) != null);
		Assert.assertEquals(expected[0], rec1.getField(0, TestData.Key.class).getKey());
		
		while ((rec2 = iterator.next(rec2)) != null) {
			k1.setKey(rec1.getField(0, TestData.Key.class).getKey());
			k2.setKey(rec2.getField(0, TestData.Key.class).getKey());
			
			Assert.assertTrue(comparator.compare(k1, k2) <= 0);
			Assert.assertEquals(expected[pos++], k2.getKey()); 
			
			Record tmp = rec1;
			rec1 = rec2;
			rec2 = tmp;
		}
	}
	
	@Test
	public void testMergeOfTenStreams() throws Exception
	{
		// iterators
		List<MutableObjectIterator<Record>> iterators = new ArrayList<MutableObjectIterator<Record>>();
		iterators.add(newIterator(new int[] { 1, 2, 17, 23, 23 }, new String[] { "A", "B", "C", "D", "E" }));
		iterators.add(newIterator(new int[] { 2, 6, 7, 8, 9 }, new String[] { "A", "B", "C", "D", "E" }));
		iterators.add(newIterator(new int[] { 4, 10, 11, 11, 12 }, new String[] { "A", "B", "C", "D", "E" }));
		iterators.add(newIterator(new int[] { 3, 6, 7, 10, 12 }, new String[] { "A", "B", "C", "D", "E" }));
		iterators.add(newIterator(new int[] { 7, 10, 15, 19, 44 }, new String[] { "A", "B", "C", "D", "E" }));
		iterators.add(newIterator(new int[] { 6, 6, 11, 17, 18 }, new String[] { "A", "B", "C", "D", "E" }));
		iterators.add(newIterator(new int[] { 1, 2, 4, 5, 10 }, new String[] { "A", "B", "C", "D", "E" }));
		iterators.add(newIterator(new int[] { 5, 10, 19, 23, 29 }, new String[] { "A", "B", "C", "D", "E" }));
		iterators.add(newIterator(new int[] { 9, 9, 9, 9, 9 }, new String[] { "A", "B", "C", "D", "E" }));
		iterators.add(newIterator(new int[] { 8, 8, 14, 14, 15 }, new String[] { "A", "B", "C", "D", "E" }));

		// comparator
		Comparator<TestData.Key> comparator = new TestData.KeyComparator();

		// merge iterator
		MutableObjectIterator<Record> iterator = new MergeIterator<Record>(iterators, this.comparator);

		int elementsFound = 1;
		// check expected order
		Record rec1 = new Record();
		Record rec2 = new Record();
		final Key k1 = new Key();
		final Key k2 = new Key();
		
		Assert.assertTrue((rec1 = iterator.next(rec1)) != null);
		while ((rec2 = iterator.next(rec2)) != null) {
			elementsFound++;
			k1.setKey(rec1.getField(0, TestData.Key.class).getKey());
			k2.setKey(rec2.getField(0, TestData.Key.class).getKey());
			Assert.assertTrue(comparator.compare(k1, k2) <= 0);
			
			Record tmp = rec1;
			rec1 = rec2;
			rec2 = tmp;
		}
		
		Assert.assertEquals("Too few elements returned from stream.", 50, elementsFound);
	}
	
	@Test
	public void testInvalidMerge() throws Exception
	{
		// iterators
		List<MutableObjectIterator<Record>> iterators = new ArrayList<MutableObjectIterator<Record>>();
		iterators.add(newIterator(new int[] { 1, 2, 17, 23, 23 }, new String[] { "A", "B", "C", "D", "E" }));
		iterators.add(newIterator(new int[] { 2, 6, 7, 8, 9 }, new String[] { "A", "B", "C", "D", "E" }));
		iterators.add(newIterator(new int[] { 4, 10, 11, 11, 12 }, new String[] { "A", "B", "C", "D", "E" }));
		iterators.add(newIterator(new int[] { 3, 6, 10, 7, 12 }, new String[] { "A", "B", "C", "D", "E" }));
		iterators.add(newIterator(new int[] { 7, 10, 15, 19, 44 }, new String[] { "A", "B", "C", "D", "E" }));
		iterators.add(newIterator(new int[] { 6, 6, 11, 17, 18 }, new String[] { "A", "B", "C", "D", "E" }));
		iterators.add(newIterator(new int[] { 1, 2, 4, 5, 10 }, new String[] { "A", "B", "C", "D", "E" }));
		iterators.add(newIterator(new int[] { 5, 10, 19, 23, 29 }, new String[] { "A", "B", "C", "D", "E" }));
		iterators.add(newIterator(new int[] { 9, 9, 9, 9, 9 }, new String[] { "A", "B", "C", "D", "E" }));
		iterators.add(newIterator(new int[] { 8, 8, 14, 14, 15 }, new String[] { "A", "B", "C", "D", "E" }));

		// comparator
		Comparator<TestData.Key> comparator = new TestData.KeyComparator();

		// merge iterator
		MutableObjectIterator<Record> iterator = new MergeIterator<Record>(iterators, this.comparator);

		boolean violationFound = false;
		
		// check expected order
		Record rec1 = new Record();
		Record rec2 = new Record();
		
		Assert.assertTrue((rec1 = iterator.next(rec1)) != null);
		while ((rec2 = iterator.next(rec2)) != null)
		{
			final Key k1 = new Key();
			final Key k2 = new Key();
			k1.setKey(rec1.getField(0, TestData.Key.class).getKey());
			k2.setKey(rec2.getField(0, TestData.Key.class).getKey());
			
			if (comparator.compare(k1, k2) > 0) {
				violationFound = true;
				break;
			}
			
			Record tmp = rec1;
			rec1 = rec2;
			rec2 = tmp;
		}
		
		Assert.assertTrue("Merge must have returned a wrong result", violationFound);
	}
}
