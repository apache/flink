/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.pact.common.generic.types.TypeComparator;
import eu.stratosphere.pact.common.generic.types.TypeSerializer;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.runtime.plugable.PactRecordComparator;
import eu.stratosphere.pact.runtime.plugable.PactRecordSerializer;
import eu.stratosphere.pact.runtime.test.util.TestData;
import eu.stratosphere.pact.runtime.test.util.TestData.Key;
import eu.stratosphere.pact.runtime.test.util.TestData.Value;


public class MergeIteratorTest
{
	private TypeSerializer<PactRecord> serializer;
	
	private TypeComparator<PactRecord> comparator;
	
	
	@SuppressWarnings("unchecked")
	@Before
	public void setup() {
		this.serializer = PactRecordSerializer.get();
		this.comparator = new PactRecordComparator(new int[] {0}, new Class[] { TestData.Key.class});
	}
	
	
	private MutableObjectIterator<PactRecord> newIterator(final int[] keys, final String[] values)
	{
		return new MutableObjectIterator<PactRecord>()
		{
			private Key key = new Key();
			private Value value = new Value();
			
			private int current = 0;

			@Override
			public boolean next(PactRecord target)
			{
				if (current < keys.length) {
					key.setKey(keys[current]);
					value.setValue(values[current]);
					current++;
					target.setField(0, key);
					target.setField(1, value);
					return true;
				}
				else {
					return false;
				}
			}
		};
	}

	@Test
	public void testMergeOfTwoStreams() throws Exception
	{
		// iterators
		List<MutableObjectIterator<PactRecord>> iterators = new ArrayList<MutableObjectIterator<PactRecord>>();
		iterators.add(newIterator(new int[] { 1, 2, 4, 5, 10 }, new String[] { "1", "2", "4", "5", "10" }));
		iterators.add(newIterator(new int[] { 3, 6, 7, 10, 12 }, new String[] { "3", "6", "7", "10", "12" }));
		
		final int[] expected = new int[] {1, 2, 3, 4, 5, 6, 7, 10, 10, 12};

		// comparator
		Comparator<TestData.Key> comparator = new TestData.KeyComparator();

		// merge iterator
		MutableObjectIterator<PactRecord> iterator = new MergeIterator<PactRecord>(iterators, this.serializer, this.comparator);

		// check expected order
		PactRecord rec1 = new PactRecord();
		PactRecord rec2 = new PactRecord();
		final Key k1 = new Key();
		final Key k2 = new Key();
		
		int pos = 1;
		
		Assert.assertTrue(iterator.next(rec1));
		Assert.assertEquals(expected[0], rec1.getField(0, TestData.Key.class).getKey());
		
		while (iterator.next(rec2)) {
			k1.setKey(rec1.getField(0, TestData.Key.class).getKey());
			k2.setKey(rec2.getField(0, TestData.Key.class).getKey());
			
			Assert.assertTrue(comparator.compare(k1, k2) <= 0);
			Assert.assertEquals(expected[pos++], k2.getKey()); 
			
			PactRecord tmp = rec1;
			rec1 = rec2;
			rec2 = tmp;
		}
	}
	
	@Test
	public void testMergeOfTenStreams() throws Exception
	{
		// iterators
		List<MutableObjectIterator<PactRecord>> iterators = new ArrayList<MutableObjectIterator<PactRecord>>();
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
		MutableObjectIterator<PactRecord> iterator = new MergeIterator<PactRecord>(iterators, this.serializer, this.comparator);

		int elementsFound = 1;
		// check expected order
		PactRecord rec1 = new PactRecord();
		PactRecord rec2 = new PactRecord();
		final Key k1 = new Key();
		final Key k2 = new Key();
		
		Assert.assertTrue(iterator.next(rec1));
		while (iterator.next(rec2)) {
			elementsFound++;
			k1.setKey(rec1.getField(0, TestData.Key.class).getKey());
			k2.setKey(rec2.getField(0, TestData.Key.class).getKey());
			Assert.assertTrue(comparator.compare(k1, k2) <= 0);
			
			PactRecord tmp = rec1;
			rec1 = rec2;
			rec2 = tmp;
		}
		
		Assert.assertEquals("Too few elements returned from stream.", 50, elementsFound);
	}
	
	@Test
	public void testInvalidMerge() throws Exception
	{
		// iterators
		List<MutableObjectIterator<PactRecord>> iterators = new ArrayList<MutableObjectIterator<PactRecord>>();
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
		MutableObjectIterator<PactRecord> iterator = new MergeIterator<PactRecord>(iterators, this.serializer, this.comparator);

		boolean violationFound = false;
		
		// check expected order
		PactRecord rec1 = new PactRecord();
		PactRecord rec2 = new PactRecord();
		
		Assert.assertTrue(iterator.next(rec1));
		while (iterator.next(rec2))
		{
			final Key k1 = new Key();
			final Key k2 = new Key();
			k1.setKey(rec1.getField(0, TestData.Key.class).getKey());
			k2.setKey(rec2.getField(0, TestData.Key.class).getKey());
			
			if (comparator.compare(k1, k2) > 0) {
				violationFound = true;
				break;
			}
			
			PactRecord tmp = rec1;
			rec1 = rec2;
			rec2 = tmp;
		}
		
		Assert.assertTrue("Merge must have returned a wrong result", violationFound);
	}
}
