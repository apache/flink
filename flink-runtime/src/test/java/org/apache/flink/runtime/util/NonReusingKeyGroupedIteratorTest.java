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

package org.apache.flink.runtime.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.flink.runtime.testutils.recordutils.RecordComparator;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.Record;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.MutableObjectIterator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test for the safe key grouped iterator, which advances in windows containing the same key and provides a sub-iterator
 * over the records with the same key.
 */
public class NonReusingKeyGroupedIteratorTest {
	
	private MutableObjectIterator<Record> sourceIter;		// the iterator that provides the input
	
	private NonReusingKeyGroupedIterator<Record> psi;					// the grouping iterator, progressing in key steps
	
	@Before
	public void setup()
	{
		final ArrayList<IntStringPair> source = new ArrayList<IntStringPair>();
		
		// add elements to the source
		source.add(new IntStringPair(new IntValue(1), new StringValue("A")));
		source.add(new IntStringPair(new IntValue(2), new StringValue("B")));
		source.add(new IntStringPair(new IntValue(3), new StringValue("C")));
		source.add(new IntStringPair(new IntValue(3), new StringValue("D")));
		source.add(new IntStringPair(new IntValue(4), new StringValue("E")));
		source.add(new IntStringPair(new IntValue(4), new StringValue("F")));
		source.add(new IntStringPair(new IntValue(4), new StringValue("G")));
		source.add(new IntStringPair(new IntValue(5), new StringValue("H")));
		source.add(new IntStringPair(new IntValue(5), new StringValue("I")));
		source.add(new IntStringPair(new IntValue(5), new StringValue("J")));
		source.add(new IntStringPair(new IntValue(5), new StringValue("K")));
		source.add(new IntStringPair(new IntValue(5), new StringValue("L")));
		
		
		this.sourceIter = new MutableObjectIterator<Record>() {
			final Iterator<IntStringPair> it = source.iterator();
			
			@Override
			public Record next(Record reuse) throws IOException {
				if (it.hasNext()) {
					IntStringPair pair = it.next();
					reuse.setField(0, pair.getInteger());
					reuse.setField(1, pair.getString());
					return reuse;
				}
				else {
					return null;
				}
			}

			@Override
			public Record next() throws IOException {
				if (it.hasNext()) {
					IntStringPair pair = it.next();
					Record result = new Record(2);
					result.setField(0, pair.getInteger());
					result.setField(1, pair.getString());
					return result;
				}
				else {
					return null;
				}
			}
		};
		
		@SuppressWarnings("unchecked")
		final RecordComparator comparator = new RecordComparator(new int[] {0}, new Class[] {IntValue.class});
		
		this.psi = new NonReusingKeyGroupedIterator<Record>(this.sourceIter, comparator);
	}

	@Test
	public void testNextKeyOnly() throws Exception
	{
		try {
			Assert.assertTrue("KeyGroupedIterator must have another key.", this.psi.nextKey());
			Assert.assertTrue("KeyGroupedIterator returned a wrong key.", this.psi.getComparatorWithCurrentReference().equalToReference(new Record(new IntValue(1))));
			Assert.assertEquals("KeyGroupedIterator returned a wrong key.", 1, this.psi.getCurrent().getField(0, IntValue.class).getValue());
			
			Assert.assertTrue("KeyGroupedIterator must have another key.", this.psi.nextKey());
			Assert.assertTrue("KeyGroupedIterator returned a wrong key.", this.psi.getComparatorWithCurrentReference().equalToReference(new Record(new IntValue(2))));
			Assert.assertEquals("KeyGroupedIterator returned a wrong key.", 2, this.psi.getCurrent().getField(0, IntValue.class).getValue());
			
			Assert.assertTrue("KeyGroupedIterator must have another key.", this.psi.nextKey());
			Assert.assertTrue("KeyGroupedIterator returned a wrong key.", this.psi.getComparatorWithCurrentReference().equalToReference(new Record(new IntValue(3))));
			Assert.assertEquals("KeyGroupedIterator returned a wrong key.", 3, this.psi.getCurrent().getField(0, IntValue.class).getValue());
			
			Assert.assertTrue("KeyGroupedIterator must have another key.", this.psi.nextKey());
			Assert.assertTrue("KeyGroupedIterator returned a wrong key.", this.psi.getComparatorWithCurrentReference().equalToReference(new Record(new IntValue(4))));
			Assert.assertEquals("KeyGroupedIterator returned a wrong key.", 4, this.psi.getCurrent().getField(0, IntValue.class).getValue());
			
			Assert.assertTrue("KeyGroupedIterator must have another key.", this.psi.nextKey());
			Assert.assertTrue("KeyGroupedIterator returned a wrong key.", this.psi.getComparatorWithCurrentReference().equalToReference(new Record(new IntValue(5))));
			Assert.assertEquals("KeyGroupedIterator returned a wrong key.", 5, this.psi.getCurrent().getField(0, IntValue.class).getValue());
			
			Assert.assertFalse("KeyGroupedIterator must not have another key.", this.psi.nextKey());
			Assert.assertNull("KeyGroupedIterator must not have another value.", this.psi.getValues());
			
			Assert.assertFalse("KeyGroupedIterator must not have another key.", this.psi.nextKey());
			Assert.assertFalse("KeyGroupedIterator must not have another key.", this.psi.nextKey());
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("The test encountered an unexpected exception.");
		}
	}
	
	@Test
	public void testFullIterationThroughAllValues() throws IOException
	{
		try {
			// Key 1, Value A
			Assert.assertTrue("KeyGroupedIterator must have another key.", this.psi.nextKey());
			Assert.assertTrue("KeyGroupedIterator must have another value.", this.psi.getValues().hasNext());
			Assert.assertTrue("KeyGroupedIterator returned a wrong key.", this.psi.getComparatorWithCurrentReference().equalToReference(new Record(new IntValue(1))));
			Assert.assertEquals("KeyGroupedIterator returned a wrong key.", 1, this.psi.getCurrent().getField(0, IntValue.class).getValue());
			Assert.assertEquals("KeyGroupedIterator returned a wrong value.", new StringValue("A"), this.psi.getValues().next().getField(1, StringValue.class));
			Assert.assertFalse("KeyGroupedIterator must not have another value.", this.psi.getValues().hasNext());
			Assert.assertEquals("KeyGroupedIterator returned a wrong key.", 1, this.psi.getCurrent().getField(0, IntValue.class).getValue());
			
			// Key 2, Value B
			Assert.assertTrue("KeyGroupedIterator must have another key.", this.psi.nextKey());
			Assert.assertTrue("KeyGroupedIterator must have another value.", this.psi.getValues().hasNext());
			Assert.assertTrue("KeyGroupedIterator returned a wrong key.", this.psi.getComparatorWithCurrentReference().equalToReference(new Record(new IntValue(2))));
			Assert.assertEquals("KeyGroupedIterator returned a wrong key.", 2, this.psi.getCurrent().getField(0, IntValue.class).getValue());
			Assert.assertEquals("KeyGroupedIterator returned a wrong value.", new StringValue("B"), this.psi.getValues().next().getField(1, StringValue.class));
			Assert.assertFalse("KeyGroupedIterator must not have another value.", this.psi.getValues().hasNext());
			Assert.assertEquals("KeyGroupedIterator returned a wrong key.", 2, this.psi.getCurrent().getField(0, IntValue.class).getValue());
			
			// Key 3, Values C, D
			Assert.assertTrue("KeyGroupedIterator must have another key.", this.psi.nextKey());
			Assert.assertTrue("KeyGroupedIterator must have another value.", this.psi.getValues().hasNext());
			Assert.assertTrue("KeyGroupedIterator returned a wrong key.", this.psi.getComparatorWithCurrentReference().equalToReference(new Record(new IntValue(3))));
			Assert.assertEquals("KeyGroupedIterator returned a wrong key.", 3, this.psi.getCurrent().getField(0, IntValue.class).getValue());
			Assert.assertEquals("KeyGroupedIterator returned a wrong value.", new StringValue("C"), this.psi.getValues().next().getField(1, StringValue.class));
			Assert.assertTrue("KeyGroupedIterator must have another value.", this.psi.getValues().hasNext());
			Assert.assertTrue("KeyGroupedIterator returned a wrong key.", this.psi.getComparatorWithCurrentReference().equalToReference(new Record(new IntValue(3))));
			Assert.assertEquals("KeyGroupedIterator returned a wrong key.", 3, this.psi.getCurrent().getField(0, IntValue.class).getValue());
			Assert.assertEquals("KeyGroupedIterator returned a wrong value.", new StringValue("D"), this.psi.getValues().next().getField(1, StringValue.class));
			Assert.assertTrue("KeyGroupedIterator returned a wrong key.", this.psi.getComparatorWithCurrentReference().equalToReference(new Record(new IntValue(3))));
			Assert.assertEquals("KeyGroupedIterator returned a wrong key.", 3, this.psi.getCurrent().getField(0, IntValue.class).getValue());
			try {
				this.psi.getValues().next();
				Assert.fail("A new KeyGroupedIterator must not have any value available and hence throw an exception on next().");
			}
			catch (NoSuchElementException nseex) {}
			Assert.assertFalse("KeyGroupedIterator must not have another value.", this.psi.getValues().hasNext());
			try {
				this.psi.getValues().next();
				Assert.fail("A new KeyGroupedIterator must not have any value available and hence throw an exception on next().");
			}
			catch (NoSuchElementException nseex) {}
			Assert.assertTrue("KeyGroupedIterator returned a wrong key.", this.psi.getComparatorWithCurrentReference().equalToReference(new Record(new IntValue(3))));
			Assert.assertEquals("KeyGroupedIterator returned a wrong key.", 3, this.psi.getCurrent().getField(0, IntValue.class).getValue());
			
			// Key 4, Values E, F, G
			Assert.assertTrue("KeyGroupedIterator must have another key.", this.psi.nextKey());
			Assert.assertTrue("KeyGroupedIterator must have another value.", this.psi.getValues().hasNext());
			Assert.assertTrue("KeyGroupedIterator returned a wrong key.", this.psi.getComparatorWithCurrentReference().equalToReference(new Record(new IntValue(4))));
			Assert.assertEquals("KeyGroupedIterator returned a wrong key.", 4, this.psi.getCurrent().getField(0, IntValue.class).getValue());
			Assert.assertEquals("KeyGroupedIterator returned a wrong value.", new StringValue("E"), this.psi.getValues().next().getField(1, StringValue.class));
			Assert.assertTrue("KeyGroupedIterator must have another value.", this.psi.getValues().hasNext());
			Assert.assertTrue("KeyGroupedIterator returned a wrong key.", this.psi.getComparatorWithCurrentReference().equalToReference(new Record(new IntValue(4))));
			Assert.assertEquals("KeyGroupedIterator returned a wrong key.", 4, this.psi.getCurrent().getField(0, IntValue.class).getValue());
			Assert.assertEquals("KeyGroupedIterator returned a wrong value.", new StringValue("F"), this.psi.getValues().next().getField(1, StringValue.class));
			Assert.assertTrue("KeyGroupedIterator must have another value.", this.psi.getValues().hasNext());
			Assert.assertTrue("KeyGroupedIterator returned a wrong key.", this.psi.getComparatorWithCurrentReference().equalToReference(new Record(new IntValue(4))));
			Assert.assertEquals("KeyGroupedIterator returned a wrong key.", 4, this.psi.getCurrent().getField(0, IntValue.class).getValue());
			Assert.assertEquals("KeyGroupedIterator returned a wrong value.", new StringValue("G"), this.psi.getValues().next().getField(1, StringValue.class));
			Assert.assertTrue("KeyGroupedIterator returned a wrong key.", this.psi.getComparatorWithCurrentReference().equalToReference(new Record(new IntValue(4))));
			Assert.assertEquals("KeyGroupedIterator returned a wrong key.", 4, this.psi.getCurrent().getField(0, IntValue.class).getValue());
			Assert.assertFalse("KeyGroupedIterator must not have another value.", this.psi.getValues().hasNext());
			Assert.assertTrue("KeyGroupedIterator returned a wrong key.", this.psi.getComparatorWithCurrentReference().equalToReference(new Record(new IntValue(4))));
			Assert.assertEquals("KeyGroupedIterator returned a wrong key.", 4, this.psi.getCurrent().getField(0, IntValue.class).getValue());
			
			// Key 5, Values H, I, J, K, L
			Assert.assertTrue("KeyGroupedIterator must have another key.", this.psi.nextKey());
			Assert.assertTrue("KeyGroupedIterator must have another value.", this.psi.getValues().hasNext());
			Assert.assertTrue("KeyGroupedIterator returned a wrong key.", this.psi.getComparatorWithCurrentReference().equalToReference(new Record(new IntValue(5))));
			Assert.assertEquals("KeyGroupedIterator returned a wrong key.", 5, this.psi.getCurrent().getField(0, IntValue.class).getValue());
			Assert.assertEquals("KeyGroupedIterator returned a wrong value.", new StringValue("H"), this.psi.getValues().next().getField(1, StringValue.class));
			Assert.assertTrue("KeyGroupedIterator must have another value.", this.psi.getValues().hasNext());
			Assert.assertTrue("KeyGroupedIterator returned a wrong key.", this.psi.getComparatorWithCurrentReference().equalToReference(new Record(new IntValue(5))));
			Assert.assertEquals("KeyGroupedIterator returned a wrong key.", 5, this.psi.getCurrent().getField(0, IntValue.class).getValue());
			Assert.assertEquals("KeyGroupedIterator returned a wrong value.", new StringValue("I"), this.psi.getValues().next().getField(1, StringValue.class));
			Assert.assertTrue("KeyGroupedIterator must have another value.", this.psi.getValues().hasNext());
			Assert.assertTrue("KeyGroupedIterator returned a wrong key.", this.psi.getComparatorWithCurrentReference().equalToReference(new Record(new IntValue(5))));
			Assert.assertEquals("KeyGroupedIterator returned a wrong key.", 5, this.psi.getCurrent().getField(0, IntValue.class).getValue());
			Assert.assertEquals("KeyGroupedIterator returned a wrong value.", new StringValue("J"), this.psi.getValues().next().getField(1, StringValue.class));
			Assert.assertTrue("KeyGroupedIterator must have another value.", this.psi.getValues().hasNext());
			Assert.assertTrue("KeyGroupedIterator returned a wrong key.", this.psi.getComparatorWithCurrentReference().equalToReference(new Record(new IntValue(5))));
			Assert.assertEquals("KeyGroupedIterator returned a wrong key.", 5, this.psi.getCurrent().getField(0, IntValue.class).getValue());
			Assert.assertEquals("KeyGroupedIterator returned a wrong value.", new StringValue("K"), this.psi.getValues().next().getField(1, StringValue.class));
			Assert.assertTrue("KeyGroupedIterator must have another value.", this.psi.getValues().hasNext());
			Assert.assertTrue("KeyGroupedIterator returned a wrong key.", this.psi.getComparatorWithCurrentReference().equalToReference(new Record(new IntValue(5))));
			Assert.assertEquals("KeyGroupedIterator returned a wrong key.", 5, this.psi.getCurrent().getField(0, IntValue.class).getValue());
			Assert.assertEquals("KeyGroupedIterator returned a wrong value.", new StringValue("L"), this.psi.getValues().next().getField(1, StringValue.class));
			Assert.assertTrue("KeyGroupedIterator returned a wrong key.", this.psi.getComparatorWithCurrentReference().equalToReference(new Record(new IntValue(5))));
			Assert.assertEquals("KeyGroupedIterator returned a wrong key.", 5, this.psi.getCurrent().getField(0, IntValue.class).getValue());
			try {
				this.psi.getValues().next();
				Assert.fail("A new KeyGroupedIterator must not have any value available and hence throw an exception on next().");
			}
			catch (NoSuchElementException nseex) {}
			Assert.assertFalse("KeyGroupedIterator must not have another value.", this.psi.getValues().hasNext());
			Assert.assertTrue("KeyGroupedIterator returned a wrong key.", this.psi.getComparatorWithCurrentReference().equalToReference(new Record(new IntValue(5))));
			Assert.assertEquals("KeyGroupedIterator returned a wrong key.", 5, this.psi.getCurrent().getField(0, IntValue.class).getValue());
			try {
				this.psi.getValues().next();
				Assert.fail("A new KeyGroupedIterator must not have any value available and hence throw an exception on next().");
			}
			catch (NoSuchElementException nseex) {}
			
			Assert.assertFalse("KeyGroupedIterator must not have another key.", this.psi.nextKey());
			Assert.assertFalse("KeyGroupedIterator must not have another key.", this.psi.nextKey());
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("The test encountered an unexpected exception.");
		}
	}
	
	@Test
	public void testMixedProgress() throws Exception
	{
		try {
			// Progression only via nextKey() and hasNext() - Key 1, Value A
			Assert.assertTrue("KeyGroupedIterator must have another key.", this.psi.nextKey());
			Assert.assertTrue("KeyGroupedIterator must have another value.", this.psi.getValues().hasNext());
			
			// Progression only through nextKey() - Key 2, Value B
			Assert.assertTrue("KeyGroupedIterator must have another key.", this.psi.nextKey());
			
			// Progression first though haNext() and next(), then through hasNext() - Key 3, Values C, D
			Assert.assertTrue("KeyGroupedIterator must have another key.", this.psi.nextKey());
			Assert.assertTrue("KeyGroupedIterator must have another value.", this.psi.getValues().hasNext());
			Assert.assertTrue("KeyGroupedIterator returned a wrong key.", this.psi.getComparatorWithCurrentReference().equalToReference(new Record(new IntValue(3))));
			Assert.assertEquals("KeyGroupedIterator returned a wrong key.", 3, this.psi.getCurrent().getField(0, IntValue.class).getValue());
			Assert.assertEquals("KeyGroupedIterator returned a wrong value.", new StringValue("C"), this.psi.getValues().next().getField(1, StringValue.class));
			Assert.assertTrue("KeyGroupedIterator must have another value.", this.psi.getValues().hasNext());
			Assert.assertTrue("KeyGroupedIterator returned a wrong key.", this.psi.getComparatorWithCurrentReference().equalToReference(new Record(new IntValue(3))));
			Assert.assertEquals("KeyGroupedIterator returned a wrong key.", 3, this.psi.getCurrent().getField(0, IntValue.class).getValue());
			
			// Progression first via next() only, then hasNext() only Key 4, Values E, F, G
			Assert.assertTrue("KeyGroupedIterator must have another key.", this.psi.nextKey());
			Assert.assertEquals("KeyGroupedIterator returned a wrong value.", new StringValue("E"), this.psi.getValues().next().getField(1, StringValue.class));
			Assert.assertTrue("KeyGroupedIterator must have another value.", this.psi.getValues().hasNext());
			
			// Key 5, Values H, I, J, K, L
			Assert.assertTrue("KeyGroupedIterator must have another key.", this.psi.nextKey());
			Assert.assertEquals("KeyGroupedIterator returned a wrong value.", new StringValue("H"), this.psi.getValues().next().getField(1, StringValue.class));
			Assert.assertTrue("KeyGroupedIterator must have another value.", this.psi.getValues().hasNext());
			Assert.assertTrue("KeyGroupedIterator returned a wrong key.", this.psi.getComparatorWithCurrentReference().equalToReference(new Record(new IntValue(5))));
			Assert.assertEquals("KeyGroupedIterator returned a wrong key.", 5, this.psi.getCurrent().getField(0, IntValue.class).getValue());
			Assert.assertEquals("KeyGroupedIterator returned a wrong value.", new StringValue("I"), this.psi.getValues().next().getField(1, StringValue.class));
			Assert.assertTrue("KeyGroupedIterator must have another value.", this.psi.getValues().hasNext());
			
			// end
			Assert.assertFalse("KeyGroupedIterator must not have another key.", this.psi.nextKey());
			Assert.assertFalse("KeyGroupedIterator must not have another key.", this.psi.nextKey());
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("The test encountered an unexpected exception.");
		}
	}
	
	@Test
	public void testHasNextDoesNotOverweiteCurrentRecord() throws Exception
	{
		try {
			Iterator<Record> valsIter = null;
			Record rec = null;
			
			Assert.assertTrue("KeyGroupedIterator must have another key.", this.psi.nextKey());
			valsIter = this.psi.getValues();
			Assert.assertNotNull("Returned Iterator must not be null", valsIter);
			Assert.assertTrue("KeyGroupedIterator's value iterator must have another value.", valsIter.hasNext());
			rec = valsIter.next();
			Assert.assertEquals("KeyGroupedIterator returned a wrong key.", 1, rec.getField(0, IntValue.class).getValue());
			Assert.assertEquals("KeyGroupedIterator returned a wrong value.", new StringValue("A"), rec.getField(1, StringValue.class));
			Assert.assertFalse("KeyGroupedIterator must have another value.", valsIter.hasNext());
			Assert.assertEquals("KeyGroupedIterator returned a wrong key.", 1, rec.getField(0, IntValue.class).getValue());
			Assert.assertEquals("KeyGroupedIterator returned a wrong value.", new StringValue("A"), rec.getField(1, StringValue.class));			
			Assert.assertFalse("KeyGroupedIterator's value iterator must not have another value.", valsIter.hasNext());
			
			Assert.assertTrue("KeyGroupedIterator must have another key.", this.psi.nextKey());
			valsIter = this.psi.getValues();
			Assert.assertNotNull("Returned Iterator must not be null", valsIter);
			Assert.assertTrue("KeyGroupedIterator's value iterator must have another value.", valsIter.hasNext());
			rec = valsIter.next();
			Assert.assertEquals("KeyGroupedIterator returned a wrong key.", 2, rec.getField(0, IntValue.class).getValue());
			Assert.assertEquals("KeyGroupedIterator returned a wrong value.", new StringValue("B"), rec.getField(1, StringValue.class));
			Assert.assertFalse("KeyGroupedIterator must have another value.", valsIter.hasNext());
			Assert.assertEquals("KeyGroupedIterator returned a wrong key.", 2, rec.getField(0, IntValue.class).getValue());
			Assert.assertEquals("KeyGroupedIterator returned a wrong value.", new StringValue("B"), rec.getField(1, StringValue.class));
			Assert.assertFalse("KeyGroupedIterator's value iterator must not have another value.", valsIter.hasNext());
			
			Assert.assertTrue("KeyGroupedIterator must have another key.", this.psi.nextKey());
			valsIter = this.psi.getValues();
			Assert.assertNotNull("Returned Iterator must not be null", valsIter);
			Assert.assertTrue("KeyGroupedIterator's value iterator must have another value.", valsIter.hasNext());
			rec = valsIter.next();
			Assert.assertEquals("KeyGroupedIterator returned a wrong key.", 3, rec.getField(0, IntValue.class).getValue());
			Assert.assertEquals("KeyGroupedIterator returned a wrong value.", new StringValue("C"), rec.getField(1, StringValue.class));
			Assert.assertTrue("KeyGroupedIterator's value iterator must have another value.", valsIter.hasNext());
			Assert.assertEquals("KeyGroupedIterator returned a wrong key.", 3, rec.getField(0, IntValue.class).getValue());
			Assert.assertEquals("KeyGroupedIterator returned a wrong value.", new StringValue("C"), rec.getField(1, StringValue.class));
			rec = valsIter.next();
			Assert.assertEquals("KeyGroupedIterator returned a wrong key.", 3, rec.getField(0, IntValue.class).getValue());
			Assert.assertEquals("KeyGroupedIterator returned a wrong value.", new StringValue("D"), rec.getField(1, StringValue.class));
			Assert.assertFalse("KeyGroupedIterator's value iterator must have another value.", valsIter.hasNext());
			Assert.assertEquals("KeyGroupedIterator returned a wrong key.", 3, rec.getField(0, IntValue.class).getValue());
			Assert.assertEquals("KeyGroupedIterator returned a wrong value.", new StringValue("D"), rec.getField(1, StringValue.class));
			Assert.assertFalse("KeyGroupedIterator's value iterator must have another value.", valsIter.hasNext());
			Assert.assertEquals("KeyGroupedIterator returned a wrong key.", 3, rec.getField(0, IntValue.class).getValue());
			Assert.assertEquals("KeyGroupedIterator returned a wrong value.", new StringValue("D"), rec.getField(1, StringValue.class));
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("The test encountered an unexpected exception.");
		}
	}
	
	private static final class IntStringPair
	{
		private final IntValue integer;
		private final StringValue string;

		IntStringPair(IntValue integer, StringValue string) {
			this.integer = integer;
			this.string = string;
		}

		public IntValue getInteger() {
			return integer;
		}

		public StringValue getString() {
			return string;
		}
	}
}
