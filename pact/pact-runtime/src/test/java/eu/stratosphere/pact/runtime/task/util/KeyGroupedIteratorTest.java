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


import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.runtime.util.ReadingIterator;


/**
 * Test for the key grouped iterator, which advances in windows containing the same key and provides a sub-iterator
 * over the records with the same key.
 * 
 * @author Stephan Ewen
 */
public class KeyGroupedIteratorTest
{
	private ReadingIterator<PactRecord> sourceIter;		// the iterator that provides the input
	
	private KeyGroupedIterator psi;						// the grouping iterator, progressing in key steps
	
	
	@SuppressWarnings("unchecked")
	@Before
	public void setup()
	{
		final ArrayList<IntStringPair> source = new ArrayList<IntStringPair>();
		
		// add elements to the source
		source.add(new IntStringPair(new PactInteger(1), new PactString("A")));
		source.add(new IntStringPair(new PactInteger(2), new PactString("B")));
		source.add(new IntStringPair(new PactInteger(3), new PactString("C")));
		source.add(new IntStringPair(new PactInteger(3), new PactString("D")));
		source.add(new IntStringPair(new PactInteger(4), new PactString("E")));
		source.add(new IntStringPair(new PactInteger(4), new PactString("F")));
		source.add(new IntStringPair(new PactInteger(4), new PactString("G")));
		source.add(new IntStringPair(new PactInteger(5), new PactString("H")));
		source.add(new IntStringPair(new PactInteger(5), new PactString("I")));
		source.add(new IntStringPair(new PactInteger(5), new PactString("J")));
		source.add(new IntStringPair(new PactInteger(5), new PactString("K")));
		source.add(new IntStringPair(new PactInteger(5), new PactString("L")));
		
		
		this.sourceIter = new ReadingIterator<PactRecord>() {
			final Iterator<IntStringPair> it = source.iterator();
			
			@Override
			public PactRecord next(PactRecord target) throws IOException {
				if (it.hasNext()) {
					IntStringPair pair = it.next();
					target.setField(0, pair.getInteger());
					target.setField(1, pair.getString());
					return target;
				}
				else {
					return null;
				}
			}
		};
		this.psi = new KeyGroupedIterator(this.sourceIter, new int[] {0}, new Class[] {PactInteger.class});
	}

	@Test
	public void testNextKeyOnly() throws Exception
	{
		Assert.assertTrue("KeyGroupedIterator must have another key.", this.psi.nextKey());
		Assert.assertEquals("KeyGroupedIterator returned a wrong key.", 1, ((PactInteger) this.psi.getKeys()[0]).getValue());
		
		Assert.assertTrue("KeyGroupedIterator must have another key.", this.psi.nextKey());
		Assert.assertEquals("KeyGroupedIterator returned a wrong key.", 2, ((PactInteger) this.psi.getKeys()[0]).getValue());
		
		Assert.assertTrue("KeyGroupedIterator must have another key.", this.psi.nextKey());
		Assert.assertEquals("KeyGroupedIterator returned a wrong key.", 3, ((PactInteger) this.psi.getKeys()[0]).getValue());
		
		Assert.assertTrue("KeyGroupedIterator must have another key.", this.psi.nextKey());
		Assert.assertEquals("KeyGroupedIterator returned a wrong key.", 4, ((PactInteger) this.psi.getKeys()[0]).getValue());
		
		Assert.assertTrue("KeyGroupedIterator must have another key.", this.psi.nextKey());
		Assert.assertEquals("KeyGroupedIterator returned a wrong key.", 5, ((PactInteger) this.psi.getKeys()[0]).getValue());
		
		Assert.assertFalse("KeyGroupedIterator must not have another key.", this.psi.nextKey());
		Assert.assertNull("KeyGroupedIterator must not have another value.", this.psi.getValues());
		
		Assert.assertFalse("KeyGroupedIterator must not have another key.", this.psi.nextKey());
		Assert.assertFalse("KeyGroupedIterator must not have another key.", this.psi.nextKey());
	}
	
	@Test
	public void testFullIterationThroughAllValues() throws IOException
	{
		// Key 1, Value A
		Assert.assertTrue("KeyGroupedIterator must have another key.", this.psi.nextKey());
		Assert.assertTrue("KeyGroupedIterator must have another value.", this.psi.getValues().hasNext());
		Assert.assertEquals("KeyGroupedIterator returned a wrong key.", 1, ((PactInteger) this.psi.getKeys()[0]).getValue());
		Assert.assertEquals("KeyGroupedIterator returned a wrong value.", new PactString("A"), this.psi.getValues().next().getField(1, PactString.class));
		Assert.assertFalse("KeyGroupedIterator must not have another value.", this.psi.getValues().hasNext());
		
		// Key 2, Value B
		Assert.assertTrue("KeyGroupedIterator must have another key.", this.psi.nextKey());
		Assert.assertTrue("KeyGroupedIterator must have another value.", this.psi.getValues().hasNext());
		Assert.assertEquals("KeyGroupedIterator returned a wrong key.", 2, ((PactInteger) this.psi.getKeys()[0]).getValue());
		Assert.assertEquals("KeyGroupedIterator returned a wrong value.", new PactString("B"), this.psi.getValues().next().getField(1, PactString.class));
		Assert.assertFalse("KeyGroupedIterator must not have another value.", this.psi.getValues().hasNext());
		
		// Key 3, Values C, D
		Assert.assertTrue("KeyGroupedIterator must have another key.", this.psi.nextKey());
		Assert.assertTrue("KeyGroupedIterator must have another value.", this.psi.getValues().hasNext());
		Assert.assertEquals("KeyGroupedIterator returned a wrong key.", 3, ((PactInteger) this.psi.getKeys()[0]).getValue());
		Assert.assertEquals("KeyGroupedIterator returned a wrong value.", new PactString("C"), this.psi.getValues().next().getField(1, PactString.class));
		Assert.assertTrue("KeyGroupedIterator must have another value.", this.psi.getValues().hasNext());
		Assert.assertEquals("KeyGroupedIterator returned a wrong key.", 3, ((PactInteger) this.psi.getKeys()[0]).getValue());
		Assert.assertEquals("KeyGroupedIterator returned a wrong value.", new PactString("D"), this.psi.getValues().next().getField(1, PactString.class));
		Assert.assertEquals("KeyGroupedIterator returned a wrong key.", 3, ((PactInteger) this.psi.getKeys()[0]).getValue());
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
		Assert.assertEquals("KeyGroupedIterator returned a wrong key.", 3, ((PactInteger) this.psi.getKeys()[0]).getValue());
		
		// Key 4, Values E, F, G
		Assert.assertTrue("KeyGroupedIterator must have another key.", this.psi.nextKey());
		Assert.assertTrue("KeyGroupedIterator must have another value.", this.psi.getValues().hasNext());
		Assert.assertEquals("KeyGroupedIterator returned a wrong key.", 4, ((PactInteger) this.psi.getKeys()[0]).getValue());
		Assert.assertEquals("KeyGroupedIterator returned a wrong value.", new PactString("E"), this.psi.getValues().next().getField(1, PactString.class));
		Assert.assertTrue("KeyGroupedIterator must have another value.", this.psi.getValues().hasNext());
		Assert.assertEquals("KeyGroupedIterator returned a wrong key.", 4, ((PactInteger) this.psi.getKeys()[0]).getValue());
		Assert.assertEquals("KeyGroupedIterator returned a wrong value.", new PactString("F"), this.psi.getValues().next().getField(1, PactString.class));
		Assert.assertTrue("KeyGroupedIterator must have another value.", this.psi.getValues().hasNext());
		Assert.assertEquals("KeyGroupedIterator returned a wrong key.", 4, ((PactInteger) this.psi.getKeys()[0]).getValue());
		Assert.assertEquals("KeyGroupedIterator returned a wrong value.", new PactString("G"), this.psi.getValues().next().getField(1, PactString.class));
		Assert.assertEquals("KeyGroupedIterator returned a wrong key.", 4, ((PactInteger) this.psi.getKeys()[0]).getValue());
		Assert.assertFalse("KeyGroupedIterator must not have another value.", this.psi.getValues().hasNext());
		Assert.assertEquals("KeyGroupedIterator returned a wrong key.", 4, ((PactInteger) this.psi.getKeys()[0]).getValue());
		
		// Key 5, Values H, I, J, K, L
		Assert.assertTrue("KeyGroupedIterator must have another key.", this.psi.nextKey());
		Assert.assertTrue("KeyGroupedIterator must have another value.", this.psi.getValues().hasNext());
		Assert.assertEquals("KeyGroupedIterator returned a wrong key.", 5, ((PactInteger) this.psi.getKeys()[0]).getValue());
		Assert.assertEquals("KeyGroupedIterator returned a wrong value.", new PactString("H"), this.psi.getValues().next().getField(1, PactString.class));
		Assert.assertTrue("KeyGroupedIterator must have another value.", this.psi.getValues().hasNext());
		Assert.assertEquals("KeyGroupedIterator returned a wrong key.", 5, ((PactInteger) this.psi.getKeys()[0]).getValue());
		Assert.assertEquals("KeyGroupedIterator returned a wrong value.", new PactString("I"), this.psi.getValues().next().getField(1, PactString.class));
		Assert.assertTrue("KeyGroupedIterator must have another value.", this.psi.getValues().hasNext());
		Assert.assertEquals("KeyGroupedIterator returned a wrong key.", 5, ((PactInteger) this.psi.getKeys()[0]).getValue());
		Assert.assertEquals("KeyGroupedIterator returned a wrong value.", new PactString("J"), this.psi.getValues().next().getField(1, PactString.class));
		Assert.assertTrue("KeyGroupedIterator must have another value.", this.psi.getValues().hasNext());
		Assert.assertEquals("KeyGroupedIterator returned a wrong key.", 5, ((PactInteger) this.psi.getKeys()[0]).getValue());
		Assert.assertEquals("KeyGroupedIterator returned a wrong value.", new PactString("K"), this.psi.getValues().next().getField(1, PactString.class));
		Assert.assertTrue("KeyGroupedIterator must have another value.", this.psi.getValues().hasNext());
		Assert.assertEquals("KeyGroupedIterator returned a wrong key.", 5, ((PactInteger) this.psi.getKeys()[0]).getValue());
		Assert.assertEquals("KeyGroupedIterator returned a wrong value.", new PactString("L"), this.psi.getValues().next().getField(1, PactString.class));
		Assert.assertEquals("KeyGroupedIterator returned a wrong key.", 5, ((PactInteger) this.psi.getKeys()[0]).getValue());
		try {
			this.psi.getValues().next();
			Assert.fail("A new KeyGroupedIterator must not have any value available and hence throw an exception on next().");
		}
		catch (NoSuchElementException nseex) {}
		Assert.assertFalse("KeyGroupedIterator must not have another value.", this.psi.getValues().hasNext());
		Assert.assertEquals("KeyGroupedIterator returned a wrong key.", 5, ((PactInteger) this.psi.getKeys()[0]).getValue());
		try {
			this.psi.getValues().next();
			Assert.fail("A new KeyGroupedIterator must not have any value available and hence throw an exception on next().");
		}
		catch (NoSuchElementException nseex) {}
		
		Assert.assertFalse("KeyGroupedIterator must not have another key.", this.psi.nextKey());
		Assert.assertFalse("KeyGroupedIterator must not have another key.", this.psi.nextKey());
	}
	
	@Test
	public void testMixedProgress() throws Exception
	{
		// Progression only via nextKey() and hasNext() - Key 1, Value A
		Assert.assertTrue("KeyGroupedIterator must have another key.", this.psi.nextKey());
		Assert.assertTrue("KeyGroupedIterator must have another value.", this.psi.getValues().hasNext());
		
		// Progression only through nextKey() - Key 2, Value B
		Assert.assertTrue("KeyGroupedIterator must have another key.", this.psi.nextKey());
		
		// Progression first though haNext() and next(), then through hasNext() - Key 3, Values C, D
		Assert.assertTrue("KeyGroupedIterator must have another key.", this.psi.nextKey());
		Assert.assertTrue("KeyGroupedIterator must have another value.", this.psi.getValues().hasNext());
		Assert.assertEquals("KeyGroupedIterator returned a wrong key.", 3, ((PactInteger) this.psi.getKeys()[0]).getValue());
		Assert.assertEquals("KeyGroupedIterator returned a wrong value.", new PactString("C"), this.psi.getValues().next().getField(1, PactString.class));
		Assert.assertTrue("KeyGroupedIterator must have another value.", this.psi.getValues().hasNext());
		Assert.assertEquals("KeyGroupedIterator returned a wrong key.", 3, ((PactInteger) this.psi.getKeys()[0]).getValue());
		
		// Progression first via next() only, then hasNext() only Key 4, Values E, F, G
		Assert.assertTrue("KeyGroupedIterator must have another key.", this.psi.nextKey());
		Assert.assertEquals("KeyGroupedIterator returned a wrong value.", new PactString("E"), this.psi.getValues().next().getField(1, PactString.class));
		Assert.assertTrue("KeyGroupedIterator must have another value.", this.psi.getValues().hasNext());
		
		// Key 5, Values H, I, J, K, L
		Assert.assertTrue("KeyGroupedIterator must have another key.", this.psi.nextKey());
		Assert.assertEquals("KeyGroupedIterator returned a wrong value.", new PactString("H"), this.psi.getValues().next().getField(1, PactString.class));
		Assert.assertTrue("KeyGroupedIterator must have another value.", this.psi.getValues().hasNext());
		Assert.assertEquals("KeyGroupedIterator returned a wrong key.", 5, ((PactInteger) this.psi.getKeys()[0]).getValue());
		Assert.assertEquals("KeyGroupedIterator returned a wrong value.", new PactString("I"), this.psi.getValues().next().getField(1, PactString.class));
		Assert.assertTrue("KeyGroupedIterator must have another value.", this.psi.getValues().hasNext());
		
		// end
		Assert.assertFalse("KeyGroupedIterator must not have another key.", this.psi.nextKey());
		Assert.assertFalse("KeyGroupedIterator must not have another key.", this.psi.nextKey());
	}
	
	private static final class IntStringPair
	{
		private final PactInteger integer;
		private final PactString string;

		IntStringPair(PactInteger integer, PactString string) {
			this.integer = integer;
			this.string = string;
		}

		public PactInteger getInteger() {
			return integer;
		}

		public PactString getString() {
			return string;
		}
	}
}
