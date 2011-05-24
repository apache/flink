package eu.stratosphere.pact.runtime.hash;


import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.runtime.hash.HashJoin.ProbeSideIterator;



/**
 * @author Stephan Ewen
 */
public class ProbeSideIteratorTest
{
	private Iterator<KeyValuePair<PactInteger, PactString>> source;		// the iterator that provides the input
	
	private ProbeSideIterator<PactInteger, PactString> psi;				// the probe iterator, progressing in key steps
	
	
	@Before
	public void setup()
	{
		ArrayList<KeyValuePair<PactInteger, PactString>> source = new ArrayList<KeyValuePair<PactInteger,PactString>>();
		
		// add elements to the source
		source.add(new KeyValuePair<PactInteger, PactString>(new PactInteger(1), new PactString("A")));
		source.add(new KeyValuePair<PactInteger, PactString>(new PactInteger(2), new PactString("B")));
		source.add(new KeyValuePair<PactInteger, PactString>(new PactInteger(3), new PactString("C")));
		source.add(new KeyValuePair<PactInteger, PactString>(new PactInteger(3), new PactString("D")));
		source.add(new KeyValuePair<PactInteger, PactString>(new PactInteger(4), new PactString("E")));
		source.add(new KeyValuePair<PactInteger, PactString>(new PactInteger(4), new PactString("F")));
		source.add(new KeyValuePair<PactInteger, PactString>(new PactInteger(4), new PactString("G")));
		source.add(new KeyValuePair<PactInteger, PactString>(new PactInteger(5), new PactString("H")));
		source.add(new KeyValuePair<PactInteger, PactString>(new PactInteger(5), new PactString("I")));
		source.add(new KeyValuePair<PactInteger, PactString>(new PactInteger(5), new PactString("J")));
		source.add(new KeyValuePair<PactInteger, PactString>(new PactInteger(5), new PactString("K")));
		source.add(new KeyValuePair<PactInteger, PactString>(new PactInteger(5), new PactString("L")));
		
		this.source = source.iterator();
		this.psi = new ProbeSideIterator<PactInteger, PactString>(this.source);
	}
	
	@Test
	public void testNewNoHasNext()
	{
		Assert.assertFalse("A new ProbeSideIterator must not have any value available before nextKey() is called.", this.psi.hasNext()); 
	}
	
	@Test
	public void testNewNextException()
	{
		try {
			this.psi.next();
			Assert.fail("A new ProbeSideIterator must not have any value available and hence throw an exception on next().");
		}
		catch (NoSuchElementException nseex) {
			// all right!
		}
	}
	
	@Test
	public void testNextKeyOnly()
	{
		Assert.assertTrue("ProbeSideIterator must have another key.", this.psi.nextKey());
		Assert.assertEquals("ProbeSideIterator returned a wrong key.", 1, this.psi.getCurrentKey().getValue());
		
		Assert.assertTrue("ProbeSideIterator must have another key.", this.psi.nextKey());
		Assert.assertEquals("ProbeSideIterator returned a wrong key.", 2, this.psi.getCurrentKey().getValue());
		
		Assert.assertTrue("ProbeSideIterator must have another key.", this.psi.nextKey());
		Assert.assertEquals("ProbeSideIterator returned a wrong key.", 3, this.psi.getCurrentKey().getValue());
		
		Assert.assertTrue("ProbeSideIterator must have another key.", this.psi.nextKey());
		Assert.assertEquals("ProbeSideIterator returned a wrong key.", 4, this.psi.getCurrentKey().getValue());
		
		Assert.assertTrue("ProbeSideIterator must have another key.", this.psi.nextKey());
		Assert.assertEquals("ProbeSideIterator returned a wrong key.", 5, this.psi.getCurrentKey().getValue());
		
		Assert.assertFalse("ProbeSideIterator must not have another key.", this.psi.nextKey());
		Assert.assertFalse("ProbeSideIterator must not have another value.", this.psi.hasNext());
		
		try {
			this.psi.next();
			Assert.fail("A new ProbeSideIterator must not have any value available and hence throw an exception on next().");
		}
		catch (NoSuchElementException nseex) {}
		
		Assert.assertFalse("ProbeSideIterator must not have another key.", this.psi.nextKey());
		Assert.assertFalse("ProbeSideIterator must not have another key.", this.psi.nextKey());
	}
	
	@Test
	public void testFullIterationThroughAllValues()
	{
		// Key 1, Value A
		Assert.assertTrue("ProbeSideIterator must have another key.", this.psi.nextKey());
		Assert.assertTrue("ProbeSideIterator must have another value.", this.psi.hasNext());
		Assert.assertEquals("ProbeSideIterator returned a wrong key.", 1, this.psi.getCurrentKey().getValue());
		Assert.assertEquals("ProbeSideIterator returned a wrong value.", new PactString("A"), this.psi.next());
		Assert.assertFalse("ProbeSideIterator must not have another value.", this.psi.hasNext());
		
		// Key 2, Value B
		Assert.assertTrue("ProbeSideIterator must have another key.", this.psi.nextKey());
		Assert.assertTrue("ProbeSideIterator must have another value.", this.psi.hasNext());
		Assert.assertEquals("ProbeSideIterator returned a wrong key.", 2, this.psi.getCurrentKey().getValue());
		Assert.assertEquals("ProbeSideIterator returned a wrong value.", new PactString("B"), this.psi.next());
		Assert.assertFalse("ProbeSideIterator must not have another value.", this.psi.hasNext());
		
		// Key 3, Values C, D
		Assert.assertTrue("ProbeSideIterator must have another key.", this.psi.nextKey());
		Assert.assertTrue("ProbeSideIterator must have another value.", this.psi.hasNext());
		Assert.assertEquals("ProbeSideIterator returned a wrong key.", 3, this.psi.getCurrentKey().getValue());
		Assert.assertEquals("ProbeSideIterator returned a wrong value.", new PactString("C"), this.psi.next());
		Assert.assertTrue("ProbeSideIterator must have another value.", this.psi.hasNext());
		Assert.assertEquals("ProbeSideIterator returned a wrong key.", 3, this.psi.getCurrentKey().getValue());
		Assert.assertEquals("ProbeSideIterator returned a wrong value.", new PactString("D"), this.psi.next());
		Assert.assertEquals("ProbeSideIterator returned a wrong key.", 3, this.psi.getCurrentKey().getValue());
		try {
			this.psi.next();
			Assert.fail("A new ProbeSideIterator must not have any value available and hence throw an exception on next().");
		}
		catch (NoSuchElementException nseex) {}
		Assert.assertFalse("ProbeSideIterator must not have another value.", this.psi.hasNext());
		try {
			this.psi.next();
			Assert.fail("A new ProbeSideIterator must not have any value available and hence throw an exception on next().");
		}
		catch (NoSuchElementException nseex) {}
		Assert.assertEquals("ProbeSideIterator returned a wrong key.", 3, this.psi.getCurrentKey().getValue());
		
		// Key 4, Values E, F, G
		Assert.assertTrue("ProbeSideIterator must have another key.", this.psi.nextKey());
		Assert.assertTrue("ProbeSideIterator must have another value.", this.psi.hasNext());
		Assert.assertEquals("ProbeSideIterator returned a wrong key.", 4, this.psi.getCurrentKey().getValue());
		Assert.assertEquals("ProbeSideIterator returned a wrong value.", new PactString("E"), this.psi.next());
		Assert.assertTrue("ProbeSideIterator must have another value.", this.psi.hasNext());
		Assert.assertEquals("ProbeSideIterator returned a wrong key.", 4, this.psi.getCurrentKey().getValue());
		Assert.assertEquals("ProbeSideIterator returned a wrong value.", new PactString("F"), this.psi.next());
		Assert.assertTrue("ProbeSideIterator must have another value.", this.psi.hasNext());
		Assert.assertEquals("ProbeSideIterator returned a wrong key.", 4, this.psi.getCurrentKey().getValue());
		Assert.assertEquals("ProbeSideIterator returned a wrong value.", new PactString("G"), this.psi.next());
		Assert.assertEquals("ProbeSideIterator returned a wrong key.", 4, this.psi.getCurrentKey().getValue());
		Assert.assertFalse("ProbeSideIterator must not have another value.", this.psi.hasNext());
		Assert.assertEquals("ProbeSideIterator returned a wrong key.", 4, this.psi.getCurrentKey().getValue());
		
		// Key 5, Values H, I, J, K, L
		Assert.assertTrue("ProbeSideIterator must have another key.", this.psi.nextKey());
		Assert.assertTrue("ProbeSideIterator must have another value.", this.psi.hasNext());
		Assert.assertEquals("ProbeSideIterator returned a wrong key.", 5, this.psi.getCurrentKey().getValue());
		Assert.assertEquals("ProbeSideIterator returned a wrong value.", new PactString("H"), this.psi.next());
		Assert.assertTrue("ProbeSideIterator must have another value.", this.psi.hasNext());
		Assert.assertEquals("ProbeSideIterator returned a wrong key.", 5, this.psi.getCurrentKey().getValue());
		Assert.assertEquals("ProbeSideIterator returned a wrong value.", new PactString("I"), this.psi.next());
		Assert.assertTrue("ProbeSideIterator must have another value.", this.psi.hasNext());
		Assert.assertEquals("ProbeSideIterator returned a wrong key.", 5, this.psi.getCurrentKey().getValue());
		Assert.assertEquals("ProbeSideIterator returned a wrong value.", new PactString("J"), this.psi.next());
		Assert.assertTrue("ProbeSideIterator must have another value.", this.psi.hasNext());
		Assert.assertEquals("ProbeSideIterator returned a wrong key.", 5, this.psi.getCurrentKey().getValue());
		Assert.assertEquals("ProbeSideIterator returned a wrong value.", new PactString("K"), this.psi.next());
		Assert.assertTrue("ProbeSideIterator must have another value.", this.psi.hasNext());
		Assert.assertEquals("ProbeSideIterator returned a wrong key.", 5, this.psi.getCurrentKey().getValue());
		Assert.assertEquals("ProbeSideIterator returned a wrong value.", new PactString("L"), this.psi.next());
		Assert.assertEquals("ProbeSideIterator returned a wrong key.", 5, this.psi.getCurrentKey().getValue());
		try {
			this.psi.next();
			Assert.fail("A new ProbeSideIterator must not have any value available and hence throw an exception on next().");
		}
		catch (NoSuchElementException nseex) {}
		Assert.assertFalse("ProbeSideIterator must not have another value.", this.psi.hasNext());
		Assert.assertEquals("ProbeSideIterator returned a wrong key.", 5, this.psi.getCurrentKey().getValue());
		try {
			this.psi.next();
			Assert.fail("A new ProbeSideIterator must not have any value available and hence throw an exception on next().");
		}
		catch (NoSuchElementException nseex) {}
		
		// end
		Assert.assertFalse("ProbeSideIterator must not have another key.", this.psi.nextKey());
		try {
			this.psi.next();
			Assert.fail("A new ProbeSideIterator must not have any value available and hence throw an exception on next().");
		}
		catch (NoSuchElementException nseex) {}
		Assert.assertFalse("ProbeSideIterator must not have another value.", this.psi.hasNext());
		try {
			this.psi.next();
			Assert.fail("A new ProbeSideIterator must not have any value available and hence throw an exception on next().");
		}
		catch (NoSuchElementException nseex) {}
		
		Assert.assertFalse("ProbeSideIterator must not have another key.", this.psi.nextKey());
		Assert.assertFalse("ProbeSideIterator must not have another key.", this.psi.nextKey());
	}
	
	@Test
	public void testMixedProgress()
	{
		// Progression only via nextKey() and hasNext() - Key 1, Value A
		Assert.assertTrue("ProbeSideIterator must have another key.", this.psi.nextKey());
		Assert.assertTrue("ProbeSideIterator must have another value.", this.psi.hasNext());
		
		// Progression only through nextKey() - Key 2, Value B
		Assert.assertTrue("ProbeSideIterator must have another key.", this.psi.nextKey());
		
		// Progression first though haNext() and next(), then through hasNext() - Key 3, Values C, D
		Assert.assertTrue("ProbeSideIterator must have another key.", this.psi.nextKey());
		Assert.assertTrue("ProbeSideIterator must have another value.", this.psi.hasNext());
		Assert.assertEquals("ProbeSideIterator returned a wrong key.", 3, this.psi.getCurrentKey().getValue());
		Assert.assertEquals("ProbeSideIterator returned a wrong value.", new PactString("C"), this.psi.next());
		Assert.assertTrue("ProbeSideIterator must have another value.", this.psi.hasNext());
		Assert.assertEquals("ProbeSideIterator returned a wrong key.", 3, this.psi.getCurrentKey().getValue());
		
		// Progression first via next() only, then hasNext() only Key 4, Values E, F, G
		Assert.assertTrue("ProbeSideIterator must have another key.", this.psi.nextKey());
		Assert.assertEquals("ProbeSideIterator returned a wrong value.", new PactString("E"), this.psi.next());
		Assert.assertTrue("ProbeSideIterator must have another value.", this.psi.hasNext());
		
		// Key 5, Values H, I, J, K, L
		Assert.assertTrue("ProbeSideIterator must have another key.", this.psi.nextKey());
		Assert.assertEquals("ProbeSideIterator returned a wrong value.", new PactString("H"), this.psi.next());
		Assert.assertTrue("ProbeSideIterator must have another value.", this.psi.hasNext());
		Assert.assertEquals("ProbeSideIterator returned a wrong key.", 5, this.psi.getCurrentKey().getValue());
		Assert.assertEquals("ProbeSideIterator returned a wrong value.", new PactString("I"), this.psi.next());
		Assert.assertTrue("ProbeSideIterator must have another value.", this.psi.hasNext());
		
		// end
		Assert.assertFalse("ProbeSideIterator must not have another key.", this.psi.nextKey());
		try {
			this.psi.next();
			Assert.fail("A new ProbeSideIterator must not have any value available and hence throw an exception on next().");
		}
		catch (NoSuchElementException nseex) {}
		Assert.assertFalse("ProbeSideIterator must not have another value.", this.psi.hasNext());
		try {
			this.psi.next();
			Assert.fail("A new ProbeSideIterator must not have any value available and hence throw an exception on next().");
		}
		catch (NoSuchElementException nseex) {}
		
		Assert.assertFalse("ProbeSideIterator must not have another key.", this.psi.nextKey());
		Assert.assertFalse("ProbeSideIterator must not have another key.", this.psi.nextKey());
	}
}
