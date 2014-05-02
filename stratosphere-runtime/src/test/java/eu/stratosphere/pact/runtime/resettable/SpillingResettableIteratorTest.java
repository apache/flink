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

package eu.stratosphere.pact.runtime.resettable;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.services.memorymanager.spi.DefaultMemoryManager;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.pact.runtime.test.util.DummyInvokable;
import eu.stratosphere.pact.runtime.test.util.types.IntValueSerializer;
import eu.stratosphere.types.IntValue;

public class SpillingResettableIteratorTest {

	private static final int NUM_TESTRECORDS = 50000;

	private static final int MEMORY_CAPACITY = 10 * 1024 * 1024;
	
	private final AbstractInvokable memOwner = new DummyInvokable();

	private IOManager ioman;

	private MemoryManager memman;

	private Iterator<IntValue> reader;
	
	private final TypeSerializer<IntValue> serializer = new IntValueSerializer();

	
	@Before
	public void startup() {
		// set up IO and memory manager
		this.memman = new DefaultMemoryManager(MEMORY_CAPACITY, 32 * 1024);
		this.ioman = new IOManager();

		// create test objects
		ArrayList<IntValue> objects = new ArrayList<IntValue>(NUM_TESTRECORDS);

		for (int i = 0; i < NUM_TESTRECORDS; ++i) {
			IntValue tmp = new IntValue(i);
			objects.add(tmp);
		}
		this.reader = objects.iterator();
	}

	@After
	public void shutdown() {
		this.ioman.shutdown();
		if (!this.ioman.isProperlyShutDown()) {
			Assert.fail("I/O Manager Shutdown was not completed properly.");
		}
		this.ioman = null;

		if (!this.memman.verifyEmpty()) {
			Assert.fail("A memory leak has occurred: Not all memory was properly returned to the memory manager.");
		}
		this.memman.shutdown();
		this.memman = null;
	}

	/**
	 * Tests the resettable iterator with too few memory, so that the data
	 * has to be written to disk.
	 */
	@Test
	public void testResettableIterator() {
		try {
			// create the resettable Iterator
			SpillingResettableIterator<IntValue> iterator = new SpillingResettableIterator<IntValue>(
					this.reader, this.serializer, this.memman, this.ioman, 2, this.memOwner);
			// open the iterator
			iterator.open();

			// now test walking through the iterator
			int count = 0;
			while (iterator.hasNext()) {
				Assert.assertEquals("In initial run, element " + count + " does not match expected value!", count++,
					iterator.next().getValue());
			}
			Assert.assertEquals("Too few elements were deserialzied in initial run!", NUM_TESTRECORDS, count);
			// test resetting the iterator a few times
			for (int j = 0; j < 10; ++j) {
				count = 0;
				iterator.reset();
				// now we should get the same results
				while (iterator.hasNext()) {
					Assert.assertEquals("After reset nr. " + j + 1 + " element " + count
						+ " does not match expected value!", count++, iterator.next().getValue());
				}
				Assert.assertEquals("Too few elements were deserialzied after reset nr. " + j + 1 + "!", NUM_TESTRECORDS,
					count);
			}
			// close the iterator
			iterator.close();
		} catch (Exception ex)  {
			ex.printStackTrace();
			Assert.fail("Test encountered an exception.");
		}
	}

	/**
	 * Tests the resettable iterator with enough memory so that all data
	 * is kept locally in a membuffer.
	 */
	@Test
	public void testResettableIteratorInMemory() {
		try {
			// create the resettable Iterator
			SpillingResettableIterator<IntValue> iterator = new SpillingResettableIterator<IntValue>(
					this.reader, this.serializer, this.memman, this.ioman, 20, this.memOwner);
			// open the iterator
			iterator.open();

			// now test walking through the iterator
			int count = 0;
			while (iterator.hasNext()) {
				Assert.assertEquals("In initial run, element " + count + " does not match expected value!", count++,
					iterator.next().getValue());
			}
			Assert.assertEquals("Too few elements were deserialzied in initial run!", NUM_TESTRECORDS, count);
			// test resetting the iterator a few times
			for (int j = 0; j < 10; ++j) {
				count = 0;
				iterator.reset();
				// now we should get the same results
				while (iterator.hasNext()) {
					Assert.assertEquals("After reset nr. " + j + 1 + " element " + count
						+ " does not match expected value!", count++, iterator.next().getValue());
				}
				Assert.assertEquals("Too few elements were deserialzied after reset nr. " + j + 1 + "!", NUM_TESTRECORDS,
					count);
			}
			// close the iterator
			iterator.close();
		} catch (Exception ex)  {
			ex.printStackTrace();
			Assert.fail("Test encountered an exception.");
		}
	}

	/**
	 * Tests whether multiple call of hasNext() changes the state of the iterator
	 */
	@Test
	public void testHasNext() {
		try {
			// create the resettable Iterator
			SpillingResettableIterator<IntValue> iterator = new SpillingResettableIterator<IntValue>(
					this.reader, this.serializer, this.memman, this.ioman, 2, this.memOwner);
			// open the iterator
			iterator.open();
	
			int cnt = 0;
			while (iterator.hasNext()) {
				iterator.hasNext();
				iterator.next();
				cnt++;
			}
	
			Assert.assertTrue(cnt + " elements read from iterator, but " + NUM_TESTRECORDS + " expected",
				cnt == NUM_TESTRECORDS);
			
			iterator.close();
		} catch (Exception ex)  {
			ex.printStackTrace();
			Assert.fail("Test encountered an exception.");
		}
	}

	/**
	 * Test whether next() depends on previous call of hasNext()
	 */
	@Test
	public void testNext() {
		try {
			// create the resettable Iterator
			SpillingResettableIterator<IntValue> iterator = new SpillingResettableIterator<IntValue>(
					this.reader, this.serializer, this.memman, this.ioman, 2, this.memOwner);
			// open the iterator
			iterator.open();
	
			IntValue record;
			int cnt = 0;
			while (cnt < NUM_TESTRECORDS) {
				record = iterator.next();
				Assert.assertTrue("Record was not read from iterator", record != null);
				cnt++;
			}
	
			try {
				record = iterator.next();
				Assert.fail("Too many records were read from iterator.");
			} catch (NoSuchElementException nseex) {
				// expected
			}
			
			iterator.close();
		} catch (Exception ex)  {
			ex.printStackTrace();
			Assert.fail("Test encountered an exception.");
		}
	}
}
