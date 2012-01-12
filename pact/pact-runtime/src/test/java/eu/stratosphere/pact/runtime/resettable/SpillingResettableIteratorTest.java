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

package eu.stratosphere.pact.runtime.resettable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.nephele.services.ServiceException;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.services.memorymanager.spi.DefaultMemoryManager;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.runtime.resettable.SpillingResettableIterator;
import eu.stratosphere.pact.runtime.test.util.DummyInvokable;
import junit.framework.Assert;

public class SpillingResettableIteratorTest {

	private static final int NUM_TESTRECORDS = 50000;

	private static final int MEMORY_CAPACITY = 10 * 1024 * 1024;

	private IOManager ioman;

	private MemoryManager memman;

	private Iterator<PactInteger> reader;

	private List<PactInteger> objects;


	@Before
	public void startup() {
		// set up IO and memory manager
		this.memman = new DefaultMemoryManager(MEMORY_CAPACITY);
		this.ioman = new IOManager();

		// create test objects
		this.objects = new ArrayList<PactInteger>(NUM_TESTRECORDS);

		for (int i = 0; i < NUM_TESTRECORDS; ++i) {
			PactInteger tmp = new PactInteger(i);
			this.objects.add(tmp);
		}
	}

	@After
	public void shutdown() {
		this.objects = null;

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
	 * 
	 * @throws ServiceException
	 * @throws InterruptedException
	 */
	@Test
	public void testResettableIterator() throws ServiceException, InterruptedException {
		final AbstractInvokable memOwner = new DummyInvokable();

		// create the reader
		reader = objects.iterator();
		// create the resettable Iterator
		SpillingResettableIterator<PactInteger> iterator = new SpillingResettableIterator<PactInteger>(memman, ioman,
			reader, new PactInteger(),
			SpillingResettableIterator.MIN_BUFFER_SIZE * SpillingResettableIterator.MINIMUM_NUMBER_OF_BUFFERS, memOwner);
		// open the iterator
		try {
			iterator.open();
		} catch (IOException e) {
			Assert.fail("Could not open resettable iterator:" + e.getMessage());
		}
		// now test walking through the iterator
		int count = 0;
		while (iterator.hasNext())
			Assert.assertEquals("In initial run, element " + count + " does not match expected value!", count++,
				iterator.next().getValue());
		Assert.assertEquals("Too few elements were deserialzied in initial run!", NUM_TESTRECORDS, count);
		// test resetting the iterator a few times
		for (int j = 0; j < 10; ++j) {
			count = 0;
			iterator.reset();
			// now we should get the same results
			while (iterator.hasNext())
				Assert.assertEquals("After reset nr. " + j + 1 + " element " + count
					+ " does not match expected value!", count++, iterator.next().getValue());
			Assert.assertEquals("Too few elements were deserialzied after reset nr. " + j + 1 + "!", NUM_TESTRECORDS,
				count);
		}
		// close the iterator
		iterator.close();
	}

	/**
	 * Tests the resettable iterator with enough memory so that all data
	 * is kept locally in a membuffer.
	 * 
	 * @throws ServiceException
	 * @throws InterruptedException
	 */
	@Test
	public void testResettableIteratorInMemory() throws ServiceException, InterruptedException {
		final AbstractInvokable memOwner = new DummyInvokable();

		// create the reader
		reader = objects.iterator();
		// create the resettable iterator
		SpillingResettableIterator<PactInteger> iterator = new SpillingResettableIterator<PactInteger>(memman, ioman,
			reader, new PactInteger(), 
			SpillingResettableIterator.MIN_BUFFER_SIZE * SpillingResettableIterator.MINIMUM_NUMBER_OF_BUFFERS * 10, memOwner);
		// open the iterator
		try {
			iterator.open();
		} catch (IOException e) {
			Assert.fail("Could not open resettable iterator:" + e.getMessage());
		}
		// now test walking through the iterator
		int count = 0;
		while (iterator.hasNext())
			Assert.assertEquals("In initial run, element " + count + " does not match expected value!", count++,
				iterator.next().getValue());
		Assert.assertEquals("Too few elements were deserialzied in initial run!", NUM_TESTRECORDS, count);
		// test resetting the iterator a few times
		for (int j = 0; j < 10; ++j) {
			count = 0;
			iterator.reset();
			// now we should get the same results
			while (iterator.hasNext())
				Assert.assertEquals("After reset nr. " + j + 1 + " element " + count
					+ " does not match expected value!", count++, iterator.next().getValue());
			Assert.assertEquals("Too few elements were deserialzied after reset nr. " + j + 1 + "!", NUM_TESTRECORDS,
				count);
		}
		// close the iterator
		iterator.close();
	}

	/**
	 * Tests whether multiple call of hasNext() changes the state of the iterator
	 */
	@Test
	public void testHasNext() throws ServiceException, InterruptedException {
		final AbstractInvokable memOwner = new DummyInvokable();

		// create the reader
		reader = objects.iterator();
		// create the resettable Iterator
		SpillingResettableIterator<PactInteger> iterator = new SpillingResettableIterator<PactInteger>(memman, ioman,
			reader, new PactInteger(),
			SpillingResettableIterator.MIN_BUFFER_SIZE * SpillingResettableIterator.MINIMUM_NUMBER_OF_BUFFERS, memOwner);
		// open the iterator
		try {
			iterator.open();
		} catch (IOException e) {
			Assert.fail("Could not open resettable iterator:" + e.getMessage());
		}

		int cnt = 0;
		while (iterator.hasNext()) {
			iterator.hasNext();
			iterator.next();
			cnt++;
		}

		Assert.assertTrue(cnt + " elements read from iterator, but " + NUM_TESTRECORDS + " expected",
			cnt == NUM_TESTRECORDS);
		
		iterator.close();
	}

	/**
	 * Test whether next() depends on previous call of hasNext()
	 * 
	 * @throws ServiceException
	 * @throws InterruptedException
	 */
	@Test
	public void testNext() throws ServiceException, InterruptedException {
		final AbstractInvokable memOwner = new DummyInvokable();

		// create the reader
		reader = objects.iterator();
		// create the resettable Iterator
		SpillingResettableIterator<PactInteger> iterator = new SpillingResettableIterator<PactInteger>(memman, ioman,
			reader, new PactInteger(),
			SpillingResettableIterator.MIN_BUFFER_SIZE * SpillingResettableIterator.MINIMUM_NUMBER_OF_BUFFERS, memOwner);
		// open the iterator
		try {
			iterator.open();
		} catch (IOException e) {
			Assert.fail("Could not open resettable iterator:" + e.getMessage());
		}

		PactInteger record;
		int cnt = 0;
		while (cnt < NUM_TESTRECORDS) {
			record = iterator.next();
			Assert.assertTrue("Record was not read from iterator", record != null);
			cnt++;
		}

		record = iterator.next();
		Assert.assertTrue("Too many records were read from iterator", record == null);
		
		iterator.close();
	}

	/**
	 * Tests whether lastReturned() returns the latest returned element read from spilled file.
	 */
	@Test
	public void testRepeatLast() throws ServiceException, InterruptedException {
		final AbstractInvokable memOwner = new DummyInvokable();

		// create the reader
		reader = objects.iterator();
		// create the resettable Iterator
		SpillingResettableIterator<PactInteger> iterator = new SpillingResettableIterator<PactInteger>(memman, ioman,
			reader, new PactInteger(), 
			SpillingResettableIterator.MIN_BUFFER_SIZE * SpillingResettableIterator.MINIMUM_NUMBER_OF_BUFFERS, memOwner);
		// open the iterator
		try {
			iterator.open();
		} catch (IOException e) {
			Assert.fail("Could not open resettable iterator:" + e.getMessage());
		}

		PactInteger record1;
		PactInteger record2;
		PactInteger compare;
		int cnt = 0;
		while (iterator.hasNext()) {

			record1 = iterator.next();
			record2 = iterator.repeatLast();
			compare = objects.get(cnt);

			Assert.assertTrue("Record read with next() does not equal expected value", record1.equals(compare));
			Assert.assertTrue("Record read with next() does not equal record read with lastReturned()",
				record1.equals(record2));
			cnt++;
		}

		Assert.assertTrue(cnt + " elements read from iterator, but " + NUM_TESTRECORDS + " expected",
			cnt == NUM_TESTRECORDS);
		
		iterator.close();
	}

	/**
	 * Tests whether lastReturned() returns the latest returned element read from memory.
	 */
	@Test
	public void testRepeatLastInMemory() throws ServiceException, InterruptedException {
		final AbstractInvokable memOwner = new DummyInvokable();

		// create the reader
		reader = objects.iterator();
		// create the resettable Iterator
		SpillingResettableIterator<PactInteger> iterator = new SpillingResettableIterator<PactInteger>(memman, ioman,
			reader, new PactInteger(),
			SpillingResettableIterator.MIN_BUFFER_SIZE * SpillingResettableIterator.MINIMUM_NUMBER_OF_BUFFERS * 10, memOwner);
		// open the iterator
		try {
			iterator.open();
		} catch (IOException e) {
			Assert.fail("Could not open resettable iterator:" + e.getMessage());
		}

		PactInteger record1;
		PactInteger record2;
		PactInteger compare;
		int cnt = 0;
		while (iterator.hasNext()) {

			record1 = iterator.next();
			record2 = iterator.repeatLast();
			compare = objects.get(cnt);

			Assert.assertTrue("Record read with next() does not equal expected value", record1.equals(compare));
			Assert.assertTrue("Record read with next() does not equal record read with lastReturned()",
				record1.equals(record2));
			cnt++;
		}

		Assert.assertTrue(cnt + " elements read from iterator, but " + NUM_TESTRECORDS + " expected",
			cnt == NUM_TESTRECORDS);

		iterator.close();
	}
}