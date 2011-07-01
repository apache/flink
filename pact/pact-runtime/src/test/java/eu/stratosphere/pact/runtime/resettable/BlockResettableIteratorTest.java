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

import eu.stratosphere.nephele.io.DefaultRecordDeserializer;
import eu.stratosphere.nephele.io.RecordDeserializer;
import eu.stratosphere.nephele.services.ServiceException;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.services.memorymanager.spi.DefaultMemoryManager;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.runtime.resettable.BlockResettableIterator;
import eu.stratosphere.pact.runtime.test.util.DummyInvokable;
import junit.framework.Assert;

public class BlockResettableIteratorTest
{
	private static final int MEMORY_CAPACITY = 100000;
	
	private static final int NUM_VALUES = 20000;
	
	private MemoryManager memman;

	private Iterator<PactInteger> reader;

	private List<PactInteger> objects;

	private RecordDeserializer<PactInteger> deserializer;
	


	@Before
	public void startup() {
		// set up IO and memory manager
		this.memman = new DefaultMemoryManager(MEMORY_CAPACITY);
		
		// create test objects
		this.objects = new ArrayList<PactInteger>(20000);
		for (int i = 0; i < NUM_VALUES; ++i) {
			PactInteger tmp = new PactInteger(i);
			this.objects.add(tmp);
		}
		// create the deserializer
		this.deserializer = new DefaultRecordDeserializer<PactInteger>(PactInteger.class);
	}
	
	@After
	public void shutdown() {
		this.deserializer = null;
		this.objects = null;
		
		// check that the memory manager got all segments back
		if (!this.memman.verifyEmpty()) {
			Assert.fail("A memory leak has occurred: Not all memory was properly returned to the memory manager.");
		}
		
		this.memman.shutdown();
		this.memman = null;
	}

	@Test
	public void testSerialBlockResettableIterator() throws ServiceException, IOException, InterruptedException {
		final AbstractInvokable memOwner = new DummyInvokable();
		
		// create the reader
		reader = new CollectionIterator<PactInteger>(objects);
		// create the resettable Iterator
		BlockResettableIterator<PactInteger> iterator = new BlockResettableIterator<PactInteger>(memman, reader,
				BlockResettableIterator.MIN_BUFFER_SIZE, 1, deserializer, memOwner);
		// open the iterator
		iterator.open();
		
		// now test walking through the iterator
		int lower = 0;
		int upper = 0;
		do {
			lower = upper;
			upper = lower;
			// find the upper bound
			while (iterator.hasNext())
				Assert.assertEquals(upper++, iterator.next().getValue());
			// now reset the buffer a few times
			for (int i = 0; i < 5; ++i) {
				iterator.reset();
				int count = 0;
				while (iterator.hasNext())
					Assert.assertEquals(lower + (count++), iterator.next().getValue());
				Assert.assertEquals(upper - lower, count);
			}
		} while (iterator.nextBlock());
		Assert.assertEquals(NUM_VALUES, upper);
		// close the iterator
		iterator.close();
	}

	@Test
	public void testDoubleBufferedBlockResettableIterator() throws ServiceException, IOException, InterruptedException {
		final AbstractInvokable memOwner = new DummyInvokable();
		
		// create the reader
		reader = new CollectionIterator<PactInteger>(objects);
		// create the resettable Iterator
		BlockResettableIterator<PactInteger> iterator = new BlockResettableIterator<PactInteger>(memman, reader,
				2 * BlockResettableIterator.MIN_BUFFER_SIZE, 2,
			deserializer, memOwner);
		// open the iterator
		iterator.open();
		// now test walking through the iterator
		int lower = 0;
		int upper = 0;
		do {
			lower = upper;
			upper = lower;
			// find the upper bound
			while (iterator.hasNext())
				Assert.assertEquals(upper++, iterator.next().getValue());
			// now reset the buffer a few times
			for (int i = 0; i < 5; ++i) {
				iterator.reset();
				int count = 0;
				while (iterator.hasNext())
					Assert.assertEquals(lower + (count++), iterator.next().getValue());
				Assert.assertEquals(upper - lower, count);
			}
		} while (iterator.nextBlock());
		Assert.assertEquals(NUM_VALUES, upper);
		// close the iterator
		iterator.close();
	}

	@Test
	public void testTripleBufferedBlockResettableIterator() throws ServiceException, IOException, InterruptedException {
		final AbstractInvokable memOwner = new DummyInvokable();
		
		// create the reader
		reader = new CollectionIterator<PactInteger>(objects);
		// create the resettable Iterator
		BlockResettableIterator<PactInteger> iterator = new BlockResettableIterator<PactInteger>(memman, reader, 
				3 * BlockResettableIterator.MIN_BUFFER_SIZE, 3, deserializer, memOwner);
		// open the iterator
		iterator.open();
		// now test walking through the iterator
		int lower = 0;
		int upper = 0;
		do {
			lower = upper;
			upper = lower;
			// find the upper bound
			while (iterator.hasNext())
				Assert.assertEquals(upper++, iterator.next().getValue());
			// now reset the buffer a few times
			for (int i = 0; i < 5; ++i) {
				iterator.reset();
				int count = 0;
				while (iterator.hasNext())
					Assert.assertEquals(lower + (count++), iterator.next().getValue());
				Assert.assertEquals(upper - lower, count);
			}
		} while (iterator.nextBlock());
		Assert.assertEquals(NUM_VALUES, upper);
		// close the iterator
		iterator.close();
	}

}
