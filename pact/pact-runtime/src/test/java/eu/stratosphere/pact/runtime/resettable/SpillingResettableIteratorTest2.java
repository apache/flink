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
import java.util.Collection;
import java.util.List;
import java.util.Vector;

import org.junit.BeforeClass;
import org.junit.Test;

import eu.stratosphere.nephele.io.DefaultRecordDeserializer;
import eu.stratosphere.nephele.io.Reader;
import eu.stratosphere.nephele.io.RecordDeserializer;
import eu.stratosphere.nephele.io.channels.AbstractInputChannel;
import eu.stratosphere.nephele.services.ServiceException;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.nephele.services.memorymanager.spi.DefaultMemoryManager;
import eu.stratosphere.nephele.types.Record;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.runtime.resettable.SpillingResettableIterator;
import junit.framework.Assert;

public class SpillingResettableIteratorTest2 {

	protected static IOManager ioman;

	protected static MemoryManager memman;

	protected static final int memoryCapacity = 100000;

	protected static Reader<PactInteger> reader;

	protected static Vector<PactInteger> objects;

	protected static RecordDeserializer<PactInteger> deserializer;

	protected class CollectionReader<T extends Record> implements Reader<T> {
		private Vector<T> objects;

		private int position = 0;

		public CollectionReader(Collection<T> objects) {
			this.objects = new Vector<T>(objects);
		}

		@Override
		public boolean hasNext() {
			if (position < objects.size())
				return true;
			return false;
		}

		@Override
		public T next() throws IOException, InterruptedException {
			if (hasNext()) {
				T tmp = objects.get(position);
				position++;
				return tmp;
			}
			return null;
		}

		@Override
		public List<AbstractInputChannel<T>> getInputChannels() {
			// do nothing
			return null;
		}

	}

	@BeforeClass
	public static void initialize() {
		// set up IO and memory manager
		ioman = new IOManager();
		memman = new DefaultMemoryManager(memoryCapacity);
		// create test objects
		objects = new Vector<PactInteger>(1000);
		for (int i = 0; i < 1000; ++i) {
			PactInteger tmp = new PactInteger(i);
			objects.add(tmp);
		}
		// create the deserializer
		deserializer = new DefaultRecordDeserializer<PactInteger>(PactInteger.class);
	}

	/**
	 * Tests the resettable iterato with too few memory, so that the data
	 * has to be written to disk.
	 * 
	 * @throws ServiceException
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Test
	public void testResettableIterator() throws ServiceException, IOException, InterruptedException {
		// create the reader
		reader = new CollectionReader<PactInteger>(objects);
		// create the resettable Iterator
		SpillingResettableIterator<PactInteger> iterator = new SpillingResettableIterator<PactInteger>(memman, ioman,
			reader, 1000, deserializer);
		// open the iterator
		iterator.open();
		// now test walking through the iterator
		int count = 0;
		while (iterator.hasNext())
			Assert.assertEquals("In initial run, element " + count + " does not match expected value!", count++,
				iterator.next().getValue());
		Assert.assertEquals("Too few elements were deserialzied in initial run!", 1000, count);
		// test resetting the iterator a few times
		for (int j = 0; j < 10; ++j) {
			count = 0;
			iterator.reset();
			// now we should get the same resutls
			while (iterator.hasNext())
				Assert.assertEquals("After reset nr. " + j + 1 + " element " + count
					+ " does not match expected value!", count++, iterator.next().getValue());
			Assert.assertEquals("Too few elements were deserialzied after reset nr. " + j + 1 + "!", 1000, count);
		}
		// close the iterator
		iterator.close();
		// make sure there are no memory leaks
		try {
			MemorySegment test = memman.allocate(memoryCapacity);
			memman.release(test);
		} catch (Exception e) {
			Assert.fail("Memory leak detected!");
		}
	}

	/**
	 * Tests the resettable iterator with enough memory so that all data
	 * is kept locally in a membuffer.
	 * 
	 * @throws ServiceException
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Test
	public void testResettableIteratorInMemory() throws ServiceException, IOException, InterruptedException {
		// create the reader
		reader = new CollectionReader<PactInteger>(objects);
		// create the resettable Iterator
		SpillingResettableIterator<PactInteger> iterator = new SpillingResettableIterator<PactInteger>(memman, ioman,
			reader, 10000, deserializer);
		// open the iterator
		iterator.open();
		// now test walking through the iterator
		int count = 0;
		while (iterator.hasNext())
			Assert.assertEquals("In initial run, element " + count + " does not match expected value!", count++,
				iterator.next().getValue());
		Assert.assertEquals("Too few elements were deserialzied in initial run!", 1000, count);
		// test resetting the iterator a few times
		for (int j = 0; j < 10; ++j) {
			count = 0;
			iterator.reset();
			// now we should get the same resutls
			while (iterator.hasNext())
				Assert.assertEquals("After reset nr. " + j + 1 + " element " + count
					+ " does not match expected value!", count++, iterator.next().getValue());
			Assert.assertEquals("Too few elements were deserialzied after reset nr. " + j + 1 + "!", 1000, count);
		}
		// close the iterator
		iterator.close();
		// make sure there are no memory leaks
		try {
			MemorySegment test = memman.allocate(memoryCapacity);
			memman.release(test);
		} catch (Exception e) {
			Assert.fail("Memory leak detected!");
		}
	}

}
