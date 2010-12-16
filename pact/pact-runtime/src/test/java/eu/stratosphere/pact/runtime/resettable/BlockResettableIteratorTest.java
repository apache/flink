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
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.nephele.services.memorymanager.spi.DefaultMemoryManager;
import eu.stratosphere.nephele.types.Record;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.runtime.resettable.BlockResettableIterator;
import junit.framework.Assert;

public class BlockResettableIteratorTest {
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

	@Test
	public void testSerialBlockResettableIterator() throws ServiceException, IOException, InterruptedException {
		// create the reader
		reader = new CollectionReader<PactInteger>(objects);
		// create the resettable Iterator
		BlockResettableIterator<PactInteger> iterator = new BlockResettableIterator<PactInteger>(memman, reader, 1000, 1,
			deserializer);
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
		Assert.assertEquals(1000, upper);
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

	@Test
	public void testDoubleBufferedBlockResettableIterator() throws ServiceException, IOException, InterruptedException {
		// create the reader
		reader = new CollectionReader<PactInteger>(objects);
		// create the resettable Iterator
		BlockResettableIterator<PactInteger> iterator = new BlockResettableIterator<PactInteger>(memman, reader, 1000, 2,
			deserializer);
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
		Assert.assertEquals(1000, upper);
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

	@Test
	public void testTripleBufferedBlockResettableIterator() throws ServiceException, IOException, InterruptedException {
		// create the reader
		reader = new CollectionReader<PactInteger>(objects);
		// create the resettable Iterator
		BlockResettableIterator<PactInteger> iterator = new BlockResettableIterator<PactInteger>(memman, reader, 1000, 3,
			deserializer);
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
		Assert.assertEquals(1000, upper);
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
