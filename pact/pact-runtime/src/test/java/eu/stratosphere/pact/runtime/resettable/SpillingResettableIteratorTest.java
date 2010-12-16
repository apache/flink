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

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.nephele.io.Reader;
import eu.stratosphere.nephele.services.ServiceException;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.services.memorymanager.spi.DefaultMemoryManager;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.runtime.serialization.KeyValuePairDeserializer;
import eu.stratosphere.pact.runtime.test.util.TestData;

public class SpillingResettableIteratorTest {
	public static final long SEED = 3246134645613471L;

	public static final int NUMBER_OF_RECORDS = 1000;

	public static final int KEY_MAX = 10000;

	public static final int VALUE_LENGTH = 96;

	public static final int MEMORY_SIZE = 1024 * 1024 * 16;

	MemoryManager memoryManager;

	IOManager ioManager;

	Reader<KeyValuePair<TestData.Key, TestData.Value>> reader;

	@Before
	public void beforeTest() {
		memoryManager = new DefaultMemoryManager(MEMORY_SIZE);
		ioManager = new IOManager();

		TestData.Generator generator = new TestData.Generator(SEED, KEY_MAX, VALUE_LENGTH);
		reader = new TestData.RecordReaderMock(generator, NUMBER_OF_RECORDS);
	}

	@Test
	public void testIterator() throws InterruptedException, ServiceException {
		KeyValuePair<TestData.Key, TestData.Value> actual, expected;

		TestData.Generator generator = new TestData.Generator(SEED, KEY_MAX, VALUE_LENGTH);
		SpillingResettableIterator<KeyValuePair<TestData.Key, TestData.Value>> iterator = new SpillingResettableIterator<KeyValuePair<TestData.Key, TestData.Value>>(
			memoryManager, ioManager, reader, MEMORY_SIZE, new KeyValuePairDeserializer<TestData.Key, TestData.Value>(
				TestData.Key.class, TestData.Value.class));

		try {
			iterator.open();
		}
		catch (IOException e) {
			Assert.fail("Could not open resettable iterator:" + e.getMessage());
		}
		
		for (int i = 0; i < NUMBER_OF_RECORDS; i++) {
			Assert.assertTrue("Next pair expected but does not exist", iterator.hasNext());

			actual = iterator.next();
			expected = generator.next();

			Assert.assertEquals("Pairs don't match at index " + i, expected, actual);
		}

		Assert.assertFalse("Unexpected next pair", iterator.hasNext());

		iterator.reset();
		generator.reset();

		while (iterator.hasNext()) {
			actual = iterator.next();
			expected = generator.next();

			Assert.assertEquals("Pairs don't match", expected, actual);
		}

		iterator.close();
	}
}
