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

package eu.stratosphere.pact.runtime.hash;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;

import junit.framework.Assert;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.nephele.services.memorymanager.MemoryAllocationException;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.services.memorymanager.OutOfMemoryException;
import eu.stratosphere.nephele.services.memorymanager.spi.DefaultMemoryManager;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.runtime.hash.SerializingHashMap;
import eu.stratosphere.pact.runtime.test.util.TestData;
import eu.stratosphere.pact.runtime.test.util.TestData.Key;
import eu.stratosphere.pact.runtime.test.util.TestData.Value;

public class SerializingHashMapTest {
	private static final long SEED = 649180756312423613L;

	private static final int KEY_MAX = 150000;

	private static final int VALUE_LENGTH = 128;

	private static final int SEGMENT_SIZE = 30 * 1024 * 1024;

	private TestData.Generator generator;

	private MemoryManager memoryManager;

	private HashMap<Key, LinkedList<Value>> javaHashMap;

	private SerializingHashMap<Key, Value> pactHashMap;

	@Before
	public void setUp() throws MemoryAllocationException {
		generator = new TestData.Generator(SEED, KEY_MAX, VALUE_LENGTH);
		memoryManager = new DefaultMemoryManager(SEGMENT_SIZE);
		pactHashMap = new SerializingHashMap<Key, Value>(Key.class, Value.class, memoryManager.allocate(SEGMENT_SIZE));
		javaHashMap = new HashMap<Key, LinkedList<Value>>();
	}

	@After
	public void tearDown() {
		memoryManager.shutdown();
	}

	@Test
	public void testHashMapFunctionality() {
		int occupiedMemory = 0;
		int numberOfKeys = 0;
		int numberOfValues = 0;
		boolean insertValues = true;

		while (insertValues) {
			numberOfValues++;
			KeyValuePair<TestData.Key, TestData.Value> pair = generator.next();

			// calculate memory segment free memory after insert
			if (javaHashMap.containsKey(pair.getKey())) {
				occupiedMemory += 2 * VALUE_LENGTH + 4;
			} else {
				occupiedMemory += 2 * VALUE_LENGTH + 16;
			}

			try {
				// add pair to pact hash map
				pactHashMap.put(pair.getKey(), pair.getValue());

				// add pair do java hash map
				if (!javaHashMap.containsKey(pair.getKey())) {
					javaHashMap.put(pair.getKey(), new LinkedList<Value>());
					numberOfKeys++;
				}

				LinkedList<Value> values = javaHashMap.get(pair.getKey());
				values.addFirst(pair.getValue());

				// check sizes
				Assert.assertEquals("number of values don't match", numberOfValues, pactHashMap.numberOfValues());
				Assert.assertEquals("number of keys don't match", numberOfKeys, pactHashMap.numberOfKeys());
			} catch (OutOfMemoryException e) {
				Assert.assertTrue("Unexpected OutOfMemoryException", occupiedMemory > SEGMENT_SIZE);
				insertValues = false;
			}
		}

		Assert.assertTrue("Number of values in map is below lower bound", pactHashMap.numberOfValues() >= SEGMENT_SIZE
			/ (2 * VALUE_LENGTH + 16));

		Logger.getRootLogger().debug("Inserted " + pactHashMap.numberOfKeys() + " keys");
		Logger.getRootLogger().debug("Inserted " + pactHashMap.numberOfValues() + " values");

		// test value iterators
		for (Key key : javaHashMap.keySet()) {
			Iterator<Value> expIt = javaHashMap.get(key).iterator();
			Iterator<Value> actIt = pactHashMap.get(key).iterator();

			while (expIt.hasNext()) {
				Assert.assertEquals("iterator count does not match", expIt.hasNext(), actIt.hasNext());

				Value expValue = expIt.next();
				Value actValue = actIt.next();

				Assert.assertEquals("values don't match", expValue, actValue);
			}
		}

		// test key iterators
		Set<Key> keySet = javaHashMap.keySet();

		for (Key key : keySet) {
			Assert.assertTrue("", pactHashMap.contains(key));
		}

		for (Key key : pactHashMap.keys()) {
			Assert.assertTrue("Unexpected key " + key, keySet.contains(key));
			keySet.remove(key);
		}

		Assert.assertTrue("Key set is not empty (size is " + keySet.size() + ")", keySet.isEmpty());
	}
}
