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

package eu.stratosphere.pact.runtime.sort;

import java.util.Comparator;
import java.util.Hashtable;
import java.util.Iterator;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.services.memorymanager.spi.DefaultMemoryManager;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.runtime.test.util.DummyInvokable;
import eu.stratosphere.pact.runtime.test.util.TestData;
import eu.stratosphere.pact.runtime.test.util.TestData.Key;
import eu.stratosphere.pact.runtime.test.util.TestData.Generator.KeyMode;
import eu.stratosphere.pact.runtime.test.util.TestData.Generator.ValueMode;
import eu.stratosphere.pact.runtime.util.MutableObjectIterator;

/**
 * @author Fabian Hueske
 * @author Stephan Ewen
 */
public class CombiningUnilateralSortMergerITCase
{
	private static final Log LOG = LogFactory.getLog(CombiningUnilateralSortMergerITCase.class);

	private static final long SEED = 649180756312423613L;

	private static final int KEY_MAX = 1000;

	private static final int VALUE_LENGTH = 118;

	private static final int NUM_PAIRS = 50000;

	public static final int MEMORY_SIZE = 1024 * 1024 * 256;

	private final AbstractTask parentTask = new DummyInvokable();
	
	private IOManager ioManager;

	private MemoryManager memoryManager;

	@BeforeClass
	public static void beforeClass() {
	}

	@AfterClass
	public static void afterClass() {
	}

	@Before
	public void beforeTest() {
		memoryManager = new DefaultMemoryManager(MEMORY_SIZE);
		ioManager = new IOManager();
	}

	@After
	public void afterTest() {
		if (memoryManager != null) {
			memoryManager.shutdown();
			memoryManager = null;
		}
	}

	@Test
	public void testCombine() throws Exception
	{
		int noKeys = 100;
		int noKeyCnt = 10000;

		final Comparator<TestData.Key> keyComparator = new TestData.KeyComparator();
		MockRecordReader reader = new MockRecordReader();

		LOG.debug("initializing sortmerger");
		
		@SuppressWarnings("unchecked")
		SortMerger merger = new CombiningUnilateralSortMerger(
			new TestCountCombiner(), memoryManager, ioManager, 64L * 1024 * 1024, 64,
			new Comparator[] {keyComparator}, new int[] {0}, new Class[]{TestData.Key.class}, reader, parentTask, 0.7f, true);

		final PactRecord rec = new PactRecord();
		rec.setField(1, new PactInteger(1));
		final TestData.Key key = new TestData.Key();
		
		for (int i = 0; i < noKeyCnt; i++) {
			for (int j = 0; j < noKeys; j++) {
				key.setKey(j);
				rec.setField(0, key);
				reader.emit(rec);
			}
		}
		reader.close();
		
		MutableObjectIterator<PactRecord> iterator = merger.getIterator();

		PactRecord target = new PactRecord();
		while (iterator.next(target)) {
			Assert.assertEquals(noKeyCnt, target.getField(1, PactInteger.class).getValue());
		}
	}

	@Test
	public void testSortAndValidate() throws Exception
	{
		final Hashtable<TestData.Key, Integer> countTable = new Hashtable<TestData.Key, Integer>(KEY_MAX);
		for (int i = 1; i <= KEY_MAX; i++) {
			countTable.put(new TestData.Key(i), new Integer(0));
		}

		// comparator
		final Comparator<TestData.Key> keyComparator = new TestData.KeyComparator();

		// reader
		MockRecordReader reader = new MockRecordReader();

		// merge iterator
		LOG.debug("initializing sortmerger");
		@SuppressWarnings("unchecked")
		SortMerger merger = new CombiningUnilateralSortMerger(
			new TestCountCombiner2(), memoryManager, ioManager, 64L * 1024 * 1024, 2,
			new Comparator[] {keyComparator}, new int[] {0}, new Class[]{TestData.Key.class}, reader, parentTask, 0.7f, true);

		// emit data
		LOG.debug("emitting data");
		TestData.Generator generator = new TestData.Generator(SEED, KEY_MAX, VALUE_LENGTH, KeyMode.RANDOM, ValueMode.FIX_LENGTH);
		PactRecord rec = new PactRecord();
		final TestData.Value value = new TestData.Value("1");
		
		for (int i = 0; i < NUM_PAIRS; i++) {
			Assert.assertTrue(generator.next(rec));
			final TestData.Key key = rec.getField(0, TestData.Key.class);
			rec.setField(1, value);
			reader.emit(rec);
			
			countTable.put(new TestData.Key(key.getKey()), countTable.get(key) + 1);
		}
		reader.close();
		rec = null;

		// check order
		MutableObjectIterator<PactRecord> iterator = merger.getIterator();
		
		LOG.debug("checking results");
		
		PactRecord rec1 = new PactRecord();
		PactRecord rec2 = new PactRecord();
		
		Assert.assertTrue(iterator.next(rec1));
		countTable.put(new TestData.Key(rec1.getField(0, TestData.Key.class).getKey()), countTable.get(rec1.getField(0, TestData.Key.class)) - (Integer.parseInt(rec1.getField(1, TestData.Value.class).toString())));

		while (iterator.next(rec2)) {
			final Key k1 = rec1.getField(0, TestData.Key.class);
			final Key k2 = rec2.getField(0, TestData.Key.class);
			
			Assert.assertTrue(keyComparator.compare(k1, k2) <= 0); 
			countTable.put(new TestData.Key(k2.getKey()), countTable.get(k2) - (Integer.parseInt(rec2.getField(1, TestData.Value.class).toString())));
			
			PactRecord tmp = rec1;
			rec1 = rec2;
			k1.setKey(k2.getKey());
			rec2 = tmp;
		}

		for (Integer cnt : countTable.values()) {
			Assert.assertTrue(cnt == 0);
		}

	}

	// --------------------------------------------------------------------------------------------
	
	public class TestCountCombiner extends ReduceStub
	{
		@Override
		public void combine(Iterator<PactRecord> values, Collector out)
		{
			PactRecord rec = null;
			int cnt = 0;
			while (values.hasNext()) {
				rec = values.next();
				cnt += rec.getField(1, PactInteger.class).getValue();
			}

			out.collect(new PactRecord(rec.getField(0, Key.class), new PactInteger(cnt)));
		}

		@Override
		public void reduce(Iterator<PactRecord> values, Collector out) {
			// yo, nothing, mon
		}
	}

	public class TestCountCombiner2 extends ReduceStub
	{
		@Override
		public void combine(Iterator<PactRecord> values, Collector out)
		{
			PactRecord rec = null;
			int cnt = 0;
			while (values.hasNext()) {
				rec = values.next();
				cnt += Integer.parseInt(rec.getField(1, TestData.Value.class).toString());
			}

			out.collect(new PactRecord(rec.getField(0, Key.class), new TestData.Value(cnt + "")));
		}

		@Override
		public void reduce(Iterator<PactRecord> values, Collector out) {
			// yo, nothing, mon
		}
	}
}
