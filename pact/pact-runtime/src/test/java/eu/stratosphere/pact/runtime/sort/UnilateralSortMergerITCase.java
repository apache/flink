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
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.runtime.test.util.DummyInvokable;
import eu.stratosphere.pact.runtime.test.util.TestData;
import eu.stratosphere.pact.runtime.test.util.TestData.Key;
import eu.stratosphere.pact.runtime.test.util.TestData.Generator.KeyMode;
import eu.stratosphere.pact.runtime.test.util.TestData.Generator.ValueMode;
import eu.stratosphere.pact.runtime.util.MutableObjectIterator;

/**
 * @author Erik Nijkamp
 */
public class UnilateralSortMergerITCase
{
	private static final Log LOG = LogFactory.getLog(UnilateralSortMergerITCase.class);

	private static final long SEED = 649180756312423613L;

	private static final int KEY_MAX = Integer.MAX_VALUE;

	private static final int VALUE_LENGTH = 118;

	private static final int NUM_PAIRS = 200000;

	public static final int MEMORY_SIZE = 1024 * 1024 * 64;
	
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
		ioManager.shutdown();
		if (!ioManager.isProperlyShutDown()) {
			Assert.fail("I/O Manager was not properly shut down.");
		}
		
		if (memoryManager != null) {
			Assert.assertTrue("Memory leak: not all segments have been returned to the memory manager.", 
				memoryManager.verifyEmpty());
			memoryManager.shutdown();
			memoryManager = null;
		}
	}

	@Test
	public void testSort() throws Exception
	{
		// comparator
		final Comparator<TestData.Key> keyComparator = new TestData.KeyComparator();

		// reader
		MockRecordReader reader = new MockRecordReader();

		// merge iterator
		LOG.debug("Initializing sortmerger...");
		@SuppressWarnings("unchecked")
		SortMerger merger = new UnilateralSortMerger(
			memoryManager, ioManager, 16 * 1024 * 1024, 1024 * 1024 * 4, 1, 2,
			new Comparator[] {keyComparator},
			new int[] {0},
			new Class[] {TestData.Key.class}, reader, parentTask, 0.7f);

		// emit data
		LOG.debug("Emitting data...");
		TestData.Generator generator = new TestData.Generator(SEED, KEY_MAX, VALUE_LENGTH, KeyMode.RANDOM,
			ValueMode.FIX_LENGTH);
		PactRecord rec = new PactRecord();
		
		for (int i = 0; i < NUM_PAIRS; i++) {
			reader.emit(generator.next(rec));
		}
		reader.close();

		// check order
		MutableObjectIterator<PactRecord> iterator = merger.getIterator();
		
		LOG.debug("Checking results...");
		int pairsEmitted = 1;

		PactRecord rec1 = new PactRecord();
		PactRecord rec2 = new PactRecord();
		
		rec1 = iterator.next(rec1);
		while ((rec2 = iterator.next(rec2)) != null) {
			final Key k1 = rec1.getField(0, TestData.Key.class);
			final Key k2 = rec2.getField(0, TestData.Key.class);
			pairsEmitted++;
			
			Assert.assertTrue(keyComparator.compare(k1, k2) <= 0); 
			
			PactRecord tmp = rec1;
			rec1 = rec2;
			k1.setKey(k2.getKey());
			
			rec2 = tmp;
		}
		Assert.assertTrue(NUM_PAIRS == pairsEmitted);
		
		merger.close();
	}

	@Test
	public void testSortInMemory() throws Exception
	{
		// comparator
		final Comparator<TestData.Key> keyComparator = new TestData.KeyComparator();

		// reader
		MockRecordReader reader = new MockRecordReader();

		// merge iterator
		LOG.debug("Initializing sortmerger...");
		@SuppressWarnings("unchecked")
		SortMerger merger = new UnilateralSortMerger(
			memoryManager, ioManager, 40 * 1024 * 1024, 1024 * 1024 * 1, 10, 2,
			new Comparator[] {keyComparator},
			new int[] {0},
			new Class[] {TestData.Key.class}, reader, parentTask, 0.7f);

		// emit data
		LOG.debug("Emitting data...");
		TestData.Generator generator = new TestData.Generator(SEED, KEY_MAX, VALUE_LENGTH, KeyMode.RANDOM,
			ValueMode.FIX_LENGTH);
		PactRecord rec = new PactRecord();
		
		for (int i = 0; i < NUM_PAIRS; i++) {
			reader.emit(generator.next(rec));
		}
		reader.close();

		// check order
		MutableObjectIterator<PactRecord> iterator = merger.getIterator();
		
		LOG.debug("Checking results...");
		int pairsEmitted = 1;

		PactRecord rec1 = new PactRecord();
		PactRecord rec2 = new PactRecord();
		
		rec1 = iterator.next(rec1);
		while ((rec2 = iterator.next(rec2)) != null) {
			final Key k1 = rec1.getField(0, TestData.Key.class);
			final Key k2 = rec2.getField(0, TestData.Key.class);
			pairsEmitted++;
			
			Assert.assertTrue(keyComparator.compare(k1, k2) <= 0); 
			
			PactRecord tmp = rec1;
			rec1 = rec2;
			k1.setKey(k2.getKey());
			
			rec2 = tmp;
		}
		Assert.assertTrue(NUM_PAIRS == pairsEmitted);
		
		merger.close();
	}
	
	@Test
	public void testSortTenBuffers() throws Exception
	{
		// comparator
		final Comparator<TestData.Key> keyComparator = new TestData.KeyComparator();

		// reader
		MockRecordReader reader = new MockRecordReader();

		// merge iterator
		LOG.debug("Initializing sortmerger...");
		@SuppressWarnings("unchecked")
		SortMerger merger = new UnilateralSortMerger(
			memoryManager, ioManager, 1024 * 1024 * 42, 1024 * 1024 * 2, 10, 2, 
			new Comparator[] {keyComparator},
			new int[] {0},
			new Class[] {TestData.Key.class}, reader, parentTask, 0.7f);

		// emit data
		LOG.debug("Emitting data...");
		TestData.Generator generator = new TestData.Generator(SEED, KEY_MAX, VALUE_LENGTH, KeyMode.RANDOM,
			ValueMode.FIX_LENGTH);
		PactRecord rec = new PactRecord();
		
		for (int i = 0; i < NUM_PAIRS; i++) {
			reader.emit(generator.next(rec));
		}
		reader.close();

		// check order
		MutableObjectIterator<PactRecord> iterator = merger.getIterator();
		
		LOG.debug("Checking results...");
		int pairsEmitted = 1;

		PactRecord rec1 = new PactRecord();
		PactRecord rec2 = new PactRecord();
		
		rec1 = iterator.next(rec1);
		while ((rec2 = iterator.next(rec2)) != null) {
			final Key k1 = rec1.getField(0, TestData.Key.class);
			final Key k2 = rec2.getField(0, TestData.Key.class);
			pairsEmitted++;
			
			Assert.assertTrue(keyComparator.compare(k1, k2) <= 0); 
			
			PactRecord tmp = rec1;
			rec1 = rec2;
			k1.setKey(k2.getKey());
			
			rec2 = tmp;
		}
		Assert.assertTrue(NUM_PAIRS == pairsEmitted);
		
		merger.close();
	}

	@Test
	public void testSortHugeAmountOfPairs() throws Exception
	{
		// amount of pairs
		final int PAIRS = 10000000;

		// comparator
		final Comparator<TestData.Key> keyComparator = new TestData.KeyComparator();

		// reader
		MockRecordReader reader = new MockRecordReader();

		// merge iterator
		LOG.debug("Initializing sortmerger...");
		@SuppressWarnings("unchecked")
		SortMerger merger = new UnilateralSortMerger(
			memoryManager, ioManager, 1024 * 1024 * 64, 16, 
			new Comparator[] {keyComparator},
			new int[] {0},
			new Class[] {TestData.Key.class}, reader, parentTask, 0.7f);
		
		// emit data
		LOG.debug("Emitting data...");
		TestData.Generator generator = new TestData.Generator(SEED, KEY_MAX, VALUE_LENGTH, KeyMode.RANDOM,
			ValueMode.FIX_LENGTH);
		PactRecord rec = new PactRecord();
		System.out.println("Starting large sort test...");
		
		long start = System.currentTimeMillis();
		for (int i = 0, percent = 0, nextPercent = PAIRS / 20; i < PAIRS; i++) {
			reader.emit(generator.next(rec));
			if (i == nextPercent) {
				percent += 5;
				nextPercent += PAIRS / 20;
				System.out.println(percent + "% records emitted...");
			}
		}
		reader.close();
		System.out.println("100% records emitted...");
		System.out.println("Final merge phase starting...");

		// check order
		MutableObjectIterator<PactRecord> iterator = merger.getIterator();
		
		LOG.debug("Checking results...");
		int pairsRead = 1;
		int nextStep = PAIRS / 20;

		PactRecord rec1 = new PactRecord();
		PactRecord rec2 = new PactRecord();
		
		rec1 = iterator.next(rec1);
		while ((rec2 = iterator.next(rec2)) != null) {
			final Key k1 = rec1.getField(0, TestData.Key.class);
			final Key k2 = rec2.getField(0, TestData.Key.class);
			pairsRead++;
			
			Assert.assertTrue(keyComparator.compare(k1, k2) <= 0); 
			
			PactRecord tmp = rec1;
			rec1 = rec2;
			k1.setKey(k2.getKey());
			rec2 = tmp;
			
			// log
			if (pairsRead == nextStep) {
				nextStep += PAIRS / 20;
				System.out.println((pairsRead * 100 / PAIRS) + "% pairs merged and read.");
			}
			
		}
		System.out.println("...Done");
		Assert.assertEquals("Not all pairs were read back in.", PAIRS, pairsRead);
		
		merger.close();

		// throughput
		long end = System.currentTimeMillis();
		long diff = end - start;
		long secs = diff / 1000;
		LOG.debug("sorting a workload of " + PAIRS + " pairs took " + secs + " seconds.");
	}
}
