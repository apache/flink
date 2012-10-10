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
import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.services.memorymanager.spi.DefaultMemoryManager;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.generic.types.TypeComparator;
import eu.stratosphere.pact.generic.types.TypeSerializer;
import eu.stratosphere.pact.runtime.plugable.PactRecordComparator;
import eu.stratosphere.pact.runtime.plugable.PactRecordSerializer;
import eu.stratosphere.pact.runtime.test.util.DummyInvokable;
import eu.stratosphere.pact.runtime.test.util.TestData;
import eu.stratosphere.pact.runtime.test.util.TestData.Key;
import eu.stratosphere.pact.runtime.test.util.TestData.Generator.KeyMode;
import eu.stratosphere.pact.runtime.test.util.TestData.Generator.ValueMode;
import eu.stratosphere.pact.runtime.test.util.TestData.Value;

/**
 * @author Erik Nijkamp
 * @author Stephan Ewen
 */
public class UnilateralSortMergerITCase
{
	private static final Log LOG = LogFactory.getLog(UnilateralSortMergerITCase.class);

	private static final long SEED = 649180756312423613L;

	private static final int KEY_MAX = Integer.MAX_VALUE;

	private static final int VALUE_LENGTH = 114;
	
	private static final Value VAL = new Value("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ");

	private static final int NUM_PAIRS = 200000;

	private static final int MEMORY_SIZE = 1024 * 1024 * 78;
	
	private final AbstractTask parentTask = new DummyInvokable();

	private IOManager ioManager;

	private MemoryManager memoryManager;
	
	private TypeSerializer<PactRecord> pactRecordSerializer;
	
	private TypeComparator<PactRecord> pactRecordComparator;

	// --------------------------------------------------------------------------------------------

	@SuppressWarnings("unchecked")
	@Before
	public void beforeTest() {
		this.memoryManager = new DefaultMemoryManager(MEMORY_SIZE);
		this.ioManager = new IOManager();
		
		this.pactRecordSerializer = PactRecordSerializer.get();
		this.pactRecordComparator = new PactRecordComparator(new int[] {0}, new Class[] {TestData.Key.class});
	}

	@After
	public void afterTest() {
		this.ioManager.shutdown();
		if (!this.ioManager.isProperlyShutDown()) {
			Assert.fail("I/O Manager was not properly shut down.");
		}
		
		if (this.memoryManager != null) {
			Assert.assertTrue("Memory leak: not all segments have been returned to the memory manager.", 
				this.memoryManager.verifyEmpty());
			this.memoryManager.shutdown();
			this.memoryManager = null;
		}
	}

	// --------------------------------------------------------------------------------------------
	
	@Test
	public void testInMemorySort() throws Exception
	{
		// comparator
		final Comparator<TestData.Key> keyComparator = new TestData.KeyComparator();
		
		final TestData.Generator generator = new TestData.Generator(SEED, KEY_MAX, VALUE_LENGTH, KeyMode.RANDOM, ValueMode.CONSTANT, VAL);
		final MutableObjectIterator<PactRecord> source = new TestData.GeneratorIterator(generator, NUM_PAIRS);

		// merge iterator
		LOG.debug("Initializing sortmerger...");
		
		Sorter<PactRecord> merger = new UnilateralSortMerger<PactRecord>(this.memoryManager, this.ioManager, 
			source, this.parentTask, this.pactRecordSerializer, this.pactRecordComparator,
			64 * 1024 * 1024, 2, 0.9f);

		// emit data
		LOG.debug("Reading and sorting data...");

		// check order
		MutableObjectIterator<PactRecord> iterator = merger.getIterator();
		
		LOG.debug("Checking results...");
		int pairsEmitted = 1;

		PactRecord rec1 = new PactRecord();
		PactRecord rec2 = new PactRecord();
		
		Assert.assertTrue(iterator.next(rec1));
		while (iterator.next(rec2)) {
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
	public void testInMemorySortUsing10Buffers() throws Exception
	{
		// comparator
		final Comparator<TestData.Key> keyComparator = new TestData.KeyComparator();
		
		final TestData.Generator generator = new TestData.Generator(SEED, KEY_MAX, VALUE_LENGTH, KeyMode.RANDOM, ValueMode.CONSTANT, VAL);
		final MutableObjectIterator<PactRecord> source = new TestData.GeneratorIterator(generator, NUM_PAIRS);

		// merge iterator
		LOG.debug("Initializing sortmerger...");
		
		Sorter<PactRecord> merger = new UnilateralSortMerger<PactRecord>(this.memoryManager, this.ioManager, 
				source, this.parentTask, this.pactRecordSerializer, this.pactRecordComparator,
				64 * 1024 * 1024, 10, 2, 0.9f);

		// emit data
		LOG.debug("Reading and sorting data...");

		// check order
		MutableObjectIterator<PactRecord> iterator = merger.getIterator();
		
		LOG.debug("Checking results...");
		int pairsEmitted = 1;

		PactRecord rec1 = new PactRecord();
		PactRecord rec2 = new PactRecord();
		
		Assert.assertTrue(iterator.next(rec1));
		while (iterator.next(rec2)) {
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
	public void testSpillingSort() throws Exception
	{
		// comparator
		final Comparator<TestData.Key> keyComparator = new TestData.KeyComparator();
		
		final TestData.Generator generator = new TestData.Generator(SEED, KEY_MAX, VALUE_LENGTH, KeyMode.RANDOM, ValueMode.CONSTANT, VAL);
		final MutableObjectIterator<PactRecord> source = new TestData.GeneratorIterator(generator, NUM_PAIRS);

		// merge iterator
		LOG.debug("Initializing sortmerger...");
		
		Sorter<PactRecord> merger = new UnilateralSortMerger<PactRecord>(this.memoryManager, this.ioManager, 
				source, this.parentTask, this.pactRecordSerializer, this.pactRecordComparator,
				16 * 1024 * 1024, 64, 0.7f);

		// emit data
		LOG.debug("Reading and sorting data...");

		// check order
		MutableObjectIterator<PactRecord> iterator = merger.getIterator();
		
		LOG.debug("Checking results...");
		int pairsEmitted = 1;

		PactRecord rec1 = new PactRecord();
		PactRecord rec2 = new PactRecord();
		
		Assert.assertTrue(iterator.next(rec1));
		while (iterator.next(rec2)) {
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
	public void testSpillingSortWithIntermediateMerge() throws Exception
	{
		// amount of pairs
		final int PAIRS = 10000000;

		// comparator
		final Comparator<TestData.Key> keyComparator = new TestData.KeyComparator();

		final TestData.Generator generator = new TestData.Generator(SEED, KEY_MAX, VALUE_LENGTH, KeyMode.RANDOM, ValueMode.FIX_LENGTH);
		final MutableObjectIterator<PactRecord> source = new TestData.GeneratorIterator(generator, PAIRS);
		
		// merge iterator
		LOG.debug("Initializing sortmerger...");
		
		Sorter<PactRecord> merger = new UnilateralSortMerger<PactRecord>(this.memoryManager, this.ioManager, 
				source, this.parentTask, this.pactRecordSerializer, this.pactRecordComparator,
				64 * 1024 * 1024, 16, 0.7f);
		
		// emit data
		LOG.debug("Emitting data...");

		// check order
		MutableObjectIterator<PactRecord> iterator = merger.getIterator();
		
		LOG.debug("Checking results...");
		int pairsRead = 1;
		int nextStep = PAIRS / 20;

		PactRecord rec1 = new PactRecord();
		PactRecord rec2 = new PactRecord();
		
		Assert.assertTrue(iterator.next(rec1));
		while (iterator.next(rec2)) {
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
			}
			
		}
		Assert.assertEquals("Not all pairs were read back in.", PAIRS, pairsRead);
		merger.close();
	}
}
