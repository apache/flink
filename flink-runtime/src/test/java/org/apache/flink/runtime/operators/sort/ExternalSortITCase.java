/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.operators.sort;

import java.util.Comparator;

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.api.common.typeutils.record.RecordComparator;
import org.apache.flink.api.common.typeutils.record.RecordSerializerFactory;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.testutils.DummyInvokable;
import org.apache.flink.runtime.operators.testutils.RandomIntPairGenerator;
import org.apache.flink.runtime.operators.testutils.TestData;
import org.apache.flink.runtime.operators.testutils.TestData.Generator.KeyMode;
import org.apache.flink.runtime.operators.testutils.TestData.Generator.ValueMode;
import org.apache.flink.runtime.operators.testutils.TestData.Key;
import org.apache.flink.runtime.operators.testutils.TestData.Value;
import org.apache.flink.runtime.operators.testutils.types.IntPair;
import org.apache.flink.runtime.operators.testutils.types.IntPairComparator;
import org.apache.flink.runtime.operators.testutils.types.IntPairSerializer;
import org.apache.flink.types.Record;
import org.apache.flink.util.MutableObjectIterator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ExternalSortITCase {
	
	private static final Logger LOG = LoggerFactory.getLogger(ExternalSortITCase.class);

	private static final long SEED = 649180756312423613L;

	private static final int KEY_MAX = Integer.MAX_VALUE;

	private static final int VALUE_LENGTH = 114;
	
	private static final Value VAL = new Value("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ");

	private static final int NUM_PAIRS = 200000;

	private static final int MEMORY_SIZE = 1024 * 1024 * 78;	
	
	private final AbstractInvokable parentTask = new DummyInvokable();

	private IOManager ioManager;

	private MemoryManager memoryManager;
	
	private TypeSerializerFactory<Record> pactRecordSerializer;
	
	private TypeComparator<Record> pactRecordComparator;
	
	private boolean testSuccess;

	// --------------------------------------------------------------------------------------------

	@SuppressWarnings("unchecked")
	@Before
	public void beforeTest() {
		this.memoryManager = new MemoryManager(MEMORY_SIZE, 1);
		this.ioManager = new IOManagerAsync();
		
		this.pactRecordSerializer = RecordSerializerFactory.get();
		this.pactRecordComparator = new RecordComparator(new int[] {0}, new Class[] {TestData.Key.class});
	}

	@After
	public void afterTest() {
		this.ioManager.shutdown();
		if (!this.ioManager.isProperlyShutDown()) {
			Assert.fail("I/O Manager was not properly shut down.");
		}
		
		if (this.memoryManager != null && testSuccess) {
			Assert.assertTrue("Memory leak: not all segments have been returned to the memory manager.",
					this.memoryManager.verifyEmpty());
			this.memoryManager.shutdown();
			this.memoryManager = null;
		}
	}

	// --------------------------------------------------------------------------------------------
	
	@Test
	public void testInMemorySort() {
		try {
			// comparator
			final Comparator<TestData.Key> keyComparator = new TestData.KeyComparator();
			
			final TestData.Generator generator = new TestData.Generator(SEED, KEY_MAX, VALUE_LENGTH, KeyMode.RANDOM, ValueMode.CONSTANT, VAL);
			final MutableObjectIterator<Record> source = new TestData.GeneratorIterator(generator, NUM_PAIRS);
	
			// merge iterator
			LOG.debug("Initializing sortmerger...");
			
			Sorter<Record> merger = new UnilateralSortMerger<Record>(this.memoryManager, this.ioManager, 
				source, this.parentTask, this.pactRecordSerializer, this.pactRecordComparator,
					(double)64/78, 2, 0.9f, true);
	
			// emit data
			LOG.debug("Reading and sorting data...");
	
			// check order
			MutableObjectIterator<Record> iterator = merger.getIterator();
			
			LOG.debug("Checking results...");
			int pairsEmitted = 1;
	
			Record rec1 = new Record();
			Record rec2 = new Record();
			
			Assert.assertTrue((rec1 = iterator.next(rec1)) != null);
			while ((rec2 = iterator.next(rec2)) != null) {
				final Key k1 = rec1.getField(0, TestData.Key.class);
				final Key k2 = rec2.getField(0, TestData.Key.class);
				pairsEmitted++;
				
				Assert.assertTrue(keyComparator.compare(k1, k2) <= 0); 
				
				Record tmp = rec1;
				rec1 = rec2;
				k1.setKey(k2.getKey());
				
				rec2 = tmp;
			}
			Assert.assertTrue(NUM_PAIRS == pairsEmitted);
			
			merger.close();
			testSuccess = true;
		}
		catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}
	
	@Test
	public void testInMemorySortUsing10Buffers() {
		try {
			// comparator
			final Comparator<TestData.Key> keyComparator = new TestData.KeyComparator();
			
			final TestData.Generator generator = new TestData.Generator(SEED, KEY_MAX, VALUE_LENGTH, KeyMode.RANDOM, ValueMode.CONSTANT, VAL);
			final MutableObjectIterator<Record> source = new TestData.GeneratorIterator(generator, NUM_PAIRS);
	
			// merge iterator
			LOG.debug("Initializing sortmerger...");
			
			Sorter<Record> merger = new UnilateralSortMerger<Record>(this.memoryManager, this.ioManager, 
					source, this.parentTask, this.pactRecordSerializer, this.pactRecordComparator,
					(double)64/78, 10, 2, 0.9f, false);
	
			// emit data
			LOG.debug("Reading and sorting data...");
	
			// check order
			MutableObjectIterator<Record> iterator = merger.getIterator();
			
			LOG.debug("Checking results...");
			int pairsEmitted = 1;
	
			Record rec1 = new Record();
			Record rec2 = new Record();
			
			Assert.assertTrue((rec1 = iterator.next(rec1)) != null);
			while ((rec2 = iterator.next(rec2)) != null) {
				final Key k1 = rec1.getField(0, TestData.Key.class);
				final Key k2 = rec2.getField(0, TestData.Key.class);
				pairsEmitted++;
				
				Assert.assertTrue(keyComparator.compare(k1, k2) <= 0); 
				
				Record tmp = rec1;
				rec1 = rec2;
				k1.setKey(k2.getKey());
				
				rec2 = tmp;
			}
			Assert.assertTrue(NUM_PAIRS == pairsEmitted);
			
			merger.close();
			testSuccess = true;
		}
		catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testSpillingSort() {
		try {
			// comparator
			final Comparator<TestData.Key> keyComparator = new TestData.KeyComparator();
			
			final TestData.Generator generator = new TestData.Generator(SEED, KEY_MAX, VALUE_LENGTH, KeyMode.RANDOM, ValueMode.CONSTANT, VAL);
			final MutableObjectIterator<Record> source = new TestData.GeneratorIterator(generator, NUM_PAIRS);
	
			// merge iterator
			LOG.debug("Initializing sortmerger...");
			
			Sorter<Record> merger = new UnilateralSortMerger<Record>(this.memoryManager, this.ioManager, 
					source, this.parentTask, this.pactRecordSerializer, this.pactRecordComparator,
					(double)16/78, 64, 0.7f, true);
	
			// emit data
			LOG.debug("Reading and sorting data...");
	
			// check order
			MutableObjectIterator<Record> iterator = merger.getIterator();
			
			LOG.debug("Checking results...");
			int pairsEmitted = 1;
	
			Record rec1 = new Record();
			Record rec2 = new Record();
			
			Assert.assertTrue((rec1 = iterator.next(rec1)) != null);
			while ((rec2 = iterator.next(rec2)) != null) {
				final Key k1 = rec1.getField(0, TestData.Key.class);
				final Key k2 = rec2.getField(0, TestData.Key.class);
				pairsEmitted++;
				
				Assert.assertTrue(keyComparator.compare(k1, k2) <= 0); 
				
				Record tmp = rec1;
				rec1 = rec2;
				k1.setKey(k2.getKey());
				
				rec2 = tmp;
			}
			Assert.assertTrue(NUM_PAIRS == pairsEmitted);
			
			merger.close();
			testSuccess = true;
		}
		catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testSpillingSortWithIntermediateMerge() {
		try {
			// amount of pairs
			final int PAIRS = 10000000;
	
			// comparator
			final Comparator<TestData.Key> keyComparator = new TestData.KeyComparator();
	
			final TestData.Generator generator = new TestData.Generator(SEED, KEY_MAX, VALUE_LENGTH, KeyMode.RANDOM, ValueMode.FIX_LENGTH);
			final MutableObjectIterator<Record> source = new TestData.GeneratorIterator(generator, PAIRS);
			
			// merge iterator
			LOG.debug("Initializing sortmerger...");
			
			Sorter<Record> merger = new UnilateralSortMerger<Record>(this.memoryManager, this.ioManager, 
					source, this.parentTask, this.pactRecordSerializer, this.pactRecordComparator,
					(double)64/78, 16, 0.7f, false);
			
			// emit data
			LOG.debug("Emitting data...");
	
			// check order
			MutableObjectIterator<Record> iterator = merger.getIterator();
			
			LOG.debug("Checking results...");
			int pairsRead = 1;
			int nextStep = PAIRS / 20;
	
			Record rec1 = new Record();
			Record rec2 = new Record();
			
			Assert.assertTrue((rec1 = iterator.next(rec1)) != null);
			while ((rec2 = iterator.next(rec2)) != null) {
				final Key k1 = rec1.getField(0, TestData.Key.class);
				final Key k2 = rec2.getField(0, TestData.Key.class);
				pairsRead++;
				
				Assert.assertTrue(keyComparator.compare(k1, k2) <= 0); 
				
				Record tmp = rec1;
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
			testSuccess = true;
		}
		catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}
	
	@Test
	public void testSpillingSortWithIntermediateMergeIntPair() {
		try {
			// amount of pairs
			final int PAIRS = 50000000;
	
			// comparator
			final RandomIntPairGenerator generator = new RandomIntPairGenerator(12345678, PAIRS);
			
			final TypeSerializerFactory<IntPair> serializerFactory = new IntPairSerializer.IntPairSerializerFactory();
			final TypeComparator<IntPair> comparator = new IntPairComparator();
			
			// merge iterator
			LOG.debug("Initializing sortmerger...");
			
			Sorter<IntPair> merger = new UnilateralSortMerger<IntPair>(this.memoryManager, this.ioManager, 
					generator, this.parentTask, serializerFactory, comparator, (double)64/78, 4, 0.7f, true);
	
			// emit data
			LOG.debug("Emitting data...");
			
			// check order
			MutableObjectIterator<IntPair> iterator = merger.getIterator();
			
			LOG.debug("Checking results...");
			int pairsRead = 1;
			int nextStep = PAIRS / 20;
	
			IntPair rec1 = new IntPair();
			IntPair rec2 = new IntPair();
			
			Assert.assertTrue((rec1 = iterator.next(rec1)) != null);
			
			while ((rec2 = iterator.next(rec2)) != null) {
				final int k1 = rec1.getKey();
				final int k2 = rec2.getKey();
				pairsRead++;
				
				Assert.assertTrue(k1 - k2 <= 0); 
				
				IntPair tmp = rec1;
				rec1 = rec2;
				rec2 = tmp;
				
				// log
				if (pairsRead == nextStep) {
					nextStep += PAIRS / 20;
				}
			}
			Assert.assertEquals("Not all pairs were read back in.", PAIRS, pairsRead);
			merger.close();
			testSuccess = true;
		}
		catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}
}
