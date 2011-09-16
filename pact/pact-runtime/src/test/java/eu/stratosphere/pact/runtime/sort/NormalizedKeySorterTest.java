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

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.nephele.services.memorymanager.spi.DefaultMemoryManager;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.runtime.plugable.PactRecordAccessors;
import eu.stratosphere.pact.runtime.test.util.DummyInvokable;
import eu.stratosphere.pact.runtime.test.util.TestData;
import eu.stratosphere.pact.runtime.test.util.TestData.Generator.KeyMode;
import eu.stratosphere.pact.runtime.test.util.TestData.Generator.ValueMode;
import eu.stratosphere.pact.runtime.test.util.TestData.Key;
import eu.stratosphere.pact.runtime.test.util.TestData.Value;

/**
 * @author Stephan Ewen
 */
public class NormalizedKeySorterTest
{
	private static final Log LOG = LogFactory.getLog(NormalizedKeySorterTest.class);

	private static final long SEED = 649180756312423613L;

	private static final int KEY_MAX = Integer.MAX_VALUE;

	private static final int VALUE_LENGTH = 118;

	private static final int MEMORY_SIZE = 1024 * 1024 * 16;
	
	private static final int MEMORY_SEGMENT_SIZE = 32 * 1024; 

	private DefaultMemoryManager memoryManager;


	@Before
	public void beforeTest() {
		this.memoryManager = new DefaultMemoryManager(MEMORY_SIZE);
	}

	@After
	public void afterTest() {
		if (!this.memoryManager.verifyEmpty()) {
			Assert.fail("Memory Leak: Some memory has not been returned to the memory manager.");
		}
		
		if (this.memoryManager != null) {
			this.memoryManager.shutdown();
			this.memoryManager = null;
		}
	}

	private NormalizedKeySorter<PactRecord> newSortBuffer(List<MemorySegment> memory) throws Exception
	{
		@SuppressWarnings("unchecked")
		PactRecordAccessors accessors = new PactRecordAccessors(new int[] {0}, new Class[]{Key.class});
		return new NormalizedKeySorter<PactRecord>(accessors, memory);
	}

	@Test
	public void testWriteAndRead() throws Exception
	{
		final int numSegments = MEMORY_SIZE / MEMORY_SEGMENT_SIZE;
		final List<MemorySegment> memory = this.memoryManager.allocate(new DummyInvokable(), numSegments, MEMORY_SEGMENT_SIZE);
		
		NormalizedKeySorter<PactRecord> sorter = newSortBuffer(memory);
		TestData.Generator generator = new TestData.Generator(SEED, KEY_MAX, VALUE_LENGTH, KeyMode.RANDOM,
			ValueMode.RANDOM_LENGTH);
		
		// write the records
		PactRecord record = new PactRecord();
		do {
			generator.next(record);
		}
		while (sorter.write(record));
		
		// re-read the records
		generator.reset();
		MutableObjectIterator<PactRecord> iter = sorter.getIterator();
		PactRecord readTarget = new PactRecord();
		
		while (iter.next(readTarget)) {
			generator.next(record);
			
			Key rk = readTarget.getField(0, Key.class);
			Key gk = record.getField(0, Key.class);
			
			Value rv = readTarget.getField(1, Value.class);
			Value gv = record.getField(1, Value.class);
			
			Assert.assertEquals("The re-read key is wrong", gk, rk);
			Assert.assertEquals("The re-read value is wrong", gv, rv);
		}
		
		// release the memory occupied by the buffers
		this.memoryManager.release(sorter.dispose());
	}
	
	@Test
	public void testSort() throws Exception
	{
		final int numSegments = MEMORY_SIZE / MEMORY_SEGMENT_SIZE;
		final List<MemorySegment> memory = this.memoryManager.allocate(new DummyInvokable(), numSegments, MEMORY_SEGMENT_SIZE);
		
		NormalizedKeySorter<PactRecord> sorter = newSortBuffer(memory);
		TestData.Generator generator = new TestData.Generator(SEED, KEY_MAX, VALUE_LENGTH, KeyMode.RANDOM,
			ValueMode.RANDOM_LENGTH);
		
		// write the records
		PactRecord record = new PactRecord();
		do {
			generator.next(record);
		}
		while (sorter.write(record));
		

		QuickSort qs = new QuickSort();
		qs.sort(sorter);
		
		MutableObjectIterator<PactRecord> iter = sorter.getIterator();
		PactRecord readTarget = new PactRecord();
		
		while (iter.next(readTarget)) {
			Key rk = readTarget.getField(0, Key.class);
			Value rv = readTarget.getField(1, Value.class);
		}
		
		// release the memory occupied by the buffers
		this.memoryManager.release(sorter.dispose());
	}
}
