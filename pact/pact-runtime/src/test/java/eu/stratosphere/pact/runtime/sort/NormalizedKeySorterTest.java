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
import java.util.Random;

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
		int num = -1;
		do {
			generator.next(record);
			num++;
		}
		while (sorter.write(record));
		
		// re-read the records
		generator.reset();
		PactRecord readTarget = new PactRecord();
		
		int i = 0;
		while (i < num) {
			generator.next(record);
			sorter.getRecord(readTarget, i++);
			
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
	public void testWriteAndIterator() throws Exception
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
	
	/**
	 * The swap test fills the sort buffer and swaps all elements such that they are
	 * backwards. It then resets the generator, goes backwards through the buffer
	 * and compares for equality.
	 */
	@Test
	public void testSwap() throws Exception
	{
		final int numSegments = MEMORY_SIZE / MEMORY_SEGMENT_SIZE;
		final List<MemorySegment> memory = this.memoryManager.allocate(new DummyInvokable(), numSegments, MEMORY_SEGMENT_SIZE);
		
		NormalizedKeySorter<PactRecord> sorter = newSortBuffer(memory);
		TestData.Generator generator = new TestData.Generator(SEED, KEY_MAX, VALUE_LENGTH, KeyMode.RANDOM,
			ValueMode.RANDOM_LENGTH);
		
		// write the records
		PactRecord record = new PactRecord();
		int num = -1;
		do {
			generator.next(record);
			num++;
		}
		while (sorter.write(record));
		
		// swap the records
		int start = 0, end = num - 1;
		while (start < end) {
			sorter.swap(start++, end--);
		}
		
		// re-read the records
		generator.reset();
		PactRecord readTarget = new PactRecord();
		
		int i = num - 1;
		while (i >= 0) {
			generator.next(record);
			sorter.getRecord(readTarget, i--);
			
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
	
	/**
	 * The compare test creates a sorted stream, writes it to the buffer and
	 * compares random elements. It expects that earlier elements are lower than later
	 * ones.
	 */
	@Test
	public void testCompare() throws Exception
	{
		final int numSegments = MEMORY_SIZE / MEMORY_SEGMENT_SIZE;
		final List<MemorySegment> memory = this.memoryManager.allocate(new DummyInvokable(), numSegments, MEMORY_SEGMENT_SIZE);
		
		NormalizedKeySorter<PactRecord> sorter = newSortBuffer(memory);
		TestData.Generator generator = new TestData.Generator(SEED, KEY_MAX, VALUE_LENGTH, KeyMode.SORTED,
			ValueMode.RANDOM_LENGTH);
		
		// write the records
		PactRecord record = new PactRecord();
		int num = -1;
		do {
			generator.next(record);
			num++;
		}
		while (sorter.write(record));
		
		// compare random elements
		Random rnd = new Random(SEED << 1);
		for (int i = 0; i < 2 * num; i++) {
			int pos1 = rnd.nextInt(num);
			int pos2 = rnd.nextInt(num);
			
			int cmp = sorter.compare(pos1, pos2);
			
			if (pos1 < pos2) {
				Assert.assertTrue(cmp <= 0);
			}
			else {
				Assert.assertTrue(cmp >= 0);
			}
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
		
		long writeStart = System.nanoTime();
		// write the records
		PactRecord record = new PactRecord();
		do {
			generator.next(record);
		}
		while (sorter.write(record));
		long writeStop = System.nanoTime();
		
		System.out.println("Writing took: " + (writeStop - writeStart) / 1000000 + " msec");
		
		long sortStart = System.nanoTime();
		
		QuickSort qs = new QuickSort();
		qs.sort(sorter);
		
		long sortStop = System.nanoTime();
		
		System.out.println("Sorting took: " + (sortStop - sortStart) / 1000000 + " msec");
		
		MutableObjectIterator<PactRecord> iter = sorter.getIterator();
		PactRecord readTarget = new PactRecord();
		
		Key current = new Key();
		Key last = new Key();
		
		iter.next(readTarget);
		readTarget.getFieldInto(0, last);
		
		while (iter.next(readTarget)) {
			readTarget.getFieldInto(0, current);
			
			final int cmp = last.compareTo(current);
			if (cmp > 0)
				Assert.fail("Next key is not larger or equal to previous key.");
			
			Key tmp = current;
			current = last;
			last = tmp;
		}
		
		// release the memory occupied by the buffers
		this.memoryManager.release(sorter.dispose());
	}
}
