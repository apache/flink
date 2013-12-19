/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.sort;

import java.util.List;
import java.util.Random;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.api.common.typeutils.TypeComparator;
import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.core.memory.MemorySegment;
import eu.stratosphere.nephele.services.memorymanager.spi.DefaultMemoryManager;
import eu.stratosphere.pact.runtime.test.util.DummyInvokable;
import eu.stratosphere.pact.runtime.test.util.RandomIntPairGenerator;
import eu.stratosphere.pact.runtime.test.util.UniformIntPairGenerator;
import eu.stratosphere.pact.runtime.test.util.types.IntPair;
import eu.stratosphere.pact.runtime.test.util.types.IntPairComparator;
import eu.stratosphere.pact.runtime.test.util.types.IntPairSerializer;
import eu.stratosphere.util.MutableObjectIterator;

/**
 * 
 */
public class FixedLengthRecordSorterTest
{
	private static final long SEED = 649180756312423613L;

	private static final int MEMORY_SIZE = 1024 * 1024 * 64;
	
	private static final int MEMORY_PAGE_SIZE = 32 * 1024; 

	private DefaultMemoryManager memoryManager;
	
	private TypeSerializer<IntPair> serializer;
	
	private TypeComparator<IntPair> comparator;


	@Before
	public void beforeTest() {
		this.memoryManager = new DefaultMemoryManager(MEMORY_SIZE, MEMORY_PAGE_SIZE);
		this.serializer = new IntPairSerializer();
		this.comparator = new IntPairComparator();
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

	private FixedLengthRecordSorter<IntPair> newSortBuffer(List<MemorySegment> memory) throws Exception {
		return new FixedLengthRecordSorter<IntPair>(this.serializer, this.comparator, memory);
	}

	@Test
	public void testWriteAndRead() throws Exception {
		final int numSegments = MEMORY_SIZE / MEMORY_PAGE_SIZE;
		final List<MemorySegment> memory = this.memoryManager.allocatePages(new DummyInvokable(), numSegments);
		
		FixedLengthRecordSorter<IntPair> sorter = newSortBuffer(memory);
		RandomIntPairGenerator generator = new RandomIntPairGenerator(SEED);
		
//		long startTime = System.currentTimeMillis();
		// write the records
		IntPair record = new IntPair();
		int num = -1;
		do {
			generator.next(record);
			num++;
		}
		while (sorter.write(record) && num < 3354624);
//		System.out.println("WRITE TIME " + (System.currentTimeMillis() - startTime));
		
		// re-read the records
		generator.reset();
		IntPair readTarget = new IntPair();
		
//		startTime = System.currentTimeMillis();
		int i = 0;
		while (i < num) {
			generator.next(record);
			sorter.getRecord(readTarget, i++);
			
			int rk = readTarget.getKey();
			int gk = record.getKey();
			
			int rv = readTarget.getValue();
			int gv = record.getValue();
			
			if (gk != rk) {
				Assert.fail("The re-read key is wrong " + i);
			}
			if (gv != rv) {
				Assert.fail("The re-read value is wrong");
			}
		}
//		System.out.println("READ TIME " + (System.currentTimeMillis() - startTime));
//		System.out.println("RECORDS " + num);
		
		// release the memory occupied by the buffers
		this.memoryManager.release(sorter.dispose());
	}
	
	@Test
	public void testWriteAndIterator() throws Exception {
		final int numSegments = MEMORY_SIZE / MEMORY_PAGE_SIZE;
		final List<MemorySegment> memory = this.memoryManager.allocatePages(new DummyInvokable(), numSegments);
		
		FixedLengthRecordSorter<IntPair> sorter = newSortBuffer(memory);
		RandomIntPairGenerator generator = new RandomIntPairGenerator(SEED);
		
		// write the records
		IntPair record = new IntPair();
		int num = -1;
		do {
			generator.next(record);
			num++;
		}
		while (sorter.write(record));
		
		// re-read the records
		generator.reset();
		
		MutableObjectIterator<IntPair> iter = sorter.getIterator();
		IntPair readTarget = new IntPair();
		int count = 0;
		
		while (iter.next(readTarget)) {
			count++;
			
			generator.next(record);
			
			int rk = readTarget.getKey();
			int gk = record.getKey();
			
			int rv = readTarget.getValue();
			int gv = record.getValue();
			
			Assert.assertEquals("The re-read key is wrong", gk, rk);
			Assert.assertEquals("The re-read value is wrong", gv, rv);
		}
		
		Assert.assertEquals("Incorrect number of records", num, count);
		
		// release the memory occupied by the buffers
		this.memoryManager.release(sorter.dispose());
	}
	
	@Test
	public void testReset() throws Exception {
		final int numSegments = MEMORY_SIZE / MEMORY_PAGE_SIZE;
		final List<MemorySegment> memory = this.memoryManager.allocatePages(new DummyInvokable(), numSegments);
		
		FixedLengthRecordSorter<IntPair> sorter = newSortBuffer(memory);
		RandomIntPairGenerator generator = new RandomIntPairGenerator(SEED);
		
		// write the buffer full with the first set of records
		IntPair record = new IntPair();
		int num = -1;
		do {
			generator.next(record);
			num++;
		}
		while (sorter.write(record) && num < 3354624);
		
		sorter.reset();
		
		// write a second sequence of records. since the values are of fixed length, we must be able to write an equal number
		generator.reset();
		
		// write the buffer full with the first set of records
		int num2 = -1;
		do {
			generator.next(record);
			num2++;
		}
		while (sorter.write(record) && num2 < 3354624);
		
		Assert.assertEquals("The number of records written after the reset was not the same as before.", num, num2);
		
		// re-read the records
		generator.reset();
		IntPair readTarget = new IntPair();
		
		int i = 0;
		while (i < num) {
			generator.next(record);
			sorter.getRecord(readTarget, i++);
			
			int rk = readTarget.getKey();
			int gk = record.getKey();
			
			int rv = readTarget.getValue();
			int gv = record.getValue();
			
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
	public void testSwap() throws Exception {
		final int numSegments = MEMORY_SIZE / MEMORY_PAGE_SIZE;
		final List<MemorySegment> memory = this.memoryManager.allocatePages(new DummyInvokable(), numSegments);
		
		FixedLengthRecordSorter<IntPair> sorter = newSortBuffer(memory);
		RandomIntPairGenerator generator = new RandomIntPairGenerator(SEED);
		
		// write the records
		IntPair record = new IntPair();
		int num = -1;
		do {
			generator.next(record);
			num++;
		}
		while (sorter.write(record) && num < 3354624);
		
		// swap the records
		int start = 0, end = num - 1;
		while (start < end) {
			sorter.swap(start++, end--);
		}
		
		// re-read the records
		generator.reset();
		IntPair readTarget = new IntPair();
		
		int i = num - 1;
		while (i >= 0) {
			generator.next(record);
			sorter.getRecord(readTarget, i--);
			
			int rk = readTarget.getKey();
			int gk = record.getKey();
			
			int rv = readTarget.getValue();
			int gv = record.getValue();
			
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
	public void testCompare() throws Exception {
		final int numSegments = MEMORY_SIZE / MEMORY_PAGE_SIZE;
		final List<MemorySegment> memory = this.memoryManager.allocatePages(new DummyInvokable(), numSegments);
		
		FixedLengthRecordSorter<IntPair> sorter = newSortBuffer(memory);
		UniformIntPairGenerator generator = new UniformIntPairGenerator(Integer.MAX_VALUE, 1, true);
		
		// write the records
		IntPair record = new IntPair();
		int num = -1;
		do {
			generator.next(record);
			num++;
		}
		while (sorter.write(record) && num < 3354624);
		
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
	public void testSort() throws Exception {
		final int NUM_RECORDS = 559273;
		final int numSegments = MEMORY_SIZE / MEMORY_PAGE_SIZE;
		final List<MemorySegment> memory = this.memoryManager.allocatePages(new DummyInvokable(), numSegments);
		
		FixedLengthRecordSorter<IntPair> sorter = newSortBuffer(memory);
		RandomIntPairGenerator generator = new RandomIntPairGenerator(SEED);
		
		// write the records
		IntPair record = new IntPair();
		int num = -1;
		do {
			generator.next(record);
			num++;
		}
		while (sorter.write(record) && num < NUM_RECORDS);
		
		QuickSort qs = new QuickSort();
		qs.sort(sorter);
		
		MutableObjectIterator<IntPair> iter = sorter.getIterator();
		IntPair readTarget = new IntPair();
		
		int current = 0;
		int last = 0;
		
		iter.next(readTarget);
		//readTarget.getFieldInto(0, last);
		last = readTarget.getKey();
		
		while (iter.next(readTarget)) {
			current = readTarget.getKey();
			
			final int cmp = last - current;
			if (cmp > 0)
				Assert.fail("Next key is not larger or equal to previous key.");
			
			int tmp = current;
			current = last;
			last = tmp;
		}
		
		// release the memory occupied by the buffers
		this.memoryManager.release(sorter.dispose());
	}
}
