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

import java.util.List;
import java.util.Random;

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemoryType;
import org.apache.flink.runtime.io.disk.ChannelReaderInputViewIterator;
import org.apache.flink.runtime.io.disk.iomanager.BlockChannelReader;
import org.apache.flink.runtime.io.disk.iomanager.BlockChannelWriter;
import org.apache.flink.runtime.io.disk.iomanager.ChannelReaderInputView;
import org.apache.flink.runtime.io.disk.iomanager.ChannelWriterOutputView;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.testutils.DummyInvokable;
import org.apache.flink.runtime.operators.testutils.RandomIntPairGenerator;
import org.apache.flink.runtime.operators.testutils.UniformIntPairGenerator;
import org.apache.flink.runtime.operators.testutils.types.IntPair;
import org.apache.flink.runtime.operators.testutils.types.IntPairComparator;
import org.apache.flink.runtime.operators.testutils.types.IntPairSerializer;
import org.apache.flink.util.MutableObjectIterator;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class FixedLengthRecordSorterTest {
	
	private static final long SEED = 649180756312423613L;

	private static final int MEMORY_SIZE = 1024 * 1024 * 64;
	
	private static final int MEMORY_PAGE_SIZE = 32 * 1024; 

	private MemoryManager memoryManager;

	private IOManager ioManager;
	
	private TypeSerializer<IntPair> serializer;
	
	private TypeComparator<IntPair> comparator;


	@Before
	public void beforeTest() {
		this.memoryManager = new MemoryManager(MEMORY_SIZE, 1, MEMORY_PAGE_SIZE, MemoryType.HEAP, true);
		this.ioManager = new IOManagerAsync();
		this.serializer = new IntPairSerializer();
		this.comparator = new IntPairComparator();
	}

	@After
	public void afterTest() throws Exception {
		if (!this.memoryManager.verifyEmpty()) {
			Assert.fail("Memory Leak: Some memory has not been returned to the memory manager.");
		}

		if (this.ioManager != null) {
			ioManager.close();
			ioManager = null;
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
			readTarget = sorter.getRecord(readTarget, i++);
			
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
		sorter.dispose();
		this.memoryManager.release(memory);
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
		
		while ((readTarget = iter.next(readTarget)) != null) {
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
		sorter.dispose();
		this.memoryManager.release(memory);
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
			readTarget = sorter.getRecord(readTarget, i++);
			
			int rk = readTarget.getKey();
			int gk = record.getKey();
			
			int rv = readTarget.getValue();
			int gv = record.getValue();
			
			Assert.assertEquals("The re-read key is wrong", gk, rk);
			Assert.assertEquals("The re-read value is wrong", gv, rv);
		}
		
		// release the memory occupied by the buffers
		sorter.dispose();
		this.memoryManager.release(memory);
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
			readTarget = sorter.getRecord(readTarget, i--);
			
			int rk = readTarget.getKey();
			int gk = record.getKey();
			
			int rv = readTarget.getValue();
			int gv = record.getValue();
			
			Assert.assertEquals("The re-read key is wrong", gk, rk);
			Assert.assertEquals("The re-read value is wrong", gv, rv);
		}
		
		// release the memory occupied by the buffers
		sorter.dispose();
		this.memoryManager.release(memory);
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
		sorter.dispose();
		this.memoryManager.release(memory);
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
		
		int current;
		int last;
		
		iter.next(readTarget);
		last = readTarget.getKey();
		
		while ((readTarget = iter.next(readTarget)) != null) {
			current = readTarget.getKey();
			
			final int cmp = last - current;
			if (cmp > 0) {
				Assert.fail("Next key is not larger or equal to previous key.");
			}
			last = current;
		}
		
		// release the memory occupied by the buffers
		sorter.dispose();
		this.memoryManager.release(memory);
	}

	@Test
	public void testFlushFullMemoryPage() throws Exception {
		// Insert IntPair which would fill 2 memory pages.
		final int NUM_RECORDS = 2 * MEMORY_PAGE_SIZE / 8;
		final List<MemorySegment> memory = this.memoryManager.allocatePages(new DummyInvokable(), 3);

		FixedLengthRecordSorter<IntPair> sorter = newSortBuffer(memory);
		UniformIntPairGenerator generator = new UniformIntPairGenerator(Integer.MAX_VALUE, 1, false);

		// write the records
		IntPair record = new IntPair();
		int num = -1;
		do {
			generator.next(record);
			num++;
		}
		while (sorter.write(record) && num < NUM_RECORDS);

		FileIOChannel.ID channelID = this.ioManager.createChannelEnumerator().next();
		BlockChannelWriter<MemorySegment> blockChannelWriter = this.ioManager.createBlockChannelWriter(channelID);
		final List<MemorySegment> writeBuffer = this.memoryManager.allocatePages(new DummyInvokable(), 3);
		ChannelWriterOutputView outputView = new ChannelWriterOutputView(blockChannelWriter, writeBuffer, writeBuffer.get(0).size());

		sorter.writeToOutput(outputView, 0, NUM_RECORDS);

		this.memoryManager.release(outputView.close());

		BlockChannelReader<MemorySegment> blockChannelReader = this.ioManager.createBlockChannelReader(channelID);
		final List<MemorySegment> readBuffer = this.memoryManager.allocatePages(new DummyInvokable(), 3);
		ChannelReaderInputView readerInputView = new ChannelReaderInputView(blockChannelReader, readBuffer, false);
		final List<MemorySegment> dataBuffer = this.memoryManager.allocatePages(new DummyInvokable(), 3);
		ChannelReaderInputViewIterator<IntPair> iterator = new ChannelReaderInputViewIterator(readerInputView, dataBuffer, this.serializer);

		record = iterator.next(record);
		int i =0;
		while (record != null) {
			Assert.assertEquals(i, record.getKey());
			record = iterator.next(record);
			i++;
		}

		Assert.assertEquals(NUM_RECORDS, i);

		this.memoryManager.release(dataBuffer);
		// release the memory occupied by the buffers
		sorter.dispose();
		this.memoryManager.release(memory);
	}

	@Test
	public void testFlushPartialMemoryPage() throws Exception {
		// Insert IntPair which would fill 2 memory pages.
		final int NUM_RECORDS = 2 * MEMORY_PAGE_SIZE / 8;
		final List<MemorySegment> memory = this.memoryManager.allocatePages(new DummyInvokable(), 3);

		FixedLengthRecordSorter<IntPair> sorter = newSortBuffer(memory);
		UniformIntPairGenerator generator = new UniformIntPairGenerator(Integer.MAX_VALUE, 1, false);

		// write the records
		IntPair record = new IntPair();
		int num = -1;
		do {
			generator.next(record);
			num++;
		}
		while (sorter.write(record) && num < NUM_RECORDS);

		FileIOChannel.ID channelID = this.ioManager.createChannelEnumerator().next();
		BlockChannelWriter<MemorySegment> blockChannelWriter = this.ioManager.createBlockChannelWriter(channelID);
		final List<MemorySegment> writeBuffer = this.memoryManager.allocatePages(new DummyInvokable(), 3);
		ChannelWriterOutputView outputView = new ChannelWriterOutputView(blockChannelWriter, writeBuffer, writeBuffer.get(0).size());

		sorter.writeToOutput(outputView, 1, NUM_RECORDS - 1);

		this.memoryManager.release(outputView.close());

		BlockChannelReader<MemorySegment> blockChannelReader = this.ioManager.createBlockChannelReader(channelID);
		final List<MemorySegment> readBuffer = this.memoryManager.allocatePages(new DummyInvokable(), 3);
		ChannelReaderInputView readerInputView = new ChannelReaderInputView(blockChannelReader, readBuffer, false);
		final List<MemorySegment> dataBuffer = this.memoryManager.allocatePages(new DummyInvokable(), 3);
		ChannelReaderInputViewIterator<IntPair> iterator = new ChannelReaderInputViewIterator(readerInputView, dataBuffer, this.serializer);

		record = iterator.next(record);
		int i =1;
		while (record != null) {
			Assert.assertEquals(i, record.getKey());
			record = iterator.next(record);
			i++;
		}

		Assert.assertEquals(NUM_RECORDS, i);

		this.memoryManager.release(dataBuffer);
		// release the memory occupied by the buffers
		sorter.dispose();
		this.memoryManager.release(memory);
	}
}
