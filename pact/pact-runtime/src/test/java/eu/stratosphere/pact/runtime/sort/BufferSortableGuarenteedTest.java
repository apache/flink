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

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.services.iomanager.Buffer;
import eu.stratosphere.nephele.services.iomanager.RawComparator;
import eu.stratosphere.nephele.services.iomanager.Writer;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.nephele.services.memorymanager.spi.DefaultMemoryManager;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.runtime.test.util.DummyInvokable;
import eu.stratosphere.pact.runtime.test.util.TestData;
import eu.stratosphere.pact.runtime.test.util.TestData.Generator.KeyMode;
import eu.stratosphere.pact.runtime.test.util.TestData.Generator.ValueMode;
import eu.stratosphere.pact.runtime.test.util.TestData.Key;
import eu.stratosphere.pact.runtime.util.MutableObjectIterator;

/**
 * @author Erik Nijkamp
 */
public class BufferSortableGuarenteedTest {
	private static final Log LOG = LogFactory.getLog(BufferSortableGuarenteedTest.class);

	private static final long SEED = 649180756312423613L;

	private static final int KEY_MAX = Integer.MAX_VALUE;

	private static final int VALUE_LENGTH = 118;

	public static final int MEMORY_SIZE = 1024 * 1024 * 16;

	private DefaultMemoryManager memoryManager;

	@SuppressWarnings("unused")
	private static Level rootLevel, pkqLevel;

	@BeforeClass
	public static void beforeClass() {
		
	}

	@AfterClass
	public static void afterClass() {
		
	}

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

	private BufferSortableGuaranteed newSortBuffer(MemorySegment memory) throws Exception
	{
		Comparator<TestData.Key> keyComparator = new TestData.KeyComparator();
		@SuppressWarnings("unchecked")
		RawComparator comparator = new DeserializerComparator(new int[]{0}, new Class[]{TestData.Key.class},
			new Comparator[]{keyComparator});
		
		return new BufferSortableGuaranteed(memory, comparator);
	}

	@Test
	public void testWrite() throws Exception {
		// allocate memory segment
		AbstractInvokable memOwner = new DummyInvokable();
		MemorySegment memory = memoryManager.allocate(memOwner, MEMORY_SIZE);

		int writtenPairs = 0, readPairs = 0, position;
		
		// write pairs to buffer
		{
			TestData.Generator generator = new TestData.Generator(SEED, KEY_MAX, VALUE_LENGTH, KeyMode.SORTED,
				ValueMode.FIX_LENGTH);
			BufferSortableGuaranteed buffer = newSortBuffer(memory);
			int writtenBytes = 0;
			PactRecord rec = new PactRecord();
			
			rec = generator.next(rec);
			while (buffer.write(rec)) {
				writtenBytes += generator.sizeOf(rec);
				writtenPairs++;
				rec = generator.next(rec);
			}
			LOG.debug("Written " + writtenPairs + " pairs to buffer which occupied " + writtenBytes + " of "
				+ MEMORY_SIZE + " bytes.");
			position = buffer.getPosition();
			memory = buffer.unbind();
		}
		
		// read pairs from memory
		{
			Buffer.Input buffer = new Buffer.Input(memory);
			buffer.reset(position);
			PactRecord readRec = new PactRecord();
			while (buffer.read(readRec)) {
				readPairs++;
			}
			LOG.debug("Read " + readPairs + " pairs from buffer.");
			memory = buffer.dispose();
		}

		// assert
		Assert.assertEquals(writtenPairs, readPairs);

		// release the memory occupied by the buffers
		memoryManager.release(memory);
	}

	@Test
	public void testWriteRandom() throws Exception {
		// allocate memory segment
		AbstractInvokable memOwner = new DummyInvokable();
		MemorySegment memory = memoryManager.allocate(memOwner, 1024);

		int writtenPairs = 0, readPairs = 0, limit;

		// write pairs to buffer
		{
			TestData.Generator generator = new TestData.Generator(SEED, KEY_MAX, VALUE_LENGTH, KeyMode.RANDOM,
				ValueMode.RANDOM_LENGTH);
			BufferSortableGuaranteed buffer = newSortBuffer(memory);
			int writtenBytes = 0;
			PactRecord rec = new PactRecord();
			rec = generator.next(rec);
			while (buffer.write(rec)) {
				LOG.debug("<- " + rec);
				writtenBytes += generator.sizeOf(rec);
				writtenPairs++;
				rec = generator.next(rec);
			}
			LOG.debug("Written " + writtenPairs + " pairs to buffer which occupied " + writtenBytes + " of " + 1024
				+ " bytes.");
			limit = buffer.getPosition();
			memory = buffer.unbind();
		}

		// read pairs from memory
		{
			Buffer.Input buffer = new Buffer.Input(memory);
			buffer.reset(limit);
			
			PactRecord readRec = new PactRecord();
			
			while (buffer.read(readRec) && buffer.getPosition() <= limit) {
				LOG.debug("-> " + readRec);
				readPairs++;
			}
			LOG.debug("Read " + readPairs + " pairs from buffer.");
			memory = buffer.dispose();
		}

		// assert
		Assert.assertEquals(writtenPairs, readPairs);

		// release the memory occupied by the buffers
		memoryManager.release(memory);
	}

	@Test
	public void testSort() throws Exception {
		AbstractInvokable memOwner = new DummyInvokable();
		int writtenPairs = 0, readPairs = 0;

		// allocate buffer for unsorted pairs
		MemorySegment unsortedMemory = memoryManager.allocate(memOwner, MEMORY_SIZE >> 1);
		final BufferSortableGuaranteed unsortedBuffer = newSortBuffer(unsortedMemory);

		// write pairs to buffer
		{
			TestData.Generator generator = new TestData.Generator(SEED, KEY_MAX, VALUE_LENGTH, KeyMode.RANDOM,
				ValueMode.RANDOM_LENGTH);
			PactRecord rec = new PactRecord();
			while (unsortedBuffer.write(generator.next(rec))) {
				writtenPairs++;
			}
			LOG.debug("Written " + writtenPairs + " pairs.");

		}

		// allocate buffer for sorted pairs
		MemorySegment sortedMemory = memoryManager.allocate(memOwner, MEMORY_SIZE >> 1);
		final Buffer.Output sortedBuffer = new Buffer.Output(sortedMemory);

		// write pairs in sorted fashion
		{
			// sort
			new QuickSort().sort(unsortedBuffer);

			// buffer to buffer mock writer
			Writer writer = new Writer() {
				@Override
				public Collection<MemorySegment> close() {
					return Collections.emptyList();
				}

				@Override
				public boolean write(IOReadableWritable readable) {
					return sortedBuffer.write(readable);
				}
			};

			// write pairs in sorted way
			unsortedBuffer.writeToChannel(writer);
		}

		// unbind
		unsortedMemory = unsortedBuffer.unbind();
		
		int sortedPos = sortedBuffer.getPosition();
		sortedMemory = sortedBuffer.dispose();

		// read pairs
		{
			// comparator
			Comparator<TestData.Key> keyComparator = new TestData.KeyComparator();

			// read buffer
			Buffer.Input buffer = new Buffer.Input(sortedMemory);
			buffer.reset(sortedPos);
			
			// comparable pairs
			PactRecord rec1 = new PactRecord();
			PactRecord rec2 = new PactRecord();
			Key k1 = new Key();
			Key k2 = new Key();

			buffer.read(rec1);
			rec1.getFieldInto(0, k1);
			
			readPairs++;
			while (buffer.read(rec2)) {
				rec2.getFieldInto(0, k2);
				readPairs++;
				
				Assert.assertTrue(keyComparator.compare(k1, k2) <= 0);
				
				PactRecord tmp = rec1;
				rec1 = rec2;
				k1.setKey(k2.getKey());
				
				rec2 = tmp;
			}
		}

		// assert
		Assert.assertEquals(writtenPairs, readPairs);

		// release the memory occupied by the buffers
		memoryManager.release(unsortedMemory);
		memoryManager.release(sortedMemory);
	}
	
	@Test
	public void testSwap() throws Exception {

		// allocate memory segment
		AbstractInvokable memOwner = new DummyInvokable();
		MemorySegment memory = memoryManager.allocate(memOwner, 256);

		// write pairs to buffer
		{
			BufferSortableGuaranteed buffer = newSortBuffer(memory);
			for(int i = 1; i <= 3; i++)
			{
				PactRecord rec = new PactRecord(new TestData.Key(i), new TestData.Value(""+i));
				buffer.write(rec);
			}
			
			buffer.swap(0, 1);
			
			{
				final MutableObjectIterator<PactRecord> iter = buffer.getIterator();
				final PactRecord rec = new PactRecord();
				Assert.assertEquals(2, iter.next(rec).getField(0, TestData.Key.class).getKey());
				Assert.assertEquals(1, iter.next(rec).getField(0, TestData.Key.class).getKey());
				Assert.assertEquals(3, iter.next(rec).getField(0, TestData.Key.class).getKey());
			}
			
			buffer.swap(1, 2);
			
			{
				MutableObjectIterator<PactRecord> iter = buffer.getIterator();
				final PactRecord rec = new PactRecord();
				Assert.assertEquals(2, iter.next(rec).getField(0, TestData.Key.class).getKey());
				Assert.assertEquals(3, iter.next(rec).getField(0, TestData.Key.class).getKey());
				Assert.assertEquals(1, iter.next(rec).getField(0, TestData.Key.class).getKey());
			}
			
			{
				MemorySegment memory2 = memoryManager.allocate(memOwner, 256);
				
				{
					final Buffer.Output buffer2 = new Buffer.Output(memory2);
					Writer writer = new Writer() {
						@Override
						public Collection<MemorySegment> close() {
							return Collections.emptyList();
						}
		
						@Override
						public boolean write(IOReadableWritable readable) {
							return buffer2.write(readable);
						}
					};
					buffer.writeToChannel(writer);
					memory2 = buffer2.dispose();
				}
				
				{
					Buffer.Input buffer2 = new Buffer.Input(memory2);
					
					@SuppressWarnings("unused")
					PactRecord rec = new PactRecord(new TestData.Key(), new TestData.Value());
					/*
					while (buffer2.read(pair) && buffer2.getPosition() <= position) 
					{
						System.out.println(pair);
					}
					*/
					memory2 = buffer2.dispose();
				}
				
				memoryManager.release(memory2);
			}

			memory = buffer.unbind();
		}

		// release the memory occupied by the buffers
		memoryManager.release(memory);

	}
	
	@Test
	public void testSimple() throws Exception {

		// allocate memory segment
		AbstractInvokable memOwner = new DummyInvokable();
		MemorySegment memory = memoryManager.allocate(memOwner, 256);

		// write pairs to buffer
		{
			BufferSortableGuaranteed buffer = newSortBuffer(memory);
			for(int i = 1; i < 4; i++)
			{
				PactRecord pair = new PactRecord(new TestData.Key(i), new TestData.Value(""+i));
				buffer.write(pair);
			}
			

			final MutableObjectIterator<PactRecord> iter = buffer.getIterator();
			final PactRecord rec = new PactRecord();
			for(int i = 1; i < 4; i++)
			{
				PactRecord pair = iter.next(rec);
				Assert.assertNotNull("Iterator returned not enough pairs.", pair);				
				Assert.assertEquals(i, pair.getField(0, TestData.Key.class).getKey());
			}
			
			{
				MemorySegment memory2 = memoryManager.allocate(memOwner, 256);
				final Buffer.Output buffer2 = new Buffer.Output(memory2);
				Writer writer = new Writer() {
					@Override
					public Collection<MemorySegment> close() {
						return Collections.emptyList();
					}
	
					@Override
					public boolean write(IOReadableWritable readable) {
						return buffer2.write(readable);
					}
				};
				buffer.writeToChannel(writer);
				
				memory2 = buffer2.dispose();
				memoryManager.release(memory2);
			}

			memory = buffer.unbind();
		}

		// release the memory occupied by the buffers
		memoryManager.release(memory);

	}

	@Test
	public void testIterator() throws Exception {

		// allocate memory segment
		AbstractInvokable memOwner = new DummyInvokable();
		MemorySegment memory = memoryManager.allocate(memOwner, MEMORY_SIZE);

		int writtenPairs = 0, readPairs = 0;

		// write pairs to buffer
		{
			TestData.Generator generator = new TestData.Generator(SEED, KEY_MAX, VALUE_LENGTH, KeyMode.SORTED,
				ValueMode.FIX_LENGTH);
			BufferSortableGuaranteed buffer = newSortBuffer(memory);
			int writtenBytes = 0;
			
			PactRecord rec = new PactRecord();
			rec = generator.next(rec);
			while (buffer.write(rec)) {
				writtenBytes += generator.sizeOf(rec) + Integer.SIZE / 8;
				writtenPairs++;
				rec = generator.next(rec);
			}
			LOG.debug("Written " + writtenPairs + " pairs to buffer which occupied " + writtenBytes + " of "
				+ MEMORY_SIZE + " bytes.");

			final MutableObjectIterator<PactRecord> iter = buffer.getIterator();
			
			while ((rec = iter.next(rec)) != null) {
				readPairs++;
			}
			memory = buffer.unbind();

			Assert.assertEquals(writtenPairs, readPairs);
		}

		// release the memory occupied by the buffers
		memoryManager.release(memory);

	}

}
