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
import java.util.Iterator;

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
import eu.stratosphere.nephele.services.iomanager.SerializationFactory;
import eu.stratosphere.nephele.services.iomanager.Writer;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.nephele.services.memorymanager.spi.DefaultMemoryManager;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.runtime.serialization.WritableSerializationFactory;
import eu.stratosphere.pact.runtime.test.util.DummyInvokable;
import eu.stratosphere.pact.runtime.test.util.TestData;
import eu.stratosphere.pact.runtime.test.util.TestData.Generator.KeyMode;
import eu.stratosphere.pact.runtime.test.util.TestData.Generator.ValueMode;

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

	private BufferSortableGuaranteed<TestData.Key, TestData.Value> newSortBuffer(MemorySegment memory)
			throws Exception {
		SerializationFactory<TestData.Key> keySerialization = new WritableSerializationFactory<TestData.Key>(
			TestData.Key.class);
		SerializationFactory<TestData.Value> valSerialization = new WritableSerializationFactory<TestData.Value>(
			TestData.Value.class);
		Comparator<TestData.Key> keyComparator = new TestData.KeyComparator();
		RawComparator comparator = new DeserializerComparator<TestData.Key>(keySerialization.getDeserializer(),
			keyComparator);
		return new BufferSortableGuaranteed<TestData.Key, TestData.Value>(memory, comparator, keySerialization, valSerialization);
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
			BufferSortableGuaranteed<TestData.Key, TestData.Value> buffer = newSortBuffer(memory);
			int writtenBytes = 0;
			KeyValuePair<TestData.Key, TestData.Value> pair = generator.next();
			while (buffer.write(pair)) {
				writtenBytes += generator.sizeOf(pair);
				writtenPairs++;
				pair = generator.next();
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
			KeyValuePair<TestData.Key, TestData.Value> pair = new KeyValuePair<TestData.Key, TestData.Value>(
				new TestData.Key(), new TestData.Value());
			while (buffer.read(pair)) {
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
			BufferSortableGuaranteed<TestData.Key, TestData.Value> buffer = newSortBuffer(memory);
			int writtenBytes = 0;
			KeyValuePair<TestData.Key, TestData.Value> pair = generator.next();
			while (buffer.write(pair)) {
				LOG.debug("<- " + pair);
				writtenBytes += generator.sizeOf(pair);
				writtenPairs++;
				pair = generator.next();
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
			
			KeyValuePair<TestData.Key, TestData.Value> pair = new KeyValuePair<TestData.Key, TestData.Value>(
				new TestData.Key(), new TestData.Value());
			
			while (buffer.read(pair) && buffer.getPosition() <= limit) {
				LOG.debug("-> " + pair);
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
		final BufferSortableGuaranteed<TestData.Key, TestData.Value> unsortedBuffer = newSortBuffer(unsortedMemory);

		// write pairs to buffer
		{
			TestData.Generator generator = new TestData.Generator(SEED, KEY_MAX, VALUE_LENGTH, KeyMode.RANDOM,
				ValueMode.RANDOM_LENGTH);
			while (unsortedBuffer.write(generator.next())) {
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
			KeyValuePair<TestData.Key, TestData.Value> pair1 = new KeyValuePair<TestData.Key, TestData.Value>(
				new TestData.Key(), new TestData.Value());
			KeyValuePair<TestData.Key, TestData.Value> pair2 = new KeyValuePair<TestData.Key, TestData.Value>(
				new TestData.Key(), new TestData.Value());
			buffer.read(pair1);
			readPairs++;
			while (buffer.read(pair2)) {
				readPairs++;
				Assert.assertTrue(keyComparator.compare(pair1.getKey(), pair2.getKey()) <= 0);
				
				KeyValuePair<TestData.Key, TestData.Value> tmp = pair1;
				pair1 = pair2;
				pair2 = tmp;
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
			BufferSortableGuaranteed<TestData.Key, TestData.Value> buffer = newSortBuffer(memory);
			for(int i = 1; i <= 3; i++)
			{
				KeyValuePair<TestData.Key, TestData.Value> pair = new KeyValuePair<TestData.Key, TestData.Value>(new TestData.Key(i), new TestData.Value(""+i));
				buffer.write(pair);
			}
			
			buffer.swap(0, 1);
			
			{
				Iterator<KeyValuePair<TestData.Key, TestData.Value>> iter = buffer.getIterator();
				Assert.assertEquals(2, iter.next().getKey().getKey());
				Assert.assertEquals(1, iter.next().getKey().getKey());
				Assert.assertEquals(3, iter.next().getKey().getKey());
			}
			
			buffer.swap(1, 2);
			
			{
				Iterator<KeyValuePair<TestData.Key, TestData.Value>> iter = buffer.getIterator();
				Assert.assertEquals(2, iter.next().getKey().getKey());
				Assert.assertEquals(3, iter.next().getKey().getKey());
				Assert.assertEquals(1, iter.next().getKey().getKey());
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
					KeyValuePair<TestData.Key, TestData.Value> pair = new KeyValuePair<TestData.Key, TestData.Value>(new TestData.Key(), new TestData.Value());
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
			BufferSortableGuaranteed<TestData.Key, TestData.Value> buffer = newSortBuffer(memory);
			for(int i = 1; i < 4; i++)
			{
				KeyValuePair<TestData.Key, TestData.Value> pair = new KeyValuePair<TestData.Key, TestData.Value>(new TestData.Key(i), new TestData.Value(""+i));
				buffer.write(pair);
			}
			

			Iterator<KeyValuePair<TestData.Key, TestData.Value>> iter = buffer.getIterator();
			for(int i = 1; i < 4; i++)
			{
				if(!iter.hasNext())
				{
					throw new IllegalStateException();
				}
				KeyValuePair<TestData.Key, TestData.Value> pair = iter.next();
				Assert.assertEquals(i, pair.getKey().getKey());
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
			BufferSortableGuaranteed<TestData.Key, TestData.Value> buffer = newSortBuffer(memory);
			int writtenBytes = 0;
			KeyValuePair<TestData.Key, TestData.Value> pair = generator.next();
			while (buffer.write(pair)) {
				writtenBytes += generator.sizeOf(pair) + Integer.SIZE / 8;
				writtenPairs++;
				pair = generator.next();
			}
			LOG.debug("Written " + writtenPairs + " pairs to buffer which occupied " + writtenBytes + " of "
				+ MEMORY_SIZE + " bytes.");

			Iterator<KeyValuePair<TestData.Key, TestData.Value>> iter = buffer.getIterator();

			while (iter.hasNext()) {
				iter.next();
				readPairs++;
			}
			memory = buffer.unbind();

			Assert.assertEquals(writtenPairs, readPairs);
		}

		// release the memory occupied by the buffers
		memoryManager.release(memory);

	}

}
