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
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.services.ServiceException;
import eu.stratosphere.nephele.services.iomanager.Buffer;
import eu.stratosphere.nephele.services.iomanager.RawComparator;
import eu.stratosphere.nephele.services.iomanager.SerializationFactory;
import eu.stratosphere.nephele.services.iomanager.Writer;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.nephele.services.memorymanager.spi.DefaultMemoryManager;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.runtime.serialization.WritableSerializationFactory;
import eu.stratosphere.pact.runtime.sort.BufferSortable;
import eu.stratosphere.pact.runtime.sort.DeserializerComparator;
import eu.stratosphere.pact.runtime.sort.QuickSort;
import eu.stratosphere.pact.runtime.test.util.TestData;
import eu.stratosphere.pact.runtime.test.util.TestData.Generator.KeyMode;
import eu.stratosphere.pact.runtime.test.util.TestData.Generator.ValueMode;

/**
 * @author Erik Nijkamp
 */
public class TestBufferSortable {
	private static final Log LOG = LogFactory.getLog(TestBufferSortable.class);

	private static final long SEED = 649180756312423613L;

	private static final int KEY_MAX = Integer.MAX_VALUE;

	private static final int VALUE_LENGTH = 118;

	public static final int MEMORY_SIZE = 1024 * 1024 * 16;

	public static final float OFFSETS_PERCENTAGE = 0.2f;

	private MemoryManager memoryManager;

	private static Level rootLevel, pkqLevel;

	@BeforeClass
	public static void beforeClass() {
		Logger rootLogger = Logger.getRootLogger();
		rootLevel = rootLogger.getLevel();
		rootLogger.setLevel(Level.INFO);

		Logger pkgLogger = rootLogger.getLoggerRepository().getLogger(BufferSortable.class.getPackage().getName());
		pkqLevel = pkgLogger.getLevel();
		pkgLogger.setLevel(Level.DEBUG);
	}

	@AfterClass
	public static void afterClass() {
		Logger rootLogger = Logger.getRootLogger();
		rootLogger.setLevel(rootLevel);

		Logger pkgLogger = rootLogger.getLoggerRepository().getLogger(BufferSortable.class.getPackage().getName());
		pkgLogger.setLevel(pkqLevel);
	}

	@Before
	public void beforeTest() {
		memoryManager = new DefaultMemoryManager(MEMORY_SIZE);
	}

	@After
	public void afterTest() {
		if (memoryManager != null)
			memoryManager.shutdown();
	}

	private BufferSortable<TestData.Key, TestData.Value> newSortBuffer(MemorySegment memory, float offsetsPercentage)
			throws Exception {
		SerializationFactory<TestData.Key> keySerialization = new WritableSerializationFactory<TestData.Key>(
			TestData.Key.class);
		SerializationFactory<TestData.Value> valSerialization = new WritableSerializationFactory<TestData.Value>(
			TestData.Value.class);
		Comparator<TestData.Key> keyComparator = new TestData.KeyComparator();
		RawComparator comparator = new DeserializerComparator<TestData.Key>(keySerialization.getDeserializer(),
			keyComparator);
		return new BufferSortable<TestData.Key, TestData.Value>(memory, comparator, keySerialization, valSerialization,
			offsetsPercentage);
	}

	@Test
	public void testWrite() throws Exception {
		// allocate memory segment
		MemorySegment memory = memoryManager.allocate(MEMORY_SIZE);

		int writtenPairs = 0, readPairs = 0;

		// write pairs to buffer
		{
			TestData.Generator generator = new TestData.Generator(SEED, KEY_MAX, VALUE_LENGTH, KeyMode.SORTED,
				ValueMode.FIX_LENGTH);
			BufferSortable<TestData.Key, TestData.Value> buffer = newSortBuffer(memory, OFFSETS_PERCENTAGE);
			int writtenBytes = 0;
			KeyValuePair<TestData.Key, TestData.Value> pair = generator.next();
			while (buffer.write(pair)) {
				writtenBytes += generator.sizeOf(pair) + Integer.SIZE / 8;
				writtenPairs++;
				pair = generator.next();
			}
			LOG.info("Written " + writtenPairs + " pairs to buffer which occupied " + writtenBytes + " of "
				+ MEMORY_SIZE + " bytes.");
			memory = buffer.unbind();
		}

		// read pairs from memory
		{
			Buffer.Input buffer = new Buffer.Input();
			buffer.bind(memory);
			KeyValuePair<TestData.Key, TestData.Value> pair = new KeyValuePair<TestData.Key, TestData.Value>(
				new TestData.Key(), new TestData.Value());
			while (buffer.read(pair)) {
				readPairs++;
			}
			LOG.info("Read " + readPairs + " pairs from buffer.");
			memory = buffer.unbind();
		}

		// assert
		Assert.assertEquals(writtenPairs, readPairs);

		// release the memory occupied by the buffers
		memoryManager.release(memory);
	}

	@Test
	public void testWriteRandom() throws Exception {
		// allocate memory segment
		MemorySegment memory = memoryManager.allocate(1024);

		int writtenPairs = 0, readPairs = 0, limit;

		// write pairs to buffer
		{
			TestData.Generator generator = new TestData.Generator(SEED, KEY_MAX, VALUE_LENGTH, KeyMode.RANDOM,
				ValueMode.RANDOM_LENGTH);
			BufferSortable<TestData.Key, TestData.Value> buffer = newSortBuffer(memory, OFFSETS_PERCENTAGE);
			int writtenBytes = 0;
			KeyValuePair<TestData.Key, TestData.Value> pair = generator.next();
			while (buffer.write(pair)) {
				LOG.debug("<- " + pair);
				writtenBytes += generator.sizeOf(pair) + Integer.SIZE / 8;
				writtenPairs++;
				pair = generator.next();
			}
			LOG.info("Written " + writtenPairs + " pairs to buffer which occupied " + writtenBytes + " of " + 1024
				+ " bytes.");
			limit = buffer.getPosition();
			memory = buffer.unbind();
		}

		// read pairs from memory
		{
			Buffer.Input buffer = new Buffer.Input();
			buffer.bind(memory);
			buffer.reset(limit);
			KeyValuePair<TestData.Key, TestData.Value> pair = new KeyValuePair<TestData.Key, TestData.Value>(
				new TestData.Key(), new TestData.Value());
			while (buffer.read(pair)) {
				LOG.debug("-> " + pair);
				readPairs++;
			}
			LOG.info("Read " + readPairs + " pairs from buffer.");
			memory = buffer.unbind();
		}

		// assert
		Assert.assertEquals(writtenPairs, readPairs);

		// release the memory occupied by the buffers
		memoryManager.release(memory);
	}

	@Test
	public void testAccoutingSpace() throws Exception {
		// allocate memory segment
		MemorySegment memory = memoryManager.allocate(MEMORY_SIZE);

		// write pairs to buffer
		{
			TestData.Generator generator = new TestData.Generator(SEED, KEY_MAX, VALUE_LENGTH, KeyMode.SORTED,
				ValueMode.FIX_LENGTH);
			BufferSortable<TestData.Key, TestData.Value> buffer = newSortBuffer(memory, 0.001f);
			int writtenBytes = 0;
			KeyValuePair<TestData.Key, TestData.Value> pair = generator.next();
			while (buffer.write(pair)) {
				writtenBytes += generator.sizeOf(pair) + Integer.SIZE / 8;
				pair = generator.next();
			}
			LOG.info("Occupied " + writtenBytes + " of " + MEMORY_SIZE + " bytes.");
			memory = buffer.unbind();
		}

		// release the memory occupied by the buffers
		memoryManager.release(memory);
	}

	@Test
	public void testSort() throws Exception {
		int writtenPairs = 0, readPairs = 0;

		// allocate buffer for unsorted pairs
		MemorySegment unsortedMemory = memoryManager.allocate(MEMORY_SIZE >> 1);
		final BufferSortable<TestData.Key, TestData.Value> unsortedBuffer = newSortBuffer(unsortedMemory,
			OFFSETS_PERCENTAGE);

		// write pairs to buffer
		{
			TestData.Generator generator = new TestData.Generator(SEED, KEY_MAX, VALUE_LENGTH, KeyMode.RANDOM,
				ValueMode.RANDOM_LENGTH);
			while (unsortedBuffer.write(generator.next())) {
				writtenPairs++;
			}
			LOG.info("Written " + writtenPairs + " pairs.");

		}

		// allocate buffer for sorted pairs
		MemorySegment sortedMemory = memoryManager.allocate(MEMORY_SIZE >> 1);
		final Buffer.Output sortedBuffer = new Buffer.Output();
		sortedBuffer.bind(sortedMemory);

		// write pairs in sorted fashion
		{
			// sort
			
			long start = System.currentTimeMillis();
			
			new QuickSort().sort(unsortedBuffer);

			long elapsed = System.currentTimeMillis() - start;
			LOG.info("Sorting took " + (((float) elapsed) / 1000f) + " secs.");
			
			// buffer to buffer mock writer
			Writer writer = new Writer() {
				@Override
				public Collection<MemorySegment> close() throws ServiceException {
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
		sortedMemory = sortedBuffer.unbind();

		// read pairs
		{
			// comparator
			Comparator<TestData.Key> keyComparator = new TestData.KeyComparator();

			// read buffer
			Buffer.Input buffer = new Buffer.Input();
			buffer.bind(sortedMemory);
			buffer.reset(sortedBuffer.getPosition());

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
				pair1 = new KeyValuePair<TestData.Key, TestData.Value>(new TestData.Key(pair2.getKey().getKey()),
					new TestData.Value(pair2.getValue().getValue()));
			}
		}

		// assert
		Assert.assertEquals(writtenPairs, readPairs);

		// release the memory occupied by the buffers
		memoryManager.release(unsortedMemory);
		memoryManager.release(sortedMemory);
	}

	@Test
	public void testIterator() throws Exception {

		// allocate memory segment
		MemorySegment memory = memoryManager.allocate(MEMORY_SIZE);

		int writtenPairs = 0, readPairs = 0;

		Iterator<KeyValuePair<TestData.Key, TestData.Value>> it;

		// write pairs to buffer
		{
			TestData.Generator generator = new TestData.Generator(SEED, KEY_MAX, VALUE_LENGTH, KeyMode.SORTED,
				ValueMode.FIX_LENGTH);
			BufferSortable<TestData.Key, TestData.Value> buffer = newSortBuffer(memory, OFFSETS_PERCENTAGE);
			int writtenBytes = 0;
			KeyValuePair<TestData.Key, TestData.Value> pair = generator.next();
			while (buffer.write(pair)) {
				writtenBytes += generator.sizeOf(pair) + Integer.SIZE / 8;
				writtenPairs++;
				pair = generator.next();
			}
			LOG.info("Written " + writtenPairs + " pairs to buffer which occupied " + writtenBytes + " of "
				+ MEMORY_SIZE + " bytes.");

			it = buffer.getIterator();

			while (it.hasNext()) {
				it.next();
				readPairs++;
			}
			memory = buffer.unbind();

			Assert.assertEquals(writtenPairs, readPairs);
		}

		// release the memory occupied by the buffers
		memoryManager.release(memory);

	}

}
