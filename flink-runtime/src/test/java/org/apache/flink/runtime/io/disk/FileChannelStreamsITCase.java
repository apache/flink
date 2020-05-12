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

package org.apache.flink.runtime.io.disk;

import static org.junit.Assert.*;

import org.apache.flink.runtime.memory.MemoryManagerBuilder;
import org.apache.flink.util.TestLogger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.iomanager.BlockChannelReader;
import org.apache.flink.runtime.io.disk.iomanager.BlockChannelWriter;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.testutils.DummyInvokable;
import org.apache.flink.runtime.operators.testutils.PairGenerator;
import org.apache.flink.runtime.operators.testutils.PairGenerator.KeyMode;
import org.apache.flink.runtime.operators.testutils.PairGenerator.Pair;
import org.apache.flink.runtime.operators.testutils.PairGenerator.ValueMode;

import java.io.EOFException;
import java.util.List;

public class FileChannelStreamsITCase extends TestLogger {
	
	private static final long SEED = 649180756312423613L;

	private static final int KEY_MAX = Integer.MAX_VALUE;

	private static final int VALUE_SHORT_LENGTH = 114;
	
	private static final int VALUE_LONG_LENGTH = 112 * 1024;

	private static final int NUM_PAIRS_SHORT = 1000000;
	
	private static final int NUM_PAIRS_LONG = 3000;
	
	private static final int MEMORY_PAGE_SIZE = 32 * 1024;
	
	private static final int NUM_MEMORY_SEGMENTS = 3;

	private IOManager ioManager;

	private MemoryManager memManager;

	// --------------------------------------------------------------------------------------------

	@Before
	public void beforeTest() {
		memManager = MemoryManagerBuilder
			.newBuilder()
			.setMemorySize(NUM_MEMORY_SEGMENTS * MEMORY_PAGE_SIZE)
			.setPageSize(MEMORY_PAGE_SIZE)
			.build();
		ioManager = new IOManagerAsync();
	}

	@After
	public void afterTest() throws Exception {
		ioManager.close();
		assertTrue("The memory has not been properly released", memManager.verifyEmpty());
	}

	// --------------------------------------------------------------------------------------------
	
	@Test
	public void testWriteReadSmallRecords() {
		try {
			List<MemorySegment> memory = memManager.allocatePages(new DummyInvokable(), NUM_MEMORY_SEGMENTS);
			
			final PairGenerator generator = new PairGenerator(SEED, KEY_MAX, VALUE_SHORT_LENGTH, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);
			final FileIOChannel.ID channel = ioManager.createChannel();
			
			// create the writer output view
			final BlockChannelWriter<MemorySegment> writer = ioManager.createBlockChannelWriter(channel);
			final FileChannelOutputView outView = new FileChannelOutputView(writer, memManager, memory, MEMORY_PAGE_SIZE);
			
			// write a number of pairs
			Pair pair = new Pair();
			for (int i = 0; i < NUM_PAIRS_SHORT; i++) {
				generator.next(pair);
				pair.write(outView);
			}
			outView.close();
			
			// create the reader input view
			List<MemorySegment> readMemory = memManager.allocatePages(new DummyInvokable(), NUM_MEMORY_SEGMENTS);
			
			final BlockChannelReader<MemorySegment> reader = ioManager.createBlockChannelReader(channel);
			final FileChannelInputView inView = new FileChannelInputView(reader, memManager, readMemory, outView.getBytesInLatestSegment());
			generator.reset();
			
			// read and re-generate all records and compare them
			Pair readPair = new Pair();
			for (int i = 0; i < NUM_PAIRS_SHORT; i++) {
				generator.next(pair);
				readPair.read(inView);
				assertEquals("The re-generated and the read record do not match.", pair, readPair);
			}
			
			inView.close();
			reader.deleteChannel();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testWriteAndReadLongRecords() {
		try {
			final List<MemorySegment> memory = memManager.allocatePages(new DummyInvokable(), NUM_MEMORY_SEGMENTS);
			
			final PairGenerator generator = new PairGenerator(SEED, KEY_MAX, VALUE_LONG_LENGTH, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);
			final FileIOChannel.ID channel = this.ioManager.createChannel();
			
			// create the writer output view
			final BlockChannelWriter<MemorySegment> writer = this.ioManager.createBlockChannelWriter(channel);
			final FileChannelOutputView outView = new FileChannelOutputView(writer, memManager, memory, MEMORY_PAGE_SIZE);
			
			// write a number of pairs
			Pair pair = new Pair();
			for (int i = 0; i < NUM_PAIRS_LONG; i++) {
				generator.next(pair);
				pair.write(outView);
			}
			outView.close();
			
			// create the reader input view
			List<MemorySegment> readMemory = memManager.allocatePages(new DummyInvokable(), NUM_MEMORY_SEGMENTS);
			
			final BlockChannelReader<MemorySegment> reader = ioManager.createBlockChannelReader(channel);
			final FileChannelInputView inView = new FileChannelInputView(reader, memManager, readMemory, outView.getBytesInLatestSegment());
			generator.reset();
			
			// read and re-generate all records and compare them
			Pair readPair = new Pair();
			for (int i = 0; i < NUM_PAIRS_LONG; i++) {
				generator.next(pair);
				readPair.read(inView);
				assertEquals("The re-generated and the read record do not match.", pair, readPair);
			}
			
			inView.close();
			reader.deleteChannel();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testReadTooMany() {
		try {
			final List<MemorySegment> memory = memManager.allocatePages(new DummyInvokable(), NUM_MEMORY_SEGMENTS);
			
			final PairGenerator generator = new PairGenerator(SEED, KEY_MAX, VALUE_SHORT_LENGTH, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);
			final FileIOChannel.ID channel = this.ioManager.createChannel();
			
			// create the writer output view
			final BlockChannelWriter<MemorySegment> writer = this.ioManager.createBlockChannelWriter(channel);
			final FileChannelOutputView outView = new FileChannelOutputView(writer, memManager, memory, MEMORY_PAGE_SIZE);
	
			// write a number of pairs
			Pair pair = new Pair();
			for (int i = 0; i < NUM_PAIRS_SHORT; i++) {
				generator.next(pair);
				pair.write(outView);
			}
			outView.close();
	
			// create the reader input view
			List<MemorySegment> readMemory = memManager.allocatePages(new DummyInvokable(), NUM_MEMORY_SEGMENTS);
			
			final BlockChannelReader<MemorySegment> reader = ioManager.createBlockChannelReader(channel);
			final FileChannelInputView inView = new FileChannelInputView(reader, memManager, readMemory, outView.getBytesInLatestSegment());
			generator.reset();
	
			// read and re-generate all records and compare them
			try {
				Pair readPair = new Pair();
				for (int i = 0; i < NUM_PAIRS_SHORT + 1; i++) {
					generator.next(pair);
					readPair.read(inView);
					assertEquals("The re-generated and the read record do not match.", pair, readPair);
				}
				fail("Expected an EOFException which did not occur.");
			}
			catch (EOFException eofex) {
				// expected
			}
			
			inView.close();
			reader.deleteChannel();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testWriteReadOneBufferOnly() {
		try {
			final List<MemorySegment> memory = memManager.allocatePages(new DummyInvokable(), 1);
			
			final PairGenerator generator = new PairGenerator(SEED, KEY_MAX, VALUE_SHORT_LENGTH, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);
			final FileIOChannel.ID channel = this.ioManager.createChannel();
			
			// create the writer output view
			final BlockChannelWriter<MemorySegment> writer = this.ioManager.createBlockChannelWriter(channel);
			final FileChannelOutputView outView = new FileChannelOutputView(writer, memManager, memory, MEMORY_PAGE_SIZE);
			
			// write a number of pairs
			Pair pair = new Pair();
			for (int i = 0; i < NUM_PAIRS_SHORT; i++) {
				generator.next(pair);
				pair.write(outView);
			}
			outView.close();
			
			// create the reader input view
			List<MemorySegment> readMemory = memManager.allocatePages(new DummyInvokable(), 1);
			
			final BlockChannelReader<MemorySegment> reader = ioManager.createBlockChannelReader(channel);
			final FileChannelInputView inView = new FileChannelInputView(reader, memManager, readMemory, outView.getBytesInLatestSegment());
			generator.reset();
			
			// read and re-generate all records and compare them
			Pair readPair = new Pair();
			for (int i = 0; i < NUM_PAIRS_SHORT; i++) {
				generator.next(pair);
				readPair.read(inView);
				assertEquals("The re-generated and the read record do not match.", pair, readPair);
			}
			
			inView.close();
			reader.deleteChannel();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testWriteReadNotAll() {
		try {
			final List<MemorySegment> memory = memManager.allocatePages(new DummyInvokable(), NUM_MEMORY_SEGMENTS);
			
			final PairGenerator generator = new PairGenerator(SEED, KEY_MAX, VALUE_SHORT_LENGTH, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);
			final FileIOChannel.ID channel = this.ioManager.createChannel();
			
			// create the writer output view
			final BlockChannelWriter<MemorySegment> writer = this.ioManager.createBlockChannelWriter(channel);
			final FileChannelOutputView outView = new FileChannelOutputView(writer, memManager, memory, MEMORY_PAGE_SIZE);
			
			// write a number of pairs
			Pair pair = new Pair();
			for (int i = 0; i < NUM_PAIRS_SHORT; i++) {
				generator.next(pair);
				pair.write(outView);
			}
			outView.close();
			
			// create the reader input view
			List<MemorySegment> readMemory = memManager.allocatePages(new DummyInvokable(), NUM_MEMORY_SEGMENTS);
			
			final BlockChannelReader<MemorySegment> reader = ioManager.createBlockChannelReader(channel);
			final FileChannelInputView inView = new FileChannelInputView(reader, memManager, readMemory, outView.getBytesInLatestSegment());
			generator.reset();
			
			// read and re-generate all records and compare them
			Pair readPair = new Pair();
			for (int i = 0; i < NUM_PAIRS_SHORT / 2; i++) {
				generator.next(pair);
				readPair.read(inView);
				assertEquals("The re-generated and the read record do not match.", pair, readPair);
			}
			
			inView.close();
			reader.deleteChannel();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
