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

import java.io.EOFException;
import java.util.List;

import org.apache.flink.core.memory.MemoryType;
import org.junit.Assert;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.iomanager.BlockChannelReader;
import org.apache.flink.runtime.io.disk.iomanager.BlockChannelWriter;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;
import org.apache.flink.runtime.io.disk.iomanager.ChannelReaderInputView;
import org.apache.flink.runtime.io.disk.iomanager.ChannelWriterOutputView;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.testutils.DummyInvokable;
import org.apache.flink.runtime.operators.testutils.TestData;
import org.apache.flink.runtime.operators.testutils.TestData.TupleGenerator.KeyMode;
import org.apache.flink.runtime.operators.testutils.TestData.TupleGenerator.ValueMode;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


/**
 */
public class ChannelViewsTest
{
	private static final long SEED = 649180756312423613L;

	private static final int KEY_MAX = Integer.MAX_VALUE;

	private static final int VALUE_SHORT_LENGTH = 114;
	
	private static final int VALUE_LONG_LENGTH = 112 * 1024;

	private static final int NUM_PAIRS_SHORT = 1000000;
	
	private static final int NUM_PAIRS_LONG = 3000;

	private static final int MEMORY_SIZE = 1024 * 1024;
	
	private static final int MEMORY_PAGE_SIZE = 64 * 1024;
	
	private static final int NUM_MEMORY_SEGMENTS = 3;
	
	private final AbstractInvokable parentTask = new DummyInvokable();

	private IOManager ioManager;

	private MemoryManager memoryManager;

	// --------------------------------------------------------------------------------------------

	@Before
	public void beforeTest() {
		this.memoryManager = new MemoryManager(MEMORY_SIZE, 1, MEMORY_PAGE_SIZE, MemoryType.HEAP, true);
		this.ioManager = new IOManagerAsync();
	}

	@After
	public void afterTest() {
		this.ioManager.shutdown();
		if (!this.ioManager.isProperlyShutDown()) {
			Assert.fail("I/O Manager was not properly shut down.");
		}
		
		if (memoryManager != null) {
			Assert.assertTrue("Memory leak: not all segments have been returned to the memory manager.", 
				this.memoryManager.verifyEmpty());
			this.memoryManager.shutdown();
			this.memoryManager = null;
		}
	}

	// --------------------------------------------------------------------------------------------
	
	@Test
	public void testWriteReadSmallRecords() throws Exception
	{
		final TestData.TupleGenerator generator = new TestData.TupleGenerator(SEED, KEY_MAX, VALUE_SHORT_LENGTH, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);
		final FileIOChannel.ID channel = this.ioManager.createChannel();
		final TypeSerializer<Tuple2<Integer, String>> serializer = TestData.getIntStringTupleSerializer();
		
		// create the writer output view
		List<MemorySegment> memory = this.memoryManager.allocatePages(this.parentTask, NUM_MEMORY_SEGMENTS);
		final BlockChannelWriter<MemorySegment> writer = this.ioManager.createBlockChannelWriter(channel);
		final ChannelWriterOutputView outView = new ChannelWriterOutputView(writer, memory, MEMORY_PAGE_SIZE);
		// write a number of pairs
		final Tuple2<Integer, String> rec = new Tuple2<>();
		for (int i = 0; i < NUM_PAIRS_SHORT; i++) {
			generator.next(rec);
			serializer.serialize(rec, outView);
		}
		this.memoryManager.release(outView.close());
		
		// create the reader input view
		memory = this.memoryManager.allocatePages(this.parentTask, NUM_MEMORY_SEGMENTS);
		final BlockChannelReader<MemorySegment> reader = this.ioManager.createBlockChannelReader(channel);
		final ChannelReaderInputView inView = new ChannelReaderInputView(reader, memory, outView.getBlockCount(), true);
		generator.reset();
		
		// read and re-generate all records and compare them
		final Tuple2<Integer, String> readRec = new Tuple2<>();
		for (int i = 0; i < NUM_PAIRS_SHORT; i++) {
			generator.next(rec);
			serializer.deserialize(readRec, inView);
			
			int k1 = rec.f0;
			String v1 = rec.f1;
			
			int k2 = readRec.f0;
			String v2 = readRec.f1;
			
			Assert.assertTrue("The re-generated and the read record do not match.", k1 == k2 && v1.equals(v2));
		}
		
		this.memoryManager.release(inView.close());
		reader.deleteChannel();
	}
	
	@Test
	public void testWriteAndReadLongRecords() throws Exception
	{
		final TestData.TupleGenerator generator = new TestData.TupleGenerator(SEED, KEY_MAX, VALUE_LONG_LENGTH, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);
		final FileIOChannel.ID channel = this.ioManager.createChannel();
		final TypeSerializer<Tuple2<Integer, String>> serializer = TestData.getIntStringTupleSerializer();
		
		// create the writer output view
		List<MemorySegment> memory = this.memoryManager.allocatePages(this.parentTask, NUM_MEMORY_SEGMENTS);
		final BlockChannelWriter<MemorySegment> writer = this.ioManager.createBlockChannelWriter(channel);
		final ChannelWriterOutputView outView = new ChannelWriterOutputView(writer, memory, MEMORY_PAGE_SIZE);
		
		// write a number of pairs
		final Tuple2<Integer, String> rec = new Tuple2<>();
		for (int i = 0; i < NUM_PAIRS_LONG; i++) {
			generator.next(rec);
			serializer.serialize(rec, outView);
		}
		this.memoryManager.release(outView.close());
		
		// create the reader input view
		memory = this.memoryManager.allocatePages(this.parentTask, NUM_MEMORY_SEGMENTS);
		final BlockChannelReader<MemorySegment> reader = this.ioManager.createBlockChannelReader(channel);
		final ChannelReaderInputView inView = new ChannelReaderInputView(reader, memory, outView.getBlockCount(), true);
		generator.reset();
		
		// read and re-generate all records and compare them
		final Tuple2<Integer, String> readRec = new Tuple2<>();
		for (int i = 0; i < NUM_PAIRS_LONG; i++) {
			generator.next(rec);
			serializer.deserialize(readRec, inView);
			final int k1 = rec.f0;
			final String v1 = rec.f1;
			final int k2 = readRec.f0;
			final String v2 = readRec.f1;
			Assert.assertTrue("The re-generated and the read record do not match.", k1 == k2 && v1.equals(v2));
		}
		
		this.memoryManager.release(inView.close());
		reader.deleteChannel();
	}
	
	@Test
	public void testReadTooMany() throws Exception
	{
		final TestData.TupleGenerator generator = new TestData.TupleGenerator(SEED, KEY_MAX, VALUE_SHORT_LENGTH, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);
		final FileIOChannel.ID channel = this.ioManager.createChannel();
		final TypeSerializer<Tuple2<Integer, String>> serializer = TestData.getIntStringTupleSerializer();
		
		// create the writer output view
		List<MemorySegment> memory = this.memoryManager.allocatePages(this.parentTask, NUM_MEMORY_SEGMENTS);
		final BlockChannelWriter<MemorySegment> writer = this.ioManager.createBlockChannelWriter(channel);
		final ChannelWriterOutputView outView = new ChannelWriterOutputView(writer, memory, MEMORY_PAGE_SIZE);

		// write a number of pairs
		final Tuple2<Integer, String> rec = new Tuple2<>();
		for (int i = 0; i < NUM_PAIRS_SHORT; i++) {
			generator.next(rec);
			serializer.serialize(rec, outView);
		}
		this.memoryManager.release(outView.close());

		// create the reader input view
		memory = this.memoryManager.allocatePages(this.parentTask, NUM_MEMORY_SEGMENTS);
		final BlockChannelReader<MemorySegment> reader = this.ioManager.createBlockChannelReader(channel);
		final ChannelReaderInputView inView = new ChannelReaderInputView(reader, memory, outView.getBlockCount(), true);
		generator.reset();

		// read and re-generate all records and compare them
		try {
			final Tuple2<Integer, String> readRec = new Tuple2<>();
			for (int i = 0; i < NUM_PAIRS_SHORT + 1; i++) {
				generator.next(rec);
				serializer.deserialize(readRec, inView);
				final int k1 = rec.f0;
				final String v1 = rec.f1;
				final int k2 = readRec.f0;
				final String v2 = readRec.f1;
				Assert.assertTrue("The re-generated and the read record do not match.", k1 == k2 && v1.equals(v2));
			}
			Assert.fail("Expected an EOFException which did not occur.");
		}
		catch (EOFException eofex) {
			// expected
		}
		catch (Throwable t) {
			// unexpected
			Assert.fail("Unexpected Exception: " + t.getMessage());
		}
		
		this.memoryManager.release(inView.close());
		reader.deleteChannel();
	}
	
	@Test
	public void testReadWithoutKnownBlockCount() throws Exception
	{
		final TestData.TupleGenerator generator = new TestData.TupleGenerator(SEED, KEY_MAX, VALUE_SHORT_LENGTH, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);
		final FileIOChannel.ID channel = this.ioManager.createChannel();
		final TypeSerializer<Tuple2<Integer, String>> serializer = TestData.getIntStringTupleSerializer();
		
		// create the writer output view
		List<MemorySegment> memory = this.memoryManager.allocatePages(this.parentTask, NUM_MEMORY_SEGMENTS);
		final BlockChannelWriter<MemorySegment> writer = this.ioManager.createBlockChannelWriter(channel);
		final ChannelWriterOutputView outView = new ChannelWriterOutputView(writer, memory, MEMORY_PAGE_SIZE);
		
		// write a number of pairs
		final Tuple2<Integer, String> rec = new Tuple2<>();
		for (int i = 0; i < NUM_PAIRS_SHORT; i++) {
			generator.next(rec);
			serializer.serialize(rec, outView);
		}
		this.memoryManager.release(outView.close());
		
		// create the reader input view
		memory = this.memoryManager.allocatePages(this.parentTask, NUM_MEMORY_SEGMENTS);
		final BlockChannelReader<MemorySegment> reader = this.ioManager.createBlockChannelReader(channel);
		final ChannelReaderInputView inView = new ChannelReaderInputView(reader, memory, true);
		generator.reset();
		
		// read and re-generate all records and compare them
		final Tuple2<Integer, String> readRec = new Tuple2<>();
		for (int i = 0; i < NUM_PAIRS_SHORT; i++) {
			generator.next(rec);
			serializer.deserialize(readRec, inView);
			
			int k1 = rec.f0;
			String v1 = rec.f1;
			
			int k2 = readRec.f0;
			String v2 = readRec.f1;
			
			Assert.assertTrue("The re-generated and the read record do not match.", k1 == k2 && v1.equals(v2));
		}
		
		this.memoryManager.release(inView.close());
		reader.deleteChannel();
	}
	
	@Test
	public void testWriteReadOneBufferOnly() throws Exception
	{
		final TestData.TupleGenerator generator = new TestData.TupleGenerator(SEED, KEY_MAX, VALUE_SHORT_LENGTH, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);
		final FileIOChannel.ID channel = this.ioManager.createChannel();
		final TypeSerializer<Tuple2<Integer, String>> serializer = TestData.getIntStringTupleSerializer();
		
		// create the writer output view
		List<MemorySegment> memory = this.memoryManager.allocatePages(this.parentTask, 1);
		final BlockChannelWriter<MemorySegment> writer = this.ioManager.createBlockChannelWriter(channel);
		final ChannelWriterOutputView outView = new ChannelWriterOutputView(writer, memory, MEMORY_PAGE_SIZE);
		
		// write a number of pairs
		final Tuple2<Integer, String> rec = new Tuple2<>();
		for (int i = 0; i < NUM_PAIRS_SHORT; i++) {
			generator.next(rec);
			serializer.serialize(rec, outView);
		}
		this.memoryManager.release(outView.close());
		
		// create the reader input view
		memory = this.memoryManager.allocatePages(this.parentTask, 1);
		final BlockChannelReader<MemorySegment> reader = this.ioManager.createBlockChannelReader(channel);
		final ChannelReaderInputView inView = new ChannelReaderInputView(reader, memory, outView.getBlockCount(), true);
		generator.reset();
		
		// read and re-generate all records and compare them
		final Tuple2<Integer, String> readRec = new Tuple2<>();
		for (int i = 0; i < NUM_PAIRS_SHORT; i++) {
			generator.next(rec);
			serializer.deserialize(readRec, inView);
			
			int k1 = rec.f0;
			String v1 = rec.f1;
			
			int k2 = readRec.f0;
			String v2 = readRec.f1;
			
			Assert.assertTrue("The re-generated and the read record do not match.", k1 == k2 && v1.equals(v2));
		}
		
		this.memoryManager.release(inView.close());
		reader.deleteChannel();
	}
	
	@Test
	public void testWriteReadNotAll() throws Exception
	{
		final TestData.TupleGenerator generator = new TestData.TupleGenerator(SEED, KEY_MAX, VALUE_SHORT_LENGTH, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);
		final FileIOChannel.ID channel = this.ioManager.createChannel();
		final TypeSerializer<Tuple2<Integer, String>> serializer = TestData.getIntStringTupleSerializer();
		
		// create the writer output view
		List<MemorySegment> memory = this.memoryManager.allocatePages(this.parentTask, NUM_MEMORY_SEGMENTS);
		final BlockChannelWriter<MemorySegment> writer = this.ioManager.createBlockChannelWriter(channel);
		final ChannelWriterOutputView outView = new ChannelWriterOutputView(writer, memory, MEMORY_PAGE_SIZE);
		
		// write a number of pairs
		final Tuple2<Integer, String> rec = new Tuple2<>();
		for (int i = 0; i < NUM_PAIRS_SHORT; i++) {
			generator.next(rec);
			serializer.serialize(rec, outView);
		}
		this.memoryManager.release(outView.close());
		
		// create the reader input view
		memory = this.memoryManager.allocatePages(this.parentTask, NUM_MEMORY_SEGMENTS);
		final BlockChannelReader<MemorySegment> reader = this.ioManager.createBlockChannelReader(channel);
		final ChannelReaderInputView inView = new ChannelReaderInputView(reader, memory, outView.getBlockCount(), true);
		generator.reset();
		
		// read and re-generate all records and compare them
		final Tuple2<Integer, String> readRec = new Tuple2<>();
		for (int i = 0; i < NUM_PAIRS_SHORT / 2; i++) {
			generator.next(rec);
			serializer.deserialize(readRec, inView);
			
			int k1 = rec.f0;
			String v1 = rec.f1;
			
			int k2 = readRec.f0;
			String v2 = readRec.f1;
			
			Assert.assertTrue("The re-generated and the read record do not match.", k1 == k2 && v1.equals(v2));
		}
		
		this.memoryManager.release(inView.close());
		reader.deleteChannel();
	}
}
