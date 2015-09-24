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

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memory.ListMemorySegmentSource;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.testutils.DummyInvokable;
import org.apache.flink.runtime.operators.testutils.TestData;
import org.apache.flink.runtime.operators.testutils.TestData.Generator.KeyMode;
import org.apache.flink.runtime.operators.testutils.TestData.Generator.ValueMode;
import org.apache.flink.runtime.operators.testutils.TestData.Key;
import org.apache.flink.runtime.operators.testutils.TestData.Value;
import org.apache.flink.types.Record;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.EOFException;
import java.util.ArrayList;

public class SpillingBufferTest {
	
	private static final long SEED = 649180756312423613L;

	private static final int KEY_MAX = Integer.MAX_VALUE;

	private static final int VALUE_LENGTH = 114;
	
	private static final int NUM_PAIRS_INMEM = 6000;
	
	private static final int NUM_PAIRS_EXTERNAL = 30000;

	private static final int MEMORY_SIZE = 1024 * 1024;
	
	private static final int NUM_MEMORY_SEGMENTS = 23;
	
	private final AbstractInvokable parentTask = new DummyInvokable();

	private IOManager ioManager;

	private MemoryManager memoryManager;

	// --------------------------------------------------------------------------------------------

	@Before
	public void beforeTest() {
		memoryManager = new MemoryManager(MEMORY_SIZE, 1);
		ioManager = new IOManagerAsync();
	}

	@After
	public void afterTest() {
		ioManager.shutdown();
		if (!ioManager.isProperlyShutDown()) {
			Assert.fail("I/O Manager was not properly shut down.");
		}
		
		if (memoryManager != null) {
			Assert.assertTrue("Memory leak: not all segments have been returned to the memory manager.", 
				memoryManager.verifyEmpty());
			memoryManager.shutdown();
			memoryManager = null;
		}
	}

	// --------------------------------------------------------------------------------------------
	
	@Test
	public void testWriteReadInMemory() throws Exception {
		final TestData.Generator generator = new TestData.Generator(SEED, KEY_MAX, VALUE_LENGTH, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);
		
		// create the writer output view
		final ArrayList<MemorySegment> memory = new ArrayList<MemorySegment>(NUM_MEMORY_SEGMENTS);
		this.memoryManager.allocatePages(this.parentTask, memory, NUM_MEMORY_SEGMENTS);
		final SpillingBuffer outView = new SpillingBuffer(this.ioManager, 
							new ListMemorySegmentSource(memory), this.memoryManager.getPageSize());
		
		// write a number of pairs
		final Record rec = new Record();
		for (int i = 0; i < NUM_PAIRS_INMEM; i++) {
			generator.next(rec);
			rec.write(outView);
		}
		
		// create the reader input view
		DataInputView inView = outView.flip();
		generator.reset();
		
		// read and re-generate all records and compare them
		final Record readRec = new Record();
		for (int i = 0; i < NUM_PAIRS_INMEM; i++) {
			generator.next(rec);
			readRec.read(inView);
			
			Key k1 = rec.getField(0, Key.class);
			Value v1 = rec.getField(1, Value.class);
			
			Key k2 = readRec.getField(0, Key.class);
			Value v2 = readRec.getField(1, Value.class);
			
			Assert.assertTrue("The re-generated and the read record do not match.", k1.equals(k2) && v1.equals(v2));
		}
		
		// re-read the data
		inView = outView.flip();
		generator.reset();
		
		// read and re-generate all records and compare them
		for (int i = 0; i < NUM_PAIRS_INMEM; i++) {
			generator.next(rec);
			readRec.read(inView);
			
			Key k1 = rec.getField(0, Key.class);
			Value v1 = rec.getField(1, Value.class);
			
			Key k2 = readRec.getField(0, Key.class);
			Value v2 = readRec.getField(1, Value.class);
			
			Assert.assertTrue("The re-generated and the read record do not match.", k1.equals(k2) && v1.equals(v2));
		}
		
		this.memoryManager.release(outView.close());
		this.memoryManager.release(memory);
	}
	
	@Test
	public void testWriteReadTooMuchInMemory() throws Exception {
		final TestData.Generator generator = new TestData.Generator(
				SEED, KEY_MAX, VALUE_LENGTH, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);
		
		// create the writer output view
		final ArrayList<MemorySegment> memory = new ArrayList<MemorySegment>(NUM_MEMORY_SEGMENTS);
		this.memoryManager.allocatePages(this.parentTask, memory, NUM_MEMORY_SEGMENTS);
		final SpillingBuffer outView = new SpillingBuffer(this.ioManager, 
							new ListMemorySegmentSource(memory), this.memoryManager.getPageSize());
		
		// write a number of pairs
		final Record rec = new Record();
		for (int i = 0; i < NUM_PAIRS_INMEM; i++) {
			generator.next(rec);
			rec.write(outView);
		}
		
		// create the reader input view
		DataInputView inView = outView.flip();
		generator.reset();
		
		// read and re-generate all records and compare them
		final Record readRec = new Record();
		try {
			for (int i = 0; i < NUM_PAIRS_INMEM + 1; i++) {
				generator.next(rec);
				readRec.read(inView);
				
				Key k1 = rec.getField(0, Key.class);
				Value v1 = rec.getField(1, Value.class);
				
				Key k2 = readRec.getField(0, Key.class);
				Value v2 = readRec.getField(1, Value.class);
				
				Assert.assertTrue("The re-generated and the read record do not match.", k1.equals(k2) && v1.equals(v2));
			}
			Assert.fail("Read too much, expected EOFException.");
		}
		catch (EOFException eofex) {
			// expected
		}
		
		// re-read the data
		inView = outView.flip();
		generator.reset();
		
		// read and re-generate all records and compare them
		for (int i = 0; i < NUM_PAIRS_INMEM; i++) {
			generator.next(rec);
			readRec.read(inView);
			
			Key k1 = rec.getField(0, Key.class);
			Value v1 = rec.getField(1, Value.class);
			
			Key k2 = readRec.getField(0, Key.class);
			Value v2 = readRec.getField(1, Value.class);
			
			Assert.assertTrue("The re-generated and the read record do not match.", k1.equals(k2) && v1.equals(v2));
		}
		
		this.memoryManager.release(outView.close());
		this.memoryManager.release(memory);
	}
	
	// --------------------------------------------------------------------------------------------
	
	@Test
	public void testWriteReadExternal() throws Exception {
		final TestData.Generator generator = new TestData.Generator(
				SEED, KEY_MAX, VALUE_LENGTH, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);
		
		// create the writer output view
		final ArrayList<MemorySegment> memory = new ArrayList<MemorySegment>(NUM_MEMORY_SEGMENTS);
		this.memoryManager.allocatePages(this.parentTask, memory, NUM_MEMORY_SEGMENTS);
		final SpillingBuffer outView = new SpillingBuffer(this.ioManager, 
							new ListMemorySegmentSource(memory), this.memoryManager.getPageSize());
		
		// write a number of pairs
		final Record rec = new Record();
		for (int i = 0; i < NUM_PAIRS_EXTERNAL; i++) {
			generator.next(rec);
			rec.write(outView);
		}
		
		// create the reader input view
		DataInputView inView = outView.flip();
		generator.reset();
		
		// read and re-generate all records and compare them
		final Record readRec = new Record();
		for (int i = 0; i < NUM_PAIRS_EXTERNAL; i++) {
			generator.next(rec);
			readRec.read(inView);
			
			Key k1 = rec.getField(0, Key.class);
			Value v1 = rec.getField(1, Value.class);
			
			Key k2 = readRec.getField(0, Key.class);
			Value v2 = readRec.getField(1, Value.class);
			
			Assert.assertTrue("The re-generated and the read record do not match.", k1.equals(k2) && v1.equals(v2));
		}
		
		// re-read the data
		inView = outView.flip();
		generator.reset();
		
		// read and re-generate all records and compare them
		for (int i = 0; i < NUM_PAIRS_EXTERNAL; i++) {
			generator.next(rec);
			readRec.read(inView);
			
			Key k1 = rec.getField(0, Key.class);
			Value v1 = rec.getField(1, Value.class);
			
			Key k2 = readRec.getField(0, Key.class);
			Value v2 = readRec.getField(1, Value.class);
			
			Assert.assertTrue("The re-generated and the read record do not match.", k1.equals(k2) && v1.equals(v2));
		}
		
		this.memoryManager.release(outView.close());
		this.memoryManager.release(memory);
	}

	@Test
	public void testWriteReadTooMuchExternal() throws Exception {
		final TestData.Generator generator = new TestData.Generator(
				SEED, KEY_MAX, VALUE_LENGTH, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);
		
		// create the writer output view
		final ArrayList<MemorySegment> memory = new ArrayList<MemorySegment>(NUM_MEMORY_SEGMENTS);
		this.memoryManager.allocatePages(this.parentTask, memory, NUM_MEMORY_SEGMENTS);
		final SpillingBuffer outView = new SpillingBuffer(this.ioManager, 
							new ListMemorySegmentSource(memory), this.memoryManager.getPageSize());
		
		// write a number of pairs
		final Record rec = new Record();
		for (int i = 0; i < NUM_PAIRS_EXTERNAL; i++) {
			generator.next(rec);
			rec.write(outView);
		}
		
		// create the reader input view
		DataInputView inView = outView.flip();
		generator.reset();
		
		// read and re-generate all records and compare them
		final Record readRec = new Record();
		try {
			for (int i = 0; i < NUM_PAIRS_EXTERNAL + 1; i++) {
				generator.next(rec);
				readRec.read(inView);
				
				Key k1 = rec.getField(0, Key.class);
				Value v1 = rec.getField(1, Value.class);
				
				Key k2 = readRec.getField(0, Key.class);
				Value v2 = readRec.getField(1, Value.class);
				
				Assert.assertTrue("The re-generated and the read record do not match.", k1.equals(k2) && v1.equals(v2));
			}
			Assert.fail("Read too much, expected EOFException.");
		}
		catch (EOFException eofex) {
			// expected
		}
		
		// re-read the data
		inView = outView.flip();
		generator.reset();
		
		// read and re-generate all records and compare them
		for (int i = 0; i < NUM_PAIRS_EXTERNAL; i++) {
			generator.next(rec);
			readRec.read(inView);
			
			Key k1 = rec.getField(0, Key.class);
			Value v1 = rec.getField(1, Value.class);
			
			Key k2 = readRec.getField(0, Key.class);
			Value v2 = readRec.getField(1, Value.class);
			
			Assert.assertTrue("The re-generated and the read record do not match.", k1.equals(k2) && v1.equals(v2));
		}
		
		this.memoryManager.release(outView.close());
		this.memoryManager.release(memory);
	}
}
