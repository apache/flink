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

package org.apache.flink.runtime.operators.resettable;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.flink.runtime.memory.MemoryManagerBuilder;
import org.junit.Assert;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.testutils.recordutils.RecordSerializer;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.testutils.DummyInvokable;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.Record;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class NonReusingBlockResettableIteratorTest {
	
	private static final int MEMORY_CAPACITY = 3 * 128 * 1024;
	
	private static final int NUM_VALUES = 20000;
	
	private MemoryManager memman;

	private Iterator<Record> reader;

	private List<Record> objects;
	
	private final TypeSerializer<Record> serializer = RecordSerializer.get();

	@Before
	public void startup() {
		// set up IO and memory manager
		this.memman = MemoryManagerBuilder.newBuilder().setMemorySize(MEMORY_CAPACITY).build();
		
		// create test objects
		this.objects = new ArrayList<Record>(20000);
		for (int i = 0; i < NUM_VALUES; ++i) {
			this.objects.add(new Record(new IntValue(i)));
		}
		
		// create the reader
		this.reader = objects.iterator();
	}
	
	@After
	public void shutdown() {
		this.objects = null;
		
		// check that the memory manager got all segments back
		if (!this.memman.verifyEmpty()) {
			Assert.fail("A memory leak has occurred: Not all memory was properly returned to the memory manager.");
		}
		
		this.memman.shutdown();
		this.memman = null;
	}

	@Test
	public void testSerialBlockResettableIterator() throws Exception
	{
		final AbstractInvokable memOwner = new DummyInvokable();
		// create the resettable Iterator
		final NonReusingBlockResettableIterator<Record> iterator = new NonReusingBlockResettableIterator<Record>(
				this.memman, this.reader, this.serializer, 1, memOwner);
		// open the iterator
		iterator.open();
		
		// now test walking through the iterator
		int lower = 0;
		int upper = 0;
		do {
			lower = upper;
			upper = lower;
			// find the upper bound
			while (iterator.hasNext()) {
				Record target = iterator.next();
				int val = target.getField(0, IntValue.class).getValue();
				Assert.assertEquals(upper++, val);
			}
			// now reset the buffer a few times
			for (int i = 0; i < 5; ++i) {
				iterator.reset();
				int count = 0;
				while (iterator.hasNext()) {
					Record target = iterator.next();
					int val = target.getField(0, IntValue.class).getValue();
					Assert.assertEquals(lower + (count++), val);
				}
				Assert.assertEquals(upper - lower, count);
			}
		} while (iterator.nextBlock());
		Assert.assertEquals(NUM_VALUES, upper);
		// close the iterator
		iterator.close();
	}

	@Test
	public void testDoubleBufferedBlockResettableIterator() throws Exception
	{
		final AbstractInvokable memOwner = new DummyInvokable();
		// create the resettable Iterator
		final NonReusingBlockResettableIterator<Record> iterator = new NonReusingBlockResettableIterator<Record>(
				this.memman, this.reader, this.serializer, 2, memOwner);
		// open the iterator
		iterator.open();
		
		// now test walking through the iterator
		int lower = 0;
		int upper = 0;
		do {
			lower = upper;
			upper = lower;
			// find the upper bound
			while (iterator.hasNext()) {
				Record target = iterator.next();
				int val = target.getField(0, IntValue.class).getValue();
				Assert.assertEquals(upper++, val);
			}
			// now reset the buffer a few times
			for (int i = 0; i < 5; ++i) {
				iterator.reset();
				int count = 0;
				while (iterator.hasNext()) {
					Record target = iterator.next();
					int val = target.getField(0, IntValue.class).getValue();
					Assert.assertEquals(lower + (count++), val);
				}
				Assert.assertEquals(upper - lower, count);
			}
		} while (iterator.nextBlock());
		Assert.assertEquals(NUM_VALUES, upper);
		
		// close the iterator
		iterator.close();
	}

	@Test
	public void testTwelveFoldBufferedBlockResettableIterator() throws Exception
	{
		final AbstractInvokable memOwner = new DummyInvokable();
		// create the resettable Iterator
		final NonReusingBlockResettableIterator<Record> iterator = new NonReusingBlockResettableIterator<Record>(
				this.memman, this.reader, this.serializer, 12, memOwner);
		// open the iterator
		iterator.open();
		
		// now test walking through the iterator
		int lower = 0;
		int upper = 0;
		do {
			lower = upper;
			upper = lower;
			// find the upper bound
			while (iterator.hasNext()) {
				Record target = iterator.next();
				int val = target.getField(0, IntValue.class).getValue();
				Assert.assertEquals(upper++, val);
			}
			// now reset the buffer a few times
			for (int i = 0; i < 5; ++i) {
				iterator.reset();
				int count = 0;
				while (iterator.hasNext()) {
					Record target = iterator.next();
					int val = target.getField(0, IntValue.class).getValue();
					Assert.assertEquals(lower + (count++), val);
				}
				Assert.assertEquals(upper - lower, count);
			}
		} while (iterator.nextBlock());
		Assert.assertEquals(NUM_VALUES, upper);
		
		// close the iterator
		iterator.close();
	}

}
