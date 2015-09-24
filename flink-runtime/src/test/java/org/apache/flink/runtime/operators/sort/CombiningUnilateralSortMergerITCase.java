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

import java.io.IOException;
import java.util.Comparator;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.api.common.typeutils.record.RecordComparator;
import org.apache.flink.api.common.typeutils.record.RecordSerializerFactory;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.testutils.DummyInvokable;
import org.apache.flink.runtime.operators.testutils.TestData;
import org.apache.flink.runtime.operators.testutils.TestData.Key;
import org.apache.flink.runtime.operators.testutils.TestData.Generator.KeyMode;
import org.apache.flink.runtime.operators.testutils.TestData.Generator.ValueMode;
import org.apache.flink.runtime.util.ReusingKeyGroupedIterator;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.Record;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CombiningUnilateralSortMergerITCase {
	
	private static final Logger LOG = LoggerFactory.getLogger(CombiningUnilateralSortMergerITCase.class);

	private static final long SEED = 649180756312423613L;

	private static final int KEY_MAX = 1000;

	private static final int VALUE_LENGTH = 118;

	private static final int NUM_PAIRS = 50000;

	public static final int MEMORY_SIZE = 1024 * 1024 * 256;

	private final AbstractInvokable parentTask = new DummyInvokable();
	
	private IOManager ioManager;

	private MemoryManager memoryManager;

	private TypeSerializerFactory<Record> serializerFactory;
	
	private TypeComparator<Record> comparator;

	@SuppressWarnings("unchecked")
	@Before
	public void beforeTest() {
		this.memoryManager = new MemoryManager(MEMORY_SIZE, 1);
		this.ioManager = new IOManagerAsync();
		
		this.serializerFactory = RecordSerializerFactory.get();
		this.comparator = new RecordComparator(new int[] {0}, new Class[] {TestData.Key.class});
	}

	@After
	public void afterTest() {
		this.ioManager.shutdown();
		if (!this.ioManager.isProperlyShutDown()) {
			Assert.fail("I/O Manager was not properly shut down.");
		}
		
		if (this.memoryManager != null) {
			Assert.assertTrue("Memory leak: not all segments have been returned to the memory manager.", 
				this.memoryManager.verifyEmpty());
			this.memoryManager.shutdown();
			this.memoryManager = null;
		}
	}

	@Test
	public void testCombine() throws Exception
	{
		int noKeys = 100;
		int noKeyCnt = 10000;

		MockRecordReader reader = new MockRecordReader();

		LOG.debug("initializing sortmerger");
		
		TestCountCombiner comb = new TestCountCombiner();
		
		Sorter<Record> merger = new CombiningUnilateralSortMerger<Record>(comb, 
				this.memoryManager, this.ioManager, reader, this.parentTask, this.serializerFactory, this.comparator,
				0.25, 64, 0.7f);

		final Record rec = new Record();
		rec.setField(1, new IntValue(1));
		final TestData.Key key = new TestData.Key();
		
		for (int i = 0; i < noKeyCnt; i++) {
			for (int j = 0; j < noKeys; j++) {
				key.setKey(j);
				rec.setField(0, key);
				reader.emit(rec);
			}
		}
		reader.close();
		
		MutableObjectIterator<Record> iterator = merger.getIterator();

		Iterator<Integer> result = getReducingIterator(iterator, serializerFactory.getSerializer(), comparator.duplicate());
		while (result.hasNext()) {
			Assert.assertEquals(noKeyCnt, result.next().intValue());
		}
		
		merger.close();
		
		// if the combiner was opened, it must have been closed
		Assert.assertTrue(comb.opened == comb.closed);
	}
	
	@Test
	public void testCombineSpilling() throws Exception {
		int noKeys = 100;
		int noKeyCnt = 10000;

		MockRecordReader reader = new MockRecordReader();

		LOG.debug("initializing sortmerger");
		
		TestCountCombiner comb = new TestCountCombiner();
		
		Sorter<Record> merger = new CombiningUnilateralSortMerger<Record>(comb, 
				this.memoryManager, this.ioManager, reader, this.parentTask, this.serializerFactory, this.comparator,
				0.01, 64, 0.005f);

		final Record rec = new Record();
		rec.setField(1, new IntValue(1));
		final TestData.Key key = new TestData.Key();
		
		for (int i = 0; i < noKeyCnt; i++) {
			for (int j = 0; j < noKeys; j++) {
				key.setKey(j);
				rec.setField(0, key);
				reader.emit(rec);
			}
		}
		reader.close();
		
		MutableObjectIterator<Record> iterator = merger.getIterator();

		Iterator<Integer> result = getReducingIterator(iterator, serializerFactory.getSerializer(), comparator.duplicate());
		while (result.hasNext()) {
			Assert.assertEquals(noKeyCnt, result.next().intValue());
		}
		
		merger.close();
		
		// if the combiner was opened, it must have been closed
		Assert.assertTrue(comb.opened == comb.closed);
	}

	@Test
	public void testSortAndValidate() throws Exception
	{
		final Hashtable<TestData.Key, Integer> countTable = new Hashtable<TestData.Key, Integer>(KEY_MAX);
		for (int i = 1; i <= KEY_MAX; i++) {
			countTable.put(new TestData.Key(i), 0);
		}

		// comparator
		final Comparator<TestData.Key> keyComparator = new TestData.KeyComparator();

		// reader
		MockRecordReader reader = new MockRecordReader();

		// merge iterator
		LOG.debug("initializing sortmerger");
		
		TestCountCombiner2 comb = new TestCountCombiner2();
		
		Sorter<Record> merger = new CombiningUnilateralSortMerger<Record>(comb, 
				this.memoryManager, this.ioManager, reader, this.parentTask, this.serializerFactory, this.comparator,
				0.25, 2, 0.7f);

		// emit data
		LOG.debug("emitting data");
		TestData.Generator generator = new TestData.Generator(SEED, KEY_MAX, VALUE_LENGTH, KeyMode.RANDOM, ValueMode.FIX_LENGTH);
		Record rec = new Record();
		final TestData.Value value = new TestData.Value("1");
		
		for (int i = 0; i < NUM_PAIRS; i++) {
			Assert.assertTrue((rec = generator.next(rec)) != null);
			final TestData.Key key = rec.getField(0, TestData.Key.class);
			rec.setField(1, value);
			reader.emit(rec);
			
			countTable.put(new TestData.Key(key.getKey()), countTable.get(key) + 1);
		}
		reader.close();
		rec = null;

		// check order
		MutableObjectIterator<Record> iterator = merger.getIterator();
		
		LOG.debug("checking results");
		
		Record rec1 = new Record();
		Record rec2 = new Record();
		
		Assert.assertTrue((rec1 = iterator.next(rec1)) != null);
		countTable.put(new TestData.Key(rec1.getField(0, TestData.Key.class).getKey()), countTable.get(rec1.getField(0, TestData.Key.class)) - (Integer.parseInt(rec1.getField(1, TestData.Value.class).toString())));

		while ((rec2 = iterator.next(rec2)) != null) {
			final Key k1 = rec1.getField(0, TestData.Key.class);
			final Key k2 = rec2.getField(0, TestData.Key.class);
			
			Assert.assertTrue(keyComparator.compare(k1, k2) <= 0); 
			countTable.put(new TestData.Key(k2.getKey()), countTable.get(k2) - (Integer.parseInt(rec2.getField(1, TestData.Value.class).toString())));
			
			Record tmp = rec1;
			rec1 = rec2;
			k1.setKey(k2.getKey());
			rec2 = tmp;
		}

		for (Integer cnt : countTable.values()) {
			Assert.assertTrue(cnt == 0);
		}
		
		merger.close();
		
		// if the combiner was opened, it must have been closed
		Assert.assertTrue(comb.opened == comb.closed);
	}

	// --------------------------------------------------------------------------------------------
	
	public static class TestCountCombiner extends RichGroupReduceFunction<Record, Record> {
		private static final long serialVersionUID = 1L;
		
		private final IntValue count = new IntValue();
		
		public volatile boolean opened = false;
		
		public volatile boolean closed = false;
		
		
		@Override
		public void combine(Iterable<Record> values, Collector<Record> out) {
			Record rec = null;
			int cnt = 0;
			for (Record next : values) {
				rec = next;
				cnt += rec.getField(1, IntValue.class).getValue();
			}
			
			this.count.setValue(cnt);
			rec.setField(1, this.count);
			out.collect(rec);
		}

		@Override
		public void reduce(Iterable<Record> values, Collector<Record> out) {}
		
		@Override
		public void open(Configuration parameters) throws Exception {
			opened = true;
		}
		
		@Override
		public void close() throws Exception {
			closed = true;
		}
	}

	public static class TestCountCombiner2 extends RichGroupReduceFunction<Record, Record> {
		private static final long serialVersionUID = 1L;
		
		public volatile boolean opened = false;
		
		public volatile boolean closed = false;
		
		@Override
		public void combine(Iterable<Record> values, Collector<Record> out) {
			Record rec = null;
			int cnt = 0;
			for (Record next : values) {
				rec = next;
				cnt += Integer.parseInt(rec.getField(1, TestData.Value.class).toString());
			}

			out.collect(new Record(rec.getField(0, Key.class), new TestData.Value(cnt + "")));
		}

		@Override
		public void reduce(Iterable<Record> values, Collector<Record> out) {
			// yo, nothing, mon
		}
		
		@Override
		public void open(Configuration parameters) throws Exception {
			opened = true;
		}
		
		@Override
		public void close() throws Exception {
			closed = true;
		}
	}
	
	private static Iterator<Integer> getReducingIterator(MutableObjectIterator<Record> data, TypeSerializer<Record> serializer, TypeComparator<Record> comparator) {
		
		final ReusingKeyGroupedIterator<Record> groupIter = new ReusingKeyGroupedIterator<Record>(data, serializer, comparator);
		
		return new Iterator<Integer>() {
			
			private boolean hasNext = false;

			@Override
			public boolean hasNext() {
				if (hasNext) {
					return true;
				}
				
				try {
					hasNext = groupIter.nextKey();
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
				return hasNext;
			}

			@Override
			public Integer next() {
				if (hasNext()) {
					hasNext = false;
					
					Iterator<Record> values = groupIter.getValues();
					
					Record rec = null;
					int cnt = 0;
					while (values.hasNext()) {
						rec = values.next();
						cnt += rec.getField(1, IntValue.class).getValue();
					}
					
					return cnt;
				} else {
					throw new NoSuchElementException();
				}
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
			
		};
	}
}
