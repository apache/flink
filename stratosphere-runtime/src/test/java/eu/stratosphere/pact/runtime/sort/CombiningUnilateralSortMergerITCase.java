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

import java.io.IOException;
import java.util.Comparator;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.NoSuchElementException;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import eu.stratosphere.api.common.typeutils.TypeComparator;
import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.api.common.typeutils.TypeSerializerFactory;
import eu.stratosphere.api.java.record.functions.ReduceFunction;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.services.memorymanager.spi.DefaultMemoryManager;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.pact.runtime.plugable.pactrecord.RecordComparator;
import eu.stratosphere.pact.runtime.plugable.pactrecord.RecordSerializerFactory;
import eu.stratosphere.pact.runtime.test.util.DummyInvokable;
import eu.stratosphere.pact.runtime.test.util.TestData;
import eu.stratosphere.pact.runtime.test.util.TestData.Generator.KeyMode;
import eu.stratosphere.pact.runtime.test.util.TestData.Generator.ValueMode;
import eu.stratosphere.pact.runtime.test.util.TestData.Key;
import eu.stratosphere.pact.runtime.util.KeyGroupedIterator;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;
import eu.stratosphere.util.LogUtils;
import eu.stratosphere.util.MutableObjectIterator;


public class CombiningUnilateralSortMergerITCase {
	
	private static final Log LOG = LogFactory.getLog(CombiningUnilateralSortMergerITCase.class);

	private static final long SEED = 649180756312423613L;

	private static final int KEY_MAX = 1000;

	private static final int VALUE_LENGTH = 118;

	private static final int NUM_PAIRS = 50000;

	public static final int MEMORY_SIZE = 1024 * 1024 * 256;

	private final AbstractTask parentTask = new DummyInvokable();
	
	private IOManager ioManager;

	private MemoryManager memoryManager;

	private TypeSerializerFactory<Record> serializerFactory;
	
	private TypeComparator<Record> comparator;

	
	@BeforeClass
	public static void setup() {
		LogUtils.initializeDefaultTestConsoleLogger();
	}
	
	@SuppressWarnings("unchecked")
	@Before
	public void beforeTest() {
		this.memoryManager = new DefaultMemoryManager(MEMORY_SIZE);
		this.ioManager = new IOManager();
		
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
				64 * 1024 * 1024, 64, 0.7f);

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
				3 * 1024 * 1024, 64, 0.005f);

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
			countTable.put(new TestData.Key(i), new Integer(0));
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
				64 * 1024 * 1024, 2, 0.7f);

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
	
	public static class TestCountCombiner extends ReduceFunction {
		private static final long serialVersionUID = 1L;
		
		private final IntValue count = new IntValue();
		
		public volatile boolean opened = false;
		
		public volatile boolean closed = false;
		
		
		@Override
		public void combine(Iterator<Record> values, Collector<Record> out) {
			Record rec = null;
			int cnt = 0;
			while (values.hasNext()) {
				rec = values.next();
				cnt += rec.getField(1, IntValue.class).getValue();
			}
			
			this.count.setValue(cnt);
			rec.setField(1, this.count);
			out.collect(rec);
		}

		@Override
		public void reduce(Iterator<Record> values, Collector<Record> out) {}
		
		@Override
		public void open(Configuration parameters) throws Exception {
			opened = true;
		}
		
		@Override
		public void close() throws Exception {
			closed = true;
		}
	}

	public static class TestCountCombiner2 extends ReduceFunction {
		private static final long serialVersionUID = 1L;
		
		public volatile boolean opened = false;
		
		public volatile boolean closed = false;
		
		@Override
		public void combine(Iterator<Record> values, Collector<Record> out) {
			Record rec = null;
			int cnt = 0;
			while (values.hasNext()) {
				rec = values.next();
				cnt += Integer.parseInt(rec.getField(1, TestData.Value.class).toString());
			}

			out.collect(new Record(rec.getField(0, Key.class), new TestData.Value(cnt + "")));
		}

		@Override
		public void reduce(Iterator<Record> values, Collector<Record> out) {
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
		
		final KeyGroupedIterator<Record> groupIter = new KeyGroupedIterator<Record>(data, serializer, comparator);
		
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
