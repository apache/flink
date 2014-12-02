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

import static org.junit.Assert.*;

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Comparator;
import java.util.Random;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.api.common.typeutils.record.RecordComparator;
import org.apache.flink.api.common.typeutils.record.RecordSerializerFactory;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.ValueTypeInfo;
import org.apache.flink.api.java.typeutils.runtime.RuntimeStatefulSerializerFactory;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memorymanager.DefaultMemoryManager;
import org.apache.flink.runtime.memorymanager.MemoryManager;
import org.apache.flink.runtime.operators.testutils.DummyInvokable;
import org.apache.flink.runtime.operators.testutils.RandomIntPairGenerator;
import org.apache.flink.runtime.operators.testutils.TestData;
import org.apache.flink.runtime.operators.testutils.TestData.Key;
import org.apache.flink.runtime.operators.testutils.TestData.Value;
import org.apache.flink.runtime.operators.testutils.TestData.Generator.KeyMode;
import org.apache.flink.runtime.operators.testutils.TestData.Generator.ValueMode;
import org.apache.flink.runtime.operators.testutils.types.IntPair;
import org.apache.flink.runtime.operators.testutils.types.IntPairComparator;
import org.apache.flink.runtime.operators.testutils.types.IntPairSerializer;
import org.apache.flink.types.Record;
import org.apache.flink.util.MutableObjectIterator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class ExternalSortITCase {
	
	private static final Logger LOG = LoggerFactory.getLogger(ExternalSortITCase.class);

	private static final long SEED = 649180756312423613L;

	private static final int KEY_MAX = Integer.MAX_VALUE;

	private static final int VALUE_LENGTH = 114;
	
	private static final Value VAL = new Value("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ");

	private static final int NUM_PAIRS = 200000;

	private static final int MEMORY_SIZE = 1024 * 1024 * 78;
	
	private final AbstractInvokable parentTask = new DummyInvokable();

	private IOManager ioManager;

	private MemoryManager memoryManager;
	
	private TypeSerializerFactory<Record> pactRecordSerializer;
	
	private TypeComparator<Record> pactRecordComparator;

	// --------------------------------------------------------------------------------------------

	@SuppressWarnings("unchecked")
	@Before
	public void beforeTest() {
		this.memoryManager = new DefaultMemoryManager(MEMORY_SIZE, 1);
		this.ioManager = new IOManagerAsync();
		
		this.pactRecordSerializer = RecordSerializerFactory.get();
		this.pactRecordComparator = new RecordComparator(new int[] {0}, new Class[] {TestData.Key.class});
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

	// --------------------------------------------------------------------------------------------
	
	@Test
	public void testInMemorySort() throws Exception {
		// comparator
		final Comparator<TestData.Key> keyComparator = new TestData.KeyComparator();
		
		final TestData.Generator generator = new TestData.Generator(SEED, KEY_MAX, VALUE_LENGTH, KeyMode.RANDOM, ValueMode.CONSTANT, VAL);
		final MutableObjectIterator<Record> source = new TestData.GeneratorIterator(generator, NUM_PAIRS);

		// merge iterator
		LOG.debug("Initializing sortmerger...");
		
		Sorter<Record> merger = new UnilateralSortMerger<Record>(this.memoryManager, this.ioManager, 
			source, this.parentTask, this.pactRecordSerializer, this.pactRecordComparator,
				(double)64/78, 2, 0.9f);

		// emit data
		LOG.debug("Reading and sorting data...");

		// check order
		MutableObjectIterator<Record> iterator = merger.getIterator();
		
		LOG.debug("Checking results...");
		int pairsEmitted = 1;

		Record rec1 = new Record();
		Record rec2 = new Record();
		
		Assert.assertTrue((rec1 = iterator.next(rec1)) != null);
		while ((rec2 = iterator.next(rec2)) != null) {
			final Key k1 = rec1.getField(0, TestData.Key.class);
			final Key k2 = rec2.getField(0, TestData.Key.class);
			pairsEmitted++;
			
			Assert.assertTrue(keyComparator.compare(k1, k2) <= 0); 
			
			Record tmp = rec1;
			rec1 = rec2;
			k1.setKey(k2.getKey());
			
			rec2 = tmp;
		}
		Assert.assertTrue(NUM_PAIRS == pairsEmitted);
		
		merger.close();
	}
	
	@Test
	public void testInMemorySortUsing10Buffers() throws Exception {
		// comparator
		final Comparator<TestData.Key> keyComparator = new TestData.KeyComparator();
		
		final TestData.Generator generator = new TestData.Generator(SEED, KEY_MAX, VALUE_LENGTH, KeyMode.RANDOM, ValueMode.CONSTANT, VAL);
		final MutableObjectIterator<Record> source = new TestData.GeneratorIterator(generator, NUM_PAIRS);

		// merge iterator
		LOG.debug("Initializing sortmerger...");
		
		Sorter<Record> merger = new UnilateralSortMerger<Record>(this.memoryManager, this.ioManager, 
				source, this.parentTask, this.pactRecordSerializer, this.pactRecordComparator,
				(double)64/78, 10, 2, 0.9f);

		// emit data
		LOG.debug("Reading and sorting data...");

		// check order
		MutableObjectIterator<Record> iterator = merger.getIterator();
		
		LOG.debug("Checking results...");
		int pairsEmitted = 1;

		Record rec1 = new Record();
		Record rec2 = new Record();
		
		Assert.assertTrue((rec1 = iterator.next(rec1)) != null);
		while ((rec2 = iterator.next(rec2)) != null) {
			final Key k1 = rec1.getField(0, TestData.Key.class);
			final Key k2 = rec2.getField(0, TestData.Key.class);
			pairsEmitted++;
			
			Assert.assertTrue(keyComparator.compare(k1, k2) <= 0); 
			
			Record tmp = rec1;
			rec1 = rec2;
			k1.setKey(k2.getKey());
			
			rec2 = tmp;
		}
		Assert.assertTrue(NUM_PAIRS == pairsEmitted);
		
		merger.close();
	}

	@Test
	public void testSpillingSort() throws Exception {
		// comparator
		final Comparator<TestData.Key> keyComparator = new TestData.KeyComparator();
		
		final TestData.Generator generator = new TestData.Generator(SEED, KEY_MAX, VALUE_LENGTH, KeyMode.RANDOM, ValueMode.CONSTANT, VAL);
		final MutableObjectIterator<Record> source = new TestData.GeneratorIterator(generator, NUM_PAIRS);

		// merge iterator
		LOG.debug("Initializing sortmerger...");
		
		Sorter<Record> merger = new UnilateralSortMerger<Record>(this.memoryManager, this.ioManager, 
				source, this.parentTask, this.pactRecordSerializer, this.pactRecordComparator,
				(double)16/78, 64, 0.7f);

		// emit data
		LOG.debug("Reading and sorting data...");

		// check order
		MutableObjectIterator<Record> iterator = merger.getIterator();
		
		LOG.debug("Checking results...");
		int pairsEmitted = 1;

		Record rec1 = new Record();
		Record rec2 = new Record();
		
		Assert.assertTrue((rec1 = iterator.next(rec1)) != null);
		while ((rec2 = iterator.next(rec2)) != null) {
			final Key k1 = rec1.getField(0, TestData.Key.class);
			final Key k2 = rec2.getField(0, TestData.Key.class);
			pairsEmitted++;
			
			Assert.assertTrue(keyComparator.compare(k1, k2) <= 0); 
			
			Record tmp = rec1;
			rec1 = rec2;
			k1.setKey(k2.getKey());
			
			rec2 = tmp;
		}
		Assert.assertTrue(NUM_PAIRS == pairsEmitted);
		
		merger.close();
	}

//	@Test
	public void testSpillingSortWithIntermediateMerge() throws Exception {
		// amount of pairs
		final int PAIRS = 10000000;

		// comparator
		final Comparator<TestData.Key> keyComparator = new TestData.KeyComparator();

		final TestData.Generator generator = new TestData.Generator(SEED, KEY_MAX, VALUE_LENGTH, KeyMode.RANDOM, ValueMode.FIX_LENGTH);
		final MutableObjectIterator<Record> source = new TestData.GeneratorIterator(generator, PAIRS);
		
		// merge iterator
		LOG.debug("Initializing sortmerger...");
		
		Sorter<Record> merger = new UnilateralSortMerger<Record>(this.memoryManager, this.ioManager, 
				source, this.parentTask, this.pactRecordSerializer, this.pactRecordComparator,
				(double)64/78, 16, 0.7f);
		
		// emit data
		LOG.debug("Emitting data...");

		// check order
		MutableObjectIterator<Record> iterator = merger.getIterator();
		
		LOG.debug("Checking results...");
		int pairsRead = 1;
		int nextStep = PAIRS / 20;

		Record rec1 = new Record();
		Record rec2 = new Record();
		
		Assert.assertTrue((rec1 = iterator.next(rec1)) != null);
		while ((rec2 = iterator.next(rec2)) != null) {
			final Key k1 = rec1.getField(0, TestData.Key.class);
			final Key k2 = rec2.getField(0, TestData.Key.class);
			pairsRead++;
			
			Assert.assertTrue(keyComparator.compare(k1, k2) <= 0); 
			
			Record tmp = rec1;
			rec1 = rec2;
			k1.setKey(k2.getKey());
			rec2 = tmp;
			
			// log
			if (pairsRead == nextStep) {
				nextStep += PAIRS / 20;
			}
			
		}
		Assert.assertEquals("Not all pairs were read back in.", PAIRS, pairsRead);
		merger.close();
	}
	
//	@Test
	public void testSpillingSortWithIntermediateMergeIntPair() throws Exception {
		// amount of pairs
		final int PAIRS = 50000000;

		// comparator
		final RandomIntPairGenerator generator = new RandomIntPairGenerator(12345678, PAIRS);
		
		final TypeSerializerFactory<IntPair> serializerFactory = new IntPairSerializer.IntPairSerializerFactory();
		final TypeComparator<IntPair> comparator = new IntPairComparator();
		
		// merge iterator
		LOG.debug("Initializing sortmerger...");
		
		Sorter<IntPair> merger = new UnilateralSortMerger<IntPair>(this.memoryManager, this.ioManager, 
				generator, this.parentTask, serializerFactory, comparator, (double)64/78, 4, 0.7f);

		// emit data
		LOG.debug("Emitting data...");
		
		// check order
		MutableObjectIterator<IntPair> iterator = merger.getIterator();
		
		LOG.debug("Checking results...");
		int pairsRead = 1;
		int nextStep = PAIRS / 20;

		IntPair rec1 = new IntPair();
		IntPair rec2 = new IntPair();
		
		Assert.assertTrue((rec1 = iterator.next(rec1)) != null);
		
		while ((rec2 = iterator.next(rec2)) != null) {
			final int k1 = rec1.getKey();
			final int k2 = rec2.getKey();
			pairsRead++;
			
			Assert.assertTrue(k1 - k2 <= 0); 
			
			IntPair tmp = rec1;
			rec1 = rec2;
			rec2 = tmp;
			
			// log
			if (pairsRead == nextStep) {
				nextStep += PAIRS / 20;
			}
		}
		Assert.assertEquals("Not all pairs were read back in.", PAIRS, pairsRead);
		merger.close();
	}
	
	@Test
	public void testSortWithLongRecordsOnly() {
		try {
			final int NUM_RECORDS = 10;
			
			final TypeInformation<?>[] types = new TypeInformation<?>[] {
					BasicTypeInfo.LONG_TYPE_INFO,
					new ValueTypeInfo<SomeMaybeLongValue>(SomeMaybeLongValue.class)
				};
			
			final TupleTypeInfo<Tuple2<Long, SomeMaybeLongValue>> typeInfo = 
								new TupleTypeInfo<Tuple2<Long,SomeMaybeLongValue>>(types);
			final TypeSerializer<Tuple2<Long, SomeMaybeLongValue>> serializer = typeInfo.createSerializer();
			final TypeComparator<Tuple2<Long, SomeMaybeLongValue>> comparator = typeInfo.createComparator(new int[] {0}, new boolean[]{false}, 0);
			
			MutableObjectIterator<Tuple2<Long, SomeMaybeLongValue>> source = 
					new MutableObjectIterator<Tuple2<Long, SomeMaybeLongValue>>()
			{
				private final Random rnd = new Random();
				private int num = 0;
				
				@Override
				public Tuple2<Long, SomeMaybeLongValue> next(Tuple2<Long, SomeMaybeLongValue> reuse) {
					if (num++ < NUM_RECORDS) {
						long val = rnd.nextLong();
						return new Tuple2<Long, SomeMaybeLongValue>(val, new SomeMaybeLongValue((int) val));
					}
					else {
						return null;
					}
					
				}
			};
			
			@SuppressWarnings("unchecked")
			Sorter<Tuple2<Long, SomeMaybeLongValue>> sorter = new UnilateralSortMerger<Tuple2<Long, SomeMaybeLongValue>>(
					this.memoryManager, this.ioManager, 
					source, this.parentTask,
					new RuntimeStatefulSerializerFactory<Tuple2<Long, SomeMaybeLongValue>>(serializer, (Class<Tuple2<Long, SomeMaybeLongValue>>) (Class<?>) Tuple2.class),
					comparator, 1.0, 1, 128, 0.7f);
			
			// check order
			MutableObjectIterator<Tuple2<Long, SomeMaybeLongValue>> iterator = sorter.getIterator();
			
			Tuple2<Long, SomeMaybeLongValue> val = serializer.createInstance();
			
			long prevKey = Long.MAX_VALUE;

			for (int i = 0; i < NUM_RECORDS; i++) {
				val = iterator.next(val);
				
				assertTrue(val.f0 <= prevKey);
				assertTrue(val.f0.intValue() == val.f1.val());
			}
			
			assertNull(iterator.next(val));
			
			sorter.close();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testSortWithLongAndShortRecordsMixed() {
		try {
			final int NUM_RECORDS = 1000000;
			final int LARGE_REC_INTERVAL = 100000;
			
			final TypeInformation<?>[] types = new TypeInformation<?>[] {
					BasicTypeInfo.LONG_TYPE_INFO,
					new ValueTypeInfo<SomeMaybeLongValue>(SomeMaybeLongValue.class)
				};
			
			final TupleTypeInfo<Tuple2<Long, SomeMaybeLongValue>> typeInfo = 
								new TupleTypeInfo<Tuple2<Long,SomeMaybeLongValue>>(types);
			final TypeSerializer<Tuple2<Long, SomeMaybeLongValue>> serializer = typeInfo.createSerializer();
			final TypeComparator<Tuple2<Long, SomeMaybeLongValue>> comparator = typeInfo.createComparator(new int[] {0}, new boolean[]{false}, 0);
			
			MutableObjectIterator<Tuple2<Long, SomeMaybeLongValue>> source = 
					new MutableObjectIterator<Tuple2<Long, SomeMaybeLongValue>>()
			{
				private final Random rnd = new Random();
				private int num = -1;
				
				@Override
				public Tuple2<Long, SomeMaybeLongValue> next(Tuple2<Long, SomeMaybeLongValue> reuse) {
					if (++num < NUM_RECORDS) {
						long val = rnd.nextLong();
						return new Tuple2<Long, SomeMaybeLongValue>(val, new SomeMaybeLongValue((int) val, num % LARGE_REC_INTERVAL == 0));
					}
					else {
						return null;
					}
					
				}
			};
			
			@SuppressWarnings("unchecked")
			Sorter<Tuple2<Long, SomeMaybeLongValue>> sorter = new UnilateralSortMerger<Tuple2<Long, SomeMaybeLongValue>>(
					this.memoryManager, this.ioManager, 
					source, this.parentTask,
					new RuntimeStatefulSerializerFactory<Tuple2<Long, SomeMaybeLongValue>>(serializer, (Class<Tuple2<Long, SomeMaybeLongValue>>) (Class<?>) Tuple2.class),
					comparator, 1.0, 1, 128, 0.7f);
			
			// check order
			MutableObjectIterator<Tuple2<Long, SomeMaybeLongValue>> iterator = sorter.getIterator();
			
			Tuple2<Long, SomeMaybeLongValue> val = serializer.createInstance();
			
			long prevKey = Long.MAX_VALUE;

			for (int i = 0; i < NUM_RECORDS; i++) {
				val = iterator.next(val);
				
				assertTrue(val.f0 <= prevKey);
				assertTrue(val.f0.intValue() == val.f1.val());
			}
			
			assertNull(iterator.next(val));
			
			sorter.close();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	public static final class SomeMaybeLongValue implements org.apache.flink.types.Value {
		
		private static final long serialVersionUID = 1L;

		private static final byte[] BUFFER = new byte[100000000];
		
		static {
			for (int i = 0; i < BUFFER.length; i++) {
				BUFFER[i] = (byte) i;
			}
		}
		
		private int val;
		
		private boolean isLong;
		

		public SomeMaybeLongValue() {
			this.isLong = true;
		}
		
		public SomeMaybeLongValue(int val) {
			this.val = val;
			this.isLong = true;
		}
		
		public SomeMaybeLongValue(int val, boolean isLong) {
			this.val = val;
			this.isLong = isLong;
		}
		
		public int val() {
			return val;
		}
		
		public boolean isLong() {
			return isLong;
		}
		
		@Override
		public void read(DataInputView in) throws IOException {
			val = in.readInt();
			isLong = in.readBoolean();
			
			if (isLong) {
				for (int i = 0; i < BUFFER.length; i++) {
					byte b = in.readByte();
					assertEquals(BUFFER[i], b);
				}
			}
		}
		
		@Override
		public void write(DataOutputView out) throws IOException {
			out.writeInt(val);
			out.writeBoolean(isLong);
			if (isLong) {
				out.write(BUFFER);
			}
		}
		
		@Override
		public int hashCode() {
			return val;
		}
		
		@Override
		public boolean equals(Object obj) {
			return (obj instanceof SomeMaybeLongValue) && ((SomeMaybeLongValue) obj).val == this.val;
		}
		
		@Override
		public String toString() {
			return isLong ? "Large Value" : "Small Value";
		}
	}
}
