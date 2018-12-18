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

package org.apache.flink.runtime.operators.hash;

import org.apache.flink.api.common.typeutils.GenericPairComparator;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypePairComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ByteValueSerializer;
import org.apache.flink.api.common.typeutils.base.LongComparator;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArrayComparator;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.TupleComparator;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.api.java.typeutils.runtime.ValueComparator;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.types.ByteValue;
import org.apache.flink.util.MutableObjectIterator;

import org.junit.Test;
import org.junit.Assert;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class HashTableTest {

	private final TypeSerializer<Tuple2<Long, byte[]>> buildSerializer;
	private final TypeSerializer<Long> probeSerializer;
	
	private final TypeComparator<Tuple2<Long, byte[]>> buildComparator;
	private final TypeComparator<Long> probeComparator;

	private final TypePairComparator<Long, Tuple2<Long, byte[]>> pairComparator;


	public HashTableTest() {
		TypeSerializer<?>[] fieldSerializers = { LongSerializer.INSTANCE, BytePrimitiveArraySerializer.INSTANCE };
		@SuppressWarnings("unchecked")
		Class<Tuple2<Long, byte[]>> clazz = (Class<Tuple2<Long, byte[]>>) (Class<?>) Tuple2.class;
		this.buildSerializer = new TupleSerializer<Tuple2<Long, byte[]>>(clazz, fieldSerializers);
		
		this.probeSerializer = LongSerializer.INSTANCE;

		TypeComparator<?>[] comparators = { new LongComparator(true) };
		TypeSerializer<?>[] comparatorSerializers = { LongSerializer.INSTANCE };

		this.buildComparator = new TupleComparator<Tuple2<Long, byte[]>>(new int[] {0}, comparators, comparatorSerializers);

		this.probeComparator = new LongComparator(true);

		this.pairComparator = new TypePairComparator<Long, Tuple2<Long, byte[]>>() {

			private long ref;

			@Override
			public void setReference(Long reference) {
				ref = reference;
			}

			@Override
			public boolean equalToReference(Tuple2<Long, byte[]> candidate) {
				//noinspection UnnecessaryUnboxing
				return candidate.f0.longValue() == ref;
			}

			@Override
			public int compareToReference(Tuple2<Long, byte[]> candidate) {
				long x = ref;
				long y = candidate.f0;
				return (x < y) ? -1 : ((x == y) ? 0 : 1);
			}
		};
	}
	
	// ------------------------------------------------------------------------
	//  Tests
	// ------------------------------------------------------------------------

	/**
	 * This tests a combination of values that lead to a corner case situation where memory
	 * was missing and the computation deadlocked.
	 */
	@Test
	public void testBufferMissingForProbing() {

		final IOManager ioMan = new IOManagerAsync();

		try {
			final int pageSize = 32*1024;
			final int numSegments = 34;
			final int numRecords = 3400;
			final int recordLen = 270;

			final byte[] payload = new byte[recordLen - 8 - 4];
			
			List<MemorySegment> memory = getMemory(numSegments, pageSize);
			
			MutableHashTable<Tuple2<Long, byte[]>, Long> table = new MutableHashTable<>(
					buildSerializer, probeSerializer, buildComparator, probeComparator,
					pairComparator, memory, ioMan, 16, false);
			
			table.open(new TupleBytesIterator(payload, numRecords), new LongIterator(10000));
			
			try {
				while (table.nextRecord()) {
					MutableObjectIterator<Tuple2<Long, byte[]>> matches = table.getBuildSideIterator();
					while (matches.next() != null);
				}
			}
			catch (RuntimeException e) {
				if (!e.getMessage().contains("exceeded maximum number of recursions")) {
					e.printStackTrace();
					fail("Test failed with unexpected exception");
				} 
			}
			finally {
				table.close();
			}
			
			checkNoTempFilesRemain(ioMan);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		finally {
			ioMan.shutdown();
		}
	}

	/**
	 * This tests the case where no additional partition buffers are used at the point when spilling
	 * is triggered, testing that overflow bucket buffers are taken into account when deciding which
	 * partition to spill.
	 */
	@Test
	public void testSpillingFreesOnlyOverflowSegments() {
		final IOManager ioMan = new IOManagerAsync();
		
		final TypeSerializer<ByteValue> serializer = ByteValueSerializer.INSTANCE;
		final TypeComparator<ByteValue> buildComparator = new ValueComparator<>(true, ByteValue.class);
		final TypeComparator<ByteValue> probeComparator = new ValueComparator<>(true, ByteValue.class);
		
		@SuppressWarnings("unchecked")
		final TypePairComparator<ByteValue, ByteValue> pairComparator = Mockito.mock(TypePairComparator.class);
		
		try {
			final int pageSize = 32*1024;
			final int numSegments = 34;

			List<MemorySegment> memory = getMemory(numSegments, pageSize);

			MutableHashTable<ByteValue, ByteValue> table = new MutableHashTable<>(
					serializer, serializer, buildComparator, probeComparator,
					pairComparator, memory, ioMan, 1, false);

			table.open(new ByteValueIterator(100000000), new ByteValueIterator(1));
			
			table.close();
			
			checkNoTempFilesRemain(ioMan);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		finally {
			ioMan.shutdown();
		}
	}

	/**
	 * Tests that the MutableHashTable spills its partitions when creating the initial table
	 * without overflow segments in the partitions. This means that the records are large.
	 */
	@Test
	public void testSpillingWhenBuildingTableWithoutOverflow() throws Exception {
		final IOManager ioMan = new IOManagerAsync();

		try {
			final TypeSerializer<byte[]> serializer = BytePrimitiveArraySerializer.INSTANCE;
			final TypeComparator<byte[]> buildComparator = new BytePrimitiveArrayComparator(true);
			final TypeComparator<byte[]> probeComparator = new BytePrimitiveArrayComparator(true);

			@SuppressWarnings("unchecked") final TypePairComparator<byte[], byte[]> pairComparator =
				new GenericPairComparator<>(
					new BytePrimitiveArrayComparator(true), new BytePrimitiveArrayComparator(true));

			final int pageSize = 128;
			final int numSegments = 33;

			List<MemorySegment> memory = getMemory(numSegments, pageSize);

			MutableHashTable<byte[], byte[]> table = new MutableHashTable<byte[], byte[]>(
				serializer,
				serializer,
				buildComparator,
				probeComparator,
				pairComparator,
				memory,
				ioMan,
				1,
				false);

			int numElements = 9;

			table.open(
				new CombiningIterator<byte[]>(
					new ByteArrayIterator(numElements, 128, (byte) 0),
					new ByteArrayIterator(numElements, 128, (byte) 1)),
				new CombiningIterator<byte[]>(
					new ByteArrayIterator(1, 128, (byte) 0),
					new ByteArrayIterator(1, 128, (byte) 1)));

			while (table.nextRecord()) {
				MutableObjectIterator<byte[]> iterator = table.getBuildSideIterator();

				int counter = 0;

				while (iterator.next() != null) {
					counter++;
				}

				// check that we retrieve all our elements
				Assert.assertEquals(numElements, counter);
			}

			table.close();
		} finally {
			ioMan.shutdown();
		}
	}
	
	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------
	
	private static List<MemorySegment> getMemory(int numSegments, int segmentSize) {
		ArrayList<MemorySegment> list = new ArrayList<MemorySegment>(numSegments);
		for (int i = 0; i < numSegments; i++) {
			list.add(MemorySegmentFactory.allocateUnpooledSegment(segmentSize));
		}
		return list;
	}
	
	private static void checkNoTempFilesRemain(IOManager ioManager) {
		for (File dir : ioManager.getSpillingDirectories()) {
			for (String file : dir.list()) {
				if (file != null && !(file.equals(".") || file.equals(".."))) {
					fail("hash table did not clean up temp files. remaining file: " + file);
				}
			}
		}
	}

	private static class TupleBytesIterator implements MutableObjectIterator<Tuple2<Long, byte[]>> {

		private final byte[] payload;
		private final int numRecords;
		
		private int count = 0;

		TupleBytesIterator(byte[] payload, int numRecords) {
			this.payload = payload;
			this.numRecords = numRecords;
		}

		@Override
		public Tuple2<Long, byte[]> next(Tuple2<Long, byte[]> reuse) {
			return next();
		}

		@Override
		public Tuple2<Long, byte[]> next() {
			if (count++ < numRecords) {
				return new Tuple2<>(42L, payload);
			} else {
				return null;
			}
		}
	}

	private static class ByteArrayIterator implements MutableObjectIterator<byte[]> {

		private final long numRecords;
		private long counter = 0;
		private final byte[] arrayValue;


		ByteArrayIterator(long numRecords, int length, byte value) {
			this.numRecords = numRecords;
			arrayValue = new byte[length];
			Arrays.fill(arrayValue, value);
		}

		@Override
		public byte[] next(byte[] array) {
			return next();
		}

		@Override
		public byte[] next() {
			if (counter++ < numRecords) {
				return arrayValue;
			} else {
				return null;
			}
		}
	}

	private static class LongIterator implements MutableObjectIterator<Long> {

		private final long numRecords;
		private long value = 0;

		LongIterator(long numRecords) {
			this.numRecords = numRecords;
		}

		@Override
		public Long next(Long aLong) {
			return next();
		}

		@Override
		public Long next() {
			if (value < numRecords) {
				return value++;
			} else {
				return null;
			}
		}
	}

	private static class ByteValueIterator implements MutableObjectIterator<ByteValue> {

		private final long numRecords;
		private long value = 0;

		ByteValueIterator(long numRecords) {
			this.numRecords = numRecords;
		}

		@Override
		public ByteValue next(ByteValue aLong) {
			return next();
		}

		@Override
		public ByteValue next() {
			if (value++ < numRecords) {
				return new ByteValue((byte) 0);
			} else {
				return null;
			}
		}
	}

	private static class CombiningIterator<T> implements MutableObjectIterator<T> {

		private final MutableObjectIterator<T> left;
		private final MutableObjectIterator<T> right;

		public CombiningIterator(MutableObjectIterator<T> left, MutableObjectIterator<T> right) {
			this.left = left;
			this.right = right;
		}

		@Override
		public T next(T reuse) throws IOException {
			T value = left.next(reuse);

			if (value == null) {
				return right.next(reuse);
			} else {
				return value;
			}
		}

		@Override
		public T next() throws IOException {
			T value = left.next();

			if (value == null) {
				return right.next();
			} else {
				return value;
			}
		}
	}
}
