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

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypePairComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongComparator;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.TupleComparator;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.util.MutableObjectIterator;

import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
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
					MutableHashTable.HashBucketIterator<Tuple2<Long, byte[]>, Long> matches = table.getBuildSideIterator();
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
}
