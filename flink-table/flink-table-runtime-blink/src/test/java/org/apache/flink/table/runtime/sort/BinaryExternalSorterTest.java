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

package org.apache.flink.table.runtime.sort;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.table.api.TableConfigOptions;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.dataformat.BinaryRowWriter;
import org.apache.flink.table.dataformat.BinaryString;
import org.apache.flink.table.typeutils.AbstractRowSerializer;
import org.apache.flink.table.typeutils.BinaryRowSerializer;
import org.apache.flink.util.MutableObjectIterator;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Sort test for binary row.
 */
@RunWith(Parameterized.class)
public class BinaryExternalSorterTest {

	private static final int MEMORY_SIZE = 1024 * 1024 * 32;
	private static final Logger LOG = LoggerFactory.getLogger(BinaryExternalSorterTest.class);
	private IOManager ioManager;
	private MemoryManager memoryManager;
	private BinaryRowSerializer serializer;
	private Configuration conf;

	public BinaryExternalSorterTest(
			boolean spillCompress,
			boolean asyncMerge) {
		ioManager = new IOManagerAsync();
		conf = new Configuration();
		if (!spillCompress) {
			conf.setBoolean(TableConfigOptions.SQL_EXEC_SPILL_COMPRESSION_ENABLED, false);
		}
		if (asyncMerge) {
			conf.setBoolean(TableConfigOptions.SQL_EXEC_SORT_ASYNC_MERGE_ENABLED, true);
		}
	}

	@Parameterized.Parameters(name = "spillCompress-{0} asyncMerge-{1}")
	public static Collection<Boolean[]> parameters() {
		return Arrays.asList(
				new Boolean[]{false, false},
				new Boolean[]{false, true},
				new Boolean[]{true, false},
				new Boolean[]{true, true});
	}

	private static String getString(int count) {
		StringBuilder builder = new StringBuilder();
		for (int i = 0; i < 8; i++) {
			builder.append(count);
		}
		return builder.toString();
	}

	@SuppressWarnings("unchecked")
	@Before
	public void beforeTest() {
		this.memoryManager = new MemoryManager(MEMORY_SIZE, 1);
		this.serializer = new BinaryRowSerializer(2);
		this.conf.setInteger(TableConfigOptions.SQL_EXEC_SORT_FILE_HANDLES_MAX_NUM, 128);
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
	public void testSortTwoBufferInMemory() throws Exception {

		int size = 1_000_000;

		MockBinaryRowReader reader = new MockBinaryRowReader(size);

		LOG.debug("initializing sortmerger");

		//there are two sort buffer if sortMemory > 100 * 1024 * 1024.
		MemoryManager memoryManager = new MemoryManager(1024 * 1024 * 101, 1);
		long minMemorySize = memoryManager.computeNumberOfPages(1) * MemoryManager.DEFAULT_PAGE_SIZE;
		BinaryExternalSorter sorter = new BinaryExternalSorter(
				new Object(),
				memoryManager,
				minMemorySize,
				this.ioManager, (AbstractRowSerializer) serializer, serializer,
				IntNormalizedKeyComputer.INSTANCE, IntRecordComparator.INSTANCE,
				conf, 1f);
		sorter.startThreads();
		sorter.write(reader);

		MutableObjectIterator<BinaryRow> iterator = sorter.getIterator();

		BinaryRow next = serializer.createInstance();
		for (int i = 0; i < size; i++) {
			next = iterator.next(next);
			Assert.assertEquals(i, next.getInt(0));
			Assert.assertEquals(getString(i), next.getString(1).toString());
		}

		sorter.close();
		Assert.assertTrue(memoryManager.verifyEmpty());
		memoryManager.shutdown();
	}

	@Test
	public void testSort() throws Exception {

		int size = 10_000;

		MockBinaryRowReader reader = new MockBinaryRowReader(size);

		LOG.debug("initializing sortmerger");

		long minMemorySize = memoryManager.computeNumberOfPages(0.9) * MemoryManager.DEFAULT_PAGE_SIZE;
		BinaryExternalSorter sorter = new BinaryExternalSorter(
				new Object(),
				this.memoryManager,
				minMemorySize,
				this.ioManager, (AbstractRowSerializer) serializer, serializer,
				IntNormalizedKeyComputer.INSTANCE, IntRecordComparator.INSTANCE,
				conf, 0.7f);
		sorter.startThreads();
		sorter.write(reader);

		MutableObjectIterator<BinaryRow> iterator = sorter.getIterator();

		BinaryRow next = serializer.createInstance();
		for (int i = 0; i < size; i++) {
			next = iterator.next(next);
			Assert.assertEquals(i, next.getInt(0));
			Assert.assertEquals(getString(i), next.getString(1).toString());
		}

		sorter.close();
	}

	@Test
	public void testSortIntStringWithRepeat() throws Exception {

		int size = 10_000;

		LOG.debug("initializing sortmerger");

		long minMemorySize = memoryManager.computeNumberOfPages(0.9) * MemoryManager.DEFAULT_PAGE_SIZE;
		BinaryExternalSorter sorter = new BinaryExternalSorter(
				new Object(),
				this.memoryManager,
				minMemorySize,
				this.ioManager, (AbstractRowSerializer) serializer, serializer,
				new IntNormalizedKeyComputer() {
					@Override
					public boolean isKeyFullyDetermines() {
						return false;
					}
				},
				IntRecordComparator.INSTANCE,
				conf, 0.7f);
		sorter.startThreads();
		sorter.write(new MockBinaryRowReader(size));
		sorter.write(new MockBinaryRowReader(size));
		sorter.write(new MockBinaryRowReader(size));

		MutableObjectIterator<BinaryRow> iterator = sorter.getIterator();

		BinaryRow next = serializer.createInstance();
		for (int i = 0; i < size; i++) {
			for (int j = 0; j < 3; j++) {
				next = iterator.next(next);
				Assert.assertEquals(i, next.getInt(0));
				Assert.assertEquals(getString(i), next.getString(1).toString());
			}
		}

		sorter.close();
	}

	@Test
	public void testSpilling() throws Exception {

		int size = 1000_000;

		MockBinaryRowReader reader = new MockBinaryRowReader(size);

		LOG.debug("initializing sortmerger");

		long minMemorySize = memoryManager.computeNumberOfPages(0.1) * MemoryManager.DEFAULT_PAGE_SIZE;
		BinaryExternalSorter sorter = new BinaryExternalSorter(
				new Object(),
				this.memoryManager,
				minMemorySize,
				this.ioManager, (AbstractRowSerializer) serializer, serializer,
				IntNormalizedKeyComputer.INSTANCE, IntRecordComparator.INSTANCE,
				conf, 0.7f);
		sorter.startThreads();
		sorter.write(reader);

		MutableObjectIterator<BinaryRow> iterator = sorter.getIterator();

		BinaryRow next = serializer.createInstance();
		for (int i = 0; i < size; i++) {
			next = iterator.next(next);
			Assert.assertEquals(i, next.getInt(0));
			Assert.assertEquals(getString(i), next.getString(1).toString());
		}

		sorter.close();
	}

	@Test
	public void testSpillingDesc() throws Exception {

		int size = 1000_000;

		MockBinaryRowReader reader = new MockBinaryRowReader(size);

		LOG.debug("initializing sortmerger");

		long minMemorySize = memoryManager.computeNumberOfPages(0.1) * MemoryManager.DEFAULT_PAGE_SIZE;
		BinaryExternalSorter sorter = new BinaryExternalSorter(
				new Object(),
				this.memoryManager,
				minMemorySize,
				this.ioManager, (AbstractRowSerializer) serializer, serializer,
				new IntNormalizedKeyComputer() {
					@Override
					public boolean invertKey() {
						return true;
					}
				},
				new IntRecordComparator() {
					@Override
					public int compare(BaseRow o1, BaseRow o2) {
						return -super.compare(o1, o2);
					}
				},
				conf, 0.7f);
		sorter.startThreads();
		sorter.write(reader);

		MutableObjectIterator<BinaryRow> iterator = sorter.getIterator();

		List<Tuple2<Integer, String>> data = new ArrayList<>();
		for (int i = 0; i < size; i++) {
			data.add(new Tuple2<>(i, getString(i)));
		}
		data.sort((o1, o2) -> -o1.f0.compareTo(o2.f0));

		BinaryRow next = serializer.createInstance();
		for (int i = 0; i < size; i++) {
			next = iterator.next(next);
			Assert.assertEquals((int) data.get(i).f0, next.getInt(0));
			Assert.assertEquals(data.get(i).f1, next.getString(1).toString());
		}

		sorter.close();
	}

	@Test
	public void testMergeManyTimes() throws Exception {

		int size = 1000_000;

		MockBinaryRowReader reader = new MockBinaryRowReader(size);

		LOG.debug("initializing sortmerger");

		long minMemorySize = memoryManager.computeNumberOfPages(0.01) * MemoryManager.DEFAULT_PAGE_SIZE;
		conf.setInteger(TableConfigOptions.SQL_EXEC_SORT_FILE_HANDLES_MAX_NUM, 8);

		BinaryExternalSorter sorter = new BinaryExternalSorter(
				new Object(),
				this.memoryManager,
				minMemorySize,
				this.ioManager, (AbstractRowSerializer) serializer, serializer,
				IntNormalizedKeyComputer.INSTANCE, IntRecordComparator.INSTANCE,
				conf, 0.7f);
		sorter.startThreads();
		sorter.write(reader);

		MutableObjectIterator<BinaryRow> iterator = sorter.getIterator();

		BinaryRow next = serializer.createInstance();
		for (int i = 0; i < size; i++) {
			next = iterator.next(next);
			Assert.assertEquals(i, next.getInt(0));
			Assert.assertEquals(getString(i), next.getString(1).toString());
		}

		sorter.close();
	}

	@Test
	public void testSpillingRandom() throws Exception {

		int size = 1000_000;

		MockBinaryRowReader reader = new MockBinaryRowReader(size);

		LOG.debug("initializing sortmerger");

		long minMemorySize = memoryManager.computeNumberOfPages(0.1) * MemoryManager.DEFAULT_PAGE_SIZE;
		BinaryExternalSorter sorter = new BinaryExternalSorter(
				new Object(),
				this.memoryManager,
				minMemorySize,
				this.ioManager, (AbstractRowSerializer) serializer, serializer,
				IntNormalizedKeyComputer.INSTANCE, IntRecordComparator.INSTANCE,
				conf, 0.7f);
		sorter.startThreads();

		List<BinaryRow> data = new ArrayList<>();
		BinaryRow row = serializer.createInstance();
		for (int i = 0; i < size; i++) {
			row = reader.next(row);
			data.add(row.copy());
		}

		Collections.shuffle(data);

		for (int i = 0; i < size; i++) {
			sorter.write(data.get(i));
		}

		MutableObjectIterator<BinaryRow> iterator = sorter.getIterator();

		data.sort(Comparator.comparingInt(o -> o.getInt(0)));

		BinaryRow next = serializer.createInstance();
		for (int i = 0; i < size; i++) {
			next = iterator.next(next);
			Assert.assertEquals(data.get(i).getInt(0), next.getInt(0));
			Assert.assertEquals(data.get(i).getString(1), next.getString(1));
		}

		sorter.close();
	}

	/**
	 * Mock reader for binary row.
	 */
	public class MockBinaryRowReader implements MutableObjectIterator<BinaryRow> {

		private int size;
		private int count;
		private BinaryRow row;
		private BinaryRowWriter writer;

		public MockBinaryRowReader(int size) {
			this.size = size;
			this.row = new BinaryRow(2);
			this.writer = new BinaryRowWriter(row);
		}

		@Override
		public BinaryRow next(BinaryRow reuse) {
			return next();
		}

		@Override
		public BinaryRow next() {
			if (count >= size) {
				return null;
			}
			writer.reset();
			writer.writeInt(0, count);
			writer.writeString(1, BinaryString.fromString(getString(count)));
			writer.complete();
			count++;
			return row;
		}
	}
}
