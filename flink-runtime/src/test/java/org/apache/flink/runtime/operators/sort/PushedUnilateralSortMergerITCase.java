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

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.testutils.DummyInvokable;
import org.apache.flink.runtime.operators.testutils.TestData;
import org.apache.flink.util.MutableObjectIterator;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PushedUnilateralSortMergerITCase {
	private static final Logger LOG = LoggerFactory.getLogger(CombiningUnilateralSortMergerITCase.class);

	private static final long SEED = 649180756312423613L;

	private static final int KEY_MAX = 1000;

	private static final int VALUE_LENGTH = 118;

	private static final int NUM_PAIRS = 50000;

	public static final int MEMORY_SIZE = 1024 * 1024 * 256;

	private final AbstractInvokable parentTask = new DummyInvokable();

	private IOManager ioManager;

	private MemoryManager memoryManager;

	private TypeSerializerFactory<Tuple2<Integer, String>> serializerFactory1;
	private TypeSerializerFactory<Tuple2<Integer, Integer>> serializerFactory2;

	private TypeComparator<Tuple2<Integer, String>> comparator1;
	private TypeComparator<Tuple2<Integer, Integer>> comparator2;

	@SuppressWarnings("unchecked")
	@Before
	public void beforeTest() {
		this.memoryManager = new MemoryManager(MEMORY_SIZE, 1);
		this.ioManager = new IOManagerAsync();

		this.serializerFactory1 = TestData.getIntStringTupleSerializerFactory();
		this.comparator1 = TestData.getIntStringTupleComparator();

		this.serializerFactory2 = TestData.getIntIntTupleSerializerFactory();
		this.comparator2 = TestData.getIntIntTupleComparator();
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

	@Ignore
	public void testCombine() throws Exception {
		int noKeys = 100;
		int noKeyCnt = 100;

		SortedDataFileFactory<Tuple2<Integer, Integer>> sortedDataFileFactory = new BlockSortedDataFileFactory<>(
			ioManager.createChannelEnumerator(), serializerFactory2.getSerializer(), ioManager);
		SortedDataFileMerger<Tuple2<Integer, Integer>> merger = new RecordComparisonMerger<>(
			sortedDataFileFactory, ioManager, serializerFactory2.getSerializer(), comparator2,
			64, true);
		PushedUnilateralSortMerger<Tuple2<Integer, Integer>> sortMerger = new PushedUnilateralSortMerger<Tuple2<Integer, Integer>>(
			sortedDataFileFactory, merger,
			memoryManager, memoryManager.allocatePages(parentTask, memoryManager.computeNumberOfPages(0.8)),
			ioManager, parentTask, serializerFactory2, comparator2,
			0, 64, false, 0.7f,
			false, false, true, false);

		final Tuple2<Integer, Integer> rec = new Tuple2<>();

		for (int i = 0; i < noKeyCnt; i++) {
			for (int j = 0; j < noKeys; j++) {
				rec.setField(i, 1);
				rec.setField(j, 0);
				sortMerger.add(rec);
			}
		}

		sortMerger.finishAdding();

		MutableObjectIterator<Tuple2<Integer, Integer>> iterator = sortMerger.getIterator();
		Tuple2<Integer, Integer> tuple2 = new Tuple2<>();

		while (iterator.next(tuple2) != null) {

		}
	}

	@Ignore
	public void testCombine2() throws Exception {
		int noKeys = 100;
		int noKeyCnt = 100;

		SortedDataFileFactory<Tuple2<Integer, Integer>> sortedDataFileFactory = new BlockSortedDataFileFactory<>(
			ioManager.createChannelEnumerator(), serializerFactory2.getSerializer(), ioManager);
		SortedDataFileMerger<Tuple2<Integer, Integer>> merger = new RecordComparisonMerger<>(
			sortedDataFileFactory, ioManager, serializerFactory2.getSerializer(), comparator2,
			64, true);
		PushedUnilateralSortMerger<Tuple2<Integer, Integer>> sortMerger = new PushedUnilateralSortMerger<Tuple2<Integer, Integer>>(
			sortedDataFileFactory, merger,
			memoryManager, memoryManager.allocatePages(parentTask, memoryManager.computeNumberOfPages(0.8)),
			ioManager, parentTask, serializerFactory2, comparator2,
			0, 64, true, 0.7f,
			false, false, true, false);

		final Tuple2<Integer, Integer> rec = new Tuple2<>();

		for (int i = 0; i < noKeyCnt; i++) {
			for (int j = 0; j < noKeys; j++) {
				rec.setField(i, 1);
				rec.setField(j, 0);
				sortMerger.add(rec);
			}
		}

		sortMerger.finishAdding();

		MutableObjectIterator<Tuple2<Integer, Integer>> iterator = sortMerger.getIterator();
		Tuple2<Integer, Integer> tuple2 = new Tuple2<>();

		while (iterator.next(tuple2) != null) {

		}
	}
}
