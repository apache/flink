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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.iomanager.ChannelWriterOutputView;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.testutils.DummyInvokable;
import org.apache.flink.runtime.operators.testutils.TestData;
import org.apache.flink.runtime.util.EmptyMutableObjectIterator;
import org.apache.flink.util.MutableObjectIterator;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

/**
 * Tests for the {@link UnilateralSortMerger}.
 */
public class UnilateralSortMergerTest extends TestLogger {

	@Test
	public void testInMemorySorterDisposal() throws Exception {
		final TestingInMemorySorterFactory<Tuple2<Integer, Integer>> inMemorySorterFactory = new TestingInMemorySorterFactory<>();

		final int numPages = 32;
		final MemoryManager memoryManager = new MemoryManager(MemoryManager.DEFAULT_PAGE_SIZE * numPages, 1);
		final DummyInvokable parentTask = new DummyInvokable();

		try (final IOManagerAsync ioManager = new IOManagerAsync()) {
			final List<MemorySegment> memory = memoryManager.allocatePages(parentTask, numPages);
			final UnilateralSortMerger<Tuple2<Integer, Integer>> unilateralSortMerger = new UnilateralSortMerger<>(
				memoryManager,
				memory,
				ioManager,
				EmptyMutableObjectIterator.get(),
				parentTask,
				TestData.getIntIntTupleSerializerFactory(),
				TestData.getIntIntTupleComparator(),
				10,
				2,
				1.0f,
				true,
				false,
				false,
				inMemorySorterFactory);

			final Collection<TestingInMemorySorter<?>> inMemorySorters = inMemorySorterFactory.getInMemorySorters();

			assertThat(inMemorySorters, is(not(empty())));

			unilateralSortMerger.close();

			assertThat(unilateralSortMerger.closed, is(true));

			for (TestingInMemorySorter<?> inMemorySorter : inMemorySorters) {
				assertThat(inMemorySorter.isDisposed(), is(true));
			}
		} finally {
			memoryManager.shutdown();
		}
	}

	private static final class TestingInMemorySorterFactory<T> implements InMemorySorterFactory<T> {

		private final Collection<TestingInMemorySorter<?>> inMemorySorters = new ArrayList<>(10);

		Collection<TestingInMemorySorter<?>> getInMemorySorters() {
			return inMemorySorters;
		}

		@Override
		public InMemorySorter<T> create(List<MemorySegment> sortSegments) {
			final TestingInMemorySorter<T> testingInMemorySorter = new TestingInMemorySorter<>();
			inMemorySorters.add(testingInMemorySorter);
			return testingInMemorySorter;
		}
	}

	private static final class TestingInMemorySorter<T> implements InMemorySorter<T> {

		private volatile boolean isDisposed;

		public boolean isDisposed() {
			return isDisposed;
		}

		@Override
		public void reset() {

		}

		@Override
		public boolean isEmpty() {
			return true;
		}

		@Override
		public void dispose() {
			isDisposed = true;
		}

		@Override
		public long getCapacity() {
			return 0;
		}

		@Override
		public long getOccupancy() {
			return 0;
		}

		@Override
		public T getRecord(int logicalPosition) throws IOException {
			return null;
		}

		@Override
		public T getRecord(T reuse, int logicalPosition) throws IOException {
			return null;
		}

		@Override
		public boolean write(T record) throws IOException {
			return false;
		}

		@Override
		public MutableObjectIterator<T> getIterator() {
			return null;
		}

		@Override
		public void writeToOutput(ChannelWriterOutputView output) throws IOException {

		}

		@Override
		public void writeToOutput(ChannelWriterOutputView output, LargeRecordHandler<T> largeRecordsOutput) throws IOException {

		}

		@Override
		public void writeToOutput(ChannelWriterOutputView output, int start, int num) throws IOException {

		}

		@Override
		public int compare(int i, int j) {
			return 0;
		}

		@Override
		public int compare(int segmentNumberI, int segmentOffsetI, int segmentNumberJ, int segmentOffsetJ) {
			return 0;
		}

		@Override
		public void swap(int i, int j) {

		}

		@Override
		public void swap(int segmentNumberI, int segmentOffsetI, int segmentNumberJ, int segmentOffsetJ) {

		}

		@Override
		public int size() {
			return 0;
		}

		@Override
		public int recordSize() {
			return 0;
		}

		@Override
		public int recordsPerSegment() {
			return 0;
		}
	}

}
