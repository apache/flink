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

import org.apache.flink.api.common.functions.RichGroupCombineFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.iomanager.ChannelWriterOutputView;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryManagerBuilder;
import org.apache.flink.runtime.operators.testutils.DummyInvokable;
import org.apache.flink.runtime.operators.testutils.TestData;
import org.apache.flink.runtime.util.EmptyMutableObjectIterator;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/** Tests for the {@link ExternalSorter}. */
public class ExternalSorterTest extends TestLogger {

    @Test
    public void testInMemorySorterDisposal() throws Exception {
        final TestingInMemorySorterFactory<Tuple2<Integer, Integer>> inMemorySorterFactory =
                new TestingInMemorySorterFactory<>();

        final int numPages = 32;
        final MemoryManager memoryManager =
                MemoryManagerBuilder.newBuilder()
                        .setMemorySize(MemoryManager.DEFAULT_PAGE_SIZE * numPages)
                        .build();
        final DummyInvokable parentTask = new DummyInvokable();

        try (final IOManagerAsync ioManager = new IOManagerAsync()) {
            final List<MemorySegment> memory = memoryManager.allocatePages(parentTask, numPages);
            final Sorter<Tuple2<Integer, Integer>> unilateralSortMerger =
                    ExternalSorter.newBuilder(
                                    memoryManager,
                                    parentTask,
                                    TestData.getIntIntTupleSerializerFactory().getSerializer(),
                                    TestData.getIntIntTupleComparator())
                            .maxNumFileHandles(2)
                            .enableSpilling(ioManager, 1.0f)
                            .memory(memory)
                            .sortBuffers(10)
                            .objectReuse(false)
                            .largeRecords(false)
                            .sorterFactory(inMemorySorterFactory)
                            .build(EmptyMutableObjectIterator.get());

            final Collection<TestingInMemorySorter<?>> inMemorySorters =
                    inMemorySorterFactory.getInMemorySorters();

            assertThat(inMemorySorters, is(not(empty())));

            unilateralSortMerger.close();

            for (TestingInMemorySorter<?> inMemorySorter : inMemorySorters) {
                assertThat(inMemorySorter.isDisposed(), is(true));
            }
        } finally {
            memoryManager.shutdown();
        }
    }

    @Test
    public void testOpeningCombineUdf() throws Exception {
        final TestingInMemorySorterFactory<Tuple2<Integer, Integer>> inMemorySorterFactory =
                new TestingInMemorySorterFactory<>();

        final int numPages = 32;
        final MemoryManager memoryManager =
                MemoryManagerBuilder.newBuilder()
                        .setMemorySize(MemoryManager.DEFAULT_PAGE_SIZE * numPages)
                        .build();
        final DummyInvokable parentTask = new DummyInvokable();

        Configuration config = new Configuration();
        config.set(testOption, "TEST");

        try (final IOManagerAsync ioManager = new IOManagerAsync()) {
            final List<MemorySegment> memory = memoryManager.allocatePages(parentTask, numPages);
            RichCombiner combiner = new RichCombiner();
            final Sorter<Tuple2<Integer, Integer>> unilateralSortMerger =
                    ExternalSorter.newBuilder(
                                    memoryManager,
                                    parentTask,
                                    TestData.getIntIntTupleSerializerFactory().getSerializer(),
                                    TestData.getIntIntTupleComparator())
                            .maxNumFileHandles(2)
                            .enableSpilling(ioManager, 0f)
                            .memory(memory)
                            .sortBuffers(10)
                            .objectReuse(false)
                            .largeRecords(false)
                            .sorterFactory(inMemorySorterFactory)
                            .withCombiner(combiner, config)
                            .build(EmptyMutableObjectIterator.get());

            // wait for the results
            unilateralSortMerger.getIterator();
            unilateralSortMerger.close();
            assertTrue("Combiner was not opened", combiner.isOpen);
            assertTrue("Combiner was not closed", combiner.isClosed);
        } finally {
            memoryManager.shutdown();
        }
    }

    private static final ConfigOption<String> testOption =
            ConfigOptions.key("test").stringType().noDefaultValue();

    private static final class RichCombiner
            extends RichGroupCombineFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {

        boolean isOpen = false;
        boolean isClosed = false;

        @Override
        public void open(Configuration parameters) throws Exception {
            assertFalse("UDF was already opened", isOpen);
            isOpen = true;
            assertThat(parameters.get(testOption), equalTo("TEST"));
        }

        @Override
        public void close() throws Exception {
            isClosed = true;
        }

        @Override
        public void combine(
                Iterable<Tuple2<Integer, Integer>> values, Collector<Tuple2<Integer, Integer>> out)
                throws Exception {}
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
        public void reset() {}

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
        public void writeToOutput(ChannelWriterOutputView output) throws IOException {}

        @Override
        public void writeToOutput(
                ChannelWriterOutputView output, LargeRecordHandler<T> largeRecordsOutput)
                throws IOException {}

        @Override
        public void writeToOutput(ChannelWriterOutputView output, int start, int num)
                throws IOException {}

        @Override
        public int compare(int i, int j) {
            return 0;
        }

        @Override
        public int compare(
                int segmentNumberI, int segmentOffsetI, int segmentNumberJ, int segmentOffsetJ) {
            return 0;
        }

        @Override
        public void swap(int i, int j) {}

        @Override
        public void swap(
                int segmentNumberI, int segmentOffsetI, int segmentNumberJ, int segmentOffsetJ) {}

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
