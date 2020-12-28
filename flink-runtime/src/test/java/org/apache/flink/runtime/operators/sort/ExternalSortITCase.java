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
import org.apache.flink.api.common.typeutils.base.IntComparator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryManagerBuilder;
import org.apache.flink.runtime.operators.testutils.DummyInvokable;
import org.apache.flink.runtime.operators.testutils.RandomIntPairGenerator;
import org.apache.flink.runtime.operators.testutils.TestData;
import org.apache.flink.runtime.operators.testutils.TestData.TupleGenerator.KeyMode;
import org.apache.flink.runtime.operators.testutils.TestData.TupleGenerator.ValueMode;
import org.apache.flink.runtime.operators.testutils.types.IntPair;
import org.apache.flink.runtime.operators.testutils.types.IntPairSerializer;
import org.apache.flink.util.MutableObjectIterator;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExternalSortITCase extends TestLogger {

    private static final Logger LOG = LoggerFactory.getLogger(ExternalSortITCase.class);

    private static final long SEED = 649180756312423613L;

    private static final int KEY_MAX = Integer.MAX_VALUE;

    private static final int VALUE_LENGTH = 114;

    private static final String VAL =
            "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

    private static final int NUM_PAIRS = 200000;

    private static final int MEMORY_SIZE = 1024 * 1024 * 78;

    private final AbstractInvokable parentTask = new DummyInvokable();

    private IOManager ioManager;

    private MemoryManager memoryManager;

    private TypeSerializerFactory<Tuple2<Integer, String>> pactRecordSerializer;

    private TypeComparator<Tuple2<Integer, String>> pactRecordComparator;

    private boolean testSuccess;

    // --------------------------------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    @Before
    public void beforeTest() {
        this.memoryManager = MemoryManagerBuilder.newBuilder().setMemorySize(MEMORY_SIZE).build();
        this.ioManager = new IOManagerAsync();

        this.pactRecordSerializer = TestData.getIntStringTupleSerializerFactory();
        this.pactRecordComparator = TestData.getIntStringTupleComparator();
    }

    @After
    public void afterTest() throws Exception {
        this.ioManager.close();

        if (this.memoryManager != null && testSuccess) {
            Assert.assertTrue(
                    "Memory leak: not all segments have been returned to the memory manager.",
                    this.memoryManager.verifyEmpty());
            this.memoryManager.shutdown();
            this.memoryManager = null;
        }
    }

    // --------------------------------------------------------------------------------------------

    @Test
    public void testInMemorySort() {
        try {
            // comparator
            final TypeComparator<Integer> keyComparator = new IntComparator(true);

            final TestData.TupleGenerator generator =
                    new TestData.TupleGenerator(
                            SEED, KEY_MAX, VALUE_LENGTH, KeyMode.RANDOM, ValueMode.CONSTANT, VAL);
            final MutableObjectIterator<Tuple2<Integer, String>> source =
                    new TestData.TupleGeneratorIterator(generator, NUM_PAIRS);

            // merge iterator
            LOG.debug("Initializing sortmerger...");

            Sorter<Tuple2<Integer, String>> merger =
                    ExternalSorter.newBuilder(
                                    this.memoryManager,
                                    this.parentTask,
                                    pactRecordSerializer.getSerializer(),
                                    pactRecordComparator)
                            .maxNumFileHandles(2)
                            .enableSpilling(ioManager, 0.9f)
                            .memoryFraction((double) 64 / 78)
                            .objectReuse(true)
                            .largeRecords(true)
                            .build(source);

            // emit data
            LOG.debug("Reading and sorting data...");

            // check order
            MutableObjectIterator<Tuple2<Integer, String>> iterator = merger.getIterator();

            LOG.debug("Checking results...");
            int pairsEmitted = 1;

            Tuple2<Integer, String> rec1 = new Tuple2<>();
            Tuple2<Integer, String> rec2 = new Tuple2<>();

            Assert.assertTrue((rec1 = iterator.next(rec1)) != null);
            while ((rec2 = iterator.next(rec2)) != null) {
                pairsEmitted++;

                Assert.assertTrue(keyComparator.compare(rec1.f0, rec2.f0) <= 0);

                Tuple2<Integer, String> tmp = rec1;
                rec1 = rec2;
                rec2 = tmp;
            }
            Assert.assertTrue(NUM_PAIRS == pairsEmitted);

            merger.close();
            testSuccess = true;
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testInMemorySortUsing10Buffers() {
        try {
            // comparator
            final TypeComparator<Integer> keyComparator = new IntComparator(true);

            final TestData.TupleGenerator generator =
                    new TestData.TupleGenerator(
                            SEED, KEY_MAX, VALUE_LENGTH, KeyMode.RANDOM, ValueMode.CONSTANT, VAL);
            final MutableObjectIterator<Tuple2<Integer, String>> source =
                    new TestData.TupleGeneratorIterator(generator, NUM_PAIRS);

            // merge iterator
            LOG.debug("Initializing sortmerger...");

            Sorter<Tuple2<Integer, String>> merger =
                    ExternalSorter.newBuilder(
                                    this.memoryManager,
                                    this.parentTask,
                                    pactRecordSerializer.getSerializer(),
                                    pactRecordComparator)
                            .maxNumFileHandles(2)
                            .sortBuffers(10)
                            .enableSpilling(ioManager, 0.9f)
                            .memoryFraction((double) 64 / 78)
                            .objectReuse(false)
                            .largeRecords(true)
                            .build(source);

            // emit data
            LOG.debug("Reading and sorting data...");

            // check order
            MutableObjectIterator<Tuple2<Integer, String>> iterator = merger.getIterator();

            LOG.debug("Checking results...");
            int pairsEmitted = 1;

            Tuple2<Integer, String> rec1 = new Tuple2<>();
            Tuple2<Integer, String> rec2 = new Tuple2<>();

            Assert.assertTrue((rec1 = iterator.next(rec1)) != null);
            while ((rec2 = iterator.next(rec2)) != null) {
                pairsEmitted++;

                Assert.assertTrue(keyComparator.compare(rec1.f0, rec2.f0) <= 0);

                Tuple2<Integer, String> tmp = rec1;
                rec1 = rec2;
                rec2 = tmp;
            }
            Assert.assertTrue(NUM_PAIRS == pairsEmitted);

            merger.close();
            testSuccess = true;
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testSpillingSort() {
        try {
            // comparator
            final TypeComparator<Integer> keyComparator = new IntComparator(true);

            final TestData.TupleGenerator generator =
                    new TestData.TupleGenerator(
                            SEED, KEY_MAX, VALUE_LENGTH, KeyMode.RANDOM, ValueMode.CONSTANT, VAL);
            final MutableObjectIterator<Tuple2<Integer, String>> source =
                    new TestData.TupleGeneratorIterator(generator, NUM_PAIRS);

            // merge iterator
            LOG.debug("Initializing sortmerger...");

            Sorter<Tuple2<Integer, String>> merger =
                    ExternalSorter.newBuilder(
                                    this.memoryManager,
                                    this.parentTask,
                                    pactRecordSerializer.getSerializer(),
                                    pactRecordComparator)
                            .maxNumFileHandles(64)
                            .enableSpilling(ioManager, 0.7f)
                            .memoryFraction((double) 16 / 78)
                            .objectReuse(true)
                            .largeRecords(true)
                            .build(source);

            // emit data
            LOG.debug("Reading and sorting data...");

            // check order
            MutableObjectIterator<Tuple2<Integer, String>> iterator = merger.getIterator();

            LOG.debug("Checking results...");
            int pairsEmitted = 1;

            Tuple2<Integer, String> rec1 = new Tuple2<>();
            Tuple2<Integer, String> rec2 = new Tuple2<>();

            Assert.assertTrue((rec1 = iterator.next(rec1)) != null);
            while ((rec2 = iterator.next(rec2)) != null) {
                pairsEmitted++;

                Assert.assertTrue(keyComparator.compare(rec1.f0, rec2.f0) <= 0);

                Tuple2<Integer, String> tmp = rec1;
                rec1 = rec2;
                rec2 = tmp;
            }
            Assert.assertTrue(NUM_PAIRS == pairsEmitted);

            merger.close();
            testSuccess = true;
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testSpillingSortWithIntermediateMerge() {
        try {
            // amount of pairs
            final int PAIRS = 10000000;

            // comparator
            final TypeComparator<Integer> keyComparator = new IntComparator(true);

            final TestData.TupleGenerator generator =
                    new TestData.TupleGenerator(
                            SEED, KEY_MAX, VALUE_LENGTH, KeyMode.RANDOM, ValueMode.FIX_LENGTH);
            final MutableObjectIterator<Tuple2<Integer, String>> source =
                    new TestData.TupleGeneratorIterator(generator, PAIRS);

            // merge iterator
            LOG.debug("Initializing sortmerger...");

            Sorter<Tuple2<Integer, String>> merger =
                    ExternalSorter.newBuilder(
                                    this.memoryManager,
                                    this.parentTask,
                                    pactRecordSerializer.getSerializer(),
                                    pactRecordComparator)
                            .maxNumFileHandles(16)
                            .enableSpilling(ioManager, 0.7f)
                            .memoryFraction((double) 64 / 78)
                            .objectReuse(false)
                            .largeRecords(true)
                            .build(source);

            // emit data
            LOG.debug("Emitting data...");

            // check order
            MutableObjectIterator<Tuple2<Integer, String>> iterator = merger.getIterator();

            LOG.debug("Checking results...");
            int pairsRead = 1;
            int nextStep = PAIRS / 20;

            Tuple2<Integer, String> rec1 = new Tuple2<>();
            Tuple2<Integer, String> rec2 = new Tuple2<>();

            Assert.assertTrue((rec1 = iterator.next(rec1)) != null);
            while ((rec2 = iterator.next(rec2)) != null) {
                pairsRead++;

                Assert.assertTrue(keyComparator.compare(rec1.f0, rec2.f0) <= 0);

                Tuple2<Integer, String> tmp = rec1;
                rec1 = rec2;
                rec2 = tmp;

                // log
                if (pairsRead == nextStep) {
                    nextStep += PAIRS / 20;
                }
            }
            Assert.assertEquals("Not all pairs were read back in.", PAIRS, pairsRead);
            merger.close();
            testSuccess = true;
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testSpillingSortWithIntermediateMergeIntPair() {
        try {
            // amount of pairs
            final int PAIRS = 50000000;

            // comparator
            final RandomIntPairGenerator generator = new RandomIntPairGenerator(12345678, PAIRS);

            final TypeSerializerFactory<IntPair> serializerFactory =
                    new IntPairSerializer.IntPairSerializerFactory();
            final TypeComparator<IntPair> comparator = new TestData.IntPairComparator();

            // merge iterator
            LOG.debug("Initializing sortmerger...");

            Sorter<IntPair> merger =
                    ExternalSorter.newBuilder(
                                    this.memoryManager,
                                    this.parentTask,
                                    serializerFactory.getSerializer(),
                                    comparator)
                            .maxNumFileHandles(4)
                            .enableSpilling(ioManager, 0.7f)
                            .memoryFraction((double) 64 / 78)
                            .objectReuse(true)
                            .largeRecords(true)
                            .build(generator);

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
            testSuccess = true;
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }
}
