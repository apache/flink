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
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryManagerBuilder;
import org.apache.flink.runtime.operators.testutils.DummyInvokable;
import org.apache.flink.runtime.operators.testutils.UniformIntPairGenerator;
import org.apache.flink.runtime.operators.testutils.UniformRecordGenerator;
import org.apache.flink.runtime.operators.testutils.UnionIterator;
import org.apache.flink.runtime.operators.testutils.types.IntPair;
import org.apache.flink.runtime.operators.testutils.types.IntPairComparator;
import org.apache.flink.runtime.operators.testutils.types.IntPairPairComparator;
import org.apache.flink.runtime.operators.testutils.types.IntPairSerializer;
import org.apache.flink.runtime.testutils.recordutils.RecordComparator;
import org.apache.flink.runtime.testutils.recordutils.RecordSerializer;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.NullKeyFieldException;
import org.apache.flink.types.Record;
import org.apache.flink.types.Value;
import org.apache.flink.util.MutableObjectIterator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

class HashTableITCase {

    private static final AbstractInvokable MEM_OWNER = new DummyInvokable();

    private MemoryManager memManager;
    private IOManager ioManager;

    private TypeSerializer<Record> recordBuildSideAccesssor;
    private TypeSerializer<Record> recordProbeSideAccesssor;
    private TypeComparator<Record> recordBuildSideComparator;
    private TypeComparator<Record> recordProbeSideComparator;
    private TypePairComparator<Record, Record> pactRecordComparator;

    private TypeSerializer<IntPair> pairBuildSideAccesssor;
    private TypeSerializer<IntPair> pairProbeSideAccesssor;
    private TypeComparator<IntPair> pairBuildSideComparator;
    private TypeComparator<IntPair> pairProbeSideComparator;
    private TypePairComparator<IntPair, IntPair> pairComparator;

    @BeforeEach
    void setup() {
        final int[] keyPos = new int[] {0};
        @SuppressWarnings("unchecked")
        final Class<? extends Value>[] keyType =
                (Class<? extends Value>[]) new Class[] {IntValue.class};

        this.recordBuildSideAccesssor = RecordSerializer.get();
        this.recordProbeSideAccesssor = RecordSerializer.get();
        this.recordBuildSideComparator = new RecordComparator(keyPos, keyType);
        this.recordProbeSideComparator = new RecordComparator(keyPos, keyType);
        this.pactRecordComparator = new RecordPairComparatorFirstInt();

        this.pairBuildSideAccesssor = new IntPairSerializer();
        this.pairProbeSideAccesssor = new IntPairSerializer();
        this.pairBuildSideComparator = new IntPairComparator();
        this.pairProbeSideComparator = new IntPairComparator();
        this.pairComparator = new IntPairPairComparator();

        this.memManager = MemoryManagerBuilder.newBuilder().setMemorySize(32 * 1024 * 1024).build();
        this.ioManager = new IOManagerAsync();
    }

    @AfterEach
    void tearDown() throws Exception {
        // shut down I/O manager and Memory Manager and verify the correct shutdown
        this.ioManager.close();
        assertThat(this.memManager.verifyEmpty())
                .withFailMessage(
                        "Not all memory was properly released to the memory manager --> Memory Leak.")
                .isTrue();
    }

    @Test
    void testIOBufferCountComputation() {
        assertThat(MutableHashTable.getNumWriteBehindBuffers(32)).isOne();
        assertThat(MutableHashTable.getNumWriteBehindBuffers(33)).isOne();
        assertThat(MutableHashTable.getNumWriteBehindBuffers(40)).isOne();
        assertThat(MutableHashTable.getNumWriteBehindBuffers(64)).isOne();
        assertThat(MutableHashTable.getNumWriteBehindBuffers(127)).isOne();
        assertThat(MutableHashTable.getNumWriteBehindBuffers(128)).isEqualTo(2);
        assertThat(MutableHashTable.getNumWriteBehindBuffers(129)).isEqualTo(2);
        assertThat(MutableHashTable.getNumWriteBehindBuffers(511)).isEqualTo(2);
        assertThat(MutableHashTable.getNumWriteBehindBuffers(512)).isEqualTo(3);
        assertThat(MutableHashTable.getNumWriteBehindBuffers(513)).isEqualTo(3);
        assertThat(MutableHashTable.getNumWriteBehindBuffers(2047)).isEqualTo(3);
        assertThat(MutableHashTable.getNumWriteBehindBuffers(2048)).isEqualTo(4);
        assertThat(MutableHashTable.getNumWriteBehindBuffers(2049)).isEqualTo(4);
        assertThat(MutableHashTable.getNumWriteBehindBuffers(8191)).isEqualTo(4);
        assertThat(MutableHashTable.getNumWriteBehindBuffers(8192)).isEqualTo(5);
        assertThat(MutableHashTable.getNumWriteBehindBuffers(8193)).isEqualTo(5);
        assertThat(MutableHashTable.getNumWriteBehindBuffers(32767)).isEqualTo(5);
        assertThat(MutableHashTable.getNumWriteBehindBuffers(32768)).isEqualTo(6);
        assertThat(MutableHashTable.getNumWriteBehindBuffers(Integer.MAX_VALUE)).isEqualTo(6);
    }

    @Test
    void testInMemoryMutableHashTable() throws IOException {
        final int NUM_KEYS = 100000;
        final int BUILD_VALS_PER_KEY = 3;
        final int PROBE_VALS_PER_KEY = 10;

        // create a build input that gives 3 million pairs with 3 values sharing the same key
        MutableObjectIterator<Record> buildInput =
                new UniformRecordGenerator(NUM_KEYS, BUILD_VALS_PER_KEY, false);

        // create a probe input that gives 10 million pairs with 10 values sharing a key
        MutableObjectIterator<Record> probeInput =
                new UniformRecordGenerator(NUM_KEYS, PROBE_VALS_PER_KEY, true);

        // allocate the memory for the HashTable
        List<MemorySegment> memSegments;
        try {
            memSegments = this.memManager.allocatePages(MEM_OWNER, 896);
        } catch (MemoryAllocationException maex) {
            fail("Memory for the Join could not be provided.");
            return;
        }

        // ----------------------------------------------------------------------------------------

        final MutableHashTable<Record, Record> join =
                new MutableHashTable<Record, Record>(
                        this.recordBuildSideAccesssor,
                        this.recordProbeSideAccesssor,
                        this.recordBuildSideComparator,
                        this.recordProbeSideComparator,
                        this.pactRecordComparator,
                        memSegments,
                        ioManager);
        join.open(buildInput, probeInput);

        final Record recordReuse = new Record();
        int numRecordsInJoinResult = 0;

        while (join.nextRecord()) {
            MutableObjectIterator<Record> buildSide = join.getBuildSideIterator();
            while (buildSide.next(recordReuse) != null) {
                numRecordsInJoinResult++;
            }
        }
        assertThat(numRecordsInJoinResult)
                .describedAs("Wrong number of records in join result.")
                .isEqualTo(NUM_KEYS * BUILD_VALS_PER_KEY * PROBE_VALS_PER_KEY);

        join.close();

        // ----------------------------------------------------------------------------------------

        this.memManager.release(join.getFreedMemory());
    }

    @Test
    void testSpillingHashJoinOneRecursionPerformance() throws IOException {
        final int NUM_KEYS = 1000000;
        final int BUILD_VALS_PER_KEY = 3;
        final int PROBE_VALS_PER_KEY = 10;

        // create a build input that gives 3 million pairs with 3 values sharing the same key
        MutableObjectIterator<Record> buildInput =
                new UniformRecordGenerator(NUM_KEYS, BUILD_VALS_PER_KEY, false);

        // create a probe input that gives 10 million pairs with 10 values sharing a key
        MutableObjectIterator<Record> probeInput =
                new UniformRecordGenerator(NUM_KEYS, PROBE_VALS_PER_KEY, true);

        // allocate the memory for the HashTable
        List<MemorySegment> memSegments;
        try {
            memSegments = this.memManager.allocatePages(MEM_OWNER, 896);
        } catch (MemoryAllocationException maex) {
            fail("Memory for the Join could not be provided.");
            return;
        }

        // ----------------------------------------------------------------------------------------

        final MutableHashTable<Record, Record> join =
                new MutableHashTable<Record, Record>(
                        this.recordBuildSideAccesssor,
                        this.recordProbeSideAccesssor,
                        this.recordBuildSideComparator,
                        this.recordProbeSideComparator,
                        this.pactRecordComparator,
                        memSegments,
                        ioManager);
        join.open(buildInput, probeInput);

        final Record recordReuse = new Record();
        int numRecordsInJoinResult = 0;

        while (join.nextRecord()) {
            MutableObjectIterator<Record> buildSide = join.getBuildSideIterator();
            while (buildSide.next(recordReuse) != null) {
                numRecordsInJoinResult++;
            }
        }
        assertThat(numRecordsInJoinResult)
                .describedAs("Wrong number of records in join result.")
                .isEqualTo(NUM_KEYS * BUILD_VALS_PER_KEY * PROBE_VALS_PER_KEY);

        join.close();

        // ----------------------------------------------------------------------------------------

        this.memManager.release(join.getFreedMemory());
    }

    @Test
    void testSpillingHashJoinOneRecursionValidity() throws IOException {
        final int NUM_KEYS = 1000000;
        final int BUILD_VALS_PER_KEY = 3;
        final int PROBE_VALS_PER_KEY = 10;

        // create a build input that gives 3 million pairs with 3 values sharing the same key
        MutableObjectIterator<Record> buildInput =
                new UniformRecordGenerator(NUM_KEYS, BUILD_VALS_PER_KEY, false);

        // create a probe input that gives 10 million pairs with 10 values sharing a key
        MutableObjectIterator<Record> probeInput =
                new UniformRecordGenerator(NUM_KEYS, PROBE_VALS_PER_KEY, true);

        // allocate the memory for the HashTable
        List<MemorySegment> memSegments;
        try {
            memSegments = this.memManager.allocatePages(MEM_OWNER, 896);
        } catch (MemoryAllocationException maex) {
            fail("Memory for the Join could not be provided.");
            return;
        }

        // create the map for validating the results
        HashMap<Integer, Long> map = new HashMap<Integer, Long>(NUM_KEYS);

        // ----------------------------------------------------------------------------------------

        final MutableHashTable<Record, Record> join =
                new MutableHashTable<Record, Record>(
                        this.recordBuildSideAccesssor,
                        this.recordProbeSideAccesssor,
                        this.recordBuildSideComparator,
                        this.recordProbeSideComparator,
                        this.pactRecordComparator,
                        memSegments,
                        ioManager);
        join.open(buildInput, probeInput);

        Record record;
        final Record recordReuse = new Record();

        while (join.nextRecord()) {
            int numBuildValues = 0;

            int key = 0;

            MutableObjectIterator<Record> buildSide = join.getBuildSideIterator();
            if ((record = buildSide.next(recordReuse)) != null) {
                numBuildValues = 1;
                key = record.getField(0, IntValue.class).getValue();
            } else {
                fail("No build side values found for a probe key.");
            }
            while ((record = buildSide.next(recordReuse)) != null) {
                numBuildValues++;
            }

            if (numBuildValues != 3) {
                fail("Other than 3 build values!!!");
            }

            Record pr = join.getCurrentProbeRecord();
            assertThat(pr.getField(0, IntValue.class).getValue())
                    .withFailMessage("Probe-side key was different than build-side key.")
                    .isEqualTo(key);

            Long contained = map.get(key);
            if (contained == null) {
                contained = Long.valueOf(numBuildValues);
            } else {
                contained = Long.valueOf(contained.longValue() + (numBuildValues));
            }

            map.put(key, contained);
        }

        join.close();

        assertThat(map).withFailMessage("Wrong number of keys").hasSize(NUM_KEYS);
        for (Map.Entry<Integer, Long> entry : map.entrySet()) {
            long val = entry.getValue();
            int key = entry.getKey();

            assertThat(val)
                    .withFailMessage(
                            "Wrong number of values in per-key cross product for key %d", key)
                    .isEqualTo(PROBE_VALS_PER_KEY * BUILD_VALS_PER_KEY);
        }

        // ----------------------------------------------------------------------------------------

        this.memManager.release(join.getFreedMemory());
    }

    @Test
    void testSpillingHashJoinWithMassiveCollisions() throws IOException {
        // the following two values are known to have a hash-code collision on the initial level.
        // we use them to make sure one partition grows over-proportionally large
        final int REPEATED_VALUE_1 = 40559;
        final int REPEATED_VALUE_2 = 92882;
        final int REPEATED_VALUE_COUNT_BUILD = 200000;
        final int REPEATED_VALUE_COUNT_PROBE = 5;

        final int NUM_KEYS = 1000000;
        final int BUILD_VALS_PER_KEY = 3;
        final int PROBE_VALS_PER_KEY = 10;

        // create a build input that gives 3 million pairs with 3 values sharing the same key, plus
        // 400k pairs with two colliding keys
        MutableObjectIterator<Record> build1 =
                new UniformRecordGenerator(NUM_KEYS, BUILD_VALS_PER_KEY, false);
        MutableObjectIterator<Record> build2 =
                new ConstantsKeyValuePairsIterator(
                        REPEATED_VALUE_1, 17, REPEATED_VALUE_COUNT_BUILD);
        MutableObjectIterator<Record> build3 =
                new ConstantsKeyValuePairsIterator(
                        REPEATED_VALUE_2, 23, REPEATED_VALUE_COUNT_BUILD);
        List<MutableObjectIterator<Record>> builds = new ArrayList<MutableObjectIterator<Record>>();
        builds.add(build1);
        builds.add(build2);
        builds.add(build3);
        MutableObjectIterator<Record> buildInput = new UnionIterator<Record>(builds);

        // create a probe input that gives 10 million pairs with 10 values sharing a key
        MutableObjectIterator<Record> probe1 =
                new UniformRecordGenerator(NUM_KEYS, PROBE_VALS_PER_KEY, true);
        MutableObjectIterator<Record> probe2 =
                new ConstantsKeyValuePairsIterator(REPEATED_VALUE_1, 17, 5);
        MutableObjectIterator<Record> probe3 =
                new ConstantsKeyValuePairsIterator(REPEATED_VALUE_2, 23, 5);
        List<MutableObjectIterator<Record>> probes = new ArrayList<MutableObjectIterator<Record>>();
        probes.add(probe1);
        probes.add(probe2);
        probes.add(probe3);
        MutableObjectIterator<Record> probeInput = new UnionIterator<Record>(probes);

        // allocate the memory for the HashTable
        List<MemorySegment> memSegments;
        try {
            memSegments = this.memManager.allocatePages(MEM_OWNER, 896);
        } catch (MemoryAllocationException maex) {
            fail("Memory for the Join could not be provided.");
            return;
        }

        // create the map for validating the results
        HashMap<Integer, Long> map = new HashMap<Integer, Long>(NUM_KEYS);

        // ----------------------------------------------------------------------------------------

        final MutableHashTable<Record, Record> join =
                new MutableHashTable<Record, Record>(
                        this.recordBuildSideAccesssor,
                        this.recordProbeSideAccesssor,
                        this.recordBuildSideComparator,
                        this.recordProbeSideComparator,
                        this.pactRecordComparator,
                        memSegments,
                        ioManager);
        join.open(buildInput, probeInput);

        Record record;
        final Record recordReuse = new Record();

        while (join.nextRecord()) {
            int numBuildValues = 0;

            final Record probeRec = join.getCurrentProbeRecord();
            int key = probeRec.getField(0, IntValue.class).getValue();

            MutableObjectIterator<Record> buildSide = join.getBuildSideIterator();
            if ((record = buildSide.next(recordReuse)) != null) {
                numBuildValues = 1;
                assertThat(record.getField(0, IntValue.class).getValue())
                        .withFailMessage("Probe-side key was different than build-side key.")
                        .isEqualTo(key);
            } else {
                fail("No build side values found for a probe key.");
            }
            while ((record = buildSide.next(recordReuse)) != null) {
                numBuildValues++;
                assertThat(record.getField(0, IntValue.class).getValue())
                        .withFailMessage("Probe-side key was different than build-side key.")
                        .isEqualTo(key);
            }

            Long contained = map.get(key);
            if (contained == null) {
                contained = Long.valueOf(numBuildValues);
            } else {
                contained = Long.valueOf(contained.longValue() + numBuildValues);
            }

            map.put(key, contained);
        }

        join.close();

        assertThat(map).withFailMessage("Wrong number of keys").hasSize(NUM_KEYS);
        for (Map.Entry<Integer, Long> entry : map.entrySet()) {
            long val = entry.getValue();
            int key = entry.getKey();

            assertThat(val)
                    .withFailMessage(
                            "Wrong number of values in per-key cross product for key %d", key)
                    .isEqualTo(
                            (key == REPEATED_VALUE_1 || key == REPEATED_VALUE_2)
                                    ? (PROBE_VALS_PER_KEY + REPEATED_VALUE_COUNT_PROBE)
                                            * (BUILD_VALS_PER_KEY + REPEATED_VALUE_COUNT_BUILD)
                                    : PROBE_VALS_PER_KEY * BUILD_VALS_PER_KEY);
        }

        // ----------------------------------------------------------------------------------------

        this.memManager.release(join.getFreedMemory());
    }

    /*
     * This test is basically identical to the "testSpillingHashJoinWithMassiveCollisions" test, only that the number
     * of repeated values (causing bucket collisions) are large enough to make sure that their target partition no longer
     * fits into memory by itself and needs to be repartitioned in the recursion again.
     */
    @Test
    void testSpillingHashJoinWithTwoRecursions() throws IOException {
        // the following two values are known to have a hash-code collision on the first recursion
        // level.
        // we use them to make sure one partition grows over-proportionally large
        final int REPEATED_VALUE_1 = 40559;
        final int REPEATED_VALUE_2 = 92882;
        final int REPEATED_VALUE_COUNT_BUILD = 200000;
        final int REPEATED_VALUE_COUNT_PROBE = 5;

        final int NUM_KEYS = 1000000;
        final int BUILD_VALS_PER_KEY = 3;
        final int PROBE_VALS_PER_KEY = 10;

        // create a build input that gives 3 million pairs with 3 values sharing the same key, plus
        // 400k pairs with two colliding keys
        MutableObjectIterator<Record> build1 =
                new UniformRecordGenerator(NUM_KEYS, BUILD_VALS_PER_KEY, false);
        MutableObjectIterator<Record> build2 =
                new ConstantsKeyValuePairsIterator(
                        REPEATED_VALUE_1, 17, REPEATED_VALUE_COUNT_BUILD);
        MutableObjectIterator<Record> build3 =
                new ConstantsKeyValuePairsIterator(
                        REPEATED_VALUE_2, 23, REPEATED_VALUE_COUNT_BUILD);
        List<MutableObjectIterator<Record>> builds = new ArrayList<MutableObjectIterator<Record>>();
        builds.add(build1);
        builds.add(build2);
        builds.add(build3);
        MutableObjectIterator<Record> buildInput = new UnionIterator<Record>(builds);

        // create a probe input that gives 10 million pairs with 10 values sharing a key
        MutableObjectIterator<Record> probe1 =
                new UniformRecordGenerator(NUM_KEYS, PROBE_VALS_PER_KEY, true);
        MutableObjectIterator<Record> probe2 =
                new ConstantsKeyValuePairsIterator(REPEATED_VALUE_1, 17, 5);
        MutableObjectIterator<Record> probe3 =
                new ConstantsKeyValuePairsIterator(REPEATED_VALUE_2, 23, 5);
        List<MutableObjectIterator<Record>> probes = new ArrayList<MutableObjectIterator<Record>>();
        probes.add(probe1);
        probes.add(probe2);
        probes.add(probe3);
        MutableObjectIterator<Record> probeInput = new UnionIterator<Record>(probes);

        // allocate the memory for the HashTable
        List<MemorySegment> memSegments;
        try {
            memSegments = this.memManager.allocatePages(MEM_OWNER, 896);
        } catch (MemoryAllocationException maex) {
            fail("Memory for the Join could not be provided.");
            return;
        }

        // create the map for validating the results
        HashMap<Integer, Long> map = new HashMap<Integer, Long>(NUM_KEYS);

        // ----------------------------------------------------------------------------------------

        final MutableHashTable<Record, Record> join =
                new MutableHashTable<Record, Record>(
                        this.recordBuildSideAccesssor,
                        this.recordProbeSideAccesssor,
                        this.recordBuildSideComparator,
                        this.recordProbeSideComparator,
                        this.pactRecordComparator,
                        memSegments,
                        ioManager);
        join.open(buildInput, probeInput);

        Record record;
        final Record recordReuse = new Record();

        while (join.nextRecord()) {
            int numBuildValues = 0;

            final Record probeRec = join.getCurrentProbeRecord();
            int key = probeRec.getField(0, IntValue.class).getValue();

            MutableObjectIterator<Record> buildSide = join.getBuildSideIterator();
            if ((record = buildSide.next(recordReuse)) != null) {
                numBuildValues = 1;
                assertThat(record.getField(0, IntValue.class).getValue())
                        .withFailMessage("Probe-side key was different than build-side key.")
                        .isEqualTo(key);
            } else {
                fail("No build side values found for a probe key.");
            }
            while ((record = buildSide.next(recordReuse)) != null) {
                numBuildValues++;
                assertThat(record.getField(0, IntValue.class).getValue())
                        .withFailMessage("Probe-side key was different than build-side key.")
                        .isEqualTo(key);
            }

            Long contained = map.get(key);
            if (contained == null) {
                contained = Long.valueOf(numBuildValues);
            } else {
                contained = Long.valueOf(contained.longValue() + numBuildValues);
            }

            map.put(key, contained);
        }

        join.close();

        assertThat(map).withFailMessage("Wrong number of keys").hasSize(NUM_KEYS);
        for (Map.Entry<Integer, Long> entry : map.entrySet()) {
            long val = entry.getValue();
            int key = entry.getKey();

            assertThat(val)
                    .withFailMessage(
                            "Wrong number of values in per-key cross product for key " + key)
                    .isEqualTo(
                            (key == REPEATED_VALUE_1 || key == REPEATED_VALUE_2)
                                    ? (PROBE_VALS_PER_KEY + REPEATED_VALUE_COUNT_PROBE)
                                            * (BUILD_VALS_PER_KEY + REPEATED_VALUE_COUNT_BUILD)
                                    : PROBE_VALS_PER_KEY * BUILD_VALS_PER_KEY);
        }

        // ----------------------------------------------------------------------------------------

        this.memManager.release(join.getFreedMemory());
    }

    /*
     * This test is basically identical to the "testSpillingHashJoinWithMassiveCollisions" test, only that the number
     * of repeated values (causing bucket collisions) are large enough to make sure that their target partition no longer
     * fits into memory by itself and needs to be repartitioned in the recursion again.
     */
    @Test
    void testFailingHashJoinTooManyRecursions() throws IOException {
        // the following two values are known to have a hash-code collision on the first recursion
        // level.
        // we use them to make sure one partition grows over-proportionally large
        final int REPEATED_VALUE_1 = 40559;
        final int REPEATED_VALUE_2 = 92882;
        final int REPEATED_VALUE_COUNT = 3000000;

        final int NUM_KEYS = 1000000;
        final int BUILD_VALS_PER_KEY = 3;
        final int PROBE_VALS_PER_KEY = 10;

        // create a build input that gives 3 million pairs with 3 values sharing the same key, plus
        // 400k pairs with two colliding keys
        MutableObjectIterator<Record> build1 =
                new UniformRecordGenerator(NUM_KEYS, BUILD_VALS_PER_KEY, false);
        MutableObjectIterator<Record> build2 =
                new ConstantsKeyValuePairsIterator(REPEATED_VALUE_1, 17, REPEATED_VALUE_COUNT);
        MutableObjectIterator<Record> build3 =
                new ConstantsKeyValuePairsIterator(REPEATED_VALUE_2, 23, REPEATED_VALUE_COUNT);
        List<MutableObjectIterator<Record>> builds = new ArrayList<MutableObjectIterator<Record>>();
        builds.add(build1);
        builds.add(build2);
        builds.add(build3);
        MutableObjectIterator<Record> buildInput = new UnionIterator<Record>(builds);

        // create a probe input that gives 10 million pairs with 10 values sharing a key
        MutableObjectIterator<Record> probe1 =
                new UniformRecordGenerator(NUM_KEYS, PROBE_VALS_PER_KEY, true);
        MutableObjectIterator<Record> probe2 =
                new ConstantsKeyValuePairsIterator(REPEATED_VALUE_1, 17, REPEATED_VALUE_COUNT);
        MutableObjectIterator<Record> probe3 =
                new ConstantsKeyValuePairsIterator(REPEATED_VALUE_2, 23, REPEATED_VALUE_COUNT);
        List<MutableObjectIterator<Record>> probes = new ArrayList<MutableObjectIterator<Record>>();
        probes.add(probe1);
        probes.add(probe2);
        probes.add(probe3);
        MutableObjectIterator<Record> probeInput = new UnionIterator<Record>(probes);

        // allocate the memory for the HashTable
        List<MemorySegment> memSegments;
        try {
            memSegments = this.memManager.allocatePages(MEM_OWNER, 896);
        } catch (MemoryAllocationException maex) {
            fail("Memory for the Join could not be provided.");
            return;
        }

        // ----------------------------------------------------------------------------------------

        final MutableHashTable<Record, Record> join =
                new MutableHashTable<Record, Record>(
                        this.recordBuildSideAccesssor,
                        this.recordProbeSideAccesssor,
                        this.recordBuildSideComparator,
                        this.recordProbeSideComparator,
                        this.pactRecordComparator,
                        memSegments,
                        ioManager);
        join.open(buildInput, probeInput);

        final Record recordReuse = new Record();

        try {
            while (join.nextRecord()) {
                MutableObjectIterator<Record> buildSide = join.getBuildSideIterator();
                if (buildSide.next(recordReuse) == null) {
                    fail("No build side values found for a probe key.");
                }
                while (buildSide.next(recordReuse) != null) ;
            }

            fail("Hash Join must have failed due to too many recursions.");
        } catch (Exception ex) {
            // expected
        }

        join.close();

        // ----------------------------------------------------------------------------------------

        this.memManager.release(join.getFreedMemory());
    }

    /*
     * Spills build records, so that probe records are also spilled. But only so
     * few probe records are used that some partitions remain empty.
     */
    @Test
    void testSparseProbeSpilling() throws IOException {
        final int NUM_BUILD_KEYS = 1000000;
        final int NUM_BUILD_VALS = 1;
        final int NUM_PROBE_KEYS = 20;
        final int NUM_PROBE_VALS = 1;

        MutableObjectIterator<Record> buildInput =
                new UniformRecordGenerator(NUM_BUILD_KEYS, NUM_BUILD_VALS, false);

        // allocate the memory for the HashTable
        List<MemorySegment> memSegments;
        try {
            memSegments = this.memManager.allocatePages(MEM_OWNER, 128);
        } catch (MemoryAllocationException maex) {
            fail("Memory for the Join could not be provided.");
            return;
        }

        final MutableHashTable<Record, Record> join =
                new MutableHashTable<Record, Record>(
                        this.recordBuildSideAccesssor,
                        this.recordProbeSideAccesssor,
                        this.recordBuildSideComparator,
                        this.recordProbeSideComparator,
                        this.pactRecordComparator,
                        memSegments,
                        ioManager);
        join.open(buildInput, new UniformRecordGenerator(NUM_PROBE_KEYS, NUM_PROBE_VALS, true));

        int expectedNumResults =
                (Math.min(NUM_PROBE_KEYS, NUM_BUILD_KEYS) * NUM_BUILD_VALS) * NUM_PROBE_VALS;

        final Record recordReuse = new Record();
        int numRecordsInJoinResult = 0;

        while (join.nextRecord()) {
            MutableObjectIterator<Record> buildSide = join.getBuildSideIterator();
            while (buildSide.next(recordReuse) != null) {
                numRecordsInJoinResult++;
            }
        }
        assertThat(numRecordsInJoinResult)
                .withFailMessage("Wrong number of records in join result.")
                .isEqualTo(expectedNumResults);

        join.close();

        this.memManager.release(join.getFreedMemory());
    }

    /*
     * Same test as {@link #testSparseProbeSpilling} but using a build-side outer join
     * that requires spilled build-side records to be returned and counted.
     */
    @Test
    void testSparseProbeSpillingWithOuterJoin() throws IOException {
        final int NUM_BUILD_KEYS = 1000000;
        final int NUM_BUILD_VALS = 1;
        final int NUM_PROBE_KEYS = 20;
        final int NUM_PROBE_VALS = 1;

        MutableObjectIterator<Record> buildInput =
                new UniformRecordGenerator(NUM_BUILD_KEYS, NUM_BUILD_VALS, false);

        // allocate the memory for the HashTable
        List<MemorySegment> memSegments;
        try {
            memSegments = this.memManager.allocatePages(MEM_OWNER, 96);
        } catch (MemoryAllocationException maex) {
            fail("Memory for the Join could not be provided.");
            return;
        }

        final MutableHashTable<Record, Record> join =
                new MutableHashTable<Record, Record>(
                        this.recordBuildSideAccesssor,
                        this.recordProbeSideAccesssor,
                        this.recordBuildSideComparator,
                        this.recordProbeSideComparator,
                        this.pactRecordComparator,
                        memSegments,
                        ioManager);
        join.open(
                buildInput, new UniformRecordGenerator(NUM_PROBE_KEYS, NUM_PROBE_VALS, true), true);

        int expectedNumResults =
                (Math.max(NUM_PROBE_KEYS, NUM_BUILD_KEYS) * NUM_BUILD_VALS) * NUM_PROBE_VALS;

        final Record recordReuse = new Record();
        int numRecordsInJoinResult = 0;

        while (join.nextRecord()) {
            MutableObjectIterator<Record> buildSide = join.getBuildSideIterator();
            while (buildSide.next(recordReuse) != null) {
                numRecordsInJoinResult++;
            }
        }
        assertThat(numRecordsInJoinResult)
                .withFailMessage("Wrong number of records in join result.")
                .isEqualTo(expectedNumResults);

        join.close();

        this.memManager.release(join.getFreedMemory());
    }

    /*
     * This test validates a bug fix against former memory loss in the case where a partition was spilled
     * during an insert into the same.
     */
    @Test
    void validateSpillingDuringInsertion() throws IOException {
        final int NUM_BUILD_KEYS = 500000;
        final int NUM_BUILD_VALS = 1;
        final int NUM_PROBE_KEYS = 10;
        final int NUM_PROBE_VALS = 1;

        MutableObjectIterator<Record> buildInput =
                new UniformRecordGenerator(NUM_BUILD_KEYS, NUM_BUILD_VALS, false);

        // allocate the memory for the HashTable
        List<MemorySegment> memSegments;
        try {
            memSegments = this.memManager.allocatePages(MEM_OWNER, 85);
        } catch (MemoryAllocationException maex) {
            fail("Memory for the Join could not be provided.");
            return;
        }

        final MutableHashTable<Record, Record> join =
                new MutableHashTable<Record, Record>(
                        this.recordBuildSideAccesssor,
                        this.recordProbeSideAccesssor,
                        this.recordBuildSideComparator,
                        this.recordProbeSideComparator,
                        this.pactRecordComparator,
                        memSegments,
                        ioManager);
        join.open(buildInput, new UniformRecordGenerator(NUM_PROBE_KEYS, NUM_PROBE_VALS, true));

        final Record recordReuse = new Record();
        int numRecordsInJoinResult = 0;

        int expectedNumResults =
                (Math.min(NUM_PROBE_KEYS, NUM_BUILD_KEYS) * NUM_BUILD_VALS) * NUM_PROBE_VALS;

        while (join.nextRecord()) {
            MutableObjectIterator<Record> buildSide = join.getBuildSideIterator();
            while (buildSide.next(recordReuse) != null) {
                numRecordsInJoinResult++;
            }
        }
        assertThat(numRecordsInJoinResult)
                .withFailMessage("Wrong number of records in join result.")
                .isEqualTo(expectedNumResults);

        join.close();

        this.memManager.release(join.getFreedMemory());
    }

    // ============================================================================================
    //                                 Integer Pairs based Tests
    // ============================================================================================

    @Test
    void testInMemoryMutableHashTableIntPair() throws IOException {
        final int NUM_KEYS = 100000;
        final int BUILD_VALS_PER_KEY = 3;
        final int PROBE_VALS_PER_KEY = 10;

        // create a build input that gives 3 million pairs with 3 values sharing the same key
        MutableObjectIterator<IntPair> buildInput =
                new UniformIntPairGenerator(NUM_KEYS, BUILD_VALS_PER_KEY, false);

        // create a probe input that gives 10 million pairs with 10 values sharing a key
        MutableObjectIterator<IntPair> probeInput =
                new UniformIntPairGenerator(NUM_KEYS, PROBE_VALS_PER_KEY, true);

        // allocate the memory for the HashTable
        List<MemorySegment> memSegments;
        try {
            memSegments = this.memManager.allocatePages(MEM_OWNER, 896);
        } catch (MemoryAllocationException maex) {
            fail("Memory for the Join could not be provided.");
            return;
        }

        // ----------------------------------------------------------------------------------------

        final MutableHashTable<IntPair, IntPair> join =
                new MutableHashTable<IntPair, IntPair>(
                        this.pairBuildSideAccesssor,
                        this.pairProbeSideAccesssor,
                        this.pairBuildSideComparator,
                        this.pairProbeSideComparator,
                        this.pairComparator,
                        memSegments,
                        ioManager);
        join.open(buildInput, probeInput);

        final IntPair recordReuse = new IntPair();
        int numRecordsInJoinResult = 0;

        while (join.nextRecord()) {
            MutableObjectIterator<IntPair> buildSide = join.getBuildSideIterator();
            while (buildSide.next(recordReuse) != null) {
                numRecordsInJoinResult++;
            }
        }
        assertThat(numRecordsInJoinResult)
                .withFailMessage("Wrong number of records in join result.")
                .isEqualTo(NUM_KEYS * BUILD_VALS_PER_KEY * PROBE_VALS_PER_KEY);

        join.close();

        // ----------------------------------------------------------------------------------------

        this.memManager.release(join.getFreedMemory());
    }

    @Test
    void testSpillingHashJoinOneRecursionPerformanceIntPair() throws IOException {
        final int NUM_KEYS = 1000000;
        final int BUILD_VALS_PER_KEY = 3;
        final int PROBE_VALS_PER_KEY = 10;

        // create a build input that gives 3 million pairs with 3 values sharing the same key
        MutableObjectIterator<IntPair> buildInput =
                new UniformIntPairGenerator(NUM_KEYS, BUILD_VALS_PER_KEY, false);

        // create a probe input that gives 10 million pairs with 10 values sharing a key
        MutableObjectIterator<IntPair> probeInput =
                new UniformIntPairGenerator(NUM_KEYS, PROBE_VALS_PER_KEY, true);

        // allocate the memory for the HashTable
        List<MemorySegment> memSegments;
        try {
            memSegments = this.memManager.allocatePages(MEM_OWNER, 896);
        } catch (MemoryAllocationException maex) {
            fail("Memory for the Join could not be provided.");
            return;
        }

        // ----------------------------------------------------------------------------------------

        final MutableHashTable<IntPair, IntPair> join =
                new MutableHashTable<IntPair, IntPair>(
                        this.pairBuildSideAccesssor,
                        this.pairProbeSideAccesssor,
                        this.pairBuildSideComparator,
                        this.pairProbeSideComparator,
                        this.pairComparator,
                        memSegments,
                        ioManager);
        join.open(buildInput, probeInput);

        final IntPair recordReuse = new IntPair();
        int numRecordsInJoinResult = 0;

        while (join.nextRecord()) {
            MutableObjectIterator<IntPair> buildSide = join.getBuildSideIterator();
            while (buildSide.next(recordReuse) != null) {
                numRecordsInJoinResult++;
            }
        }
        assertThat(numRecordsInJoinResult)
                .withFailMessage("Wrong number of records in join result.")
                .isEqualTo(NUM_KEYS * BUILD_VALS_PER_KEY * PROBE_VALS_PER_KEY);

        join.close();

        // ----------------------------------------------------------------------------------------

        this.memManager.release(join.getFreedMemory());
    }

    @Test
    void testSpillingHashJoinOneRecursionValidityIntPair() throws IOException {
        final int NUM_KEYS = 1000000;
        final int BUILD_VALS_PER_KEY = 3;
        final int PROBE_VALS_PER_KEY = 10;

        // create a build input that gives 3 million pairs with 3 values sharing the same key
        MutableObjectIterator<IntPair> buildInput =
                new UniformIntPairGenerator(NUM_KEYS, BUILD_VALS_PER_KEY, false);

        // create a probe input that gives 10 million pairs with 10 values sharing a key
        MutableObjectIterator<IntPair> probeInput =
                new UniformIntPairGenerator(NUM_KEYS, PROBE_VALS_PER_KEY, true);

        // allocate the memory for the HashTable
        List<MemorySegment> memSegments;
        try {
            memSegments = this.memManager.allocatePages(MEM_OWNER, 896);
        } catch (MemoryAllocationException maex) {
            fail("Memory for the Join could not be provided.");
            return;
        }

        // create the map for validating the results
        HashMap<Integer, Long> map = new HashMap<Integer, Long>(NUM_KEYS);

        // ----------------------------------------------------------------------------------------

        final MutableHashTable<IntPair, IntPair> join =
                new MutableHashTable<IntPair, IntPair>(
                        this.pairBuildSideAccesssor,
                        this.pairProbeSideAccesssor,
                        this.pairBuildSideComparator,
                        this.pairProbeSideComparator,
                        this.pairComparator,
                        memSegments,
                        ioManager);
        join.open(buildInput, probeInput);

        IntPair record;
        final IntPair recordReuse = new IntPair();

        while (join.nextRecord()) {
            int numBuildValues = 0;

            int key = 0;

            MutableObjectIterator<IntPair> buildSide = join.getBuildSideIterator();
            if ((record = buildSide.next(recordReuse)) != null) {
                numBuildValues = 1;
                key = record.getKey();
            } else {
                fail("No build side values found for a probe key.");
            }
            while ((record = buildSide.next(recordReuse)) != null) {
                numBuildValues++;
            }

            if (numBuildValues != 3) {
                fail("Other than 3 build values!!!");
            }

            IntPair pr = join.getCurrentProbeRecord();
            assertThat(pr.getKey())
                    .withFailMessage("Probe-side key was different than build-side key.")
                    .isEqualTo(key);

            Long contained = map.get(key);
            if (contained == null) {
                contained = Long.valueOf(numBuildValues);
            } else {
                contained = Long.valueOf(contained.longValue() + (numBuildValues));
            }

            map.put(key, contained);
        }

        join.close();

        assertThat(map).withFailMessage("Wrong number of keys").hasSize(NUM_KEYS);
        for (Map.Entry<Integer, Long> entry : map.entrySet()) {
            long val = entry.getValue();
            int key = entry.getKey();

            assertThat(val)
                    .withFailMessage(
                            "Wrong number of values in per-key cross product for key %d", key)
                    .isEqualTo(PROBE_VALS_PER_KEY * BUILD_VALS_PER_KEY);
        }

        // ----------------------------------------------------------------------------------------

        this.memManager.release(join.getFreedMemory());
    }

    @Test
    void testSpillingHashJoinWithMassiveCollisionsIntPair() throws IOException {
        // the following two values are known to have a hash-code collision on the initial level.
        // we use them to make sure one partition grows over-proportionally large
        final int REPEATED_VALUE_1 = 40559;
        final int REPEATED_VALUE_2 = 92882;
        final int REPEATED_VALUE_COUNT_BUILD = 200000;
        final int REPEATED_VALUE_COUNT_PROBE = 5;

        final int NUM_KEYS = 1000000;
        final int BUILD_VALS_PER_KEY = 3;
        final int PROBE_VALS_PER_KEY = 10;

        // create a build input that gives 3 million pairs with 3 values sharing the same key, plus
        // 400k pairs with two colliding keys
        MutableObjectIterator<IntPair> build1 =
                new UniformIntPairGenerator(NUM_KEYS, BUILD_VALS_PER_KEY, false);
        MutableObjectIterator<IntPair> build2 =
                new ConstantsIntPairsIterator(REPEATED_VALUE_1, 17, REPEATED_VALUE_COUNT_BUILD);
        MutableObjectIterator<IntPair> build3 =
                new ConstantsIntPairsIterator(REPEATED_VALUE_2, 23, REPEATED_VALUE_COUNT_BUILD);
        List<MutableObjectIterator<IntPair>> builds =
                new ArrayList<MutableObjectIterator<IntPair>>();
        builds.add(build1);
        builds.add(build2);
        builds.add(build3);
        MutableObjectIterator<IntPair> buildInput = new UnionIterator<IntPair>(builds);

        // create a probe input that gives 10 million pairs with 10 values sharing a key
        MutableObjectIterator<IntPair> probe1 =
                new UniformIntPairGenerator(NUM_KEYS, PROBE_VALS_PER_KEY, true);
        MutableObjectIterator<IntPair> probe2 =
                new ConstantsIntPairsIterator(REPEATED_VALUE_1, 17, 5);
        MutableObjectIterator<IntPair> probe3 =
                new ConstantsIntPairsIterator(REPEATED_VALUE_2, 23, 5);
        List<MutableObjectIterator<IntPair>> probes =
                new ArrayList<MutableObjectIterator<IntPair>>();
        probes.add(probe1);
        probes.add(probe2);
        probes.add(probe3);
        MutableObjectIterator<IntPair> probeInput = new UnionIterator<IntPair>(probes);

        // allocate the memory for the HashTable
        List<MemorySegment> memSegments;
        try {
            memSegments = this.memManager.allocatePages(MEM_OWNER, 896);
        } catch (MemoryAllocationException maex) {
            fail("Memory for the Join could not be provided.");
            return;
        }

        // create the map for validating the results
        HashMap<Integer, Long> map = new HashMap<Integer, Long>(NUM_KEYS);

        // ----------------------------------------------------------------------------------------

        final MutableHashTable<IntPair, IntPair> join =
                new MutableHashTable<IntPair, IntPair>(
                        this.pairBuildSideAccesssor,
                        this.pairProbeSideAccesssor,
                        this.pairBuildSideComparator,
                        this.pairProbeSideComparator,
                        this.pairComparator,
                        memSegments,
                        ioManager);
        join.open(buildInput, probeInput);

        IntPair record;
        final IntPair recordReuse = new IntPair();

        while (join.nextRecord()) {
            int numBuildValues = 0;

            final IntPair probeRec = join.getCurrentProbeRecord();
            int key = probeRec.getKey();

            MutableObjectIterator<IntPair> buildSide = join.getBuildSideIterator();
            if ((record = buildSide.next(recordReuse)) != null) {
                numBuildValues = 1;
                assertThat(record.getKey())
                        .withFailMessage("Probe-side key was different than build-side key.")
                        .isEqualTo(key);
            } else {
                fail("No build side values found for a probe key.");
            }
            while ((record = buildSide.next(recordReuse)) != null) {
                numBuildValues++;
                assertThat(record.getKey())
                        .withFailMessage("Probe-side key was different than build-side key.")
                        .isEqualTo(key);
            }

            Long contained = map.get(key);
            if (contained == null) {
                contained = Long.valueOf(numBuildValues);
            } else {
                contained = Long.valueOf(contained.longValue() + numBuildValues);
            }

            map.put(key, contained);
        }

        join.close();

        assertThat(map).withFailMessage("Wrong number of keys").hasSize(NUM_KEYS);
        for (Map.Entry<Integer, Long> entry : map.entrySet()) {
            long val = entry.getValue();
            int key = entry.getKey();

            assertThat(val)
                    .withFailMessage(
                            "Wrong number of values in per-key cross product for key %d", key)
                    .isEqualTo(
                            (key == REPEATED_VALUE_1 || key == REPEATED_VALUE_2)
                                    ? (PROBE_VALS_PER_KEY + REPEATED_VALUE_COUNT_PROBE)
                                            * (BUILD_VALS_PER_KEY + REPEATED_VALUE_COUNT_BUILD)
                                    : PROBE_VALS_PER_KEY * BUILD_VALS_PER_KEY);
        }

        // ----------------------------------------------------------------------------------------

        this.memManager.release(join.getFreedMemory());
    }

    /*
     * This test is basically identical to the "testSpillingHashJoinWithMassiveCollisions" test, only that the number
     * of repeated values (causing bucket collisions) are large enough to make sure that their target partition no longer
     * fits into memory by itself and needs to be repartitioned in the recursion again.
     */
    @Test
    void testSpillingHashJoinWithTwoRecursionsIntPair() throws IOException {
        // the following two values are known to have a hash-code collision on the first recursion
        // level.
        // we use them to make sure one partition grows over-proportionally large
        final int REPEATED_VALUE_1 = 40559;
        final int REPEATED_VALUE_2 = 92882;
        final int REPEATED_VALUE_COUNT_BUILD = 200000;
        final int REPEATED_VALUE_COUNT_PROBE = 5;

        final int NUM_KEYS = 1000000;
        final int BUILD_VALS_PER_KEY = 3;
        final int PROBE_VALS_PER_KEY = 10;

        // create a build input that gives 3 million pairs with 3 values sharing the same key, plus
        // 400k pairs with two colliding keys
        MutableObjectIterator<IntPair> build1 =
                new UniformIntPairGenerator(NUM_KEYS, BUILD_VALS_PER_KEY, false);
        MutableObjectIterator<IntPair> build2 =
                new ConstantsIntPairsIterator(REPEATED_VALUE_1, 17, REPEATED_VALUE_COUNT_BUILD);
        MutableObjectIterator<IntPair> build3 =
                new ConstantsIntPairsIterator(REPEATED_VALUE_2, 23, REPEATED_VALUE_COUNT_BUILD);
        List<MutableObjectIterator<IntPair>> builds =
                new ArrayList<MutableObjectIterator<IntPair>>();
        builds.add(build1);
        builds.add(build2);
        builds.add(build3);
        MutableObjectIterator<IntPair> buildInput = new UnionIterator<IntPair>(builds);

        // create a probe input that gives 10 million pairs with 10 values sharing a key
        MutableObjectIterator<IntPair> probe1 =
                new UniformIntPairGenerator(NUM_KEYS, PROBE_VALS_PER_KEY, true);
        MutableObjectIterator<IntPair> probe2 =
                new ConstantsIntPairsIterator(REPEATED_VALUE_1, 17, 5);
        MutableObjectIterator<IntPair> probe3 =
                new ConstantsIntPairsIterator(REPEATED_VALUE_2, 23, 5);
        List<MutableObjectIterator<IntPair>> probes =
                new ArrayList<MutableObjectIterator<IntPair>>();
        probes.add(probe1);
        probes.add(probe2);
        probes.add(probe3);
        MutableObjectIterator<IntPair> probeInput = new UnionIterator<IntPair>(probes);

        // allocate the memory for the HashTable
        List<MemorySegment> memSegments;
        try {
            memSegments = this.memManager.allocatePages(MEM_OWNER, 896);
        } catch (MemoryAllocationException maex) {
            fail("Memory for the Join could not be provided.");
            return;
        }

        // create the map for validating the results
        HashMap<Integer, Long> map = new HashMap<Integer, Long>(NUM_KEYS);

        // ----------------------------------------------------------------------------------------

        final MutableHashTable<IntPair, IntPair> join =
                new MutableHashTable<IntPair, IntPair>(
                        this.pairBuildSideAccesssor,
                        this.pairProbeSideAccesssor,
                        this.pairBuildSideComparator,
                        this.pairProbeSideComparator,
                        this.pairComparator,
                        memSegments,
                        ioManager);
        join.open(buildInput, probeInput);

        IntPair record;
        final IntPair recordReuse = new IntPair();

        while (join.nextRecord()) {
            int numBuildValues = 0;

            final IntPair probeRec = join.getCurrentProbeRecord();
            int key = probeRec.getKey();

            MutableObjectIterator<IntPair> buildSide = join.getBuildSideIterator();
            if ((record = buildSide.next(recordReuse)) != null) {
                numBuildValues = 1;
                assertThat(record.getKey())
                        .withFailMessage("Probe-side key was different than build-side key.")
                        .isEqualTo(key);
            } else {
                fail("No build side values found for a probe key.");
            }
            while ((record = buildSide.next(recordReuse)) != null) {
                numBuildValues++;
                assertThat(record.getKey())
                        .withFailMessage("Probe-side key was different than build-side key.")
                        .isEqualTo(key);
            }

            Long contained = map.get(key);
            if (contained == null) {
                contained = Long.valueOf(numBuildValues);
            } else {
                contained = Long.valueOf(contained.longValue() + numBuildValues);
            }

            map.put(key, contained);
        }

        join.close();

        assertThat(map).withFailMessage("Wrong number of keys").hasSize(NUM_KEYS);
        for (Map.Entry<Integer, Long> entry : map.entrySet()) {
            long val = entry.getValue();
            int key = entry.getKey();

            assertThat(val)
                    .withFailMessage(
                            "Wrong number of values in per-key cross product for key %d", key)
                    .isEqualTo(
                            (key == REPEATED_VALUE_1 || key == REPEATED_VALUE_2)
                                    ? (PROBE_VALS_PER_KEY + REPEATED_VALUE_COUNT_PROBE)
                                            * (BUILD_VALS_PER_KEY + REPEATED_VALUE_COUNT_BUILD)
                                    : PROBE_VALS_PER_KEY * BUILD_VALS_PER_KEY);
        }

        // ----------------------------------------------------------------------------------------

        this.memManager.release(join.getFreedMemory());
    }

    /*
     * This test is basically identical to the "testSpillingHashJoinWithMassiveCollisions" test, only that the number
     * of repeated values (causing bucket collisions) are large enough to make sure that their target partition no longer
     * fits into memory by itself and needs to be repartitioned in the recursion again.
     */
    @Test
    void testFailingHashJoinTooManyRecursionsIntPair() throws IOException {
        // the following two values are known to have a hash-code collision on the first recursion
        // level.
        // we use them to make sure one partition grows over-proportionally large
        final int REPEATED_VALUE_1 = 40559;
        final int REPEATED_VALUE_2 = 92882;
        final int REPEATED_VALUE_COUNT = 3000000;

        final int NUM_KEYS = 1000000;
        final int BUILD_VALS_PER_KEY = 3;
        final int PROBE_VALS_PER_KEY = 10;

        // create a build input that gives 3 million pairs with 3 values sharing the same key, plus
        // 400k pairs with two colliding keys
        MutableObjectIterator<IntPair> build1 =
                new UniformIntPairGenerator(NUM_KEYS, BUILD_VALS_PER_KEY, false);
        MutableObjectIterator<IntPair> build2 =
                new ConstantsIntPairsIterator(REPEATED_VALUE_1, 17, REPEATED_VALUE_COUNT);
        MutableObjectIterator<IntPair> build3 =
                new ConstantsIntPairsIterator(REPEATED_VALUE_2, 23, REPEATED_VALUE_COUNT);
        List<MutableObjectIterator<IntPair>> builds =
                new ArrayList<MutableObjectIterator<IntPair>>();
        builds.add(build1);
        builds.add(build2);
        builds.add(build3);
        MutableObjectIterator<IntPair> buildInput = new UnionIterator<IntPair>(builds);

        // create a probe input that gives 10 million pairs with 10 values sharing a key
        MutableObjectIterator<IntPair> probe1 =
                new UniformIntPairGenerator(NUM_KEYS, PROBE_VALS_PER_KEY, true);
        MutableObjectIterator<IntPair> probe2 =
                new ConstantsIntPairsIterator(REPEATED_VALUE_1, 17, REPEATED_VALUE_COUNT);
        MutableObjectIterator<IntPair> probe3 =
                new ConstantsIntPairsIterator(REPEATED_VALUE_2, 23, REPEATED_VALUE_COUNT);
        List<MutableObjectIterator<IntPair>> probes =
                new ArrayList<MutableObjectIterator<IntPair>>();
        probes.add(probe1);
        probes.add(probe2);
        probes.add(probe3);
        MutableObjectIterator<IntPair> probeInput = new UnionIterator<IntPair>(probes);

        // allocate the memory for the HashTable
        List<MemorySegment> memSegments;
        try {
            memSegments = this.memManager.allocatePages(MEM_OWNER, 896);
        } catch (MemoryAllocationException maex) {
            fail("Memory for the Join could not be provided.");
            return;
        }

        // ----------------------------------------------------------------------------------------

        final MutableHashTable<IntPair, IntPair> join =
                new MutableHashTable<IntPair, IntPair>(
                        this.pairBuildSideAccesssor,
                        this.pairProbeSideAccesssor,
                        this.pairBuildSideComparator,
                        this.pairProbeSideComparator,
                        this.pairComparator,
                        memSegments,
                        ioManager);
        join.open(buildInput, probeInput);

        final IntPair recordReuse = new IntPair();

        try {
            while (join.nextRecord()) {
                MutableObjectIterator<IntPair> buildSide = join.getBuildSideIterator();
                if (buildSide.next(recordReuse) == null) {
                    fail("No build side values found for a probe key.");
                }
                while (buildSide.next(recordReuse) != null) ;
            }

            fail("Hash Join must have failed due to too many recursions.");
        } catch (Exception ex) {
            // expected
        }

        join.close();

        // ----------------------------------------------------------------------------------------

        this.memManager.release(join.getFreedMemory());
    }

    /*
     * Spills build records, so that probe records are also spilled. But only so
     * few probe records are used that some partitions remain empty.
     */
    @Test
    void testSparseProbeSpillingIntPair() throws IOException {
        final int NUM_BUILD_KEYS = 1000000;
        final int NUM_BUILD_VALS = 1;
        final int NUM_PROBE_KEYS = 20;
        final int NUM_PROBE_VALS = 1;

        MutableObjectIterator<IntPair> buildInput =
                new UniformIntPairGenerator(NUM_BUILD_KEYS, NUM_BUILD_VALS, false);

        // allocate the memory for the HashTable
        List<MemorySegment> memSegments;
        try {
            memSegments = this.memManager.allocatePages(MEM_OWNER, 128);
        } catch (MemoryAllocationException maex) {
            fail("Memory for the Join could not be provided.");
            return;
        }

        final MutableHashTable<IntPair, IntPair> join =
                new MutableHashTable<IntPair, IntPair>(
                        this.pairBuildSideAccesssor,
                        this.pairProbeSideAccesssor,
                        this.pairBuildSideComparator,
                        this.pairProbeSideComparator,
                        this.pairComparator,
                        memSegments,
                        ioManager);
        join.open(buildInput, new UniformIntPairGenerator(NUM_PROBE_KEYS, NUM_PROBE_VALS, true));

        int expectedNumResults =
                (Math.min(NUM_PROBE_KEYS, NUM_BUILD_KEYS) * NUM_BUILD_VALS) * NUM_PROBE_VALS;

        final IntPair recordReuse = new IntPair();
        int numRecordsInJoinResult = 0;

        while (join.nextRecord()) {
            MutableObjectIterator<IntPair> buildSide = join.getBuildSideIterator();
            while (buildSide.next(recordReuse) != null) {
                numRecordsInJoinResult++;
            }
        }
        assertThat(numRecordsInJoinResult)
                .withFailMessage("Wrong number of records in join result.")
                .isEqualTo(expectedNumResults);

        join.close();

        this.memManager.release(join.getFreedMemory());
    }

    /*
     * This test validates a bug fix against former memory loss in the case where a partition was spilled
     * during an insert into the same.
     */
    @Test
    void validateSpillingDuringInsertionIntPair() throws IOException {
        final int NUM_BUILD_KEYS = 500000;
        final int NUM_BUILD_VALS = 1;
        final int NUM_PROBE_KEYS = 10;
        final int NUM_PROBE_VALS = 1;

        MutableObjectIterator<IntPair> buildInput =
                new UniformIntPairGenerator(NUM_BUILD_KEYS, NUM_BUILD_VALS, false);

        // allocate the memory for the HashTable
        List<MemorySegment> memSegments;
        try {
            memSegments = this.memManager.allocatePages(MEM_OWNER, 85);
        } catch (MemoryAllocationException maex) {
            fail("Memory for the Join could not be provided.");
            return;
        }

        final MutableHashTable<IntPair, IntPair> join =
                new MutableHashTable<IntPair, IntPair>(
                        this.pairBuildSideAccesssor,
                        this.pairProbeSideAccesssor,
                        this.pairBuildSideComparator,
                        this.pairProbeSideComparator,
                        this.pairComparator,
                        memSegments,
                        ioManager);
        join.open(buildInput, new UniformIntPairGenerator(NUM_PROBE_KEYS, NUM_PROBE_VALS, true));

        final IntPair recordReuse = new IntPair();
        int numRecordsInJoinResult = 0;

        int expectedNumResults =
                (Math.min(NUM_PROBE_KEYS, NUM_BUILD_KEYS) * NUM_BUILD_VALS) * NUM_PROBE_VALS;

        while (join.nextRecord()) {
            MutableObjectIterator<IntPair> buildSide = join.getBuildSideIterator();
            while (buildSide.next(recordReuse) != null) {
                numRecordsInJoinResult++;
            }
        }
        assertThat(numRecordsInJoinResult)
                .withFailMessage("Wrong number of records in join result.")
                .isEqualTo(expectedNumResults);

        join.close();

        this.memManager.release(join.getFreedMemory());
    }

    @Test
    void testInMemoryReOpen() throws IOException {
        final int NUM_KEYS = 100000;
        final int BUILD_VALS_PER_KEY = 3;
        final int PROBE_VALS_PER_KEY = 10;

        // create a build input that gives 3 million pairs with 3 values sharing the same key
        MutableObjectIterator<IntPair> buildInput =
                new UniformIntPairGenerator(NUM_KEYS, BUILD_VALS_PER_KEY, false);

        // create a probe input that gives 10 million pairs with 10 values sharing a key
        MutableObjectIterator<IntPair> probeInput =
                new UniformIntPairGenerator(NUM_KEYS, PROBE_VALS_PER_KEY, true);

        // allocate the memory for the HashTable
        List<MemorySegment> memSegments;
        try {
            memSegments = this.memManager.allocatePages(MEM_OWNER, 896);
        } catch (MemoryAllocationException maex) {
            fail("Memory for the Join could not be provided.");
            return;
        }

        // ----------------------------------------------------------------------------------------

        final MutableHashTable<IntPair, IntPair> join =
                new MutableHashTable<IntPair, IntPair>(
                        this.pairBuildSideAccesssor,
                        this.pairProbeSideAccesssor,
                        this.pairBuildSideComparator,
                        this.pairProbeSideComparator,
                        this.pairComparator,
                        memSegments,
                        ioManager);
        join.open(buildInput, probeInput);

        final IntPair recordReuse = new IntPair();
        int numRecordsInJoinResult = 0;

        while (join.nextRecord()) {
            MutableObjectIterator<IntPair> buildSide = join.getBuildSideIterator();
            while (buildSide.next(recordReuse) != null) {
                numRecordsInJoinResult++;
            }
        }
        assertThat(numRecordsInJoinResult)
                .withFailMessage("Wrong number of records in join result.")
                .isEqualTo(NUM_KEYS * BUILD_VALS_PER_KEY * PROBE_VALS_PER_KEY);

        join.close();

        // ----------------------------------------------------------------------------------------
        // recreate the inputs

        // create a build input that gives 3 million pairs with 3 values sharing the same key
        buildInput = new UniformIntPairGenerator(NUM_KEYS, BUILD_VALS_PER_KEY, false);

        // create a probe input that gives 10 million pairs with 10 values sharing a key
        probeInput = new UniformIntPairGenerator(NUM_KEYS, PROBE_VALS_PER_KEY, true);

        join.open(buildInput, probeInput);

        numRecordsInJoinResult = 0;

        while (join.nextRecord()) {
            MutableObjectIterator<IntPair> buildSide = join.getBuildSideIterator();
            while (buildSide.next(recordReuse) != null) {
                numRecordsInJoinResult++;
            }
        }
        assertThat(numRecordsInJoinResult)
                .withFailMessage("Wrong number of records in join result.")
                .isEqualTo(NUM_KEYS * BUILD_VALS_PER_KEY * PROBE_VALS_PER_KEY);

        join.close();

        // ----------------------------------------------------------------------------------------

        this.memManager.release(join.getFreedMemory());
    }

    /*
     * This test is same as `testInMemoryReOpen()` but only number of keys and pages are different. This test
     * validates a bug fix MutableHashTable memory leakage with small memory segments.
     */
    @Test
    void testInMemoryReOpenWithSmallMemory() throws Exception {
        final int NUM_KEYS = 10000;
        final int BUILD_VALS_PER_KEY = 3;
        final int PROBE_VALS_PER_KEY = 10;

        // create a build input that gives 30000 pairs with 3 values sharing the same key
        MutableObjectIterator<IntPair> buildInput =
                new UniformIntPairGenerator(NUM_KEYS, BUILD_VALS_PER_KEY, false);

        // create a probe input that gives 100000 pairs with 10 values sharing a key
        MutableObjectIterator<IntPair> probeInput =
                new UniformIntPairGenerator(NUM_KEYS, PROBE_VALS_PER_KEY, true);

        // allocate the memory for the HashTable
        List<MemorySegment> memSegments;
        try {
            // 33 is minimum number of pages required to perform hash join this inputs
            memSegments = this.memManager.allocatePages(MEM_OWNER, 33);
        } catch (MemoryAllocationException maex) {
            fail("Memory for the Join could not be provided.");
            return;
        }

        // ----------------------------------------------------------------------------------------

        final MutableHashTable<IntPair, IntPair> join =
                new MutableHashTable<IntPair, IntPair>(
                        this.pairBuildSideAccesssor,
                        this.pairProbeSideAccesssor,
                        this.pairBuildSideComparator,
                        this.pairProbeSideComparator,
                        this.pairComparator,
                        memSegments,
                        ioManager);
        join.open(buildInput, probeInput);

        final IntPair recordReuse = new IntPair();
        int numRecordsInJoinResult = 0;

        while (join.nextRecord()) {
            MutableObjectIterator<IntPair> buildSide = join.getBuildSideIterator();
            while (buildSide.next(recordReuse) != null) {
                numRecordsInJoinResult++;
            }
        }
        assertThat(numRecordsInJoinResult)
                .withFailMessage("Wrong number of records in join result.")
                .isEqualTo(NUM_KEYS * BUILD_VALS_PER_KEY * PROBE_VALS_PER_KEY);

        join.close();

        // ----------------------------------------------------------------------------------------
        // recreate the inputs

        // create a build input that gives 30000 pairs with 3 values sharing the same key
        buildInput = new UniformIntPairGenerator(NUM_KEYS, BUILD_VALS_PER_KEY, false);

        // create a probe input that gives 100000 pairs with 10 values sharing a key
        probeInput = new UniformIntPairGenerator(NUM_KEYS, PROBE_VALS_PER_KEY, true);

        join.open(buildInput, probeInput);

        numRecordsInJoinResult = 0;

        while (join.nextRecord()) {
            MutableObjectIterator<IntPair> buildSide = join.getBuildSideIterator();
            while (buildSide.next(recordReuse) != null) {
                numRecordsInJoinResult++;
            }
        }
        assertThat(numRecordsInJoinResult)
                .withFailMessage("Wrong number of records in join result.")
                .isEqualTo(NUM_KEYS * BUILD_VALS_PER_KEY * PROBE_VALS_PER_KEY);

        join.close();

        // ----------------------------------------------------------------------------------------

        this.memManager.release(join.getFreedMemory());
    }

    @Test
    void testBucketsNotFulfillSegment() throws Exception {
        final int NUM_KEYS = 10000;
        final int BUILD_VALS_PER_KEY = 3;
        final int PROBE_VALS_PER_KEY = 10;

        // create a build input that gives 30000 pairs with 3 values sharing the same key
        MutableObjectIterator<IntPair> buildInput =
                new UniformIntPairGenerator(NUM_KEYS, BUILD_VALS_PER_KEY, false);

        // create a probe input that gives 100000 pairs with 10 values sharing a key
        MutableObjectIterator<IntPair> probeInput =
                new UniformIntPairGenerator(NUM_KEYS, PROBE_VALS_PER_KEY, true);

        // allocate the memory for the HashTable
        List<MemorySegment> memSegments;
        try {
            // 33 is minimum number of pages required to perform hash join this inputs
            memSegments = this.memManager.allocatePages(MEM_OWNER, 33);
        } catch (MemoryAllocationException maex) {
            fail("Memory for the Join could not be provided.");
            return;
        }

        // For FLINK-2545, the buckets data may not fulfill it's buffer, for example, the buffer may
        // contains 256 buckets,
        // while hash table only assign 250 bucket on it. The unused buffer bytes may contains
        // arbitrary data, which may
        // influence hash table if forget to skip it. To mock this, put the invalid bucket
        // data(partition=1, inMemory=true, count=-1)
        // at the end of buffer.
        for (MemorySegment segment : memSegments) {
            int newBucketOffset = segment.size() - 128;
            // initialize the header fields
            segment.put(newBucketOffset + 0, (byte) 0);
            segment.put(newBucketOffset + 1, (byte) 0);
            segment.putShort(newBucketOffset + 2, (short) -1);
            segment.putLong(newBucketOffset + 4, ~0x0L);
        }

        // ----------------------------------------------------------------------------------------

        final MutableHashTable<IntPair, IntPair> join =
                new MutableHashTable<IntPair, IntPair>(
                        this.pairBuildSideAccesssor,
                        this.pairProbeSideAccesssor,
                        this.pairBuildSideComparator,
                        this.pairProbeSideComparator,
                        this.pairComparator,
                        memSegments,
                        ioManager);
        join.open(buildInput, probeInput);

        final IntPair recordReuse = new IntPair();
        int numRecordsInJoinResult = 0;

        while (join.nextRecord()) {
            MutableObjectIterator<IntPair> buildSide = join.getBuildSideIterator();
            while (buildSide.next(recordReuse) != null) {
                numRecordsInJoinResult++;
            }
        }
        assertThat(numRecordsInJoinResult)
                .withFailMessage("Wrong number of records in join result.")
                .isEqualTo(NUM_KEYS * BUILD_VALS_PER_KEY * PROBE_VALS_PER_KEY);

        join.close();
        this.memManager.release(join.getFreedMemory());
    }

    @Test
    void testHashWithBuildSideOuterJoin1() throws Exception {
        final int NUM_KEYS = 20000;
        final int BUILD_VALS_PER_KEY = 1;
        final int PROBE_VALS_PER_KEY = 1;

        // create a build input that gives 40000 pairs with 1 values sharing the same key
        MutableObjectIterator<IntPair> buildInput =
                new UniformIntPairGenerator(2 * NUM_KEYS, BUILD_VALS_PER_KEY, false);

        // create a probe input that gives 20000 pairs with 1 values sharing a key
        MutableObjectIterator<IntPair> probeInput =
                new UniformIntPairGenerator(NUM_KEYS, PROBE_VALS_PER_KEY, true);

        // allocate the memory for the HashTable
        List<MemorySegment> memSegments;
        try {
            // 33 is minimum number of pages required to perform hash join this inputs
            memSegments = this.memManager.allocatePages(MEM_OWNER, 33);
        } catch (MemoryAllocationException maex) {
            fail("Memory for the Join could not be provided.");
            return;
        }

        // ----------------------------------------------------------------------------------------

        final MutableHashTable<IntPair, IntPair> join =
                new MutableHashTable<IntPair, IntPair>(
                        this.pairBuildSideAccesssor,
                        this.pairProbeSideAccesssor,
                        this.pairBuildSideComparator,
                        this.pairProbeSideComparator,
                        this.pairComparator,
                        memSegments,
                        ioManager);
        join.open(buildInput, probeInput, true);

        final IntPair recordReuse = new IntPair();
        int numRecordsInJoinResult = 0;

        while (join.nextRecord()) {
            MutableObjectIterator<IntPair> buildSide = join.getBuildSideIterator();
            while (buildSide.next(recordReuse) != null) {
                numRecordsInJoinResult++;
            }
        }
        assertThat(numRecordsInJoinResult)
                .withFailMessage("Wrong number of records in join result.")
                .isEqualTo(2 * NUM_KEYS * BUILD_VALS_PER_KEY * PROBE_VALS_PER_KEY);

        join.close();
        this.memManager.release(join.getFreedMemory());
    }

    @Test
    void testHashWithBuildSideOuterJoin2() throws Exception {
        final int NUM_KEYS = 40000;
        final int BUILD_VALS_PER_KEY = 2;
        final int PROBE_VALS_PER_KEY = 1;

        // The keys of probe and build sides are overlapped, so there would be none unmatched build
        // elements
        // after probe phase, make sure build side outer join works well in this case.

        // create a build input that gives 80000 pairs with 2 values sharing the same key
        MutableObjectIterator<IntPair> buildInput =
                new UniformIntPairGenerator(NUM_KEYS, BUILD_VALS_PER_KEY, false);

        // create a probe input that gives 40000 pairs with 1 values sharing a key
        MutableObjectIterator<IntPair> probeInput =
                new UniformIntPairGenerator(NUM_KEYS, PROBE_VALS_PER_KEY, true);

        // allocate the memory for the HashTable
        List<MemorySegment> memSegments;
        try {
            // 33 is minimum number of pages required to perform hash join this inputs
            memSegments = this.memManager.allocatePages(MEM_OWNER, 33);
        } catch (MemoryAllocationException maex) {
            fail("Memory for the Join could not be provided.");
            return;
        }

        // ----------------------------------------------------------------------------------------

        final MutableHashTable<IntPair, IntPair> join =
                new MutableHashTable<IntPair, IntPair>(
                        this.pairBuildSideAccesssor,
                        this.pairProbeSideAccesssor,
                        this.pairBuildSideComparator,
                        this.pairProbeSideComparator,
                        this.pairComparator,
                        memSegments,
                        ioManager);
        join.open(buildInput, probeInput, true);

        final IntPair recordReuse = new IntPair();
        int numRecordsInJoinResult = 0;

        while (join.nextRecord()) {
            MutableObjectIterator<IntPair> buildSide = join.getBuildSideIterator();
            IntPair next = buildSide.next(recordReuse);
            if (next == null && join.getCurrentProbeRecord() == null) {
                fail("Should not return join result that both probe and build element are null.");
            }
            while (next != null) {
                numRecordsInJoinResult++;
                next = buildSide.next(recordReuse);
            }
        }
        assertThat(numRecordsInJoinResult)
                .withFailMessage("Wrong number of records in join result.")
                .isEqualTo(NUM_KEYS * BUILD_VALS_PER_KEY * PROBE_VALS_PER_KEY);

        join.close();
        this.memManager.release(join.getFreedMemory());
    }

    // ============================================================================================

    /**
     * An iterator that returns the Key/Value pairs with identical value a given number of times.
     */
    public static final class ConstantsKeyValuePairsIterator
            implements MutableObjectIterator<Record> {

        private final IntValue key;
        private final IntValue value;

        private int numLeft;

        public ConstantsKeyValuePairsIterator(int key, int value, int count) {
            this.key = new IntValue(key);
            this.value = new IntValue(value);
            this.numLeft = count;
        }

        @Override
        public Record next(Record reuse) {
            if (this.numLeft > 0) {
                this.numLeft--;
                reuse.clear();
                reuse.setField(0, this.key);
                reuse.setField(1, this.value);
                return reuse;
            } else {
                return null;
            }
        }

        @Override
        public Record next() {
            return next(new Record(2));
        }
    }

    // ============================================================================================

    /**
     * An iterator that returns the Key/Value pairs with identical value a given number of times.
     */
    private static final class ConstantsIntPairsIterator implements MutableObjectIterator<IntPair> {

        private final int key;
        private final int value;

        private int numLeft;

        public ConstantsIntPairsIterator(int key, int value, int count) {
            this.key = key;
            this.value = value;
            this.numLeft = count;
        }

        @Override
        public IntPair next(IntPair reuse) {
            if (this.numLeft > 0) {
                this.numLeft--;
                reuse.setKey(this.key);
                reuse.setValue(this.value);
                return reuse;
            } else {
                return null;
            }
        }

        @Override
        public IntPair next() {
            return next(new IntPair());
        }
    }

    // ============================================================================================

    public static final class RecordPairComparatorFirstInt
            extends TypePairComparator<Record, Record> {

        private int key;

        @Override
        public void setReference(Record reference) {
            try {
                this.key = reference.getField(0, IntValue.class).getValue();
            } catch (NullPointerException npex) {
                throw new NullKeyFieldException();
            }
        }

        @Override
        public boolean equalToReference(Record candidate) {
            try {
                return this.key == candidate.getField(0, IntValue.class).getValue();
            } catch (NullPointerException npex) {
                throw new NullKeyFieldException();
            }
        }

        @Override
        public int compareToReference(Record candidate) {
            try {
                final int i = candidate.getField(0, IntValue.class).getValue();
                return i - this.key;
            } catch (NullPointerException npex) {
                throw new NullKeyFieldException();
            }
        }
    }
}
