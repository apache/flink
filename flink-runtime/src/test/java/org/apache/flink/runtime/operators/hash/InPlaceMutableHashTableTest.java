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

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.util.CopyingListCollector;
import org.apache.flink.api.common.typeutils.SameTypePairComparator;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypePairComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongComparator;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.TupleComparator;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.operators.testutils.UniformStringPairGenerator;
import org.apache.flink.runtime.operators.testutils.types.IntPair;
import org.apache.flink.runtime.operators.testutils.types.IntPairComparator;
import org.apache.flink.runtime.operators.testutils.types.IntPairSerializer;
import org.apache.flink.runtime.operators.testutils.types.StringPair;
import org.apache.flink.runtime.operators.testutils.types.StringPairComparator;
import org.apache.flink.runtime.operators.testutils.types.StringPairSerializer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;

import org.apache.flink.shaded.guava31.com.google.common.collect.Ordering;

import org.junit.jupiter.api.Test;

import java.io.EOFException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

class InPlaceMutableHashTableTest extends MutableHashTableTestBase {

    private static final long RANDOM_SEED = 58723953465322L;

    private static final int PAGE_SIZE = 16 * 1024;

    private final TypeSerializer<Tuple2<Long, String>> serializer;
    private final TypeComparator<Tuple2<Long, String>> comparator;

    private final TypeComparator<Long> probeComparator;

    private final TypePairComparator<Long, Tuple2<Long, String>> pairComparator;

    @Override
    protected <T> AbstractMutableHashTable<T> getHashTable(
            TypeSerializer<T> serializer,
            TypeComparator<T> comparator,
            List<MemorySegment> memory) {
        return new InPlaceMutableHashTable<>(serializer, comparator, memory);
    }

    // ------------------ Note: This part was mostly copied from CompactingHashTableTest
    // ------------------

    public InPlaceMutableHashTableTest() {
        TypeSerializer<?>[] fieldSerializers = {LongSerializer.INSTANCE, StringSerializer.INSTANCE};
        @SuppressWarnings("unchecked")
        Class<Tuple2<Long, String>> clazz = (Class<Tuple2<Long, String>>) (Class<?>) Tuple2.class;
        this.serializer = new TupleSerializer<Tuple2<Long, String>>(clazz, fieldSerializers);

        TypeComparator<?>[] comparators = {new LongComparator(true)};
        TypeSerializer<?>[] comparatorSerializers = {LongSerializer.INSTANCE};

        this.comparator =
                new TupleComparator<Tuple2<Long, String>>(
                        new int[] {0}, comparators, comparatorSerializers);

        this.probeComparator = new LongComparator(true);

        this.pairComparator =
                new TypePairComparator<Long, Tuple2<Long, String>>() {

                    private long ref;

                    @Override
                    public void setReference(Long reference) {
                        ref = reference;
                    }

                    @Override
                    public boolean equalToReference(Tuple2<Long, String> candidate) {
                        //noinspection UnnecessaryUnboxing
                        return candidate.f0.longValue() == ref;
                    }

                    @Override
                    public int compareToReference(Tuple2<Long, String> candidate) {
                        long x = ref;
                        long y = candidate.f0;
                        return (x < y) ? -1 : ((x == y) ? 0 : 1);
                    }
                };
    }

    /**
     * This has to be duplicated in InPlaceMutableHashTableTest and CompactingHashTableTest because
     * of the different constructor calls.
     */
    @Test
    void testHashTableGrowthWithInsert() {
        try {
            final int numElements = 1000000;

            List<MemorySegment> memory = getMemory(10000, 32 * 1024);

            InPlaceMutableHashTable<Tuple2<Long, String>> table =
                    new InPlaceMutableHashTable<Tuple2<Long, String>>(
                            serializer, comparator, memory);
            table.open();

            for (long i = 0; i < numElements; i++) {
                table.insert(new Tuple2<Long, String>(i, String.valueOf(i)));
            }

            // make sure that all elements are contained via the entry iterator
            {
                BitSet bitSet = new BitSet(numElements);
                MutableObjectIterator<Tuple2<Long, String>> iter = table.getEntryIterator();
                Tuple2<Long, String> next;
                while ((next = iter.next()) != null) {
                    assertThat(next.f1).isNotNull();
                    assertThat(next.f0).isNotNull().isEqualTo(Long.parseLong(next.f1));

                    bitSet.set(next.f0.intValue());
                }

                assertThat(bitSet.cardinality()).isEqualTo(numElements);
            }

            // make sure all entries are contained via the prober
            {
                InPlaceMutableHashTable<Tuple2<Long, String>>.HashTableProber<Long> proper =
                        table.getProber(probeComparator, pairComparator);

                Tuple2<Long, String> reuse = new Tuple2<>();

                for (long i = 0; i < numElements; i++) {
                    assertThat(proper.getMatchFor(i, reuse)).isNotNull();
                    assertThat(proper.getMatchFor(i + numElements, reuse)).isNull();
                }
            }

            table.close();
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    /**
     * This test validates that records are not lost via "insertOrReplace()" as in bug [FLINK-2361]
     *
     * <p>This has to be duplicated in InPlaceMutableHashTableTest and CompactingHashTableTest
     * because of the different constructor calls.
     */
    @Test
    void testHashTableGrowthWithInsertOrReplace() {
        try {
            final int numElements = 1000000;

            List<MemorySegment> memory = getMemory(1000, 32 * 1024);

            InPlaceMutableHashTable<Tuple2<Long, String>> table =
                    new InPlaceMutableHashTable<Tuple2<Long, String>>(
                            serializer, comparator, memory);
            table.open();

            for (long i = 0; i < numElements; i++) {
                table.insertOrReplaceRecord(Tuple2.of(i, String.valueOf(i)));
            }

            // make sure that all elements are contained via the entry iterator
            {
                BitSet bitSet = new BitSet(numElements);
                MutableObjectIterator<Tuple2<Long, String>> iter = table.getEntryIterator();
                Tuple2<Long, String> next;
                while ((next = iter.next()) != null) {
                    assertThat(next.f1).isNotNull();
                    assertThat(next.f0).isNotNull().isEqualTo(Long.parseLong(next.f1));

                    bitSet.set(next.f0.intValue());
                }

                assertThat(bitSet.cardinality()).isEqualTo(numElements);
            }

            // make sure all entries are contained via the prober
            {
                InPlaceMutableHashTable<Tuple2<Long, String>>.HashTableProber<Long> proper =
                        table.getProber(probeComparator, pairComparator);

                Tuple2<Long, String> reuse = new Tuple2<>();

                for (long i = 0; i < numElements; i++) {
                    assertThat(proper.getMatchFor(i, reuse)).isNotNull();
                    assertThat(proper.getMatchFor(i + numElements, reuse)).isNull();
                }
            }

            table.close();
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    // ------------------ The following are the InPlaceMutableHashTable-specific tests
    // ------------------

    private class InPlaceMutableHashTableWithJavaHashMap<T, K> {

        TypeSerializer<T> serializer;

        TypeComparator<T> comparator;

        ReduceFunction<T> reducer;

        Collector<T> outputCollector;

        HashMap<K, T> map = new HashMap<>();

        public InPlaceMutableHashTableWithJavaHashMap(
                TypeSerializer<T> serializer,
                TypeComparator<T> comparator,
                ReduceFunction<T> reducer,
                Collector<T> outputCollector) {
            this.serializer = serializer;
            this.comparator = comparator;
            this.reducer = reducer;
            this.outputCollector = outputCollector;
        }

        public void updateTableEntryWithReduce(T record, K key) throws Exception {
            record = serializer.copy(record);

            if (!map.containsKey(key)) {
                map.put(key, record);
            } else {
                T x = map.get(key);
                x = reducer.reduce(x, record);
                map.put(key, x);
            }
        }

        public void emitAndReset() {
            for (T record : map.values()) {
                outputCollector.collect(record);
            }
            map.clear();
        }
    }

    @Test
    void testWithIntPair() throws Exception {
        Random rnd = new Random(RANDOM_SEED);

        // varying the keyRange between 1000 and 1000000 can make a 5x speed difference
        // (because of cache misses (also in the segment arrays))
        final int keyRange = 1000000;
        final int valueRange = 10;
        final int numRecords = 1000000;

        final IntPairSerializer serializer = new IntPairSerializer();
        final TypeComparator<IntPair> comparator = new IntPairComparator();
        final ReduceFunction<IntPair> reducer = new SumReducer();

        // Create the InPlaceMutableHashTableWithJavaHashMap, which will provide the correct output.
        List<IntPair> expectedOutput = new ArrayList<>();
        InPlaceMutableHashTableWithJavaHashMap<IntPair, Integer> reference =
                new InPlaceMutableHashTableWithJavaHashMap<>(
                        serializer,
                        comparator,
                        reducer,
                        new CopyingListCollector<>(expectedOutput, serializer));

        // Create the InPlaceMutableHashTable to test
        final int numMemPages =
                keyRange * 32
                        / PAGE_SIZE; // memory use is proportional to the number of different keys
        List<IntPair> actualOutput = new ArrayList<>();

        InPlaceMutableHashTable<IntPair> table =
                new InPlaceMutableHashTable<>(
                        serializer, comparator, getMemory(numMemPages, PAGE_SIZE));
        InPlaceMutableHashTable<IntPair>.ReduceFacade reduceFacade =
                table
                .new ReduceFacade(
                        reducer, new CopyingListCollector<>(actualOutput, serializer), true);
        table.open();

        // Generate some input
        final List<IntPair> input = new ArrayList<>();
        for (int i = 0; i < numRecords; i++) {
            input.add(new IntPair(rnd.nextInt(keyRange), rnd.nextInt(valueRange)));
        }

        // System.out.println("start");
        // long start = System.currentTimeMillis();

        // Process the generated input
        final int numIntermingledEmits = 5;
        for (IntPair record : input) {
            reduceFacade.updateTableEntryWithReduce(serializer.copy(record));
            reference.updateTableEntryWithReduce(serializer.copy(record), record.getKey());
            if (rnd.nextDouble() < 1.0 / ((double) numRecords / numIntermingledEmits)) {
                // this will fire approx. numIntermingledEmits times
                reference.emitAndReset();
                reduceFacade.emitAndReset();
            }
        }
        reference.emitAndReset();
        reduceFacade.emit();
        table.close();

        // long end = System.currentTimeMillis();
        // System.out.println("stop, time: " + (end - start));

        // Check results

        assertThat(actualOutput).hasSameSizeAs(expectedOutput);

        Integer[] expectedValues = new Integer[expectedOutput.size()];
        for (int i = 0; i < expectedOutput.size(); i++) {
            expectedValues[i] = expectedOutput.get(i).getValue();
        }
        Integer[] actualValues = new Integer[actualOutput.size()];
        for (int i = 0; i < actualOutput.size(); i++) {
            actualValues[i] = actualOutput.get(i).getValue();
        }

        Arrays.sort(expectedValues, Ordering.<Integer>natural());
        Arrays.sort(actualValues, Ordering.<Integer>natural());
        assertThat(actualValues).isEqualTo(expectedValues);
    }

    class SumReducer implements ReduceFunction<IntPair> {
        @Override
        public IntPair reduce(IntPair a, IntPair b) throws Exception {
            if (a.getKey() != b.getKey()) {
                throw new RuntimeException(
                        "SumReducer was called with two records that have differing keys.");
            }
            a.setValue(a.getValue() + b.getValue());
            return a;
        }
    }

    @Test
    void testWithLengthChangingReduceFunction() throws Exception {
        Random rnd = new Random(RANDOM_SEED);

        final int numKeys = 10000;
        final int numVals = 10;
        final int numRecords = numKeys * numVals;

        StringPairSerializer serializer = new StringPairSerializer();
        StringPairComparator comparator = new StringPairComparator();
        ReduceFunction<StringPair> reducer = new ConcatReducer();

        // Create the InPlaceMutableHashTableWithJavaHashMap, which will provide the correct output.
        List<StringPair> expectedOutput = new ArrayList<>();
        InPlaceMutableHashTableWithJavaHashMap<StringPair, String> reference =
                new InPlaceMutableHashTableWithJavaHashMap<>(
                        serializer,
                        comparator,
                        reducer,
                        new CopyingListCollector<>(expectedOutput, serializer));

        // Create the InPlaceMutableHashTable to test
        final int numMemPages = numRecords * 10 / PAGE_SIZE;

        List<StringPair> actualOutput = new ArrayList<>();

        InPlaceMutableHashTable<StringPair> table =
                new InPlaceMutableHashTable<>(
                        serializer, comparator, getMemory(numMemPages, PAGE_SIZE));
        InPlaceMutableHashTable<StringPair>.ReduceFacade reduceFacade =
                table
                .new ReduceFacade(
                        reducer, new CopyingListCollector<>(actualOutput, serializer), true);

        // The loop is for checking the feature that multiple open / close are possible.
        for (int j = 0; j < 3; j++) {
            table.open();

            // Test emit when table is empty
            reduceFacade.emit();

            // Process some manual stuff
            reference.updateTableEntryWithReduce(
                    serializer.copy(new StringPair("foo", "bar")), "foo");
            reference.updateTableEntryWithReduce(
                    serializer.copy(new StringPair("foo", "baz")), "foo");
            reference.updateTableEntryWithReduce(
                    serializer.copy(new StringPair("alma", "xyz")), "alma");
            reduceFacade.updateTableEntryWithReduce(serializer.copy(new StringPair("foo", "bar")));
            reduceFacade.updateTableEntryWithReduce(serializer.copy(new StringPair("foo", "baz")));
            reduceFacade.updateTableEntryWithReduce(serializer.copy(new StringPair("alma", "xyz")));
            for (int i = 0; i < 5; i++) {
                reduceFacade.updateTableEntryWithReduce(
                        serializer.copy(new StringPair("korte", "abc")));
                reference.updateTableEntryWithReduce(
                        serializer.copy(new StringPair("korte", "abc")), "korte");
            }
            reference.emitAndReset();
            reduceFacade.emitAndReset();

            // Generate some input
            UniformStringPairGenerator gen = new UniformStringPairGenerator(numKeys, numVals, true);
            List<StringPair> input = new ArrayList<>();
            StringPair cur = new StringPair();
            while (gen.next(cur) != null) {
                input.add(serializer.copy(cur));
            }
            Collections.shuffle(input, rnd);

            // Process the generated input
            final int numIntermingledEmits = 5;
            for (StringPair record : input) {
                reference.updateTableEntryWithReduce(serializer.copy(record), record.getKey());
                reduceFacade.updateTableEntryWithReduce(serializer.copy(record));
                if (rnd.nextDouble() < 1.0 / ((double) numRecords / numIntermingledEmits)) {
                    // this will fire approx. numIntermingledEmits times
                    reference.emitAndReset();
                    reduceFacade.emitAndReset();
                }
            }
            reference.emitAndReset();
            reduceFacade.emit();
            table.close();

            // Check results

            assertThat(actualOutput).hasSameSizeAs(expectedOutput);

            String[] expectedValues = new String[expectedOutput.size()];
            for (int i = 0; i < expectedOutput.size(); i++) {
                expectedValues[i] = expectedOutput.get(i).getValue();
            }
            String[] actualValues = new String[actualOutput.size()];
            for (int i = 0; i < actualOutput.size(); i++) {
                actualValues[i] = actualOutput.get(i).getValue();
            }

            Arrays.sort(expectedValues, Ordering.<String>natural());
            Arrays.sort(actualValues, Ordering.<String>natural());
            assertThat(actualValues).isEqualTo(expectedValues);

            expectedOutput.clear();
            actualOutput.clear();
        }
    }

    // Warning: Generally, reduce wouldn't give deterministic results with non-commutative
    // ReduceFunction,
    // but InPlaceMutableHashTable and ReduceHashTableWithJavaHashMap calls it in the same order.
    class ConcatReducer implements ReduceFunction<StringPair> {
        @Override
        public StringPair reduce(StringPair a, StringPair b) throws Exception {
            if (a.getKey().compareTo(b.getKey()) != 0) {
                throw new RuntimeException(
                        "ConcatReducer was called with two records that have differing keys.");
            }
            return new StringPair(a.getKey(), a.getValue().concat(b.getValue()));
        }
    }

    private static List<MemorySegment> getMemory(int numPages, int pageSize) {
        List<MemorySegment> memory = new ArrayList<>();

        for (int i = 0; i < numPages; i++) {
            memory.add(MemorySegmentFactory.allocateUnpooledSegment(pageSize));
        }

        return memory;
    }

    /**
     * The records are larger than one segment. Additionally, there is just barely enough memory, so
     * lots of compactions will happen.
     */
    @Test
    void testLargeRecordsWithManyCompactions() {
        try {
            final int numElements = 1000;

            final String longString1 = getLongString(100000), longString2 = getLongString(110000);
            List<MemorySegment> memory = getMemory(3800, 32 * 1024);

            InPlaceMutableHashTable<Tuple2<Long, String>> table =
                    new InPlaceMutableHashTable<>(serializer, comparator, memory);
            table.open();

            // first, we insert some elements
            for (long i = 0; i < numElements; i++) {
                table.insertOrReplaceRecord(Tuple2.of(i, longString1));
            }

            // now, we replace the same elements with larger ones, causing fragmentation
            for (long i = 0; i < numElements; i++) {
                table.insertOrReplaceRecord(Tuple2.of(i, longString2));
            }

            // check the results
            InPlaceMutableHashTable<Tuple2<Long, String>>.HashTableProber<Tuple2<Long, String>>
                    prober = table.getProber(comparator, new SameTypePairComparator<>(comparator));
            Tuple2<Long, String> reuse = new Tuple2<>();
            for (long i = 0; i < numElements; i++) {
                assertThat(prober.getMatchFor(Tuple2.of(i, longString1), reuse)).isNotNull();
            }

            table.close();
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    private static String getLongString(int length) {
        StringBuilder bld = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            bld.append('a');
        }
        return bld.toString();
    }

    @Test
    void testOutOfMemory() {
        List<MemorySegment> memory = getMemory(100, 1024);

        InPlaceMutableHashTable<Tuple2<Long, String>> table =
                new InPlaceMutableHashTable<>(serializer, comparator, memory);

        final int numElements = 100000;

        table.open();

        // Insert too many elements
        assertThatThrownBy(
                        () -> {
                            for (long i = 0; i < numElements; i++) {
                                table.insertOrReplaceRecord(Tuple2.of(i, "alma"));
                            }
                        })
                .isInstanceOf(EOFException.class);

        table.close();
    }
}
