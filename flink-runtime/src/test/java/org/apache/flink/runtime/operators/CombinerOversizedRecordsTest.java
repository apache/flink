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

package org.apache.flink.runtime.operators;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.DoubleSerializer;
import org.apache.flink.api.common.typeutils.base.IntComparator;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.runtime.TupleComparator;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.runtime.operators.testutils.UnaryOperatorTestBase;
import org.apache.flink.runtime.operators.testutils.UniformIntTupleGenerator;
import org.apache.flink.runtime.operators.testutils.UnionIterator;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;

import org.junit.jupiter.api.TestTemplate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Test that checks how the combiner handles very large records that are too large to be written
 * into a fresh sort buffer.
 */
class CombinerOversizedRecordsTest
        extends UnaryOperatorTestBase<
                GroupCombineFunction<
                        Tuple3<Integer, Integer, String>, Tuple3<Integer, Double, String>>,
                Tuple3<Integer, Integer, String>,
                Tuple3<Integer, Double, String>> {

    private static final long COMBINE_MEM = 3 * 1024 * 1024;

    private final double combine_frac;

    private final ArrayList<Tuple3<Integer, Double, String>> outList =
            new ArrayList<Tuple3<Integer, Double, String>>();

    private final TypeSerializer<Tuple3<Integer, Integer, String>> serializer =
            new TupleSerializer<Tuple3<Integer, Integer, String>>(
                    (Class<Tuple3<Integer, Integer, String>>) (Class<?>) Tuple3.class,
                    new TypeSerializer<?>[] {
                        IntSerializer.INSTANCE, IntSerializer.INSTANCE, StringSerializer.INSTANCE
                    });

    private final TypeSerializer<Tuple3<Integer, Double, String>> outSerializer =
            new TupleSerializer<Tuple3<Integer, Double, String>>(
                    (Class<Tuple3<Integer, Double, String>>) (Class<?>) Tuple3.class,
                    new TypeSerializer<?>[] {
                        IntSerializer.INSTANCE, DoubleSerializer.INSTANCE, StringSerializer.INSTANCE
                    });

    private final TypeComparator<Tuple3<Integer, Integer, String>> comparator =
            new TupleComparator<Tuple3<Integer, Integer, String>>(
                    new int[] {0},
                    new TypeComparator<?>[] {new IntComparator(true)},
                    new TypeSerializer<?>[] {IntSerializer.INSTANCE});

    // ------------------------------------------------------------------------

    CombinerOversizedRecordsTest(ExecutionConfig config) {
        super(config, COMBINE_MEM, 0);
        combine_frac = (double) COMBINE_MEM / getMemoryManager().getMemorySize();
    }

    @TestTemplate
    void testOversizedRecordCombineTask() {
        try {
            final int keyCnt = 100;
            final int valCnt = 20;

            // create a long heavy string payload
            StringBuilder bld = new StringBuilder(10 * 1024 * 1024);
            Random rnd = new Random();

            for (int i = 0; i < 10000000; i++) {
                bld.append((char) (rnd.nextInt(26) + 'a'));
            }

            String longString = bld.toString();
            bld = null;

            // construct the input as a union of
            // 1) long string
            // 2) some random values
            // 3) long string
            // 4) random values
            // 5) long string

            // random values 1
            MutableObjectIterator<Tuple2<Integer, Integer>> gen1 =
                    new UniformIntTupleGenerator(keyCnt, valCnt, false);

            // random values 2
            MutableObjectIterator<Tuple2<Integer, Integer>> gen2 =
                    new UniformIntTupleGenerator(keyCnt, valCnt, false);

            @SuppressWarnings("unchecked")
            MutableObjectIterator<Tuple3<Integer, Integer, String>> input =
                    new UnionIterator<Tuple3<Integer, Integer, String>>(
                            new SingleValueIterator<Tuple3<Integer, Integer, String>>(
                                    new Tuple3<Integer, Integer, String>(-1, -1, longString)),
                            new StringIteratorDecorator(gen1),
                            new SingleValueIterator<Tuple3<Integer, Integer, String>>(
                                    new Tuple3<Integer, Integer, String>(-1, -1, longString)),
                            new StringIteratorDecorator(gen2),
                            new SingleValueIterator<Tuple3<Integer, Integer, String>>(
                                    new Tuple3<Integer, Integer, String>(-1, -1, longString)));

            setInput(input, serializer);
            addDriverComparator(this.comparator);
            addDriverComparator(this.comparator);
            setOutput(this.outList, this.outSerializer);

            getTaskConfig().setDriverStrategy(DriverStrategy.SORTED_GROUP_COMBINE);
            getTaskConfig().setRelativeMemoryDriver(combine_frac);
            getTaskConfig().setFilehandlesDriver(2);

            GroupReduceCombineDriver<
                            Tuple3<Integer, Integer, String>, Tuple3<Integer, Double, String>>
                    testTask =
                            new GroupReduceCombineDriver<
                                    Tuple3<Integer, Integer, String>,
                                    Tuple3<Integer, Double, String>>();

            testDriver(testTask, TestCombiner.class);

            assertThat(testTask.getOversizedRecordCount()).isEqualTo(3);
            assertThat(keyCnt + 3 == outList.size() || 2 * keyCnt + 3 == outList.size()).isTrue();
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    // ------------------------------------------------------------------------

    public static final class TestCombiner
            implements GroupCombineFunction<
                    Tuple3<Integer, Integer, String>, Tuple3<Integer, Double, String>> {
        private static final long serialVersionUID = 1L;

        @Override
        public void combine(
                Iterable<Tuple3<Integer, Integer, String>> values,
                Collector<Tuple3<Integer, Double, String>> out) {
            int key = 0;
            int sum = 0;
            String someString = null;

            for (Tuple3<Integer, Integer, String> next : values) {
                key = next.f0;
                sum += next.f1;
                someString = next.f2;
            }

            out.collect(new Tuple3<Integer, Double, String>(key, (double) sum, someString));
        }
    }

    // ------------------------------------------------------------------------

    private static class StringIteratorDecorator
            implements MutableObjectIterator<Tuple3<Integer, Integer, String>> {

        private final MutableObjectIterator<Tuple2<Integer, Integer>> input;

        private StringIteratorDecorator(MutableObjectIterator<Tuple2<Integer, Integer>> input) {
            this.input = input;
        }

        @Override
        public Tuple3<Integer, Integer, String> next(Tuple3<Integer, Integer, String> reuse)
                throws IOException {
            Tuple2<Integer, Integer> next = input.next();
            if (next == null) {
                return null;
            } else {
                reuse.f0 = next.f0;
                reuse.f1 = next.f1;
                reuse.f2 = "test string";
                return reuse;
            }
        }

        @Override
        public Tuple3<Integer, Integer, String> next() throws IOException {
            Tuple2<Integer, Integer> next = input.next();
            if (next == null) {
                return null;
            } else {
                return new Tuple3<Integer, Integer, String>(next.f0, next.f1, "test string");
            }
        }
    }

    // ------------------------------------------------------------------------

    private static class SingleValueIterator<T> implements MutableObjectIterator<T> {

        private final T value;

        private boolean pending = true;

        private SingleValueIterator(T value) {
            this.value = value;
        }

        @Override
        public T next(T reuse) {
            return next();
        }

        @Override
        public T next() {
            if (pending) {
                pending = false;
                return value;
            } else {
                return null;
            }
        }
    }
}
