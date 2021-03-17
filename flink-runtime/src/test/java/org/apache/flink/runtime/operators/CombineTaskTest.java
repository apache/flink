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
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntComparator;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.TupleComparator;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.runtime.operators.testutils.DelayingIterator;
import org.apache.flink.runtime.operators.testutils.DiscardingOutputCollector;
import org.apache.flink.runtime.operators.testutils.ExpectedTestException;
import org.apache.flink.runtime.operators.testutils.InfiniteIntTupleIterator;
import org.apache.flink.runtime.operators.testutils.UnaryOperatorTestBase;
import org.apache.flink.runtime.operators.testutils.UniformIntTupleGenerator;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;

import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.*;

public class CombineTaskTest
        extends UnaryOperatorTestBase<
                RichGroupReduceFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>,
                Tuple2<Integer, Integer>,
                Tuple2<Integer, Integer>> {

    private static final long COMBINE_MEM = 3 * 1024 * 1024;

    private final double combine_frac;

    private final ArrayList<Tuple2<Integer, Integer>> outList = new ArrayList<>();

    @SuppressWarnings("unchecked")
    private final TypeSerializer<Tuple2<Integer, Integer>> serializer =
            new TupleSerializer<>(
                    (Class<Tuple2<Integer, Integer>>) (Class<?>) Tuple2.class,
                    new TypeSerializer<?>[] {IntSerializer.INSTANCE, IntSerializer.INSTANCE});

    private final TypeComparator<Tuple2<Integer, Integer>> comparator =
            new TupleComparator<>(
                    new int[] {0},
                    new TypeComparator<?>[] {new IntComparator(true)},
                    new TypeSerializer<?>[] {IntSerializer.INSTANCE});

    public CombineTaskTest(ExecutionConfig config) {
        super(config, COMBINE_MEM, 0);

        combine_frac = (double) COMBINE_MEM / this.getMemoryManager().getMemorySize();
    }

    @Test
    public void testCombineTask() {
        try {
            int keyCnt = 100;
            int valCnt = 20;

            setInput(new UniformIntTupleGenerator(keyCnt, valCnt, false), serializer);
            addDriverComparator(this.comparator);
            addDriverComparator(this.comparator);
            setOutput(this.outList, serializer);

            getTaskConfig().setDriverStrategy(DriverStrategy.SORTED_GROUP_COMBINE);
            getTaskConfig().setRelativeMemoryDriver(combine_frac);
            getTaskConfig().setFilehandlesDriver(2);

            final GroupReduceCombineDriver<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>
                    testTask = new GroupReduceCombineDriver<>();

            testDriver(testTask, MockCombiningReduceStub.class);

            int expSum = 0;
            for (int i = 1; i < valCnt; i++) {
                expSum += i;
            }

            assertTrue(this.outList.size() == keyCnt);

            for (Tuple2<Integer, Integer> record : this.outList) {
                assertTrue(record.f1 == expSum);
            }

            this.outList.clear();
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testFailingCombineTask() {
        try {
            int keyCnt = 100;
            int valCnt = 20;

            setInput(new UniformIntTupleGenerator(keyCnt, valCnt, false), serializer);
            addDriverComparator(this.comparator);
            addDriverComparator(this.comparator);
            setOutput(new DiscardingOutputCollector<Tuple2<Integer, Integer>>());

            getTaskConfig().setDriverStrategy(DriverStrategy.SORTED_GROUP_COMBINE);
            getTaskConfig().setRelativeMemoryDriver(combine_frac);
            getTaskConfig().setFilehandlesDriver(2);

            final GroupReduceCombineDriver<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>
                    testTask = new GroupReduceCombineDriver<>();

            try {
                testDriver(testTask, MockFailingCombiningReduceStub.class);
                fail("Exception not forwarded.");
            } catch (ExpectedTestException etex) {
                // good!
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testCancelCombineTaskSorting() {
        try {
            MutableObjectIterator<Tuple2<Integer, Integer>> slowInfiniteInput =
                    new DelayingIterator<>(new InfiniteIntTupleIterator(), 1);

            setInput(slowInfiniteInput, serializer);
            addDriverComparator(this.comparator);
            addDriverComparator(this.comparator);
            setOutput(new DiscardingOutputCollector<Tuple2<Integer, Integer>>());

            getTaskConfig().setDriverStrategy(DriverStrategy.SORTED_GROUP_COMBINE);
            getTaskConfig().setRelativeMemoryDriver(combine_frac);
            getTaskConfig().setFilehandlesDriver(2);

            final GroupReduceCombineDriver<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>
                    testTask = new GroupReduceCombineDriver<>();

            Thread taskRunner =
                    new Thread() {
                        @Override
                        public void run() {
                            try {
                                testDriver(testTask, MockFailingCombiningReduceStub.class);
                            } catch (Exception e) {
                                // exceptions may happen during canceling
                            }
                        }
                    };
            taskRunner.start();

            // give the task some time
            Thread.sleep(500);

            // cancel
            testTask.cancel();

            // make sure it reacts to the canceling in some time
            long deadline = System.currentTimeMillis() + 10000;
            do {
                taskRunner.interrupt();
                taskRunner.join(5000);
            } while (taskRunner.isAlive() && System.currentTimeMillis() < deadline);

            assertFalse("Task did not cancel properly within in 10 seconds.", taskRunner.isAlive());
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    // ------------------------------------------------------------------------
    //  Test Combiners
    // ------------------------------------------------------------------------

    public static class MockCombiningReduceStub
            implements GroupReduceFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>,
                    GroupCombineFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {
        private static final long serialVersionUID = 1L;

        @Override
        public void reduce(
                Iterable<Tuple2<Integer, Integer>> records,
                Collector<Tuple2<Integer, Integer>> out) {
            int key = 0;
            int sum = 0;

            for (Tuple2<Integer, Integer> next : records) {
                key = next.f0;
                sum += next.f1;
            }

            out.collect(new Tuple2<>(key, sum));
        }

        @Override
        public void combine(
                Iterable<Tuple2<Integer, Integer>> records,
                Collector<Tuple2<Integer, Integer>> out) {
            reduce(records, out);
        }
    }

    public static final class MockFailingCombiningReduceStub
            implements GroupReduceFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>,
                    GroupCombineFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {
        private static final long serialVersionUID = 1L;

        private int cnt;

        @Override
        public void reduce(
                Iterable<Tuple2<Integer, Integer>> records,
                Collector<Tuple2<Integer, Integer>> out) {
            int key = 0;
            int sum = 0;

            for (Tuple2<Integer, Integer> next : records) {
                key = next.f0;
                sum += next.f1;
            }

            int resultValue = sum - key;
            out.collect(new Tuple2<>(key, resultValue));
        }

        @Override
        public void combine(
                Iterable<Tuple2<Integer, Integer>> records,
                Collector<Tuple2<Integer, Integer>> out) {
            int key = 0;
            int sum = 0;

            for (Tuple2<Integer, Integer> next : records) {
                key = next.f0;
                sum += next.f1;
            }

            if (++this.cnt >= 10) {
                throw new ExpectedTestException();
            }

            int resultValue = sum - key;
            out.collect(new Tuple2<>(key, resultValue));
        }
    }
}
