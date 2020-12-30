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

package org.apache.flink.runtime.operators.drivers;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.runtime.operators.DriverStrategy;
import org.apache.flink.runtime.operators.GroupReduceDriver;
import org.apache.flink.runtime.util.EmptyMutableObjectIterator;
import org.apache.flink.runtime.util.RegularToMutableObjectIterator;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("serial")
public class GroupReduceDriverTest {

    @Test
    public void testAllReduceDriverImmutableEmpty() {
        try {
            TestTaskContext<
                            GroupReduceFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>,
                            Tuple2<String, Integer>>
                    context =
                            new TestTaskContext<
                                    GroupReduceFunction<
                                            Tuple2<String, Integer>, Tuple2<String, Integer>>,
                                    Tuple2<String, Integer>>();

            List<Tuple2<String, Integer>> data = DriverTestData.createReduceImmutableData();
            TupleTypeInfo<Tuple2<String, Integer>> typeInfo =
                    (TupleTypeInfo<Tuple2<String, Integer>>)
                            TypeExtractor.getForObject(data.get(0));
            MutableObjectIterator<Tuple2<String, Integer>> input = EmptyMutableObjectIterator.get();
            TypeComparator<Tuple2<String, Integer>> comparator =
                    typeInfo.createComparator(
                            new int[] {0}, new boolean[] {true}, 0, new ExecutionConfig());
            context.setDriverStrategy(DriverStrategy.SORTED_GROUP_REDUCE);

            GatheringCollector<Tuple2<String, Integer>> result =
                    new GatheringCollector<Tuple2<String, Integer>>(
                            typeInfo.createSerializer(new ExecutionConfig()));

            context.setInput1(input, typeInfo.createSerializer(new ExecutionConfig()));
            context.setComparator1(comparator);
            context.setCollector(result);

            GroupReduceDriver<Tuple2<String, Integer>, Tuple2<String, Integer>> driver =
                    new GroupReduceDriver<Tuple2<String, Integer>, Tuple2<String, Integer>>();
            driver.setup(context);
            driver.prepare();
            driver.run();

            Assert.assertTrue(result.getList().isEmpty());
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testAllReduceDriverImmutable() {
        try {
            TestTaskContext<
                            GroupReduceFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>,
                            Tuple2<String, Integer>>
                    context =
                            new TestTaskContext<
                                    GroupReduceFunction<
                                            Tuple2<String, Integer>, Tuple2<String, Integer>>,
                                    Tuple2<String, Integer>>();

            List<Tuple2<String, Integer>> data = DriverTestData.createReduceImmutableData();
            TupleTypeInfo<Tuple2<String, Integer>> typeInfo =
                    (TupleTypeInfo<Tuple2<String, Integer>>)
                            TypeExtractor.getForObject(data.get(0));
            MutableObjectIterator<Tuple2<String, Integer>> input =
                    new RegularToMutableObjectIterator<Tuple2<String, Integer>>(
                            data.iterator(), typeInfo.createSerializer(new ExecutionConfig()));
            TypeComparator<Tuple2<String, Integer>> comparator =
                    typeInfo.createComparator(
                            new int[] {0}, new boolean[] {true}, 0, new ExecutionConfig());

            GatheringCollector<Tuple2<String, Integer>> result =
                    new GatheringCollector<Tuple2<String, Integer>>(
                            typeInfo.createSerializer(new ExecutionConfig()));

            context.setDriverStrategy(DriverStrategy.SORTED_GROUP_REDUCE);
            context.setInput1(input, typeInfo.createSerializer(new ExecutionConfig()));
            context.setCollector(result);
            context.setComparator1(comparator);
            context.setUdf(new ConcatSumReducer());

            GroupReduceDriver<Tuple2<String, Integer>, Tuple2<String, Integer>> driver =
                    new GroupReduceDriver<Tuple2<String, Integer>, Tuple2<String, Integer>>();
            driver.setup(context);
            driver.prepare();
            driver.run();

            Object[] res = result.getList().toArray();
            Object[] expected = DriverTestData.createReduceImmutableDataGroupedResult().toArray();

            DriverTestData.compareTupleArrays(expected, res);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testAllReduceDriverMutable() {
        try {
            TestTaskContext<
                            GroupReduceFunction<
                                    Tuple2<StringValue, IntValue>, Tuple2<StringValue, IntValue>>,
                            Tuple2<StringValue, IntValue>>
                    context =
                            new TestTaskContext<
                                    GroupReduceFunction<
                                            Tuple2<StringValue, IntValue>,
                                            Tuple2<StringValue, IntValue>>,
                                    Tuple2<StringValue, IntValue>>();

            List<Tuple2<StringValue, IntValue>> data = DriverTestData.createReduceMutableData();
            TupleTypeInfo<Tuple2<StringValue, IntValue>> typeInfo =
                    (TupleTypeInfo<Tuple2<StringValue, IntValue>>)
                            TypeExtractor.getForObject(data.get(0));
            MutableObjectIterator<Tuple2<StringValue, IntValue>> input =
                    new RegularToMutableObjectIterator<Tuple2<StringValue, IntValue>>(
                            data.iterator(), typeInfo.createSerializer(new ExecutionConfig()));
            TypeComparator<Tuple2<StringValue, IntValue>> comparator =
                    typeInfo.createComparator(
                            new int[] {0}, new boolean[] {true}, 0, new ExecutionConfig());

            GatheringCollector<Tuple2<StringValue, IntValue>> result =
                    new GatheringCollector<Tuple2<StringValue, IntValue>>(
                            typeInfo.createSerializer(new ExecutionConfig()));

            context.setDriverStrategy(DriverStrategy.SORTED_GROUP_REDUCE);
            context.setInput1(input, typeInfo.createSerializer(new ExecutionConfig()));
            context.setComparator1(comparator);
            context.setCollector(result);
            context.setUdf(new ConcatSumMutableReducer());

            GroupReduceDriver<Tuple2<StringValue, IntValue>, Tuple2<StringValue, IntValue>> driver =
                    new GroupReduceDriver<
                            Tuple2<StringValue, IntValue>, Tuple2<StringValue, IntValue>>();
            driver.setup(context);
            driver.prepare();
            driver.run();

            Object[] res = result.getList().toArray();
            Object[] expected = DriverTestData.createReduceMutableDataGroupedResult().toArray();

            DriverTestData.compareTupleArrays(expected, res);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testAllReduceDriverIncorrectlyAccumulatingMutable() {
        try {
            TestTaskContext<
                            GroupReduceFunction<
                                    Tuple2<StringValue, IntValue>, Tuple2<StringValue, IntValue>>,
                            Tuple2<StringValue, IntValue>>
                    context =
                            new TestTaskContext<
                                    GroupReduceFunction<
                                            Tuple2<StringValue, IntValue>,
                                            Tuple2<StringValue, IntValue>>,
                                    Tuple2<StringValue, IntValue>>();

            List<Tuple2<StringValue, IntValue>> data = DriverTestData.createReduceMutableData();
            TupleTypeInfo<Tuple2<StringValue, IntValue>> typeInfo =
                    (TupleTypeInfo<Tuple2<StringValue, IntValue>>)
                            TypeExtractor.getForObject(data.get(0));
            MutableObjectIterator<Tuple2<StringValue, IntValue>> input =
                    new RegularToMutableObjectIterator<Tuple2<StringValue, IntValue>>(
                            data.iterator(), typeInfo.createSerializer(new ExecutionConfig()));
            TypeComparator<Tuple2<StringValue, IntValue>> comparator =
                    typeInfo.createComparator(
                            new int[] {0}, new boolean[] {true}, 0, new ExecutionConfig());

            GatheringCollector<Tuple2<StringValue, IntValue>> result =
                    new GatheringCollector<Tuple2<StringValue, IntValue>>(
                            typeInfo.createSerializer(new ExecutionConfig()));

            context.setDriverStrategy(DriverStrategy.SORTED_GROUP_REDUCE);
            context.setInput1(input, typeInfo.createSerializer(new ExecutionConfig()));
            context.setComparator1(comparator);
            context.setCollector(result);
            context.setUdf(new ConcatSumMutableAccumulatingReducer());

            GroupReduceDriver<Tuple2<StringValue, IntValue>, Tuple2<StringValue, IntValue>> driver =
                    new GroupReduceDriver<
                            Tuple2<StringValue, IntValue>, Tuple2<StringValue, IntValue>>();
            driver.setup(context);
            driver.prepare();
            driver.run();

            Object[] res = result.getList().toArray();
            Object[] expected = DriverTestData.createReduceMutableDataGroupedResult().toArray();

            try {
                DriverTestData.compareTupleArrays(expected, res);
                Assert.fail(
                        "Accumulationg mutable objects is expected to result in incorrect values.");
            } catch (AssertionError e) {
                // expected
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testAllReduceDriverAccumulatingImmutable() {
        try {
            TestTaskContext<
                            GroupReduceFunction<
                                    Tuple2<StringValue, IntValue>, Tuple2<StringValue, IntValue>>,
                            Tuple2<StringValue, IntValue>>
                    context =
                            new TestTaskContext<
                                    GroupReduceFunction<
                                            Tuple2<StringValue, IntValue>,
                                            Tuple2<StringValue, IntValue>>,
                                    Tuple2<StringValue, IntValue>>();

            List<Tuple2<StringValue, IntValue>> data = DriverTestData.createReduceMutableData();
            TupleTypeInfo<Tuple2<StringValue, IntValue>> typeInfo =
                    (TupleTypeInfo<Tuple2<StringValue, IntValue>>)
                            TypeExtractor.getForObject(data.get(0));
            MutableObjectIterator<Tuple2<StringValue, IntValue>> input =
                    new RegularToMutableObjectIterator<Tuple2<StringValue, IntValue>>(
                            data.iterator(), typeInfo.createSerializer(new ExecutionConfig()));
            TypeComparator<Tuple2<StringValue, IntValue>> comparator =
                    typeInfo.createComparator(
                            new int[] {0}, new boolean[] {true}, 0, new ExecutionConfig());

            GatheringCollector<Tuple2<StringValue, IntValue>> result =
                    new GatheringCollector<Tuple2<StringValue, IntValue>>(
                            typeInfo.createSerializer(new ExecutionConfig()));

            context.setDriverStrategy(DriverStrategy.SORTED_GROUP_REDUCE);
            context.setInput1(input, typeInfo.createSerializer(new ExecutionConfig()));
            context.setComparator1(comparator);
            context.setCollector(result);
            context.setUdf(new ConcatSumMutableAccumulatingReducer());
            context.setMutableObjectMode(false);

            GroupReduceDriver<Tuple2<StringValue, IntValue>, Tuple2<StringValue, IntValue>> driver =
                    new GroupReduceDriver<
                            Tuple2<StringValue, IntValue>, Tuple2<StringValue, IntValue>>();
            driver.setup(context);
            driver.prepare();
            driver.run();

            Object[] res = result.getList().toArray();
            Object[] expected = DriverTestData.createReduceMutableDataGroupedResult().toArray();

            DriverTestData.compareTupleArrays(expected, res);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    // --------------------------------------------------------------------------------------------
    //  Test UDFs
    // --------------------------------------------------------------------------------------------

    public static final class ConcatSumReducer
            extends RichGroupReduceFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> {

        @Override
        public void reduce(
                Iterable<Tuple2<String, Integer>> values, Collector<Tuple2<String, Integer>> out) {
            Tuple2<String, Integer> current = new Tuple2<String, Integer>("", 0);

            for (Tuple2<String, Integer> next : values) {
                next.f0 = current.f0 + next.f0;
                next.f1 = current.f1 + next.f1;
                current = next;
            }

            out.collect(current);
        }
    }

    public static final class ConcatSumMutableReducer
            extends RichGroupReduceFunction<
                    Tuple2<StringValue, IntValue>, Tuple2<StringValue, IntValue>> {

        @Override
        public void reduce(
                Iterable<Tuple2<StringValue, IntValue>> values,
                Collector<Tuple2<StringValue, IntValue>> out) {
            Tuple2<StringValue, IntValue> current =
                    new Tuple2<StringValue, IntValue>(new StringValue(""), new IntValue(0));

            for (Tuple2<StringValue, IntValue> next : values) {
                next.f0.append(current.f0);
                next.f1.setValue(current.f1.getValue() + next.f1.getValue());
                current = next;
            }

            out.collect(current);
        }
    }

    public static final class ConcatSumMutableAccumulatingReducer
            implements GroupReduceFunction<
                    Tuple2<StringValue, IntValue>, Tuple2<StringValue, IntValue>> {

        @Override
        public void reduce(
                Iterable<Tuple2<StringValue, IntValue>> values,
                Collector<Tuple2<StringValue, IntValue>> out)
                throws Exception {
            List<Tuple2<StringValue, IntValue>> all =
                    new ArrayList<Tuple2<StringValue, IntValue>>();

            for (Tuple2<StringValue, IntValue> t : values) {
                all.add(t);
            }

            Tuple2<StringValue, IntValue> result = all.get(0);

            for (int i = 1; i < all.size(); i++) {
                Tuple2<StringValue, IntValue> e = all.get(i);
                result.f0.append(e.f0);
                result.f1.setValue(result.f1.getValue() + e.f1.getValue());
            }

            out.collect(result);
        }
    }
}
