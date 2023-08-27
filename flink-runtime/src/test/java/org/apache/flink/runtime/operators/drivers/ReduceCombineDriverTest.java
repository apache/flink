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
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.runtime.operators.DriverStrategy;
import org.apache.flink.runtime.operators.ReduceCombineDriver;
import org.apache.flink.runtime.util.EmptyMutableObjectIterator;
import org.apache.flink.runtime.util.RegularToMutableObjectIterator;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.MutableObjectIterator;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

@SuppressWarnings("serial")
public class ReduceCombineDriverTest {

    @Test
    void testImmutableEmpty() {
        try {
            TestTaskContext<ReduceFunction<Tuple2<String, Integer>>, Tuple2<String, Integer>>
                    context =
                            new TestTaskContext<
                                    ReduceFunction<Tuple2<String, Integer>>,
                                    Tuple2<String, Integer>>(1024 * 1024);
            context.getTaskConfig().setRelativeMemoryDriver(0.5);

            List<Tuple2<String, Integer>> data = DriverTestData.createReduceImmutableData();
            Collections.shuffle(data);

            TupleTypeInfo<Tuple2<String, Integer>> typeInfo =
                    (TupleTypeInfo<Tuple2<String, Integer>>)
                            TypeExtractor.getForObject(data.get(0));
            MutableObjectIterator<Tuple2<String, Integer>> input = EmptyMutableObjectIterator.get();

            context.setDriverStrategy(DriverStrategy.SORTED_PARTIAL_REDUCE);
            TypeComparator<Tuple2<String, Integer>> comparator =
                    typeInfo.createComparator(
                            new int[] {0}, new boolean[] {true}, 0, new ExecutionConfig());

            GatheringCollector<Tuple2<String, Integer>> result =
                    new GatheringCollector<Tuple2<String, Integer>>(
                            typeInfo.createSerializer(new ExecutionConfig()));

            context.setInput1(input, typeInfo.createSerializer(new ExecutionConfig()));
            context.setComparator1(comparator);
            context.setCollector(result);

            ReduceCombineDriver<Tuple2<String, Integer>> driver =
                    new ReduceCombineDriver<Tuple2<String, Integer>>();
            driver.setup(context);
            driver.prepare();
            driver.run();

            assertThat(result.getList()).isEmpty();
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    void testReduceDriverImmutable() {
        try {
            {
                TestTaskContext<ReduceFunction<Tuple2<String, Integer>>, Tuple2<String, Integer>>
                        context =
                                new TestTaskContext<
                                        ReduceFunction<Tuple2<String, Integer>>,
                                        Tuple2<String, Integer>>(1024 * 1024);
                context.getTaskConfig().setRelativeMemoryDriver(0.5);

                List<Tuple2<String, Integer>> data = DriverTestData.createReduceImmutableData();
                Collections.shuffle(data);

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

                context.setDriverStrategy(DriverStrategy.SORTED_PARTIAL_REDUCE);
                context.setInput1(input, typeInfo.createSerializer(new ExecutionConfig()));
                context.setComparator1(comparator);
                context.setCollector(result);
                context.setUdf(new ConcatSumFirstReducer());

                ReduceCombineDriver<Tuple2<String, Integer>> driver =
                        new ReduceCombineDriver<Tuple2<String, Integer>>();
                driver.setup(context);
                driver.prepare();
                driver.run();

                Object[] res = result.getList().toArray();
                Object[] expected =
                        DriverTestData.createReduceImmutableDataGroupedResult().toArray();

                DriverTestData.compareTupleArrays(expected, res);
            }

            {
                TestTaskContext<ReduceFunction<Tuple2<String, Integer>>, Tuple2<String, Integer>>
                        context =
                                new TestTaskContext<
                                        ReduceFunction<Tuple2<String, Integer>>,
                                        Tuple2<String, Integer>>(1024 * 1024);
                context.getTaskConfig().setRelativeMemoryDriver(0.5);

                List<Tuple2<String, Integer>> data = DriverTestData.createReduceImmutableData();
                Collections.shuffle(data);

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

                context.setDriverStrategy(DriverStrategy.SORTED_PARTIAL_REDUCE);
                context.setInput1(input, typeInfo.createSerializer(new ExecutionConfig()));
                context.setComparator1(comparator);
                context.setCollector(result);
                context.setUdf(new ConcatSumSecondReducer());

                ReduceCombineDriver<Tuple2<String, Integer>> driver =
                        new ReduceCombineDriver<Tuple2<String, Integer>>();
                driver.setup(context);
                driver.prepare();
                driver.run();

                Object[] res = result.getList().toArray();
                Object[] expected =
                        DriverTestData.createReduceImmutableDataGroupedResult().toArray();

                DriverTestData.compareTupleArrays(expected, res);
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    void testReduceDriverMutable() {
        try {
            {
                TestTaskContext<
                                ReduceFunction<Tuple2<StringValue, IntValue>>,
                                Tuple2<StringValue, IntValue>>
                        context =
                                new TestTaskContext<
                                        ReduceFunction<Tuple2<StringValue, IntValue>>,
                                        Tuple2<StringValue, IntValue>>(1024 * 1024);
                context.getTaskConfig().setRelativeMemoryDriver(0.5);

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

                context.setDriverStrategy(DriverStrategy.SORTED_PARTIAL_REDUCE);
                context.setInput1(input, typeInfo.createSerializer(new ExecutionConfig()));
                context.setComparator1(comparator);
                context.setCollector(result);
                context.setUdf(new ConcatSumFirstMutableReducer());

                ReduceCombineDriver<Tuple2<StringValue, IntValue>> driver =
                        new ReduceCombineDriver<Tuple2<StringValue, IntValue>>();
                driver.setup(context);
                driver.prepare();
                driver.run();

                Object[] res = result.getList().toArray();
                Object[] expected = DriverTestData.createReduceMutableDataGroupedResult().toArray();

                DriverTestData.compareTupleArrays(expected, res);
            }
            {
                TestTaskContext<
                                ReduceFunction<Tuple2<StringValue, IntValue>>,
                                Tuple2<StringValue, IntValue>>
                        context =
                                new TestTaskContext<
                                        ReduceFunction<Tuple2<StringValue, IntValue>>,
                                        Tuple2<StringValue, IntValue>>(1024 * 1024);
                context.getTaskConfig().setRelativeMemoryDriver(0.5);

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

                context.setDriverStrategy(DriverStrategy.SORTED_PARTIAL_REDUCE);
                context.setInput1(input, typeInfo.createSerializer(new ExecutionConfig()));
                context.setComparator1(comparator);
                context.setCollector(result);
                context.setUdf(new ConcatSumSecondMutableReducer());

                ReduceCombineDriver<Tuple2<StringValue, IntValue>> driver =
                        new ReduceCombineDriver<Tuple2<StringValue, IntValue>>();
                driver.setup(context);
                driver.prepare();
                driver.run();

                Object[] res = result.getList().toArray();
                Object[] expected = DriverTestData.createReduceMutableDataGroupedResult().toArray();

                DriverTestData.compareTupleArrays(expected, res);
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    // --------------------------------------------------------------------------------------------
    //  Test UDFs
    // --------------------------------------------------------------------------------------------

    public static final class ConcatSumFirstReducer
            extends RichReduceFunction<Tuple2<String, Integer>> {

        @Override
        public Tuple2<String, Integer> reduce(
                Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) {
            value1.f0 = value1.f0 + value2.f0;
            value1.f1 = value1.f1 + value2.f1;
            return value1;
        }
    }

    public static final class ConcatSumSecondReducer
            extends RichReduceFunction<Tuple2<String, Integer>> {

        @Override
        public Tuple2<String, Integer> reduce(
                Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) {
            value2.f0 = value1.f0 + value2.f0;
            value2.f1 = value1.f1 + value2.f1;
            return value2;
        }
    }

    public static final class ConcatSumFirstMutableReducer
            extends RichReduceFunction<Tuple2<StringValue, IntValue>> {

        @Override
        public Tuple2<StringValue, IntValue> reduce(
                Tuple2<StringValue, IntValue> value1, Tuple2<StringValue, IntValue> value2) {
            value1.f0.setValue(value1.f0.getValue() + value2.f0.getValue());
            value1.f1.setValue(value1.f1.getValue() + value2.f1.getValue());
            return value1;
        }
    }

    public static final class ConcatSumSecondMutableReducer
            extends RichReduceFunction<Tuple2<StringValue, IntValue>> {

        @Override
        public Tuple2<StringValue, IntValue> reduce(
                Tuple2<StringValue, IntValue> value1, Tuple2<StringValue, IntValue> value2) {
            value2.f0.setValue(value1.f0.getValue() + value2.f0.getValue());
            value2.f1.setValue(value1.f1.getValue() + value2.f1.getValue());
            return value2;
        }
    }
}
