/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.operators.Keys;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.functions.aggregation.AggregationFunction;
import org.apache.flink.streaming.api.functions.aggregation.AggregationFunction.AggregationType;
import org.apache.flink.streaming.api.functions.aggregation.ComparableAggregator;
import org.apache.flink.streaming.api.functions.aggregation.SumAggregator;
import org.apache.flink.streaming.api.operators.StreamGroupedReduceOperator;
import org.apache.flink.streaming.util.MockContext;
import org.apache.flink.streaming.util.keys.KeySelectorUtil;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableList;

import org.junit.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/** Tests for {@link AggregationFunction}. */
public class AggregationFunctionTest {

    @Test
    public void groupSumIntegerTest() throws Exception {

        // preparing expected outputs
        List<Tuple2<Integer, Integer>> expectedGroupSumList = new ArrayList<>();
        List<Tuple2<Integer, Integer>> expectedGroupMinList = new ArrayList<>();
        List<Tuple2<Integer, Integer>> expectedGroupMaxList = new ArrayList<>();

        int groupedSum0 = 0;
        int groupedSum1 = 0;
        int groupedSum2 = 0;

        for (int i = 0; i < 9; i++) {
            int groupedSum;
            switch (i % 3) {
                case 0:
                    groupedSum = groupedSum0 += i;
                    break;
                case 1:
                    groupedSum = groupedSum1 += i;
                    break;
                default:
                    groupedSum = groupedSum2 += i;
                    break;
            }

            expectedGroupSumList.add(new Tuple2<>(i % 3, groupedSum));
            expectedGroupMinList.add(new Tuple2<>(i % 3, i % 3));
            expectedGroupMaxList.add(new Tuple2<>(i % 3, i));
        }

        // some necessary boiler plate
        TypeInformation<Tuple2<Integer, Integer>> typeInfo =
                TypeExtractor.getForObject(new Tuple2<>(0, 0));

        ExecutionConfig config = new ExecutionConfig();

        KeySelector<Tuple2<Integer, Integer>, Tuple> keySelector =
                KeySelectorUtil.getSelectorForKeys(
                        new Keys.ExpressionKeys<>(new int[] {0}, typeInfo), typeInfo, config);
        TypeInformation<Tuple> keyType = TypeExtractor.getKeySelectorTypes(keySelector, typeInfo);

        // aggregations tested
        ReduceFunction<Tuple2<Integer, Integer>> sumFunction =
                new SumAggregator<>(1, typeInfo, config);
        ReduceFunction<Tuple2<Integer, Integer>> minFunction =
                new ComparableAggregator<>(1, typeInfo, AggregationType.MIN, config);
        ReduceFunction<Tuple2<Integer, Integer>> maxFunction =
                new ComparableAggregator<>(1, typeInfo, AggregationType.MAX, config);

        List<Tuple2<Integer, Integer>> groupedSumList =
                MockContext.createAndExecuteForKeyedStream(
                        new StreamGroupedReduceOperator<>(
                                sumFunction, typeInfo.createSerializer(config)),
                        getInputList(),
                        keySelector,
                        keyType);

        List<Tuple2<Integer, Integer>> groupedMinList =
                MockContext.createAndExecuteForKeyedStream(
                        new StreamGroupedReduceOperator<>(
                                minFunction, typeInfo.createSerializer(config)),
                        getInputList(),
                        keySelector,
                        keyType);

        List<Tuple2<Integer, Integer>> groupedMaxList =
                MockContext.createAndExecuteForKeyedStream(
                        new StreamGroupedReduceOperator<>(
                                maxFunction, typeInfo.createSerializer(config)),
                        getInputList(),
                        keySelector,
                        keyType);

        assertEquals(expectedGroupSumList, groupedSumList);
        assertEquals(expectedGroupMinList, groupedMinList);
        assertEquals(expectedGroupMaxList, groupedMaxList);
    }

    @Test
    public void pojoGroupSumIntegerTest() throws Exception {

        // preparing expected outputs
        List<MyPojo> expectedGroupSumList = new ArrayList<>();
        List<MyPojo> expectedGroupMinList = new ArrayList<>();
        List<MyPojo> expectedGroupMaxList = new ArrayList<>();

        int groupedSum0 = 0;
        int groupedSum1 = 0;
        int groupedSum2 = 0;

        for (int i = 0; i < 9; i++) {
            int groupedSum;
            switch (i % 3) {
                case 0:
                    groupedSum = groupedSum0 += i;
                    break;
                case 1:
                    groupedSum = groupedSum1 += i;
                    break;
                default:
                    groupedSum = groupedSum2 += i;
                    break;
            }

            expectedGroupSumList.add(new MyPojo(i % 3, groupedSum));
            expectedGroupMinList.add(new MyPojo(i % 3, i % 3));
            expectedGroupMaxList.add(new MyPojo(i % 3, i));
        }

        // some necessary boiler plate
        TypeInformation<MyPojo> typeInfo = TypeExtractor.getForObject(new MyPojo(0, 0));

        ExecutionConfig config = new ExecutionConfig();

        KeySelector<MyPojo, Tuple> keySelector =
                KeySelectorUtil.getSelectorForKeys(
                        new Keys.ExpressionKeys<>(new String[] {"f0"}, typeInfo), typeInfo, config);
        TypeInformation<Tuple> keyType = TypeExtractor.getKeySelectorTypes(keySelector, typeInfo);

        // aggregations tested
        ReduceFunction<MyPojo> sumFunction = new SumAggregator<>("f1", typeInfo, config);
        ReduceFunction<MyPojo> minFunction =
                new ComparableAggregator<>("f1", typeInfo, AggregationType.MIN, false, config);
        ReduceFunction<MyPojo> maxFunction =
                new ComparableAggregator<>("f1", typeInfo, AggregationType.MAX, false, config);

        List<MyPojo> groupedSumList =
                MockContext.createAndExecuteForKeyedStream(
                        new StreamGroupedReduceOperator<>(
                                sumFunction, typeInfo.createSerializer(config)),
                        getInputPojoList(),
                        keySelector,
                        keyType);

        List<MyPojo> groupedMinList =
                MockContext.createAndExecuteForKeyedStream(
                        new StreamGroupedReduceOperator<>(
                                minFunction, typeInfo.createSerializer(config)),
                        getInputPojoList(),
                        keySelector,
                        keyType);

        List<MyPojo> groupedMaxList =
                MockContext.createAndExecuteForKeyedStream(
                        new StreamGroupedReduceOperator<>(
                                maxFunction, typeInfo.createSerializer(config)),
                        getInputPojoList(),
                        keySelector,
                        keyType);

        assertEquals(expectedGroupSumList, groupedSumList);
        assertEquals(expectedGroupMinList, groupedMinList);
        assertEquals(expectedGroupMaxList, groupedMaxList);
    }

    @Test
    public void minMaxByTest() throws Exception {
        // Tuples are grouped on field 0, aggregated on field 1

        // preparing expected outputs
        List<Tuple3<Integer, Integer, Integer>> maxByFirstExpected =
                ImmutableList.of(
                        Tuple3.of(0, 0, 0),
                        Tuple3.of(0, 1, 1),
                        Tuple3.of(0, 2, 2),
                        Tuple3.of(0, 2, 2),
                        Tuple3.of(0, 2, 2),
                        Tuple3.of(0, 2, 2),
                        Tuple3.of(0, 2, 2),
                        Tuple3.of(0, 2, 2),
                        Tuple3.of(0, 2, 2));

        List<Tuple3<Integer, Integer, Integer>> maxByLastExpected =
                ImmutableList.of(
                        Tuple3.of(0, 0, 0),
                        Tuple3.of(0, 1, 1),
                        Tuple3.of(0, 2, 2),
                        Tuple3.of(0, 2, 2),
                        Tuple3.of(0, 2, 2),
                        Tuple3.of(0, 2, 5),
                        Tuple3.of(0, 2, 5),
                        Tuple3.of(0, 2, 5),
                        Tuple3.of(0, 2, 8));

        List<Tuple3<Integer, Integer, Integer>> minByFirstExpected =
                ImmutableList.of(
                        Tuple3.of(0, 0, 0),
                        Tuple3.of(0, 0, 0),
                        Tuple3.of(0, 0, 0),
                        Tuple3.of(0, 0, 0),
                        Tuple3.of(0, 0, 0),
                        Tuple3.of(0, 0, 0),
                        Tuple3.of(0, 0, 0),
                        Tuple3.of(0, 0, 0),
                        Tuple3.of(0, 0, 0));

        List<Tuple3<Integer, Integer, Integer>> minByLastExpected =
                ImmutableList.of(
                        Tuple3.of(0, 0, 0),
                        Tuple3.of(0, 0, 0),
                        Tuple3.of(0, 0, 0),
                        Tuple3.of(0, 0, 3),
                        Tuple3.of(0, 0, 3),
                        Tuple3.of(0, 0, 3),
                        Tuple3.of(0, 0, 6),
                        Tuple3.of(0, 0, 6),
                        Tuple3.of(0, 0, 6));

        // some necessary boiler plate
        TypeInformation<Tuple3<Integer, Integer, Integer>> typeInfo =
                TypeExtractor.getForObject(Tuple3.of(0, 0, 0));

        ExecutionConfig config = new ExecutionConfig();

        KeySelector<Tuple3<Integer, Integer, Integer>, Tuple> keySelector =
                KeySelectorUtil.getSelectorForKeys(
                        new Keys.ExpressionKeys<>(new int[] {0}, typeInfo), typeInfo, config);
        TypeInformation<Tuple> keyType = TypeExtractor.getKeySelectorTypes(keySelector, typeInfo);

        // aggregations tested
        ReduceFunction<Tuple3<Integer, Integer, Integer>> maxByFunctionFirst =
                new ComparableAggregator<>(1, typeInfo, AggregationType.MAXBY, true, config);
        ReduceFunction<Tuple3<Integer, Integer, Integer>> maxByFunctionLast =
                new ComparableAggregator<>(1, typeInfo, AggregationType.MAXBY, false, config);
        ReduceFunction<Tuple3<Integer, Integer, Integer>> minByFunctionFirst =
                new ComparableAggregator<>(1, typeInfo, AggregationType.MINBY, true, config);
        ReduceFunction<Tuple3<Integer, Integer, Integer>> minByFunctionLast =
                new ComparableAggregator<>(1, typeInfo, AggregationType.MINBY, false, config);

        assertEquals(
                maxByFirstExpected,
                MockContext.createAndExecuteForKeyedStream(
                        new StreamGroupedReduceOperator<>(
                                maxByFunctionFirst, typeInfo.createSerializer(config)),
                        getInputByList(),
                        keySelector,
                        keyType));

        assertEquals(
                maxByLastExpected,
                MockContext.createAndExecuteForKeyedStream(
                        new StreamGroupedReduceOperator<>(
                                maxByFunctionLast, typeInfo.createSerializer(config)),
                        getInputByList(),
                        keySelector,
                        keyType));

        assertEquals(
                minByLastExpected,
                MockContext.createAndExecuteForKeyedStream(
                        new StreamGroupedReduceOperator<>(
                                minByFunctionLast, typeInfo.createSerializer(config)),
                        getInputByList(),
                        keySelector,
                        keyType));

        assertEquals(
                minByFirstExpected,
                MockContext.createAndExecuteForKeyedStream(
                        new StreamGroupedReduceOperator<>(
                                minByFunctionFirst, typeInfo.createSerializer(config)),
                        getInputByList(),
                        keySelector,
                        keyType));
    }

    @Test
    public void pojoMinMaxByTest() throws Exception {
        // Pojos are grouped on field 0, aggregated on field 1

        // preparing expected outputs
        List<MyPojo3> maxByFirstExpected =
                ImmutableList.of(
                        new MyPojo3(0, 0),
                        new MyPojo3(1, 1),
                        new MyPojo3(2, 2),
                        new MyPojo3(2, 2),
                        new MyPojo3(2, 2),
                        new MyPojo3(2, 2),
                        new MyPojo3(2, 2),
                        new MyPojo3(2, 2),
                        new MyPojo3(2, 2));

        List<MyPojo3> maxByLastExpected =
                ImmutableList.of(
                        new MyPojo3(0, 0),
                        new MyPojo3(1, 1),
                        new MyPojo3(2, 2),
                        new MyPojo3(2, 2),
                        new MyPojo3(2, 2),
                        new MyPojo3(2, 5),
                        new MyPojo3(2, 5),
                        new MyPojo3(2, 5),
                        new MyPojo3(2, 8));

        List<MyPojo3> minByFirstExpected =
                ImmutableList.of(
                        new MyPojo3(0, 0),
                        new MyPojo3(0, 0),
                        new MyPojo3(0, 0),
                        new MyPojo3(0, 0),
                        new MyPojo3(0, 0),
                        new MyPojo3(0, 0),
                        new MyPojo3(0, 0),
                        new MyPojo3(0, 0),
                        new MyPojo3(0, 0));

        List<MyPojo3> minByLastExpected =
                ImmutableList.of(
                        new MyPojo3(0, 0),
                        new MyPojo3(0, 0),
                        new MyPojo3(0, 0),
                        new MyPojo3(0, 3),
                        new MyPojo3(0, 3),
                        new MyPojo3(0, 3),
                        new MyPojo3(0, 6),
                        new MyPojo3(0, 6),
                        new MyPojo3(0, 6));

        // some necessary boiler plate
        TypeInformation<MyPojo3> typeInfo = TypeExtractor.getForObject(new MyPojo3(0, 0));

        ExecutionConfig config = new ExecutionConfig();

        KeySelector<MyPojo3, Tuple> keySelector =
                KeySelectorUtil.getSelectorForKeys(
                        new Keys.ExpressionKeys<>(new String[] {"f0"}, typeInfo), typeInfo, config);
        TypeInformation<Tuple> keyType = TypeExtractor.getKeySelectorTypes(keySelector, typeInfo);

        // aggregations tested
        ReduceFunction<MyPojo3> maxByFunctionFirst =
                new ComparableAggregator<>("f1", typeInfo, AggregationType.MAXBY, true, config);
        ReduceFunction<MyPojo3> maxByFunctionLast =
                new ComparableAggregator<>("f1", typeInfo, AggregationType.MAXBY, false, config);
        ReduceFunction<MyPojo3> minByFunctionFirst =
                new ComparableAggregator<>("f1", typeInfo, AggregationType.MINBY, true, config);
        ReduceFunction<MyPojo3> minByFunctionLast =
                new ComparableAggregator<>("f1", typeInfo, AggregationType.MINBY, false, config);

        assertEquals(
                maxByFirstExpected,
                MockContext.createAndExecuteForKeyedStream(
                        new StreamGroupedReduceOperator<>(
                                maxByFunctionFirst, typeInfo.createSerializer(config)),
                        getInputByPojoList(),
                        keySelector,
                        keyType));

        assertEquals(
                maxByLastExpected,
                MockContext.createAndExecuteForKeyedStream(
                        new StreamGroupedReduceOperator<>(
                                maxByFunctionLast, typeInfo.createSerializer(config)),
                        getInputByPojoList(),
                        keySelector,
                        keyType));

        assertEquals(
                minByLastExpected,
                MockContext.createAndExecuteForKeyedStream(
                        new StreamGroupedReduceOperator<>(
                                minByFunctionLast, typeInfo.createSerializer(config)),
                        getInputByPojoList(),
                        keySelector,
                        keyType));

        assertEquals(
                minByFirstExpected,
                MockContext.createAndExecuteForKeyedStream(
                        new StreamGroupedReduceOperator<>(
                                minByFunctionFirst, typeInfo.createSerializer(config)),
                        getInputByPojoList(),
                        keySelector,
                        keyType));
    }

    // *************************************************************************
    //     UTILS
    // *************************************************************************

    private List<Tuple2<Integer, Integer>> getInputList() {
        ArrayList<Tuple2<Integer, Integer>> inputList = new ArrayList<>();
        for (int i = 0; i < 9; i++) {
            inputList.add(Tuple2.of(i % 3, i));
        }
        return inputList;
    }

    private List<MyPojo> getInputPojoList() {
        ArrayList<MyPojo> inputList = new ArrayList<>();
        for (int i = 0; i < 9; i++) {
            inputList.add(new MyPojo(i % 3, i));
        }
        return inputList;
    }

    private List<Tuple3<Integer, Integer, Integer>> getInputByList() {
        ArrayList<Tuple3<Integer, Integer, Integer>> inputList = new ArrayList<>();
        for (int i = 0; i < 9; i++) {
            inputList.add(Tuple3.of(0, i % 3, i));
        }
        return inputList;
    }

    private List<MyPojo3> getInputByPojoList() {
        ArrayList<MyPojo3> inputList = new ArrayList<>();
        for (int i = 0; i < 9; i++) {
            inputList.add(new MyPojo3(i % 3, i));
        }
        return inputList;
    }

    /** POJO. */
    public static class MyPojo implements Serializable {

        private static final long serialVersionUID = 1L;
        public int f0;
        public int f1;

        public MyPojo(int f0, int f1) {
            this.f0 = f0;
            this.f1 = f1;
        }

        public MyPojo() {}

        @Override
        public String toString() {
            return "POJO(" + f0 + "," + f1 + ")";
        }

        @Override
        public boolean equals(Object other) {
            if (other instanceof MyPojo) {
                return this.f0 == ((MyPojo) other).f0 && this.f1 == ((MyPojo) other).f1;
            } else {
                return false;
            }
        }
    }

    /** POJO. */
    public static class MyPojo3 implements Serializable {

        private static final long serialVersionUID = 1L;
        public int f0;
        public int f1;
        public int f2;

        // Field 0 is always initialized to 0
        public MyPojo3(int f1, int f2) {
            this.f1 = f1;
            this.f2 = f2;
        }

        public MyPojo3() {}

        @Override
        public String toString() {
            return "POJO3(" + f0 + "," + f1 + "," + f2 + ")";
        }

        @Override
        public boolean equals(Object other) {
            if (other instanceof MyPojo3) {
                return this.f0 == ((MyPojo3) other).f0
                        && this.f1 == ((MyPojo3) other).f1
                        && this.f2 == ((MyPojo3) other).f2;
            } else {
                return false;
            }
        }
    }
}
