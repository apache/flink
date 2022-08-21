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

package org.apache.flink.api.java.operator;

import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

/** Tests for {@link DataSet#groupBy(int...)}. */
class GroupingTest {

    // TUPLE DATA
    private final List<Tuple5<Integer, Long, String, Long, Integer>> emptyTupleData =
            new ArrayList<>();

    private final TupleTypeInfo<Tuple5<Integer, Long, String, Long, Integer>> tupleTypeInfo =
            new TupleTypeInfo<>(
                    BasicTypeInfo.INT_TYPE_INFO,
                    BasicTypeInfo.LONG_TYPE_INFO,
                    BasicTypeInfo.STRING_TYPE_INFO,
                    BasicTypeInfo.LONG_TYPE_INFO,
                    BasicTypeInfo.INT_TYPE_INFO);

    private final TupleTypeInfo<Tuple4<Integer, Long, CustomType, Long[]>> tupleWithCustomInfo =
            new TupleTypeInfo<>(
                    BasicTypeInfo.INT_TYPE_INFO,
                    BasicTypeInfo.LONG_TYPE_INFO,
                    TypeExtractor.createTypeInfo(CustomType.class),
                    BasicArrayTypeInfo.LONG_ARRAY_TYPE_INFO);

    // LONG DATA
    private final List<Long> emptyLongData = new ArrayList<>();

    private final List<CustomType> customTypeData = new ArrayList<>();

    private final List<Tuple4<Integer, Long, CustomType, Long[]>> tupleWithCustomData =
            new ArrayList<>();

    private final List<Tuple2<byte[], byte[]>> byteArrayData = new ArrayList<>();

    @Test
    void testGroupByKeyFields1() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs =
                env.fromCollection(emptyTupleData, tupleTypeInfo);

        // should work
        try {
            tupleDs.groupBy(0);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    void testGroupByKeyFields2() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Long> longDs = env.fromCollection(emptyLongData, BasicTypeInfo.LONG_TYPE_INFO);
        // should not work: groups on basic type
        assertThatThrownBy(() -> longDs.groupBy(0)).isInstanceOf(InvalidProgramException.class);
    }

    @Test
    void testGroupByKeyFields3() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        this.customTypeData.add(new CustomType());

        DataSet<CustomType> customDs = env.fromCollection(customTypeData);
        // should not work: groups on custom type
        assertThatThrownBy(() -> customDs.groupBy(0)).isInstanceOf(InvalidProgramException.class);
    }

    @Test
    void testGroupByKeyFields4() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs =
                env.fromCollection(emptyTupleData, tupleTypeInfo);

        // should not work, key out of tuple bounds
        assertThatThrownBy(() -> tupleDs.groupBy(5)).isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void testGroupByKeyFields5() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs =
                env.fromCollection(emptyTupleData, tupleTypeInfo);

        // should not work, negative field position
        assertThatThrownBy(() -> tupleDs.groupBy(-1)).isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void testGroupByKeyFieldsOnPrimitiveArray() {
        this.byteArrayData.add(new Tuple2<>(new byte[] {0}, new byte[] {1}));

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple2<byte[], byte[]>> tupleDs = env.fromCollection(byteArrayData);
        tupleDs.groupBy(0);
    }

    @Test
    void testGroupByKeyExpressions1() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        this.customTypeData.add(new CustomType());

        DataSet<CustomType> ds = env.fromCollection(customTypeData);

        // should work
        try {
            ds.groupBy("myInt");
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    void testGroupByKeyExpressions2() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Long> longDs = env.fromCollection(emptyLongData, BasicTypeInfo.LONG_TYPE_INFO);
        // should not work: groups on basic type
        assertThatThrownBy(() -> longDs.groupBy("myInt"))
                .isInstanceOf(InvalidProgramException.class);
    }

    @Test
    void testGroupByKeyExpressions3() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        this.customTypeData.add(new CustomType());

        DataSet<CustomType> customDs = env.fromCollection(customTypeData);
        // should not work: tuple selector on custom type
        assertThatThrownBy(() -> customDs.groupBy(0)).isInstanceOf(InvalidProgramException.class);
    }

    @Test
    void testGroupByKeyExpressions4() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // should not work, key out of tuple bounds
        assertThatThrownBy(
                        () -> {
                            DataSet<CustomType> ds = env.fromCollection(customTypeData);
                            ds.groupBy("myNonExistent");
                        })
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testGroupByKeyExpressions1Nested() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        this.customTypeData.add(new CustomType());

        DataSet<CustomType> ds = env.fromCollection(customTypeData);

        // should work
        try {
            ds.groupBy("nested.myInt");
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    void testGroupByKeyExpressions2Nested() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // should not work, key out of tuple bounds
        assertThatThrownBy(
                        () -> {
                            DataSet<CustomType> ds = env.fromCollection(customTypeData);
                            ds.groupBy("nested.myNonExistent");
                        })
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @SuppressWarnings("serial")
    void testGroupByKeySelector1() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        this.customTypeData.add(new CustomType());

        try {
            DataSet<CustomType> customDs = env.fromCollection(customTypeData);
            // should work
            customDs.groupBy((KeySelector<CustomType, Long>) value -> value.myLong);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    @SuppressWarnings("serial")
    void testGroupByKeySelector2() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        this.customTypeData.add(new CustomType());

        try {
            DataSet<CustomType> customDs = env.fromCollection(customTypeData);
            // should work
            customDs.groupBy(
                    new KeySelector<GroupingTest.CustomType, Tuple2<Integer, Long>>() {
                        @Override
                        public Tuple2<Integer, Long> getKey(CustomType value) {
                            return new Tuple2<Integer, Long>(value.myInt, value.myLong);
                        }
                    });
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    @SuppressWarnings("serial")
    void testGroupByKeySelector3() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        this.customTypeData.add(new CustomType());

        try {
            DataSet<CustomType> customDs = env.fromCollection(customTypeData);
            // should not work
            customDs.groupBy(
                    new KeySelector<GroupingTest.CustomType, CustomType>() {
                        @Override
                        public CustomType getKey(CustomType value) {
                            return value;
                        }
                    });
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    @SuppressWarnings("serial")
    void testGroupByKeySelector4() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        this.customTypeData.add(new CustomType());

        try {
            DataSet<CustomType> customDs = env.fromCollection(customTypeData);
            // should not work
            customDs.groupBy(
                    new KeySelector<
                            GroupingTest.CustomType, Tuple2<Integer, GroupingTest.CustomType>>() {
                        @Override
                        public Tuple2<Integer, CustomType> getKey(CustomType value) {
                            return new Tuple2<Integer, CustomType>(value.myInt, value);
                        }
                    });
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    @SuppressWarnings("serial")
    void testGroupByKeySelector5() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        this.customTypeData.add(new CustomType());

        DataSet<CustomType> customDs = env.fromCollection(customTypeData);
        // should not work
        assertThatThrownBy(
                        () ->
                                customDs.groupBy(
                                        (KeySelector<CustomType, CustomType2>)
                                                value -> new CustomType2()))
                .isInstanceOf(InvalidProgramException.class);
    }

    @Test
    void testGroupSortKeyFields1() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs =
                env.fromCollection(emptyTupleData, tupleTypeInfo);

        // should work
        try {
            tupleDs.groupBy(0).sortGroup(0, Order.ASCENDING);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    void testGroupSortKeyFields2() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs =
                env.fromCollection(emptyTupleData, tupleTypeInfo);

        // should not work, field index out of bounds
        assertThatThrownBy(() -> tupleDs.groupBy(0).sortGroup(5, Order.ASCENDING))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void testGroupSortKeyFields3() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Long> longDs = env.fromCollection(emptyLongData, BasicTypeInfo.LONG_TYPE_INFO);

        // should not work: sorted groups on groupings by key selectors
        assertThatThrownBy(
                        () ->
                                longDs.groupBy(
                                                new KeySelector<Long, Long>() {
                                                    private static final long serialVersionUID = 1L;

                                                    @Override
                                                    public Long getKey(Long value) {
                                                        return value;
                                                    }
                                                })
                                        .sortGroup(0, Order.ASCENDING))
                .isInstanceOf(InvalidProgramException.class);
    }

    @Test
    void testGroupSortKeyFields4() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple4<Integer, Long, CustomType, Long[]>> tupleDs =
                env.fromCollection(tupleWithCustomData, tupleWithCustomInfo);

        // should not work
        assertThatThrownBy(() -> tupleDs.groupBy(0).sortGroup(2, Order.ASCENDING))
                .isInstanceOf(InvalidProgramException.class);
    }

    @Test
    void testGroupSortKeyFields5() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple4<Integer, Long, CustomType, Long[]>> tupleDs =
                env.fromCollection(tupleWithCustomData, tupleWithCustomInfo);

        // should not work
        assertThatThrownBy(() -> tupleDs.groupBy(0).sortGroup(3, Order.ASCENDING))
                .isInstanceOf(InvalidProgramException.class);
    }

    @Test
    void testChainedGroupSortKeyFields() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs =
                env.fromCollection(emptyTupleData, tupleTypeInfo);

        // should work
        try {
            tupleDs.groupBy(0).sortGroup(0, Order.ASCENDING).sortGroup(2, Order.DESCENDING);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    void testGroupSortByKeyExpression1() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple4<Integer, Long, CustomType, Long[]>> tupleDs =
                env.fromCollection(tupleWithCustomData, tupleWithCustomInfo);

        // should work
        try {
            tupleDs.groupBy("f0").sortGroup("f1", Order.ASCENDING);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    void testGroupSortByKeyExpression2() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple4<Integer, Long, CustomType, Long[]>> tupleDs =
                env.fromCollection(tupleWithCustomData, tupleWithCustomInfo);

        // should work
        try {
            tupleDs.groupBy("f0").sortGroup("f2.myString", Order.ASCENDING);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    void testGroupSortByKeyExpression3() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple4<Integer, Long, CustomType, Long[]>> tupleDs =
                env.fromCollection(tupleWithCustomData, tupleWithCustomInfo);

        // should work
        try {
            tupleDs.groupBy("f0")
                    .sortGroup("f2.myString", Order.ASCENDING)
                    .sortGroup("f1", Order.DESCENDING);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    void testGroupSortByKeyExpression4() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple4<Integer, Long, CustomType, Long[]>> tupleDs =
                env.fromCollection(tupleWithCustomData, tupleWithCustomInfo);

        // should not work
        assertThatThrownBy(() -> tupleDs.groupBy("f0").sortGroup("f2", Order.ASCENDING))
                .isInstanceOf(InvalidProgramException.class);
    }

    @Test
    void testGroupSortByKeyExpression5() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple4<Integer, Long, CustomType, Long[]>> tupleDs =
                env.fromCollection(tupleWithCustomData, tupleWithCustomInfo);

        // should not work
        assertThatThrownBy(
                        () ->
                                tupleDs.groupBy("f0")
                                        .sortGroup("f1", Order.ASCENDING)
                                        .sortGroup("f2", Order.ASCENDING))
                .isInstanceOf(InvalidProgramException.class);
    }

    @Test
    void testGroupSortByKeyExpression6() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple4<Integer, Long, CustomType, Long[]>> tupleDs =
                env.fromCollection(tupleWithCustomData, tupleWithCustomInfo);

        // should not work
        assertThatThrownBy(() -> tupleDs.groupBy("f0").sortGroup("f3", Order.ASCENDING))
                .isInstanceOf(InvalidProgramException.class);
    }

    @SuppressWarnings("serial")
    @Test
    void testGroupSortByKeySelector1() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple4<Integer, Long, CustomType, Long[]>> tupleDs =
                env.fromCollection(tupleWithCustomData, tupleWithCustomInfo);

        // should not work
        tupleDs.groupBy(
                        new KeySelector<Tuple4<Integer, Long, CustomType, Long[]>, Long>() {
                            @Override
                            public Long getKey(Tuple4<Integer, Long, CustomType, Long[]> value)
                                    throws Exception {
                                return value.f1;
                            }
                        })
                .sortGroup(
                        new KeySelector<Tuple4<Integer, Long, CustomType, Long[]>, Integer>() {
                            @Override
                            public Integer getKey(Tuple4<Integer, Long, CustomType, Long[]> value)
                                    throws Exception {
                                return value.f0;
                            }
                        },
                        Order.ASCENDING);
    }

    @SuppressWarnings("serial")
    @Test
    void testGroupSortByKeySelector2() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple4<Integer, Long, CustomType, Long[]>> tupleDs =
                env.fromCollection(tupleWithCustomData, tupleWithCustomInfo);

        // should not work
        assertThatThrownBy(
                        () ->
                                tupleDs.groupBy(
                                                (KeySelector<
                                                                Tuple4<
                                                                        Integer,
                                                                        Long,
                                                                        CustomType,
                                                                        Long[]>,
                                                                Long>)
                                                        value -> value.f1)
                                        .sortGroup(
                                                (KeySelector<
                                                                Tuple4<
                                                                        Integer,
                                                                        Long,
                                                                        CustomType,
                                                                        Long[]>,
                                                                CustomType>)
                                                        value -> value.f2,
                                                Order.ASCENDING))
                .isInstanceOf(InvalidProgramException.class);
    }

    @SuppressWarnings("serial")
    @Test
    void testGroupSortByKeySelector3() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple4<Integer, Long, CustomType, Long[]>> tupleDs =
                env.fromCollection(tupleWithCustomData, tupleWithCustomInfo);

        // should not work
        assertThatThrownBy(
                        () ->
                                tupleDs.groupBy(
                                                (KeySelector<
                                                                Tuple4<
                                                                        Integer,
                                                                        Long,
                                                                        CustomType,
                                                                        Long[]>,
                                                                Long>)
                                                        value -> value.f1)
                                        .sortGroup(
                                                (KeySelector<
                                                                Tuple4<
                                                                        Integer,
                                                                        Long,
                                                                        CustomType,
                                                                        Long[]>,
                                                                Long[]>)
                                                        value -> value.f3,
                                                Order.ASCENDING))
                .isInstanceOf(InvalidProgramException.class);
    }

    @Test
    void testGroupingAtomicType() {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Integer> dataSet = env.fromElements(0, 1, 1, 2, 0, 0);

        dataSet.groupBy("*");
    }

    @Test
    void testGroupAtomicTypeWithInvalid1() {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Integer> dataSet = env.fromElements(0, 1, 2, 3);

        assertThatThrownBy(() -> dataSet.groupBy("*", "invalidField"))
                .isInstanceOf(InvalidProgramException.class);
    }

    @Test
    void testGroupAtomicTypeWithInvalid2() {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Integer> dataSet = env.fromElements(0, 1, 2, 3);

        assertThatThrownBy(() -> dataSet.groupBy("invalidField"))
                .isInstanceOf(InvalidProgramException.class);
    }

    @Test
    void testGroupAtomicTypeWithInvalid3() {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<ArrayList<Integer>> dataSet = env.fromElements(new ArrayList<Integer>());

        assertThatThrownBy(() -> dataSet.groupBy("*")).isInstanceOf(InvalidProgramException.class);
    }

    /** Custom data type, for testing purposes. */
    public static class CustomType implements Serializable {

        /** Custom nested data type, for testing purposes. */
        public static class Nest {
            public int myInt;
        }

        private static final long serialVersionUID = 1L;

        public int myInt;
        public long myLong;
        public String myString;
        public Nest nested;

        public CustomType() {}

        public CustomType(int i, long l, String s) {
            myInt = i;
            myLong = l;
            myString = s;
        }

        @Override
        public String toString() {
            return myInt + "," + myLong + "," + myString;
        }
    }

    /** Custom non-nested data type, for testing purposes. */
    public static class CustomType2 implements Serializable {

        public int myInt;
        public Integer[] myIntArray;
    }
}
