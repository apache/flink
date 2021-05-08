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
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.operators.SemanticProperties;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operator.JoinOperatorTest.CustomType;
import org.apache.flink.api.java.operators.CoGroupOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.util.Collector;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;

/** Tests for {@link DataSet#coGroup(DataSet)}. */
@SuppressWarnings("serial")
public class CoGroupOperatorTest {

    // TUPLE DATA
    private static final List<Tuple5<Integer, Long, String, Long, Integer>> emptyTupleData =
            new ArrayList<Tuple5<Integer, Long, String, Long, Integer>>();

    private final TupleTypeInfo<Tuple5<Integer, Long, String, Long, Integer>> tupleTypeInfo =
            new TupleTypeInfo<Tuple5<Integer, Long, String, Long, Integer>>(
                    BasicTypeInfo.INT_TYPE_INFO,
                    BasicTypeInfo.LONG_TYPE_INFO,
                    BasicTypeInfo.STRING_TYPE_INFO,
                    BasicTypeInfo.LONG_TYPE_INFO,
                    BasicTypeInfo.INT_TYPE_INFO);

    private static List<CustomType> customTypeData = new ArrayList<CustomType>();

    @BeforeClass
    public static void insertCustomData() {
        customTypeData.add(new CustomType());
    }

    @Test
    public void testCoGroupKeyFields1() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);

        // should work
        try {
            ds1.coGroup(ds2).where(0).equalTo(0);
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test(expected = InvalidProgramException.class)
    public void testCoGroupKeyFields2() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);

        // should not work, incompatible cogroup key types
        ds1.coGroup(ds2).where(0).equalTo(2);
    }

    @Test(expected = InvalidProgramException.class)
    public void testCoGroupKeyFields3() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);

        // should not work, incompatible number of cogroup keys
        ds1.coGroup(ds2).where(0, 1).equalTo(2);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testCoGroupKeyFields4() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);

        // should not work, cogroup key out of range
        ds1.coGroup(ds2).where(5).equalTo(0);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testCoGroupKeyFields5() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);

        // should not work, negative key field position
        ds1.coGroup(ds2).where(-1).equalTo(-1);
    }

    @Test(expected = InvalidProgramException.class)
    public void testCoGroupKeyFields6() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);
        DataSet<CustomType> ds2 = env.fromCollection(customTypeData);

        // should not work, cogroup key fields on custom type
        ds1.coGroup(ds2).where(4).equalTo(0);
    }

    @Test
    public void testCoGroupKeyExpressions1() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<CustomType> ds1 = env.fromCollection(customTypeData);
        DataSet<CustomType> ds2 = env.fromCollection(customTypeData);

        // should work
        try {
            ds1.coGroup(ds2).where("myInt").equalTo("myInt");
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test(expected = InvalidProgramException.class)
    public void testCoGroupKeyExpressions2() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<CustomType> ds1 = env.fromCollection(customTypeData);
        DataSet<CustomType> ds2 = env.fromCollection(customTypeData);

        // should not work, incompatible cogroup key types
        ds1.coGroup(ds2).where("myInt").equalTo("myString");
    }

    @Test(expected = InvalidProgramException.class)
    public void testCoGroupKeyExpressions3() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<CustomType> ds1 = env.fromCollection(customTypeData);
        DataSet<CustomType> ds2 = env.fromCollection(customTypeData);

        // should not work, incompatible number of cogroup keys
        ds1.coGroup(ds2).where("myInt", "myString").equalTo("myString");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCoGroupKeyExpressions4() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<CustomType> ds1 = env.fromCollection(customTypeData);
        DataSet<CustomType> ds2 = env.fromCollection(customTypeData);

        // should not work, cogroup key non-existent
        ds1.coGroup(ds2).where("myNonExistent").equalTo("myInt");
    }

    @Test
    public void testCoGroupKeyAtomicExpression1() {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<CustomType> ds1 = env.fromCollection(customTypeData);
        DataSet<Integer> ds2 = env.fromElements(0, 0, 1);

        ds1.coGroup(ds2).where("myInt").equalTo("*");
    }

    @Test
    public void testCoGroupKeyAtomicExpression2() {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Integer> ds1 = env.fromElements(0, 0, 1);
        DataSet<CustomType> ds2 = env.fromCollection(customTypeData);

        ds1.coGroup(ds2).where("*").equalTo("myInt");
    }

    @Test(expected = InvalidProgramException.class)
    public void testCoGroupKeyAtomicInvalidExpression1() {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Integer> ds1 = env.fromElements(0, 0, 1);
        DataSet<CustomType> ds2 = env.fromCollection(customTypeData);

        ds1.coGroup(ds2).where("*", "invalidKey");
    }

    @Test(expected = InvalidProgramException.class)
    public void testCoGroupKeyAtomicInvalidExpression2() {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Integer> ds1 = env.fromElements(0, 0, 1);
        DataSet<CustomType> ds2 = env.fromCollection(customTypeData);

        ds1.coGroup(ds2).where("invalidKey");
    }

    @Test(expected = InvalidProgramException.class)
    public void testCoGroupKeyAtomicInvalidExpression3() {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<CustomType> ds1 = env.fromCollection(customTypeData);
        DataSet<Integer> ds2 = env.fromElements(0, 0, 1);

        ds1.coGroup(ds2).where("myInt").equalTo("invalidKey");
    }

    @Test(expected = InvalidProgramException.class)
    public void testCoGroupKeyAtomicInvalidExpression4() {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<CustomType> ds1 = env.fromCollection(customTypeData);
        DataSet<Integer> ds2 = env.fromElements(0, 0, 1);

        ds1.coGroup(ds2).where("myInt").equalTo("*", "invalidKey");
    }

    @Test(expected = InvalidProgramException.class)
    public void testCoGroupKeyAtomicInvalidExpression5() {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<ArrayList<Integer>> ds1 = env.fromElements(new ArrayList<Integer>());
        DataSet<Integer> ds2 = env.fromElements(0, 0, 0);

        ds1.coGroup(ds2).where("*");
    }

    @Test(expected = InvalidProgramException.class)
    public void testCoGroupKeyAtomicInvalidExpression6() {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Integer> ds1 = env.fromElements(0, 0, 0);
        DataSet<ArrayList<Integer>> ds2 = env.fromElements(new ArrayList<Integer>());

        ds1.coGroup(ds2).where("*").equalTo("*");
    }

    @Test
    public void testCoGroupKeyExpressions1Nested() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<CustomType> ds1 = env.fromCollection(customTypeData);
        DataSet<CustomType> ds2 = env.fromCollection(customTypeData);

        // should work
        try {
            ds1.coGroup(ds2).where("nested.myInt").equalTo("nested.myInt");
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test(expected = InvalidProgramException.class)
    public void testCoGroupKeyExpressions2Nested() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<CustomType> ds1 = env.fromCollection(customTypeData);
        DataSet<CustomType> ds2 = env.fromCollection(customTypeData);

        // should not work, incompatible cogroup key types
        ds1.coGroup(ds2).where("nested.myInt").equalTo("nested.myString");
    }

    @Test(expected = InvalidProgramException.class)
    public void testCoGroupKeyExpressions3Nested() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<CustomType> ds1 = env.fromCollection(customTypeData);
        DataSet<CustomType> ds2 = env.fromCollection(customTypeData);

        // should not work, incompatible number of cogroup keys
        ds1.coGroup(ds2).where("nested.myInt", "nested.myString").equalTo("nested.myString");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCoGroupKeyExpressions4Nested() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<CustomType> ds1 = env.fromCollection(customTypeData);
        DataSet<CustomType> ds2 = env.fromCollection(customTypeData);

        // should not work, cogroup key non-existent
        ds1.coGroup(ds2).where("nested.myNonExistent").equalTo("nested.myInt");
    }

    @Test
    public void testCoGroupKeySelectors1() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<CustomType> ds1 = env.fromCollection(customTypeData);
        DataSet<CustomType> ds2 = env.fromCollection(customTypeData);

        // should work
        try {
            ds1.coGroup(ds2)
                    .where(
                            new KeySelector<CustomType, Long>() {

                                @Override
                                public Long getKey(CustomType value) {
                                    return value.myLong;
                                }
                            })
                    .equalTo(
                            new KeySelector<CustomType, Long>() {

                                @Override
                                public Long getKey(CustomType value) {
                                    return value.myLong;
                                }
                            });
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testCoGroupKeyMixing1() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<CustomType> ds1 = env.fromCollection(customTypeData);
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);

        // should work
        try {
            ds1.coGroup(ds2)
                    .where(
                            new KeySelector<CustomType, Long>() {

                                @Override
                                public Long getKey(CustomType value) {
                                    return value.myLong;
                                }
                            })
                    .equalTo(3);
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testCoGroupKeyMixing2() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);
        DataSet<CustomType> ds2 = env.fromCollection(customTypeData);

        // should work
        try {
            ds1.coGroup(ds2)
                    .where(3)
                    .equalTo(
                            new KeySelector<CustomType, Long>() {

                                @Override
                                public Long getKey(CustomType value) {
                                    return value.myLong;
                                }
                            });
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test(expected = InvalidProgramException.class)
    public void testCoGroupKeyMixing3() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);
        DataSet<CustomType> ds2 = env.fromCollection(customTypeData);

        // should not work, incompatible types
        ds1.coGroup(ds2)
                .where(2)
                .equalTo(
                        new KeySelector<CustomType, Long>() {

                            @Override
                            public Long getKey(CustomType value) {
                                return value.myLong;
                            }
                        });
    }

    @Test(expected = InvalidProgramException.class)
    public void testCoGroupKeyMixing4() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);
        DataSet<CustomType> ds2 = env.fromCollection(customTypeData);

        // should not work, more than one key field position
        ds1.coGroup(ds2)
                .where(1, 3)
                .equalTo(
                        new KeySelector<CustomType, Long>() {

                            @Override
                            public Long getKey(CustomType value) {
                                return value.myLong;
                            }
                        });
    }

    @Test
    public void testSemanticPropsWithKeySelector1() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs1 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs2 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);

        CoGroupOperator<?, ?, ?> coGroupOp =
                tupleDs1.coGroup(tupleDs2)
                        .where(new DummyTestKeySelector())
                        .equalTo(new DummyTestKeySelector())
                        .with(new DummyTestCoGroupFunction1());

        SemanticProperties semProps = coGroupOp.getSemanticProperties();

        assertTrue(semProps.getForwardingTargetFields(0, 0).size() == 0);
        assertTrue(semProps.getForwardingTargetFields(0, 1).size() == 0);
        assertTrue(semProps.getForwardingTargetFields(0, 2).size() == 1);
        assertTrue(semProps.getForwardingTargetFields(0, 2).contains(4));
        assertTrue(semProps.getForwardingTargetFields(0, 3).size() == 2);
        assertTrue(semProps.getForwardingTargetFields(0, 3).contains(1));
        assertTrue(semProps.getForwardingTargetFields(0, 3).contains(3));
        assertTrue(semProps.getForwardingTargetFields(0, 4).size() == 0);
        assertTrue(semProps.getForwardingTargetFields(0, 5).size() == 0);
        assertTrue(semProps.getForwardingTargetFields(0, 6).size() == 0);

        assertTrue(semProps.getForwardingTargetFields(1, 0).size() == 0);
        assertTrue(semProps.getForwardingTargetFields(1, 1).size() == 0);
        assertTrue(semProps.getForwardingTargetFields(1, 2).size() == 0);
        assertTrue(semProps.getForwardingTargetFields(1, 3).size() == 0);
        assertTrue(semProps.getForwardingTargetFields(1, 4).size() == 1);
        assertTrue(semProps.getForwardingTargetFields(1, 4).contains(2));
        assertTrue(semProps.getForwardingTargetFields(1, 5).size() == 0);
        assertTrue(semProps.getForwardingTargetFields(1, 6).size() == 1);
        assertTrue(semProps.getForwardingTargetFields(1, 6).contains(0));

        assertTrue(semProps.getReadFields(0).size() == 3);
        assertTrue(semProps.getReadFields(0).contains(2));
        assertTrue(semProps.getReadFields(0).contains(4));
        assertTrue(semProps.getReadFields(0).contains(6));

        assertTrue(semProps.getReadFields(1).size() == 2);
        assertTrue(semProps.getReadFields(1).contains(3));
        assertTrue(semProps.getReadFields(1).contains(5));
    }

    @Test
    public void testSemanticPropsWithKeySelector2() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs1 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs2 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);

        CoGroupOperator<?, ?, ?> coGroupOp =
                tupleDs1.coGroup(tupleDs2)
                        .where(new DummyTestKeySelector())
                        .equalTo(new DummyTestKeySelector())
                        .with(new DummyTestCoGroupFunction2())
                        .withForwardedFieldsFirst("2;4->0")
                        .withForwardedFieldsSecond("0->4;1;1->3");

        SemanticProperties semProps = coGroupOp.getSemanticProperties();

        assertTrue(semProps.getForwardingTargetFields(0, 0).size() == 0);
        assertTrue(semProps.getForwardingTargetFields(0, 1).size() == 0);
        assertTrue(semProps.getForwardingTargetFields(0, 2).size() == 0);
        assertTrue(semProps.getForwardingTargetFields(0, 3).size() == 0);
        assertTrue(semProps.getForwardingTargetFields(0, 4).size() == 1);
        assertTrue(semProps.getForwardingTargetFields(0, 4).contains(2));
        assertTrue(semProps.getForwardingTargetFields(0, 5).size() == 0);
        assertTrue(semProps.getForwardingTargetFields(0, 6).size() == 1);
        assertTrue(semProps.getForwardingTargetFields(0, 6).contains(0));

        assertTrue(semProps.getForwardingTargetFields(1, 0).size() == 0);
        assertTrue(semProps.getForwardingTargetFields(1, 1).size() == 0);
        assertTrue(semProps.getForwardingTargetFields(1, 2).size() == 1);
        assertTrue(semProps.getForwardingTargetFields(1, 2).contains(4));
        assertTrue(semProps.getForwardingTargetFields(1, 3).size() == 2);
        assertTrue(semProps.getForwardingTargetFields(1, 3).contains(1));
        assertTrue(semProps.getForwardingTargetFields(1, 3).contains(3));
        assertTrue(semProps.getForwardingTargetFields(1, 4).size() == 0);
        assertTrue(semProps.getForwardingTargetFields(1, 5).size() == 0);
        assertTrue(semProps.getForwardingTargetFields(1, 6).size() == 0);

        assertTrue(semProps.getReadFields(0).size() == 3);
        assertTrue(semProps.getReadFields(0).contains(2));
        assertTrue(semProps.getReadFields(0).contains(3));
        assertTrue(semProps.getReadFields(0).contains(4));

        assertTrue(semProps.getReadFields(1) == null);
    }

    private static class DummyTestKeySelector
            implements KeySelector<
                    Tuple5<Integer, Long, String, Long, Integer>, Tuple2<Long, Integer>> {
        @Override
        public Tuple2<Long, Integer> getKey(Tuple5<Integer, Long, String, Long, Integer> value)
                throws Exception {
            return new Tuple2<Long, Integer>();
        }
    }

    @FunctionAnnotation.ForwardedFieldsFirst("0->4;1;1->3")
    @FunctionAnnotation.ForwardedFieldsSecond("2;4->0")
    @FunctionAnnotation.ReadFieldsFirst("0;2;4")
    @FunctionAnnotation.ReadFieldsSecond("1;3")
    private static class DummyTestCoGroupFunction1
            implements CoGroupFunction<
                    Tuple5<Integer, Long, String, Long, Integer>,
                    Tuple5<Integer, Long, String, Long, Integer>,
                    Tuple5<Integer, Long, String, Long, Integer>> {

        @Override
        public void coGroup(
                Iterable<Tuple5<Integer, Long, String, Long, Integer>> first,
                Iterable<Tuple5<Integer, Long, String, Long, Integer>> second,
                Collector<Tuple5<Integer, Long, String, Long, Integer>> out)
                throws Exception {}
    }

    @FunctionAnnotation.ReadFieldsFirst("0;1;2")
    private static class DummyTestCoGroupFunction2
            implements CoGroupFunction<
                    Tuple5<Integer, Long, String, Long, Integer>,
                    Tuple5<Integer, Long, String, Long, Integer>,
                    Tuple5<Integer, Long, String, Long, Integer>> {

        @Override
        public void coGroup(
                Iterable<Tuple5<Integer, Long, String, Long, Integer>> first,
                Iterable<Tuple5<Integer, Long, String, Long, Integer>> second,
                Collector<Tuple5<Integer, Long, String, Long, Integer>> out)
                throws Exception {}
    }
}
