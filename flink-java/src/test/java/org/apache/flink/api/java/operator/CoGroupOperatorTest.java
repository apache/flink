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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

/** Tests for {@link DataSet#coGroup(DataSet)}. */
@SuppressWarnings("serial")
class CoGroupOperatorTest {

    // TUPLE DATA
    private static final List<Tuple5<Integer, Long, String, Long, Integer>> emptyTupleData =
            new ArrayList<>();

    private final TupleTypeInfo<Tuple5<Integer, Long, String, Long, Integer>> tupleTypeInfo =
            new TupleTypeInfo<>(
                    BasicTypeInfo.INT_TYPE_INFO,
                    BasicTypeInfo.LONG_TYPE_INFO,
                    BasicTypeInfo.STRING_TYPE_INFO,
                    BasicTypeInfo.LONG_TYPE_INFO,
                    BasicTypeInfo.INT_TYPE_INFO);

    private static final List<CustomType> customTypeData = new ArrayList<>();

    @BeforeAll
    static void insertCustomData() {
        customTypeData.add(new CustomType());
    }

    @Test
    void testCoGroupKeyFields1() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);

        // should work
        try {
            ds1.coGroup(ds2).where(0).equalTo(0);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    void testCoGroupKeyFields2() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);

        // should not work, incompatible cogroup key types
        assertThatThrownBy(() -> ds1.coGroup(ds2).where(0).equalTo(2))
                .isInstanceOf(InvalidProgramException.class);
    }

    @Test
    void testCoGroupKeyFields3() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);

        // should not work, incompatible number of cogroup keys
        assertThatThrownBy(() -> ds1.coGroup(ds2).where(0, 1).equalTo(2))
                .isInstanceOf(InvalidProgramException.class);
    }

    @Test
    void testCoGroupKeyFields4() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);

        // should not work, cogroup key out of range
        assertThatThrownBy(() -> ds1.coGroup(ds2).where(5).equalTo(0))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void testCoGroupKeyFields5() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);

        // should not work, negative key field position
        assertThatThrownBy(() -> ds1.coGroup(ds2).where(-1).equalTo(-1))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void testCoGroupKeyFields6() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);
        DataSet<CustomType> ds2 = env.fromCollection(customTypeData);

        // should not work, cogroup key fields on custom type
        assertThatThrownBy(() -> ds1.coGroup(ds2).where(4).equalTo(0))
                .isInstanceOf(InvalidProgramException.class);
    }

    @Test
    void testCoGroupKeyExpressions1() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<CustomType> ds1 = env.fromCollection(customTypeData);
        DataSet<CustomType> ds2 = env.fromCollection(customTypeData);

        // should work
        try {
            ds1.coGroup(ds2).where("myInt").equalTo("myInt");
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    void testCoGroupKeyExpressions2() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<CustomType> ds1 = env.fromCollection(customTypeData);
        DataSet<CustomType> ds2 = env.fromCollection(customTypeData);

        // should not work, incompatible cogroup key types
        assertThatThrownBy(() -> ds1.coGroup(ds2).where("myInt").equalTo("myString"))
                .isInstanceOf(InvalidProgramException.class);
    }

    @Test
    void testCoGroupKeyExpressions3() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<CustomType> ds1 = env.fromCollection(customTypeData);
        DataSet<CustomType> ds2 = env.fromCollection(customTypeData);

        // should not work, incompatible number of cogroup keys
        assertThatThrownBy(() -> ds1.coGroup(ds2).where("myInt", "myString").equalTo("myString"))
                .isInstanceOf(InvalidProgramException.class);
    }

    @Test
    void testCoGroupKeyExpressions4() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<CustomType> ds1 = env.fromCollection(customTypeData);
        DataSet<CustomType> ds2 = env.fromCollection(customTypeData);

        // should not work, cogroup key non-existent
        assertThatThrownBy(() -> ds1.coGroup(ds2).where("myNonExistent").equalTo("myInt"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testCoGroupKeyAtomicExpression1() {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<CustomType> ds1 = env.fromCollection(customTypeData);
        DataSet<Integer> ds2 = env.fromElements(0, 0, 1);

        ds1.coGroup(ds2).where("myInt").equalTo("*");
    }

    @Test
    void testCoGroupKeyAtomicExpression2() {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Integer> ds1 = env.fromElements(0, 0, 1);
        DataSet<CustomType> ds2 = env.fromCollection(customTypeData);

        ds1.coGroup(ds2).where("*").equalTo("myInt");
    }

    @Test
    void testCoGroupKeyAtomicInvalidExpression1() {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Integer> ds1 = env.fromElements(0, 0, 1);
        DataSet<CustomType> ds2 = env.fromCollection(customTypeData);
        assertThatThrownBy(() -> ds1.coGroup(ds2).where("*", "invalidKey"))
                .isInstanceOf(InvalidProgramException.class);
    }

    @Test
    void testCoGroupKeyAtomicInvalidExpression2() {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Integer> ds1 = env.fromElements(0, 0, 1);
        DataSet<CustomType> ds2 = env.fromCollection(customTypeData);

        assertThatThrownBy(() -> ds1.coGroup(ds2).where("invalidKey"))
                .isInstanceOf(InvalidProgramException.class);
    }

    @Test
    void testCoGroupKeyAtomicInvalidExpression3() {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<CustomType> ds1 = env.fromCollection(customTypeData);
        DataSet<Integer> ds2 = env.fromElements(0, 0, 1);

        assertThatThrownBy(() -> ds1.coGroup(ds2).where("myInt").equalTo("invalidKey"))
                .isInstanceOf(InvalidProgramException.class);
    }

    @Test
    void testCoGroupKeyAtomicInvalidExpression4() {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<CustomType> ds1 = env.fromCollection(customTypeData);
        DataSet<Integer> ds2 = env.fromElements(0, 0, 1);

        assertThatThrownBy(() -> ds1.coGroup(ds2).where("myInt").equalTo("*", "invalidKey"))
                .isInstanceOf(InvalidProgramException.class);
    }

    @Test
    void testCoGroupKeyAtomicInvalidExpression5() {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<ArrayList<Integer>> ds1 = env.fromElements(new ArrayList<>());
        DataSet<Integer> ds2 = env.fromElements(0, 0, 0);

        assertThatThrownBy(() -> ds1.coGroup(ds2).where("*"))
                .isInstanceOf(InvalidProgramException.class);
    }

    @Test
    void testCoGroupKeyAtomicInvalidExpression6() {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Integer> ds1 = env.fromElements(0, 0, 0);
        DataSet<ArrayList<Integer>> ds2 = env.fromElements(new ArrayList<>());

        assertThatThrownBy(() -> ds1.coGroup(ds2).where("*").equalTo("*"))
                .isInstanceOf(InvalidProgramException.class);
    }

    @Test
    void testCoGroupKeyExpressions1Nested() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<CustomType> ds1 = env.fromCollection(customTypeData);
        DataSet<CustomType> ds2 = env.fromCollection(customTypeData);

        // should work
        try {
            ds1.coGroup(ds2).where("nested.myInt").equalTo("nested.myInt");
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    void testCoGroupKeyExpressions2Nested() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<CustomType> ds1 = env.fromCollection(customTypeData);
        DataSet<CustomType> ds2 = env.fromCollection(customTypeData);

        // should not work, incompatible cogroup key types
        assertThatThrownBy(() -> ds1.coGroup(ds2).where("nested.myInt").equalTo("nested.myString"))
                .isInstanceOf(InvalidProgramException.class);
    }

    @Test
    void testCoGroupKeyExpressions3Nested() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<CustomType> ds1 = env.fromCollection(customTypeData);
        DataSet<CustomType> ds2 = env.fromCollection(customTypeData);

        // should not work, incompatible number of cogroup keys
        assertThatThrownBy(
                        () ->
                                ds1.coGroup(ds2)
                                        .where("nested.myInt", "nested.myString")
                                        .equalTo("nested.myString"))
                .isInstanceOf(InvalidProgramException.class);
    }

    @Test
    void testCoGroupKeyExpressions4Nested() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<CustomType> ds1 = env.fromCollection(customTypeData);
        DataSet<CustomType> ds2 = env.fromCollection(customTypeData);

        // should not work, cogroup key non-existent
        assertThatThrownBy(
                        () ->
                                ds1.coGroup(ds2)
                                        .where("nested.myNonExistent")
                                        .equalTo("nested.myInt"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testCoGroupKeySelectors1() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<CustomType> ds1 = env.fromCollection(customTypeData);
        DataSet<CustomType> ds2 = env.fromCollection(customTypeData);

        // should work
        try {
            ds1.coGroup(ds2)
                    .where((KeySelector<CustomType, Long>) value -> value.myLong)
                    .equalTo((KeySelector<CustomType, Long>) value -> value.myLong);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    void testCoGroupKeyMixing1() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<CustomType> ds1 = env.fromCollection(customTypeData);
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);

        // should work
        try {
            ds1.coGroup(ds2)
                    .where((KeySelector<CustomType, Long>) value -> value.myLong)
                    .equalTo(3);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    void testCoGroupKeyMixing2() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);
        DataSet<CustomType> ds2 = env.fromCollection(customTypeData);

        // should work
        try {
            ds1.coGroup(ds2)
                    .where(3)
                    .equalTo((KeySelector<CustomType, Long>) value -> value.myLong);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    void testCoGroupKeyMixing3() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);
        DataSet<CustomType> ds2 = env.fromCollection(customTypeData);

        // should not work, incompatible types
        assertThatThrownBy(
                        () ->
                                ds1.coGroup(ds2)
                                        .where(2)
                                        .equalTo(
                                                (KeySelector<CustomType, Long>)
                                                        value -> value.myLong))
                .isInstanceOf(InvalidProgramException.class);
    }

    @Test
    void testCoGroupKeyMixing4() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);
        DataSet<CustomType> ds2 = env.fromCollection(customTypeData);

        // should not work, more than one key field position
        assertThatThrownBy(
                        () ->
                                ds1.coGroup(ds2)
                                        .where(1, 3)
                                        .equalTo(
                                                (KeySelector<CustomType, Long>)
                                                        value -> value.myLong))
                .isInstanceOf(InvalidProgramException.class);
    }

    @Test
    void testSemanticPropsWithKeySelector1() {

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

        assertThat(semProps.getForwardingTargetFields(0, 0)).isEmpty();
        assertThat(semProps.getForwardingTargetFields(0, 1)).isEmpty();
        assertThat(semProps.getForwardingTargetFields(0, 2)).containsExactly(4);
        assertThat(semProps.getForwardingTargetFields(0, 3)).containsExactly(1, 3);
        assertThat(semProps.getForwardingTargetFields(0, 4)).isEmpty();
        assertThat(semProps.getForwardingTargetFields(0, 5)).isEmpty();
        assertThat(semProps.getForwardingTargetFields(0, 6)).isEmpty();

        assertThat(semProps.getForwardingTargetFields(1, 0)).isEmpty();
        assertThat(semProps.getForwardingTargetFields(1, 1)).isEmpty();
        assertThat(semProps.getForwardingTargetFields(1, 2)).isEmpty();
        assertThat(semProps.getForwardingTargetFields(1, 3)).isEmpty();
        assertThat(semProps.getForwardingTargetFields(1, 4)).containsExactly(2);
        assertThat(semProps.getForwardingTargetFields(1, 5)).isEmpty();
        assertThat(semProps.getForwardingTargetFields(1, 6)).containsExactly(0);

        assertThat(semProps.getReadFields(0)).containsExactly(2, 4, 6);
        assertThat(semProps.getReadFields(1)).containsExactly(5, 3);
    }

    @Test
    void testSemanticPropsWithKeySelector2() {

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

        assertThat(semProps.getForwardingTargetFields(0, 0)).isEmpty();
        assertThat(semProps.getForwardingTargetFields(0, 1)).isEmpty();
        assertThat(semProps.getForwardingTargetFields(0, 2)).isEmpty();
        assertThat(semProps.getForwardingTargetFields(0, 3)).isEmpty();
        assertThat(semProps.getForwardingTargetFields(0, 4)).containsExactly(2);
        assertThat(semProps.getForwardingTargetFields(0, 5)).isEmpty();
        assertThat(semProps.getForwardingTargetFields(0, 6)).containsExactly(0);

        assertThat(semProps.getForwardingTargetFields(1, 0)).isEmpty();
        assertThat(semProps.getForwardingTargetFields(1, 1)).isEmpty();
        assertThat(semProps.getForwardingTargetFields(1, 2)).containsExactly(4);
        assertThat(semProps.getForwardingTargetFields(1, 3)).containsExactly(1, 3);
        assertThat(semProps.getForwardingTargetFields(1, 4)).isEmpty();
        assertThat(semProps.getForwardingTargetFields(1, 5)).isEmpty();
        assertThat(semProps.getForwardingTargetFields(1, 6)).isEmpty();

        assertThat(semProps.getReadFields(0)).containsExactly(2, 3, 4);

        assertThat(semProps.getReadFields(1)).isNull();
    }

    private static class DummyTestKeySelector
            implements KeySelector<
                    Tuple5<Integer, Long, String, Long, Integer>, Tuple2<Long, Integer>> {
        @Override
        public Tuple2<Long, Integer> getKey(Tuple5<Integer, Long, String, Long, Integer> value)
                throws Exception {
            return new Tuple2<>();
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
