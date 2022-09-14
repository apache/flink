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
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.operators.SemanticProperties;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

/** Tests for {@link DataSet#join(DataSet)}. */
@SuppressWarnings("serial")
class JoinOperatorTest {

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
    // TUPLE DATA with nested Tuple2
    private static final List<Tuple5<Tuple2<Integer, String>, Long, String, Long, Integer>>
            emptyNestedTupleData = new ArrayList<>();

    private final TupleTypeInfo<Tuple5<Tuple2<Integer, String>, Long, String, Long, Integer>>
            nestedTupleTypeInfo =
                    new TupleTypeInfo<>(
                            new TupleTypeInfo<Tuple2<Integer, String>>(
                                    BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO),
                            BasicTypeInfo.LONG_TYPE_INFO,
                            BasicTypeInfo.STRING_TYPE_INFO,
                            BasicTypeInfo.LONG_TYPE_INFO,
                            BasicTypeInfo.INT_TYPE_INFO);

    // TUPLE DATA with nested CustomType
    private static final List<Tuple5<CustomType, Long, String, Long, Integer>>
            emptyNestedCustomTupleData = new ArrayList<>();

    private final TupleTypeInfo<Tuple5<CustomType, Long, String, Long, Integer>>
            nestedCustomTupleTypeInfo =
                    new TupleTypeInfo<>(
                            TypeExtractor.getForClass(CustomType.class),
                            BasicTypeInfo.LONG_TYPE_INFO,
                            BasicTypeInfo.STRING_TYPE_INFO,
                            BasicTypeInfo.LONG_TYPE_INFO,
                            BasicTypeInfo.INT_TYPE_INFO);

    private static final List<CustomTypeWithTuple> customTypeWithTupleData = new ArrayList<>();
    private static final List<CustomType> customTypeData = new ArrayList<>();

    private static final List<NestedCustomType> customNestedTypeData = new ArrayList<>();

    @BeforeAll
    static void insertCustomData() {
        customTypeData.add(new CustomType());
        customTypeWithTupleData.add(new CustomTypeWithTuple());
        customNestedTypeData.add(new NestedCustomType());
    }

    @Test
    void testJoinKeyFields1() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);

        // should work
        try {
            ds1.join(ds2).where(0).equalTo(0);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    void testJoinKeyFields2() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);

        // should not work, incompatible join key types
        assertThatThrownBy(() -> ds1.join(ds2).where(0).equalTo(2))
                .isInstanceOf(InvalidProgramException.class);
    }

    @Test
    void testJoinKeyFields3() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);

        // should not work, incompatible number of join keys
        assertThatThrownBy(() -> ds1.join(ds2).where(0, 1).equalTo(2))
                .isInstanceOf(InvalidProgramException.class);
    }

    @Test
    void testJoinKeyFields4() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);

        // should not work, join key out of range
        assertThatThrownBy(() -> ds1.join(ds2).where(5).equalTo(0))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void testJoinKeyFields5() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);

        // should not work, negative key field position
        assertThatThrownBy(() -> ds1.join(ds2).where(-1).equalTo(-1))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void testJoinKeyFields6() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);
        DataSet<CustomType> ds2 = env.fromCollection(customTypeData);

        // should not work, join key fields on custom type
        assertThatThrownBy(() -> ds1.join(ds2).where(4).equalTo(0))
                .isInstanceOf(InvalidProgramException.class);
    }

    @Test
    void testJoinKeyExpressions1() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<CustomType> ds1 = env.fromCollection(customTypeData);
        DataSet<CustomType> ds2 = env.fromCollection(customTypeData);

        // should work
        try {
            ds1.join(ds2).where("myInt").equalTo("myInt");
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    void testJoinKeyExpressionsNested() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<NestedCustomType> ds1 = env.fromCollection(customNestedTypeData);
        DataSet<NestedCustomType> ds2 = env.fromCollection(customNestedTypeData);

        // should work
        try {
            ds1.join(ds2).where("myInt").equalTo("myInt");
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    void testJoinKeyExpressions2() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<CustomType> ds1 = env.fromCollection(customTypeData);
        DataSet<CustomType> ds2 = env.fromCollection(customTypeData);

        // should not work, incompatible join key types
        assertThatThrownBy(() -> ds1.join(ds2).where("myInt").equalTo("myString"))
                .isInstanceOf(InvalidProgramException.class);
    }

    @Test
    void testJoinKeyExpressions3() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<CustomType> ds1 = env.fromCollection(customTypeData);
        DataSet<CustomType> ds2 = env.fromCollection(customTypeData);

        // should not work, incompatible number of join keys
        assertThatThrownBy(() -> ds1.join(ds2).where("myInt", "myString").equalTo("myString"))
                .isInstanceOf(InvalidProgramException.class);
    }

    @Test
    void testJoinKeyExpressions4() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<CustomType> ds1 = env.fromCollection(customTypeData);
        DataSet<CustomType> ds2 = env.fromCollection(customTypeData);

        // should not work, join key non-existent
        assertThatThrownBy(() -> ds1.join(ds2).where("myNonExistent").equalTo("myInt"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** Test if mixed types of key selectors are properly working. */
    @Test
    void testJoinKeyMixedKeySelector() {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<CustomType> ds1 = env.fromCollection(customTypeData);
        DataSet<CustomType> ds2 = env.fromCollection(customTypeData);
        try {
            ds1.join(ds2)
                    .where("myInt")
                    .equalTo(
                            new KeySelector<CustomType, Integer>() {
                                @Override
                                public Integer getKey(CustomType value) throws Exception {
                                    return value.myInt;
                                }
                            });
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    void testJoinKeyMixedKeySelectorTurned() {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<CustomType> ds1 = env.fromCollection(customTypeData);
        DataSet<CustomType> ds2 = env.fromCollection(customTypeData);
        try {
            ds1.join(ds2)
                    .where(
                            new KeySelector<CustomType, Integer>() {
                                @Override
                                public Integer getKey(CustomType value) throws Exception {
                                    return value.myInt;
                                }
                            })
                    .equalTo("myInt");
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    void testJoinKeyMixedTupleIndex() {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);
        try {
            ds1.join(ds2).where("f0").equalTo(4);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    void testJoinKeyNestedTuples() {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Tuple2<Integer, String>, Long, String, Long, Integer>> ds1 =
                env.fromCollection(emptyNestedTupleData, nestedTupleTypeInfo);
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);
        try {
            ds1.join(ds2).where("f0.f0").equalTo(4);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    void testJoinKeyNestedTuplesWithCustom() {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<CustomType, Long, String, Long, Integer>> ds1 =
                env.fromCollection(emptyNestedCustomTupleData, nestedCustomTupleTypeInfo);
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);
        try {
            TypeInformation<?> t = ds1.join(ds2).where("f0.myInt").equalTo(4).getType();
            assertThat(t).as("not a composite type").isInstanceOf(CompositeType.class);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    void testJoinKeyWithCustomContainingTuple0() {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<CustomTypeWithTuple> ds1 = env.fromCollection(customTypeWithTupleData);
        DataSet<CustomTypeWithTuple> ds2 = env.fromCollection(customTypeWithTupleData);
        try {
            ds1.join(ds2).where("intByString.f0").equalTo("myInt");
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    void testJoinKeyWithCustomContainingTuple1() {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<CustomTypeWithTuple> ds1 = env.fromCollection(customTypeWithTupleData);
        DataSet<CustomTypeWithTuple> ds2 = env.fromCollection(customTypeWithTupleData);
        try {
            ds1.join(ds2).where("nested.myInt").equalTo("intByString.f0");
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    void testJoinKeyWithCustomContainingTuple2() {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<CustomTypeWithTuple> ds1 = env.fromCollection(customTypeWithTupleData);
        DataSet<CustomTypeWithTuple> ds2 = env.fromCollection(customTypeWithTupleData);
        try {
            ds1.join(ds2)
                    .where("nested.myInt", "myInt", "intByString.f1")
                    .equalTo("intByString.f0", "myInt", "myString");
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    void testJoinKeyNestedTuplesWrongType() {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Tuple2<Integer, String>, Long, String, Long, Integer>> ds1 =
                env.fromCollection(emptyNestedTupleData, nestedTupleTypeInfo);
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);
        assertThatThrownBy(
                        () -> {
                            ds1.join(ds2).where("f0.f1").equalTo(4); // f0.f1 is a String
                        })
                .isInstanceOf(InvalidProgramException.class);
    }

    @Test
    void testJoinKeyMixedTupleIndexTurned() {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);
        try {
            ds1.join(ds2).where(0).equalTo("f0");
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    void testJoinKeyMixedTupleIndexWrongType() {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);
        assertThatThrownBy(
                        () -> {
                            ds1.join(ds2)
                                    .where("f0")
                                    .equalTo(3); // 3 is of type long, so it should fail
                        })
                .isInstanceOf(InvalidProgramException.class);
    }

    @Test
    void testJoinKeyMixedTupleIndex2() {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<CustomType> ds1 = env.fromCollection(customTypeData);
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);
        try {
            ds1.join(ds2).where("myInt").equalTo(4);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    void testJoinKeyMixedWrong() {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<CustomType> ds1 = env.fromCollection(customTypeData);
        DataSet<CustomType> ds2 = env.fromCollection(customTypeData);
        // wrongly mix String and Integer
        assertThatThrownBy(
                        () ->
                                ds1.join(ds2)
                                        .where("myString")
                                        .equalTo(
                                                (KeySelector<CustomType, Integer>)
                                                        value -> value.myInt))
                .isInstanceOf(InvalidProgramException.class);
    }

    @Test
    void testJoinKeyExpressions1Nested() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<CustomType> ds1 = env.fromCollection(customTypeData);
        DataSet<CustomType> ds2 = env.fromCollection(customTypeData);

        // should work
        try {
            ds1.join(ds2).where("nested.myInt").equalTo("nested.myInt");
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    void testJoinKeyExpressions2Nested() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<CustomType> ds1 = env.fromCollection(customTypeData);
        DataSet<CustomType> ds2 = env.fromCollection(customTypeData);

        // should not work, incompatible join key types
        assertThatThrownBy(() -> ds1.join(ds2).where("nested.myInt").equalTo("nested.myString"))
                .isInstanceOf(InvalidProgramException.class);
    }

    @Test
    void testJoinKeyExpressions3Nested() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<CustomType> ds1 = env.fromCollection(customTypeData);
        DataSet<CustomType> ds2 = env.fromCollection(customTypeData);

        // should not work, incompatible number of join keys
        assertThatThrownBy(
                        () ->
                                ds1.join(ds2)
                                        .where("nested.myInt", "nested.myString")
                                        .equalTo("nested.myString"))
                .isInstanceOf(InvalidProgramException.class);
    }

    @Test
    void testJoinKeyExpressions4Nested() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<CustomType> ds1 = env.fromCollection(customTypeData);
        DataSet<CustomType> ds2 = env.fromCollection(customTypeData);

        // should not work, join key non-existent
        assertThatThrownBy(
                        () -> ds1.join(ds2).where("nested.myNonExistent").equalTo("nested.myInt"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testJoinKeySelectors1() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<CustomType> ds1 = env.fromCollection(customTypeData);
        DataSet<CustomType> ds2 = env.fromCollection(customTypeData);

        // should work
        try {
            ds1.join(ds2)
                    .where((KeySelector<CustomType, Long>) value -> value.myLong)
                    .equalTo((KeySelector<CustomType, Long>) value -> value.myLong);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    void testJoinKeyMixing1() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<CustomType> ds1 = env.fromCollection(customTypeData);
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);

        // should work
        try {
            ds1.join(ds2).where((KeySelector<CustomType, Long>) value -> value.myLong).equalTo(3);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    void testJoinKeyMixing2() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);
        DataSet<CustomType> ds2 = env.fromCollection(customTypeData);

        // should work
        try {
            ds1.join(ds2).where(3).equalTo((KeySelector<CustomType, Long>) value -> value.myLong);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    void testJoinKeyMixing3() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);
        DataSet<CustomType> ds2 = env.fromCollection(customTypeData);

        // should not work, incompatible types
        assertThatThrownBy(
                        () ->
                                ds1.join(ds2)
                                        .where(2)
                                        .equalTo(
                                                (KeySelector<CustomType, Long>)
                                                        value -> value.myLong))
                .isInstanceOf(InvalidProgramException.class);
    }

    @Test
    void testJoinKeyMixing4() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);
        DataSet<CustomType> ds2 = env.fromCollection(customTypeData);

        // should not work, more than one key field position
        assertThatThrownBy(
                        () ->
                                ds1.join(ds2)
                                        .where(1, 3)
                                        .equalTo(
                                                (KeySelector<CustomType, Long>)
                                                        value -> value.myLong))
                .isInstanceOf(InvalidProgramException.class);
    }

    @Test
    void testJoinKeyAtomic1() {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Integer> ds1 = env.fromElements(0, 0, 0);
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);

        ds1.join(ds2).where("*").equalTo(0);
    }

    @Test
    void testJoinKeyAtomic2() {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);
        DataSet<Integer> ds2 = env.fromElements(0, 0, 0);

        ds1.join(ds2).where(0).equalTo("*");
    }

    @Test
    void testJoinKeyInvalidAtomic1() {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Integer> ds1 = env.fromElements(0, 0, 0);
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);

        assertThatThrownBy(() -> ds1.join(ds2).where("*", "invalidKey"))
                .isInstanceOf(InvalidProgramException.class);
    }

    @Test
    void testJoinKeyInvalidAtomic2() {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);
        DataSet<Integer> ds2 = env.fromElements(0, 0, 0);

        assertThatThrownBy(() -> ds1.join(ds2).where(0).equalTo("*", "invalidKey"))
                .isInstanceOf(InvalidProgramException.class);
    }

    @Test
    void testJoinKeyInvalidAtomic3() {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Integer> ds1 = env.fromElements(0, 0, 0);
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);

        assertThatThrownBy(() -> ds1.join(ds2).where("invalidKey"))
                .isInstanceOf(InvalidProgramException.class);
    }

    @Test
    void testJoinKeyInvalidAtomic4() {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);
        DataSet<Integer> ds2 = env.fromElements(0, 0, 0);

        assertThatThrownBy(() -> ds1.join(ds2).where(0).equalTo("invalidKey"))
                .isInstanceOf(InvalidProgramException.class);
    }

    @Test
    void testJoinKeyInvalidAtomic5() {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<ArrayList<Integer>> ds1 = env.fromElements(new ArrayList<Integer>());
        DataSet<Integer> ds2 = env.fromElements(0, 0, 0);

        assertThatThrownBy(() -> ds1.join(ds2).where("*").equalTo("*"))
                .isInstanceOf(InvalidProgramException.class);
    }

    @Test
    void testJoinKeyInvalidAtomic6() {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Integer> ds1 = env.fromElements(0, 0, 0);
        DataSet<ArrayList<Integer>> ds2 = env.fromElements(new ArrayList<Integer>());

        assertThatThrownBy(() -> ds1.join(ds2).where("*").equalTo("*"))
                .isInstanceOf(InvalidProgramException.class);
    }

    @Test
    void testJoinProjection1() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);

        // should work
        try {
            ds1.join(ds2).where(0).equalTo(0).projectFirst(0);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    void testJoinProjection21() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);

        // should work
        try {
            ds1.join(ds2).where(0).equalTo(0).projectFirst(0);
        } catch (Exception e) {
            fail(e.getMessage());
        }

        // should not work: field index is out of bounds of input tuple
        try {
            ds1.join(ds2).where(0).equalTo(0).projectFirst(-1);
            fail(null);
        } catch (IndexOutOfBoundsException iob) {
            // we're good here
        } catch (Exception e) {
            fail(e.getMessage());
        }

        // should not work: field index is out of bounds of input tuple
        try {
            ds1.join(ds2).where(0).equalTo(0).project(9);
            fail(null);
        } catch (IndexOutOfBoundsException iob) {
            // we're good here
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    void testJoinProjection2() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);

        // should work
        try {
            ds1.join(ds2).where(0).equalTo(0).projectFirst(0, 3);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    void testJoinProjection3() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);

        // should work
        try {
            ds1.join(ds2).where(0).equalTo(0).projectFirst(0).projectSecond(3);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    void testJoinProjection4() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);

        // should work
        try {
            ds1.join(ds2)
                    .where(0)
                    .equalTo(0)
                    .projectFirst(0, 2)
                    .projectSecond(1, 4)
                    .projectFirst(1);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    void testJoinProjection5() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);

        // should work
        try {
            ds1.join(ds2)
                    .where(0)
                    .equalTo(0)
                    .projectSecond(0, 2)
                    .projectFirst(1, 4)
                    .projectFirst(1);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    void testJoinProjection6() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<CustomType> ds1 = env.fromCollection(customTypeData);
        DataSet<CustomType> ds2 = env.fromCollection(customTypeData);
        // should work
        try {
            ds1.join(ds2)
                    .where((KeySelector<CustomType, Long>) value -> value.myLong)
                    .equalTo((KeySelector<CustomType, Long>) value -> value.myLong)
                    .projectFirst()
                    .projectSecond();
        } catch (Exception e) {
            System.out.println("FAILED: " + e);
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    void testJoinProjection26() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<CustomType> ds1 = env.fromCollection(customTypeData);
        DataSet<CustomType> ds2 = env.fromCollection(customTypeData);
        // should work
        try {
            ds1.join(ds2)
                    .where((KeySelector<CustomType, Long>) value -> value.myLong)
                    .equalTo((KeySelector<CustomType, Long>) value -> value.myLong)
                    .projectFirst()
                    .projectSecond();
        } catch (Exception e) {
            System.out.println("FAILED: " + e);
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    void testJoinProjection7() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);

        // should work
        try {
            ds1.join(ds2).where(0).equalTo(0).projectSecond().projectFirst(1, 4);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    void testJoinProjection27() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);

        // should work
        try {
            ds1.join(ds2).where(0).equalTo(0).projectSecond().projectFirst(1, 4);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    void testJoinProjection8() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);

        // should not work, index out of range
        assertThatThrownBy(() -> ds1.join(ds2).where(0).equalTo(0).projectFirst(5))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void testJoinProjection28() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);

        // should not work, index out of range
        assertThatThrownBy(() -> ds1.join(ds2).where(0).equalTo(0).projectFirst(5))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void testJoinProjection9() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);

        // should not work, index out of range
        assertThatThrownBy(() -> ds1.join(ds2).where(0).equalTo(0).projectSecond(5))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void testJoinProjection29() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);

        // should not work, index out of range
        assertThatThrownBy(() -> ds1.join(ds2).where(0).equalTo(0).projectSecond(5))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    void testJoinProjection10() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);

        // should work
        ds1.join(ds2).where(0).equalTo(0).projectFirst(2);
    }

    @Test
    void testJoinProjection30() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);

        // should not work, type does not match
        assertThatThrownBy(() -> ds1.join(ds2).where(0).equalTo(0).projectFirst(-1))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    void testJoinProjection11() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);

        // should not work, type does not match
        ds1.join(ds2).where(0).equalTo(0).projectSecond(2);
    }

    void testJoinProjection12() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);

        // should  work
        ds1.join(ds2).where(0).equalTo(0).projectSecond(2).projectFirst(1);
    }

    @Test
    void testJoinProjection13() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);

        // should not work, index out of range
        assertThatThrownBy(() -> ds1.join(ds2).where(0).equalTo(0).projectSecond(0).projectFirst(5))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void testJoinProjection33() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);

        // should not work, index out of range
        assertThatThrownBy(
                        () -> ds1.join(ds2).where(0).equalTo(0).projectSecond(-1).projectFirst(3))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void testJoinProjection14() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);

        // should not work, index out of range
        assertThatThrownBy(() -> ds1.join(ds2).where(0).equalTo(0).projectFirst(0).projectSecond(5))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void testJoinProjection34() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds1 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> ds2 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);

        // should not work, index out of range
        assertThatThrownBy(
                        () -> ds1.join(ds2).where(0).equalTo(0).projectFirst(0).projectSecond(-1))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void testSemanticPropsWithKeySelector1() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs1 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs2 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);

        JoinOperator<?, ?, ?> joinOp =
                tupleDs1.join(tupleDs2)
                        .where(new DummyTestKeySelector())
                        .equalTo(new DummyTestKeySelector())
                        .with(new DummyTestJoinFunction1());

        SemanticProperties semProps = joinOp.getSemanticProperties();

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

        JoinOperator<?, ?, ?> joinOp =
                tupleDs1.join(tupleDs2)
                        .where(new DummyTestKeySelector())
                        .equalTo(new DummyTestKeySelector())
                        .with(new DummyTestJoinFunction2())
                        .withForwardedFieldsFirst("2;4->0")
                        .withForwardedFieldsSecond("0->4;1;1->3");

        SemanticProperties semProps = joinOp.getSemanticProperties();

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

    @Test
    void testSemanticPropsWithKeySelector3() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs1 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs2 =
                env.fromCollection(emptyTupleData, tupleTypeInfo);

        JoinOperator<?, ?, ? extends Tuple> joinOp =
                tupleDs1.join(tupleDs2)
                        .where(new DummyTestKeySelector())
                        .equalTo(new DummyTestKeySelector())
                        .projectFirst(2)
                        .projectSecond(0, 0, 3)
                        .projectFirst(0, 4)
                        .projectSecond(2);

        SemanticProperties semProps = joinOp.getSemanticProperties();

        assertThat(semProps.getForwardingTargetFields(0, 0)).isEmpty();
        assertThat(semProps.getForwardingTargetFields(0, 1)).isEmpty();
        assertThat(semProps.getForwardingTargetFields(0, 2)).containsExactly(4);
        assertThat(semProps.getForwardingTargetFields(0, 3)).isEmpty();
        assertThat(semProps.getForwardingTargetFields(0, 4)).containsExactly(0);
        assertThat(semProps.getForwardingTargetFields(0, 5)).isEmpty();
        assertThat(semProps.getForwardingTargetFields(0, 6)).containsExactly(5);

        assertThat(semProps.getForwardingTargetFields(1, 0)).isEmpty();
        assertThat(semProps.getForwardingTargetFields(1, 1)).isEmpty();
        assertThat(semProps.getForwardingTargetFields(1, 2)).containsExactly(1, 2);
        assertThat(semProps.getForwardingTargetFields(1, 3)).isEmpty();
        assertThat(semProps.getForwardingTargetFields(1, 4)).containsExactly(6);
        assertThat(semProps.getForwardingTargetFields(1, 5)).containsExactly(3);
        assertThat(semProps.getForwardingTargetFields(1, 6)).isEmpty();
    }

    /*
     * ####################################################################
     */

    /** Custom type for testing. */
    public static class Nested implements Serializable {

        private static final long serialVersionUID = 1L;

        public int myInt;

        public Nested() {}

        public Nested(int i, long l, String s) {
            myInt = i;
        }

        @Override
        public String toString() {
            return "" + myInt;
        }
    }

    /** Simple nested type (only basic types). */
    public static class NestedCustomType implements Serializable {

        private static final long serialVersionUID = 1L;

        public int myInt;
        public long myLong;
        public String myString;
        public Nested nest;

        public NestedCustomType() {}

        public NestedCustomType(int i, long l, String s) {
            myInt = i;
            myLong = l;
            myString = s;
        }

        @Override
        public String toString() {
            return myInt + "," + myLong + "," + myString + "," + nest;
        }
    }

    /** Custom type for testing. */
    public static class CustomType implements Serializable {

        private static final long serialVersionUID = 1L;

        public int myInt;
        public long myLong;
        public NestedCustomType nested;
        public String myString;
        public Object nothing;
        public List<String> countries;

        public CustomType() {}

        public CustomType(int i, long l, String s) {
            myInt = i;
            myLong = l;
            myString = s;
            countries = null;
            nested = new NestedCustomType(i, l, s);
        }

        @Override
        public String toString() {
            return myInt + "," + myLong + "," + myString;
        }
    }

    /** Custom type for testing. */
    public static class CustomTypeWithTuple implements Serializable {

        private static final long serialVersionUID = 1L;

        public int myInt;
        public long myLong;
        public NestedCustomType nested;
        public String myString;
        public Tuple2<Integer, String> intByString;

        public CustomTypeWithTuple() {}

        public CustomTypeWithTuple(int i, long l, String s) {
            myInt = i;
            myLong = l;
            myString = s;
            nested = new NestedCustomType(i, l, s);
            intByString = new Tuple2<>(i, s);
        }

        @Override
        public String toString() {
            return myInt + "," + myLong + "," + myString;
        }
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
    private static class DummyTestJoinFunction1
            implements JoinFunction<
                    Tuple5<Integer, Long, String, Long, Integer>,
                    Tuple5<Integer, Long, String, Long, Integer>,
                    Tuple5<Integer, Long, String, Long, Integer>> {
        @Override
        public Tuple5<Integer, Long, String, Long, Integer> join(
                Tuple5<Integer, Long, String, Long, Integer> first,
                Tuple5<Integer, Long, String, Long, Integer> second)
                throws Exception {
            return new Tuple5<>();
        }
    }

    @FunctionAnnotation.ReadFieldsFirst("0;1;2")
    private static class DummyTestJoinFunction2
            implements JoinFunction<
                    Tuple5<Integer, Long, String, Long, Integer>,
                    Tuple5<Integer, Long, String, Long, Integer>,
                    Tuple5<Integer, Long, String, Long, Integer>> {
        @Override
        public Tuple5<Integer, Long, String, Long, Integer> join(
                Tuple5<Integer, Long, String, Long, Integer> first,
                Tuple5<Integer, Long, String, Long, Integer> second)
                throws Exception {
            return new Tuple5<>();
        }
    }
}
