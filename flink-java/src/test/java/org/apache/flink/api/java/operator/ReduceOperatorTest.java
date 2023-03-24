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

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.operators.SemanticProperties;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.ReduceOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DataSet#reduce(ReduceFunction)}. */
class ReduceOperatorTest {

    private final List<Tuple5<Integer, Long, String, Long, Integer>> emptyTupleData =
            new ArrayList<>();

    private final TupleTypeInfo<Tuple5<Integer, Long, String, Long, Integer>> tupleTypeInfo =
            new TupleTypeInfo<>(
                    BasicTypeInfo.INT_TYPE_INFO,
                    BasicTypeInfo.LONG_TYPE_INFO,
                    BasicTypeInfo.STRING_TYPE_INFO,
                    BasicTypeInfo.LONG_TYPE_INFO,
                    BasicTypeInfo.INT_TYPE_INFO);

    @Test
    void testSemanticPropsWithKeySelector1() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs =
                env.fromCollection(emptyTupleData, tupleTypeInfo);

        ReduceOperator<Tuple5<Integer, Long, String, Long, Integer>> reduceOp =
                tupleDs.groupBy(new DummyTestKeySelector()).reduce(new DummyReduceFunction1());

        SemanticProperties semProps = reduceOp.getSemanticProperties();

        assertThat(semProps.getForwardingTargetFields(0, 0)).isEmpty();
        assertThat(semProps.getForwardingTargetFields(0, 1)).isEmpty();
        assertThat(semProps.getForwardingTargetFields(0, 2)).containsExactly(4);
        assertThat(semProps.getForwardingTargetFields(0, 3)).containsExactly(1, 3);
        assertThat(semProps.getForwardingTargetFields(0, 4)).containsExactly(2);
        assertThat(semProps.getForwardingTargetFields(0, 5)).isEmpty();
        assertThat(semProps.getForwardingTargetFields(0, 6)).isEmpty();

        assertThat(semProps.getForwardingSourceField(0, 0)).isLessThan(0);
        assertThat(semProps.getForwardingSourceField(0, 1)).isEqualTo(3);
        assertThat(semProps.getForwardingSourceField(0, 2)).isEqualTo(4);
        assertThat(semProps.getForwardingSourceField(0, 3)).isEqualTo(3);
        assertThat(semProps.getForwardingSourceField(0, 4)).isEqualTo(2);

        assertThat(semProps.getReadFields(0)).containsExactly(2, 5, 6);
    }

    @Test
    void testSemanticPropsWithKeySelector2() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs =
                env.fromCollection(emptyTupleData, tupleTypeInfo);

        ReduceOperator<Tuple5<Integer, Long, String, Long, Integer>> reduceOp =
                tupleDs.groupBy(new DummyTestKeySelector())
                        .reduce(new DummyReduceFunction2())
                        .withForwardedFields("0->4;1;1->3;2");

        SemanticProperties semProps = reduceOp.getSemanticProperties();

        assertThat(semProps.getForwardingTargetFields(0, 0)).isEmpty();
        assertThat(semProps.getForwardingTargetFields(0, 1)).isEmpty();
        assertThat(semProps.getForwardingTargetFields(0, 2)).containsExactly(4);
        assertThat(semProps.getForwardingTargetFields(0, 3)).containsExactly(1, 3);
        assertThat(semProps.getForwardingTargetFields(0, 4)).containsExactly(2);
        assertThat(semProps.getForwardingTargetFields(0, 5)).isEmpty();
        assertThat(semProps.getForwardingTargetFields(0, 6)).isEmpty();

        assertThat(semProps.getForwardingSourceField(0, 0)).isLessThan(0);
        assertThat(semProps.getForwardingSourceField(0, 1)).isEqualTo(3);
        assertThat(semProps.getForwardingSourceField(0, 2)).isEqualTo(4);
        assertThat(semProps.getForwardingSourceField(0, 3)).isEqualTo(3);
        assertThat(semProps.getForwardingSourceField(0, 4)).isEqualTo(2);

        assertThat(semProps.getReadFields(0)).containsExactly(2, 5, 6);
    }

    @Test
    void testSemanticPropsWithKeySelector3() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs =
                env.fromCollection(emptyTupleData, tupleTypeInfo);

        ReduceOperator<Tuple5<Integer, Long, String, Long, Integer>> reduceOp =
                tupleDs.groupBy(new DummyTestKeySelector())
                        .reduce(new DummyReduceFunction3())
                        .withForwardedFields("4->0;3;3->1;2");

        SemanticProperties semProps = reduceOp.getSemanticProperties();

        assertThat(semProps.getForwardingTargetFields(0, 0)).isEmpty();
        assertThat(semProps.getForwardingTargetFields(0, 1)).isEmpty();
        assertThat(semProps.getForwardingTargetFields(0, 2)).isEmpty();
        assertThat(semProps.getForwardingTargetFields(0, 3)).isEmpty();
        assertThat(semProps.getForwardingTargetFields(0, 4)).containsExactly(2);
        assertThat(semProps.getForwardingTargetFields(0, 5)).containsExactly(1, 3);
        assertThat(semProps.getForwardingTargetFields(0, 6)).containsExactly(0);

        assertThat(semProps.getForwardingSourceField(0, 0)).isEqualTo(6);
        assertThat(semProps.getForwardingSourceField(0, 1)).isEqualTo(5);
        assertThat(semProps.getForwardingSourceField(0, 2)).isEqualTo(4);
        assertThat(semProps.getForwardingSourceField(0, 3)).isEqualTo(5);
        assertThat(semProps.getForwardingSourceField(0, 4)).isLessThan(0);

        assertThat(semProps.getReadFields(0)).isNull();
    }

    @Test
    void testSemanticPropsWithKeySelector4() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs =
                env.fromCollection(emptyTupleData, tupleTypeInfo);

        ReduceOperator<Tuple5<Integer, Long, String, Long, Integer>> reduceOp =
                tupleDs.groupBy(new DummyTestKeySelector()).reduce(new DummyReduceFunction4());

        SemanticProperties semProps = reduceOp.getSemanticProperties();

        assertThat(semProps.getForwardingTargetFields(0, 0)).isEmpty();
        assertThat(semProps.getForwardingTargetFields(0, 1)).isEmpty();
        assertThat(semProps.getForwardingTargetFields(0, 2)).containsExactly(0);
        assertThat(semProps.getForwardingTargetFields(0, 3)).containsExactly(1);
        assertThat(semProps.getForwardingTargetFields(0, 4)).isEmpty();
        assertThat(semProps.getForwardingTargetFields(0, 5)).containsExactly(3);
        assertThat(semProps.getForwardingTargetFields(0, 6)).isEmpty();

        assertThat(semProps.getForwardingSourceField(0, 0)).isEqualTo(2);
        assertThat(semProps.getForwardingSourceField(0, 1)).isEqualTo(3);
        assertThat(semProps.getForwardingSourceField(0, 2)).isLessThan(0);
        assertThat(semProps.getForwardingSourceField(0, 3)).isEqualTo(5);
        assertThat(semProps.getForwardingSourceField(0, 4)).isLessThan(0);

        assertThat(semProps.getReadFields(0)).isNull();
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

    @FunctionAnnotation.ForwardedFields("0->4;1;1->3;2")
    @FunctionAnnotation.ReadFields("0;3;4")
    private static class DummyReduceFunction1
            implements ReduceFunction<Tuple5<Integer, Long, String, Long, Integer>> {
        @Override
        public Tuple5<Integer, Long, String, Long, Integer> reduce(
                Tuple5<Integer, Long, String, Long, Integer> v1,
                Tuple5<Integer, Long, String, Long, Integer> v2)
                throws Exception {
            return new Tuple5<>();
        }
    }

    @FunctionAnnotation.ReadFields("0;3;4")
    private static class DummyReduceFunction2
            implements ReduceFunction<Tuple5<Integer, Long, String, Long, Integer>> {
        @Override
        public Tuple5<Integer, Long, String, Long, Integer> reduce(
                Tuple5<Integer, Long, String, Long, Integer> v1,
                Tuple5<Integer, Long, String, Long, Integer> v2)
                throws Exception {
            return new Tuple5<>();
        }
    }

    private static class DummyReduceFunction3
            implements ReduceFunction<Tuple5<Integer, Long, String, Long, Integer>> {
        @Override
        public Tuple5<Integer, Long, String, Long, Integer> reduce(
                Tuple5<Integer, Long, String, Long, Integer> v1,
                Tuple5<Integer, Long, String, Long, Integer> v2)
                throws Exception {
            return new Tuple5<>();
        }
    }

    @FunctionAnnotation.NonForwardedFields("2;4")
    private static class DummyReduceFunction4
            implements ReduceFunction<Tuple5<Integer, Long, String, Long, Integer>> {
        @Override
        public Tuple5<Integer, Long, String, Long, Integer> reduce(
                Tuple5<Integer, Long, String, Long, Integer> v1,
                Tuple5<Integer, Long, String, Long, Integer> v2)
                throws Exception {
            return new Tuple5<>();
        }
    }
}
