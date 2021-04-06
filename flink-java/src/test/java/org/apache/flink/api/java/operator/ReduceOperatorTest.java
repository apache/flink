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

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;

/** Tests for {@link DataSet#reduce(ReduceFunction)}. */
@SuppressWarnings("serial")
public class ReduceOperatorTest {

    private final List<Tuple5<Integer, Long, String, Long, Integer>> emptyTupleData =
            new ArrayList<Tuple5<Integer, Long, String, Long, Integer>>();

    private final TupleTypeInfo<Tuple5<Integer, Long, String, Long, Integer>> tupleTypeInfo =
            new TupleTypeInfo<Tuple5<Integer, Long, String, Long, Integer>>(
                    BasicTypeInfo.INT_TYPE_INFO,
                    BasicTypeInfo.LONG_TYPE_INFO,
                    BasicTypeInfo.STRING_TYPE_INFO,
                    BasicTypeInfo.LONG_TYPE_INFO,
                    BasicTypeInfo.INT_TYPE_INFO);

    @Test
    public void testSemanticPropsWithKeySelector1() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs =
                env.fromCollection(emptyTupleData, tupleTypeInfo);

        ReduceOperator<Tuple5<Integer, Long, String, Long, Integer>> reduceOp =
                tupleDs.groupBy(new DummyTestKeySelector()).reduce(new DummyReduceFunction1());

        SemanticProperties semProps = reduceOp.getSemanticProperties();

        assertTrue(semProps.getForwardingTargetFields(0, 0).size() == 0);
        assertTrue(semProps.getForwardingTargetFields(0, 1).size() == 0);
        assertTrue(semProps.getForwardingTargetFields(0, 2).size() == 1);
        assertTrue(semProps.getForwardingTargetFields(0, 2).contains(4));
        assertTrue(semProps.getForwardingTargetFields(0, 3).size() == 2);
        assertTrue(semProps.getForwardingTargetFields(0, 3).contains(1));
        assertTrue(semProps.getForwardingTargetFields(0, 3).contains(3));
        assertTrue(semProps.getForwardingTargetFields(0, 4).size() == 1);
        assertTrue(semProps.getForwardingTargetFields(0, 4).contains(2));
        assertTrue(semProps.getForwardingTargetFields(0, 5).size() == 0);
        assertTrue(semProps.getForwardingTargetFields(0, 6).size() == 0);

        assertTrue(semProps.getForwardingSourceField(0, 0) < 0);
        assertTrue(semProps.getForwardingSourceField(0, 1) == 3);
        assertTrue(semProps.getForwardingSourceField(0, 2) == 4);
        assertTrue(semProps.getForwardingSourceField(0, 3) == 3);
        assertTrue(semProps.getForwardingSourceField(0, 4) == 2);

        assertTrue(semProps.getReadFields(0).size() == 3);
        assertTrue(semProps.getReadFields(0).contains(2));
        assertTrue(semProps.getReadFields(0).contains(5));
        assertTrue(semProps.getReadFields(0).contains(6));
    }

    @Test
    public void testSemanticPropsWithKeySelector2() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs =
                env.fromCollection(emptyTupleData, tupleTypeInfo);

        ReduceOperator<Tuple5<Integer, Long, String, Long, Integer>> reduceOp =
                tupleDs.groupBy(new DummyTestKeySelector())
                        .reduce(new DummyReduceFunction2())
                        .withForwardedFields("0->4;1;1->3;2");

        SemanticProperties semProps = reduceOp.getSemanticProperties();

        assertTrue(semProps.getForwardingTargetFields(0, 0).size() == 0);
        assertTrue(semProps.getForwardingTargetFields(0, 1).size() == 0);
        assertTrue(semProps.getForwardingTargetFields(0, 2).size() == 1);
        assertTrue(semProps.getForwardingTargetFields(0, 2).contains(4));
        assertTrue(semProps.getForwardingTargetFields(0, 3).size() == 2);
        assertTrue(semProps.getForwardingTargetFields(0, 3).contains(1));
        assertTrue(semProps.getForwardingTargetFields(0, 3).contains(3));
        assertTrue(semProps.getForwardingTargetFields(0, 4).size() == 1);
        assertTrue(semProps.getForwardingTargetFields(0, 4).contains(2));
        assertTrue(semProps.getForwardingTargetFields(0, 5).size() == 0);
        assertTrue(semProps.getForwardingTargetFields(0, 6).size() == 0);

        assertTrue(semProps.getForwardingSourceField(0, 0) < 0);
        assertTrue(semProps.getForwardingSourceField(0, 1) == 3);
        assertTrue(semProps.getForwardingSourceField(0, 2) == 4);
        assertTrue(semProps.getForwardingSourceField(0, 3) == 3);
        assertTrue(semProps.getForwardingSourceField(0, 4) == 2);

        assertTrue(semProps.getReadFields(0).size() == 3);
        assertTrue(semProps.getReadFields(0).contains(2));
        assertTrue(semProps.getReadFields(0).contains(5));
        assertTrue(semProps.getReadFields(0).contains(6));
    }

    @Test
    public void testSemanticPropsWithKeySelector3() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs =
                env.fromCollection(emptyTupleData, tupleTypeInfo);

        ReduceOperator<Tuple5<Integer, Long, String, Long, Integer>> reduceOp =
                tupleDs.groupBy(new DummyTestKeySelector())
                        .reduce(new DummyReduceFunction3())
                        .withForwardedFields("4->0;3;3->1;2");

        SemanticProperties semProps = reduceOp.getSemanticProperties();

        assertTrue(semProps.getForwardingTargetFields(0, 0).size() == 0);
        assertTrue(semProps.getForwardingTargetFields(0, 1).size() == 0);
        assertTrue(semProps.getForwardingTargetFields(0, 2).size() == 0);
        assertTrue(semProps.getForwardingTargetFields(0, 3).size() == 0);
        assertTrue(semProps.getForwardingTargetFields(0, 4).size() == 1);
        assertTrue(semProps.getForwardingTargetFields(0, 4).contains(2));
        assertTrue(semProps.getForwardingTargetFields(0, 5).size() == 2);
        assertTrue(semProps.getForwardingTargetFields(0, 5).contains(1));
        assertTrue(semProps.getForwardingTargetFields(0, 5).contains(3));
        assertTrue(semProps.getForwardingTargetFields(0, 6).size() == 1);
        assertTrue(semProps.getForwardingTargetFields(0, 6).contains(0));

        assertTrue(semProps.getForwardingSourceField(0, 0) == 6);
        assertTrue(semProps.getForwardingSourceField(0, 1) == 5);
        assertTrue(semProps.getForwardingSourceField(0, 2) == 4);
        assertTrue(semProps.getForwardingSourceField(0, 3) == 5);
        assertTrue(semProps.getForwardingSourceField(0, 4) < 0);

        assertTrue(semProps.getReadFields(0) == null);
    }

    @Test
    public void testSemanticPropsWithKeySelector4() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs =
                env.fromCollection(emptyTupleData, tupleTypeInfo);

        ReduceOperator<Tuple5<Integer, Long, String, Long, Integer>> reduceOp =
                tupleDs.groupBy(new DummyTestKeySelector()).reduce(new DummyReduceFunction4());

        SemanticProperties semProps = reduceOp.getSemanticProperties();

        assertTrue(semProps.getForwardingTargetFields(0, 0).size() == 0);
        assertTrue(semProps.getForwardingTargetFields(0, 1).size() == 0);
        assertTrue(semProps.getForwardingTargetFields(0, 2).size() == 1);
        assertTrue(semProps.getForwardingTargetFields(0, 2).contains(0));
        assertTrue(semProps.getForwardingTargetFields(0, 3).size() == 1);
        assertTrue(semProps.getForwardingTargetFields(0, 3).contains(1));
        assertTrue(semProps.getForwardingTargetFields(0, 4).size() == 0);
        assertTrue(semProps.getForwardingTargetFields(0, 5).size() == 1);
        assertTrue(semProps.getForwardingTargetFields(0, 5).contains(3));
        assertTrue(semProps.getForwardingTargetFields(0, 6).size() == 0);

        assertTrue(semProps.getForwardingSourceField(0, 0) == 2);
        assertTrue(semProps.getForwardingSourceField(0, 1) == 3);
        assertTrue(semProps.getForwardingSourceField(0, 2) < 0);
        assertTrue(semProps.getForwardingSourceField(0, 3) == 5);
        assertTrue(semProps.getForwardingSourceField(0, 4) < 0);

        assertTrue(semProps.getReadFields(0) == null);
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

    @FunctionAnnotation.ForwardedFields("0->4;1;1->3;2")
    @FunctionAnnotation.ReadFields("0;3;4")
    private static class DummyReduceFunction1
            implements ReduceFunction<Tuple5<Integer, Long, String, Long, Integer>> {
        @Override
        public Tuple5<Integer, Long, String, Long, Integer> reduce(
                Tuple5<Integer, Long, String, Long, Integer> v1,
                Tuple5<Integer, Long, String, Long, Integer> v2)
                throws Exception {
            return new Tuple5<Integer, Long, String, Long, Integer>();
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
            return new Tuple5<Integer, Long, String, Long, Integer>();
        }
    }

    private static class DummyReduceFunction3
            implements ReduceFunction<Tuple5<Integer, Long, String, Long, Integer>> {
        @Override
        public Tuple5<Integer, Long, String, Long, Integer> reduce(
                Tuple5<Integer, Long, String, Long, Integer> v1,
                Tuple5<Integer, Long, String, Long, Integer> v2)
                throws Exception {
            return new Tuple5<Integer, Long, String, Long, Integer>();
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
            return new Tuple5<Integer, Long, String, Long, Integer>();
        }
    }
}
