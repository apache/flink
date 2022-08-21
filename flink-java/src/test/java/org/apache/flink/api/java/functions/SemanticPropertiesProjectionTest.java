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

package org.apache.flink.api.java.functions;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.operators.DualInputSemanticProperties;
import org.apache.flink.api.common.operators.GenericDataSinkBase;
import org.apache.flink.api.common.operators.SingleInputSemanticProperties;
import org.apache.flink.api.common.operators.base.CrossOperatorBase;
import org.apache.flink.api.common.operators.base.InnerJoinOperatorBase;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.operators.translation.PlanProjectOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for semantic properties of projected fields. */
class SemanticPropertiesProjectionTest {

    final List<Tuple5<Integer, Long, String, Long, Integer>> emptyTupleData = new ArrayList<>();

    final TupleTypeInfo<Tuple5<Integer, Long, String, Long, Integer>> tupleTypeInfo =
            new TupleTypeInfo<>(
                    BasicTypeInfo.INT_TYPE_INFO,
                    BasicTypeInfo.LONG_TYPE_INFO,
                    BasicTypeInfo.STRING_TYPE_INFO,
                    BasicTypeInfo.LONG_TYPE_INFO,
                    BasicTypeInfo.INT_TYPE_INFO);

    final List<Tuple4<Integer, Tuple3<String, Integer, Long>, Tuple2<Long, Long>, String>>
            emptyNestedTupleData = new ArrayList<>();

    final TupleTypeInfo<Tuple4<Integer, Tuple3<String, Integer, Long>, Tuple2<Long, Long>, String>>
            nestedTupleTypeInfo =
                    new TupleTypeInfo<>(
                            BasicTypeInfo.INT_TYPE_INFO,
                            new TupleTypeInfo<Tuple3<String, Integer, Long>>(
                                    BasicTypeInfo.STRING_TYPE_INFO,
                                    BasicTypeInfo.INT_TYPE_INFO,
                                    BasicTypeInfo.LONG_TYPE_INFO),
                            new TupleTypeInfo<Tuple2<Long, Long>>(
                                    BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO),
                            BasicTypeInfo.STRING_TYPE_INFO);

    @Test
    void testProjectionSemProps1() {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs =
                env.fromCollection(emptyTupleData, tupleTypeInfo);

        tupleDs.project(1, 3, 2, 0, 3).output(new DiscardingOutputFormat<>());

        Plan plan = env.createProgramPlan();

        GenericDataSinkBase<?> sink = plan.getDataSinks().iterator().next();
        PlanProjectOperator<?, ?> projectOperator = ((PlanProjectOperator<?, ?>) sink.getInput());

        SingleInputSemanticProperties props = projectOperator.getSemanticProperties();

        assertThat(props.getForwardingTargetFields(0, 0)).containsExactly(3);
        assertThat(props.getForwardingTargetFields(0, 1)).containsExactly(0);
        assertThat(props.getForwardingTargetFields(0, 2)).containsExactly(2);
        assertThat(props.getForwardingTargetFields(0, 3)).containsExactly(4, 1);
    }

    @Test
    void testProjectionSemProps2() {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple4<Integer, Tuple3<String, Integer, Long>, Tuple2<Long, Long>, String>>
                tupleDs = env.fromCollection(emptyNestedTupleData, nestedTupleTypeInfo);

        tupleDs.project(2, 3, 1, 2).output(new DiscardingOutputFormat<>());

        Plan plan = env.createProgramPlan();

        GenericDataSinkBase<?> sink = plan.getDataSinks().iterator().next();
        PlanProjectOperator<?, ?> projectOperator = ((PlanProjectOperator<?, ?>) sink.getInput());

        SingleInputSemanticProperties props = projectOperator.getSemanticProperties();

        assertThat(props.getForwardingTargetFields(0, 0)).isEmpty();
        assertThat(props.getForwardingTargetFields(0, 1)).containsExactly(3);
        assertThat(props.getForwardingTargetFields(0, 2)).containsExactly(4);
        assertThat(props.getForwardingTargetFields(0, 3)).containsExactly(5);
        assertThat(props.getForwardingTargetFields(0, 4)).containsExactly(0, 6);
        assertThat(props.getForwardingTargetFields(0, 5)).containsExactly(1, 7);
        assertThat(props.getForwardingTargetFields(0, 6)).containsExactly(2);
    }

    @Test
    void testJoinProjectionSemProps1() {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs =
                env.fromCollection(emptyTupleData, tupleTypeInfo);

        tupleDs.join(tupleDs)
                .where(0)
                .equalTo(0)
                .projectFirst(2, 3)
                .projectSecond(1, 4)
                .output(new DiscardingOutputFormat<>());

        Plan plan = env.createProgramPlan();

        GenericDataSinkBase<?> sink = plan.getDataSinks().iterator().next();
        InnerJoinOperatorBase<?, ?, ?, ?> projectJoinOperator =
                ((InnerJoinOperatorBase<?, ?, ?, ?>) sink.getInput());

        DualInputSemanticProperties props = projectJoinOperator.getSemanticProperties();

        assertThat(props.getForwardingTargetFields(0, 2)).containsExactly(0);
        assertThat(props.getForwardingTargetFields(0, 3)).containsExactly(1);
        assertThat(props.getForwardingTargetFields(1, 1)).containsExactly(2);
        assertThat(props.getForwardingTargetFields(1, 4)).containsExactly(3);
    }

    @Test
    void testJoinProjectionSemProps2() {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple4<Integer, Tuple3<String, Integer, Long>, Tuple2<Long, Long>, String>>
                tupleDs = env.fromCollection(emptyNestedTupleData, nestedTupleTypeInfo);

        tupleDs.join(tupleDs)
                .where(0)
                .equalTo(0)
                .projectFirst(2, 0)
                .projectSecond(1, 3)
                .output(new DiscardingOutputFormat<>());

        Plan plan = env.createProgramPlan();

        GenericDataSinkBase<?> sink = plan.getDataSinks().iterator().next();
        InnerJoinOperatorBase<?, ?, ?, ?> projectJoinOperator =
                ((InnerJoinOperatorBase<?, ?, ?, ?>) sink.getInput());

        DualInputSemanticProperties props = projectJoinOperator.getSemanticProperties();

        assertThat(props.getForwardingTargetFields(0, 0)).containsExactly(2);
        assertThat(props.getForwardingTargetFields(0, 1)).isEmpty();
        assertThat(props.getForwardingTargetFields(0, 2)).isEmpty();
        assertThat(props.getForwardingTargetFields(0, 3)).isEmpty();
        assertThat(props.getForwardingTargetFields(0, 4)).containsExactly(0);
        assertThat(props.getForwardingTargetFields(0, 5)).containsExactly(1);
        assertThat(props.getForwardingTargetFields(0, 6)).isEmpty();

        assertThat(props.getForwardingTargetFields(1, 0)).isEmpty();
        assertThat(props.getForwardingTargetFields(1, 1)).containsExactly(3);
        assertThat(props.getForwardingTargetFields(1, 2)).containsExactly(4);
        assertThat(props.getForwardingTargetFields(1, 3)).containsExactly(5);
        assertThat(props.getForwardingTargetFields(1, 4)).isEmpty();
        assertThat(props.getForwardingTargetFields(1, 5)).isEmpty();
        assertThat(props.getForwardingTargetFields(1, 6)).containsExactly(6);
    }

    @Test
    void testCrossProjectionSemProps1() {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs =
                env.fromCollection(emptyTupleData, tupleTypeInfo);

        tupleDs.cross(tupleDs)
                .projectFirst(2, 3)
                .projectSecond(1, 4)
                .output(new DiscardingOutputFormat<>());

        Plan plan = env.createProgramPlan();

        GenericDataSinkBase<?> sink = plan.getDataSinks().iterator().next();
        CrossOperatorBase<?, ?, ?, ?> projectCrossOperator =
                ((CrossOperatorBase<?, ?, ?, ?>) sink.getInput());

        DualInputSemanticProperties props = projectCrossOperator.getSemanticProperties();

        assertThat(props.getForwardingTargetFields(0, 2)).containsExactly(0);
        assertThat(props.getForwardingTargetFields(0, 3)).containsExactly(1);
        assertThat(props.getForwardingTargetFields(1, 1)).containsExactly(2);
        assertThat(props.getForwardingTargetFields(1, 4)).containsExactly(3);
    }

    @Test
    void testCrossProjectionSemProps2() {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple4<Integer, Tuple3<String, Integer, Long>, Tuple2<Long, Long>, String>>
                tupleDs = env.fromCollection(emptyNestedTupleData, nestedTupleTypeInfo);

        tupleDs.cross(tupleDs)
                .projectFirst(2, 0)
                .projectSecond(1, 3)
                .output(new DiscardingOutputFormat<>());

        Plan plan = env.createProgramPlan();

        GenericDataSinkBase<?> sink = plan.getDataSinks().iterator().next();
        CrossOperatorBase<?, ?, ?, ?> projectCrossOperator =
                ((CrossOperatorBase<?, ?, ?, ?>) sink.getInput());

        DualInputSemanticProperties props = projectCrossOperator.getSemanticProperties();

        assertThat(props.getForwardingTargetFields(0, 0)).containsExactly(2);
        assertThat(props.getForwardingTargetFields(0, 1)).isEmpty();
        assertThat(props.getForwardingTargetFields(0, 2)).isEmpty();
        assertThat(props.getForwardingTargetFields(0, 3)).isEmpty();
        assertThat(props.getForwardingTargetFields(0, 4)).containsExactly(0);
        assertThat(props.getForwardingTargetFields(0, 5)).containsExactly(1);
        assertThat(props.getForwardingTargetFields(0, 6)).isEmpty();

        assertThat(props.getForwardingTargetFields(1, 0)).isEmpty();
        assertThat(props.getForwardingTargetFields(1, 1)).containsExactly(3);
        assertThat(props.getForwardingTargetFields(1, 2)).containsExactly(4);
        assertThat(props.getForwardingTargetFields(1, 3)).containsExactly(5);
        assertThat(props.getForwardingTargetFields(1, 4)).isEmpty();
        assertThat(props.getForwardingTargetFields(1, 5)).isEmpty();
        assertThat(props.getForwardingTargetFields(1, 6)).containsExactly(6);
    }
}
