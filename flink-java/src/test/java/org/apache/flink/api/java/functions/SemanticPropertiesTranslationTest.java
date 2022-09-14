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
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.DualInputSemanticProperties;
import org.apache.flink.api.common.operators.GenericDataSinkBase;
import org.apache.flink.api.common.operators.SemanticProperties;
import org.apache.flink.api.common.operators.SingleInputSemanticProperties;
import org.apache.flink.api.common.operators.base.InnerJoinOperatorBase;
import org.apache.flink.api.common.operators.base.MapOperatorBase;
import org.apache.flink.api.common.operators.util.FieldSet;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsFirst;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsSecond;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * This is a minimal test to verify that semantic annotations are evaluated against the type
 * information properly translated correctly to the common data flow API.
 */
@SuppressWarnings("serial")
class SemanticPropertiesTranslationTest {

    @Test
    void testUnaryFunctionWildcardForwardedAnnotation() {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        @SuppressWarnings("unchecked")
        DataSet<Tuple3<Long, String, Integer>> input =
                env.fromElements(new Tuple3<>(3L, "test", 42));
        input.map(new WildcardForwardedMapper<>()).output(new DiscardingOutputFormat<>());
        Plan plan = env.createProgramPlan();

        GenericDataSinkBase<?> sink = plan.getDataSinks().iterator().next();
        MapOperatorBase<?, ?, ?> mapper = (MapOperatorBase<?, ?, ?>) sink.getInput();

        SingleInputSemanticProperties semantics = mapper.getSemanticProperties();

        FieldSet fw1 = semantics.getForwardingTargetFields(0, 0);
        FieldSet fw2 = semantics.getForwardingTargetFields(0, 1);
        FieldSet fw3 = semantics.getForwardingTargetFields(0, 2);
        assertThat(fw1).contains(0);
        assertThat(fw2).contains(1);
        assertThat(fw3).contains(2);
    }

    @Test
    void testUnaryFunctionInPlaceForwardedAnnotation() {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        @SuppressWarnings("unchecked")
        DataSet<Tuple3<Long, String, Integer>> input =
                env.fromElements(new Tuple3<>(3L, "test", 42));
        input.map(new IndividualForwardedMapper<>()).output(new DiscardingOutputFormat<>());
        Plan plan = env.createProgramPlan();

        GenericDataSinkBase<?> sink = plan.getDataSinks().iterator().next();
        MapOperatorBase<?, ?, ?> mapper = (MapOperatorBase<?, ?, ?>) sink.getInput();

        SingleInputSemanticProperties semantics = mapper.getSemanticProperties();

        FieldSet fw1 = semantics.getForwardingTargetFields(0, 0);
        FieldSet fw2 = semantics.getForwardingTargetFields(0, 2);
        assertThat(fw1).contains(0);
        assertThat(fw2).contains(2);
    }

    @Test
    void testUnaryFunctionMovingForwardedAnnotation() {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        @SuppressWarnings("unchecked")
        DataSet<Tuple3<Long, Long, Long>> input = env.fromElements(new Tuple3<>(3L, 2L, 1L));
        input.map(new ShufflingMapper<>()).output(new DiscardingOutputFormat<>());
        Plan plan = env.createProgramPlan();

        GenericDataSinkBase<?> sink = plan.getDataSinks().iterator().next();
        MapOperatorBase<?, ?, ?> mapper = (MapOperatorBase<?, ?, ?>) sink.getInput();

        SingleInputSemanticProperties semantics = mapper.getSemanticProperties();

        FieldSet fw1 = semantics.getForwardingTargetFields(0, 0);
        FieldSet fw2 = semantics.getForwardingTargetFields(0, 1);
        FieldSet fw3 = semantics.getForwardingTargetFields(0, 2);
        assertThat(fw1).contains(2);
        assertThat(fw2).contains(0);
        assertThat(fw3).contains(1);
    }

    @Test
    void testUnaryFunctionForwardedInLine1() {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        @SuppressWarnings("unchecked")
        DataSet<Tuple3<Long, Long, Long>> input = env.fromElements(new Tuple3<>(3L, 2L, 1L));
        input.map(new NoAnnotationMapper<>())
                .withForwardedFields("0->1; 2")
                .output(new DiscardingOutputFormat<>());
        Plan plan = env.createProgramPlan();

        GenericDataSinkBase<?> sink = plan.getDataSinks().iterator().next();
        MapOperatorBase<?, ?, ?> mapper = (MapOperatorBase<?, ?, ?>) sink.getInput();

        SingleInputSemanticProperties semantics = mapper.getSemanticProperties();

        FieldSet fw1 = semantics.getForwardingTargetFields(0, 0);
        FieldSet fw2 = semantics.getForwardingTargetFields(0, 2);
        assertThat(fw1).contains(1);
        assertThat(fw2).contains(2);
    }

    @Test
    void testUnaryFunctionForwardedInLine2() {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        @SuppressWarnings("unchecked")
        DataSet<Tuple3<Long, Long, Long>> input = env.fromElements(new Tuple3<>(3L, 2L, 1L));
        input.map(new ReadSetMapper<>())
                .withForwardedFields("0->1; 2")
                .output(new DiscardingOutputFormat<>());
        Plan plan = env.createProgramPlan();

        GenericDataSinkBase<?> sink = plan.getDataSinks().iterator().next();
        MapOperatorBase<?, ?, ?> mapper = (MapOperatorBase<?, ?, ?>) sink.getInput();

        SingleInputSemanticProperties semantics = mapper.getSemanticProperties();

        FieldSet fw1 = semantics.getForwardingTargetFields(0, 0);
        FieldSet fw2 = semantics.getForwardingTargetFields(0, 2);
        assertThat(fw1).contains(1);
        assertThat(fw2).contains(2);
    }

    @Test
    void testUnaryFunctionForwardedInLine3() {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        @SuppressWarnings("unchecked")
        DataSet<Tuple3<Long, Long, Long>> input = env.fromElements(new Tuple3<>(3L, 2L, 1L));
        input.map(new ReadSetMapper<>())
                .withForwardedFields("0->1; 2")
                .output(new DiscardingOutputFormat<>());
        Plan plan = env.createProgramPlan();

        GenericDataSinkBase<?> sink = plan.getDataSinks().iterator().next();
        MapOperatorBase<?, ?, ?> mapper = (MapOperatorBase<?, ?, ?>) sink.getInput();

        SingleInputSemanticProperties semantics = mapper.getSemanticProperties();

        FieldSet fw1 = semantics.getForwardingTargetFields(0, 0);
        FieldSet fw2 = semantics.getForwardingTargetFields(0, 2);
        assertThat(fw1).contains(1);
        assertThat(fw2).contains(2);
    }

    @Test
    void testUnaryFunctionAllForwardedExceptAnnotation() {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        @SuppressWarnings("unchecked")
        DataSet<Tuple3<Long, Long, Long>> input = env.fromElements(new Tuple3<>(3L, 2L, 1L));
        input.map(new AllForwardedExceptMapper<>()).output(new DiscardingOutputFormat<>());
        Plan plan = env.createProgramPlan();

        GenericDataSinkBase<?> sink = plan.getDataSinks().iterator().next();
        MapOperatorBase<?, ?, ?> mapper = (MapOperatorBase<?, ?, ?>) sink.getInput();

        SingleInputSemanticProperties semantics = mapper.getSemanticProperties();

        FieldSet fw1 = semantics.getForwardingTargetFields(0, 0);
        FieldSet fw2 = semantics.getForwardingTargetFields(0, 2);
        assertThat(fw1).contains(0);
        assertThat(fw2).contains(2);
    }

    @Test
    void testUnaryFunctionReadFieldsAnnotation() {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        @SuppressWarnings("unchecked")
        DataSet<Tuple3<Long, Long, Long>> input = env.fromElements(new Tuple3<>(3L, 2L, 1L));
        input.map(new ReadSetMapper<>()).output(new DiscardingOutputFormat<>());
        Plan plan = env.createProgramPlan();

        GenericDataSinkBase<?> sink = plan.getDataSinks().iterator().next();
        MapOperatorBase<?, ?, ?> mapper = (MapOperatorBase<?, ?, ?>) sink.getInput();

        SingleInputSemanticProperties semantics = mapper.getSemanticProperties();

        FieldSet read = semantics.getReadFields(0);
        assertThat(read).containsExactly(0, 2);
    }

    @Test
    void testUnaryForwardedOverwritingInLine1() {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        @SuppressWarnings("unchecked")
        DataSet<Tuple3<Long, Long, Long>> input = env.fromElements(new Tuple3<>(3L, 2L, 1L));
        assertThatThrownBy(
                        () ->
                                input.map(new WildcardForwardedMapper<>())
                                        .withForwardedFields("0->1; 2"))
                .isInstanceOf(SemanticProperties.InvalidSemanticAnnotationException.class);
    }

    @Test
    void testUnaryForwardedOverwritingInLine2() {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        @SuppressWarnings("unchecked")
        DataSet<Tuple3<Long, Long, Long>> input = env.fromElements(new Tuple3<>(3L, 2L, 1L));
        assertThatThrownBy(
                        () ->
                                input.map(new AllForwardedExceptMapper<>())
                                        .withForwardedFields("0->1; 2"))
                .isInstanceOf(SemanticProperties.InvalidSemanticAnnotationException.class);
    }

    @Test
    void testBinaryForwardedAnnotation() {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        @SuppressWarnings("unchecked")
        DataSet<Tuple2<Long, String>> input1 = env.fromElements(new Tuple2<>(3L, "test"));
        @SuppressWarnings("unchecked")
        DataSet<Tuple2<Long, Double>> input2 = env.fromElements(new Tuple2<>(3L, 3.1415));
        input1.join(input2)
                .where(0)
                .equalTo(0)
                .with(new ForwardedBothAnnotationJoin<>())
                .output(new DiscardingOutputFormat<>());
        Plan plan = env.createProgramPlan();

        GenericDataSinkBase<?> sink = plan.getDataSinks().iterator().next();
        InnerJoinOperatorBase<?, ?, ?, ?> join =
                (InnerJoinOperatorBase<?, ?, ?, ?>) sink.getInput();

        DualInputSemanticProperties semantics = join.getSemanticProperties();
        assertThat(semantics.getForwardingTargetFields(0, 0)).isEmpty();
        assertThat(semantics.getForwardingTargetFields(1, 0)).isEmpty();
        assertThat(semantics.getForwardingTargetFields(0, 1)).containsExactly(0);
        assertThat(semantics.getForwardingTargetFields(1, 1)).containsExactly(1);
    }

    @Test
    void testBinaryForwardedInLine1() {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        @SuppressWarnings("unchecked")
        DataSet<Tuple2<Long, Long>> input1 = env.fromElements(new Tuple2<>(3L, 4L));
        @SuppressWarnings("unchecked")
        DataSet<Tuple2<Long, Long>> input2 = env.fromElements(new Tuple2<>(3L, 2L));
        input1.join(input2)
                .where(0)
                .equalTo(0)
                .with(new NoAnnotationJoin<>())
                .withForwardedFieldsFirst("0->1; 1->2")
                .withForwardedFieldsSecond("1->0")
                .output(new DiscardingOutputFormat<>());
        Plan plan = env.createProgramPlan();

        GenericDataSinkBase<?> sink = plan.getDataSinks().iterator().next();
        InnerJoinOperatorBase<?, ?, ?, ?> join =
                (InnerJoinOperatorBase<?, ?, ?, ?>) sink.getInput();

        DualInputSemanticProperties semantics = join.getSemanticProperties();
        assertThat(semantics.getForwardingTargetFields(1, 0)).isEmpty();
        assertThat(semantics.getForwardingTargetFields(0, 0)).containsExactly(1);
        assertThat(semantics.getForwardingTargetFields(0, 1)).containsExactly(2);
        assertThat(semantics.getForwardingTargetFields(1, 1)).containsExactly(0);
    }

    @Test
    void testBinaryForwardedInLine2() {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        @SuppressWarnings("unchecked")
        DataSet<Tuple2<Long, Long>> input1 = env.fromElements(new Tuple2<>(3L, 4L));
        @SuppressWarnings("unchecked")
        DataSet<Tuple2<Long, Long>> input2 = env.fromElements(new Tuple2<>(3L, 2L));
        input1.join(input2)
                .where(0)
                .equalTo(0)
                .with(new ReadSetJoin<>())
                .withForwardedFieldsFirst("0->1; 1->2")
                .withForwardedFieldsSecond("1->0")
                .output(new DiscardingOutputFormat<>());
        Plan plan = env.createProgramPlan();

        GenericDataSinkBase<?> sink = plan.getDataSinks().iterator().next();
        InnerJoinOperatorBase<?, ?, ?, ?> join =
                (InnerJoinOperatorBase<?, ?, ?, ?>) sink.getInput();

        DualInputSemanticProperties semantics = join.getSemanticProperties();
        assertThat(semantics.getForwardingTargetFields(1, 0)).isEmpty();
        assertThat(semantics.getForwardingTargetFields(0, 0)).containsExactly(1);
        assertThat(semantics.getForwardingTargetFields(0, 1)).containsExactly(2);
        assertThat(semantics.getForwardingTargetFields(1, 1)).containsExactly(0);
        assertThat(semantics.getReadFields(0)).containsExactly(1);
        assertThat(semantics.getReadFields(1)).containsExactly(0);
    }

    @Test
    void testBinaryForwardedAnnotationInLineMixed1() {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        @SuppressWarnings("unchecked")
        DataSet<Tuple2<Long, Long>> input1 = env.fromElements(new Tuple2<>(3L, 4L));
        @SuppressWarnings("unchecked")
        DataSet<Tuple2<Long, Long>> input2 = env.fromElements(new Tuple2<>(3L, 2L));
        input1.join(input2)
                .where(0)
                .equalTo(0)
                .with(new ForwardedFirstAnnotationJoin<>())
                .withForwardedFieldsSecond("1")
                .output(new DiscardingOutputFormat<>());
        Plan plan = env.createProgramPlan();

        GenericDataSinkBase<?> sink = plan.getDataSinks().iterator().next();
        InnerJoinOperatorBase<?, ?, ?, ?> join =
                (InnerJoinOperatorBase<?, ?, ?, ?>) sink.getInput();

        DualInputSemanticProperties semantics = join.getSemanticProperties();
        assertThat(semantics.getForwardingTargetFields(0, 1)).isEmpty();
        assertThat(semantics.getForwardingTargetFields(1, 0)).isEmpty();
        assertThat(semantics.getForwardingTargetFields(0, 0)).containsExactly(2);
        assertThat(semantics.getForwardingTargetFields(1, 1)).containsExactly(1);
    }

    @Test
    void testBinaryForwardedAnnotationInLineMixed2() {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        @SuppressWarnings("unchecked")
        DataSet<Tuple2<Long, Long>> input1 = env.fromElements(new Tuple2<>(3L, 4L));
        @SuppressWarnings("unchecked")
        DataSet<Tuple2<Long, Long>> input2 = env.fromElements(new Tuple2<>(3L, 2L));
        input1.join(input2)
                .where(0)
                .equalTo(0)
                .with(new ForwardedSecondAnnotationJoin<>())
                .withForwardedFieldsFirst("0->1")
                .output(new DiscardingOutputFormat<>());
        Plan plan = env.createProgramPlan();

        GenericDataSinkBase<?> sink = plan.getDataSinks().iterator().next();
        InnerJoinOperatorBase<?, ?, ?, ?> join =
                (InnerJoinOperatorBase<?, ?, ?, ?>) sink.getInput();

        DualInputSemanticProperties semantics = join.getSemanticProperties();
        assertThat(semantics.getForwardingTargetFields(0, 1)).isEmpty();
        assertThat(semantics.getForwardingTargetFields(1, 0)).isEmpty();
        assertThat(semantics.getForwardingTargetFields(0, 0)).containsExactly(1);
        assertThat(semantics.getForwardingTargetFields(1, 1)).containsExactly(2);
    }

    @Test
    void testBinaryAllForwardedExceptAnnotation() {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        @SuppressWarnings("unchecked")
        DataSet<Tuple3<Long, Long, Long>> input1 = env.fromElements(new Tuple3<>(3L, 4L, 5L));
        @SuppressWarnings("unchecked")
        DataSet<Tuple3<Long, Long, Long>> input2 = env.fromElements(new Tuple3<>(3L, 2L, 1L));
        input1.join(input2)
                .where(0)
                .equalTo(0)
                .with(new AllForwardedExceptJoin<>())
                .output(new DiscardingOutputFormat<>());
        Plan plan = env.createProgramPlan();

        GenericDataSinkBase<?> sink = plan.getDataSinks().iterator().next();
        InnerJoinOperatorBase<?, ?, ?, ?> join =
                (InnerJoinOperatorBase<?, ?, ?, ?>) sink.getInput();

        DualInputSemanticProperties semantics = join.getSemanticProperties();
        assertThat(semantics.getForwardingTargetFields(0, 0)).isEmpty();
        assertThat(semantics.getForwardingTargetFields(0, 2)).isEmpty();
        assertThat(semantics.getForwardingTargetFields(1, 0)).isEmpty();
        assertThat(semantics.getForwardingTargetFields(1, 1)).isEmpty();
        assertThat(semantics.getForwardingTargetFields(0, 1)).containsExactly(1);
        assertThat(semantics.getForwardingTargetFields(1, 2)).containsExactly(2);
    }

    @Test
    void testBinaryReadFieldsAnnotation() {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        @SuppressWarnings("unchecked")
        DataSet<Tuple2<Long, Long>> input1 = env.fromElements(new Tuple2<>(3L, 4L));
        @SuppressWarnings("unchecked")
        DataSet<Tuple2<Long, Long>> input2 = env.fromElements(new Tuple2<>(3L, 2L));
        input1.join(input2)
                .where(0)
                .equalTo(0)
                .with(new ReadSetJoin<>())
                .output(new DiscardingOutputFormat<Tuple3<Long, Long, Long>>());
        Plan plan = env.createProgramPlan();

        GenericDataSinkBase<?> sink = plan.getDataSinks().iterator().next();
        InnerJoinOperatorBase<?, ?, ?, ?> join =
                (InnerJoinOperatorBase<?, ?, ?, ?>) sink.getInput();

        DualInputSemanticProperties semantics = join.getSemanticProperties();
        assertThat(semantics.getReadFields(0)).containsExactly(1);
        assertThat(semantics.getReadFields(1)).containsExactly(0);
    }

    @Test
    void testBinaryForwardedOverwritingInLine1() {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        @SuppressWarnings("unchecked")
        DataSet<Tuple2<Long, Long>> input1 = env.fromElements(new Tuple2<>(3L, 4L));
        @SuppressWarnings("unchecked")
        DataSet<Tuple2<Long, Long>> input2 = env.fromElements(new Tuple2<>(3L, 2L));
        assertThatThrownBy(
                        () ->
                                input1.join(input2)
                                        .where(0)
                                        .equalTo(0)
                                        .with(new ForwardedFirstAnnotationJoin<>())
                                        .withForwardedFieldsFirst("0->1"))
                .isInstanceOf(SemanticProperties.InvalidSemanticAnnotationException.class);
    }

    @Test
    void testBinaryForwardedOverwritingInLine2() {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        @SuppressWarnings("unchecked")
        DataSet<Tuple2<Long, Long>> input1 = env.fromElements(new Tuple2<>(3L, 4L));
        @SuppressWarnings("unchecked")
        DataSet<Tuple2<Long, Long>> input2 = env.fromElements(new Tuple2<>(3L, 2L));
        assertThatThrownBy(
                        () ->
                                input1.join(input2)
                                        .where(0)
                                        .equalTo(0)
                                        .with(new ForwardedSecondAnnotationJoin<>())
                                        .withForwardedFieldsSecond("0->1"))
                .isInstanceOf(SemanticProperties.InvalidSemanticAnnotationException.class);
    }

    @Test
    void testBinaryForwardedOverwritingInLine3() {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        @SuppressWarnings("unchecked")
        DataSet<Tuple2<Long, Long>> input1 = env.fromElements(new Tuple2<>(3L, 4L));
        @SuppressWarnings("unchecked")
        DataSet<Tuple2<Long, Long>> input2 = env.fromElements(new Tuple2<>(3L, 2L));
        assertThatThrownBy(
                        () ->
                                input1.join(input2)
                                        .where(0)
                                        .equalTo(0)
                                        .with(new ForwardedBothAnnotationJoin<>())
                                        .withForwardedFieldsFirst("0->1;"))
                .isInstanceOf(SemanticProperties.InvalidSemanticAnnotationException.class);
    }

    @Test
    void testBinaryForwardedOverwritingInLine4() {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        @SuppressWarnings("unchecked")
        DataSet<Tuple2<Long, Long>> input1 = env.fromElements(new Tuple2<>(3L, 4L));
        @SuppressWarnings("unchecked")
        DataSet<Tuple2<Long, Long>> input2 = env.fromElements(new Tuple2<>(3L, 2L));
        assertThatThrownBy(
                        () ->
                                input1.join(input2)
                                        .where(0)
                                        .equalTo(0)
                                        .with(new ForwardedBothAnnotationJoin<>())
                                        .withForwardedFieldsSecond("0->1;"))
                .isInstanceOf(SemanticProperties.InvalidSemanticAnnotationException.class);
    }

    @Test
    void testBinaryForwardedOverwritingInLine5() {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        @SuppressWarnings("unchecked")
        DataSet<Tuple3<Long, Long, Long>> input1 = env.fromElements(new Tuple3<>(3L, 4L, 5L));
        @SuppressWarnings("unchecked")
        DataSet<Tuple3<Long, Long, Long>> input2 = env.fromElements(new Tuple3<>(3L, 2L, 1L));
        assertThatThrownBy(
                        () ->
                                input1.join(input2)
                                        .where(0)
                                        .equalTo(0)
                                        .with(new AllForwardedExceptJoin<>())
                                        .withForwardedFieldsFirst("0->1;"))
                .isInstanceOf(SemanticProperties.InvalidSemanticAnnotationException.class);
    }

    @Test
    void testBinaryForwardedOverwritingInLine6() {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        @SuppressWarnings("unchecked")
        DataSet<Tuple3<Long, Long, Long>> input1 = env.fromElements(new Tuple3<>(3L, 4L, 5L));
        @SuppressWarnings("unchecked")
        DataSet<Tuple3<Long, Long, Long>> input2 = env.fromElements(new Tuple3<>(3L, 2L, 1L));
        assertThatThrownBy(
                        () ->
                                input1.join(input2)
                                        .where(0)
                                        .equalTo(0)
                                        .with(new AllForwardedExceptJoin<>())
                                        .withForwardedFieldsSecond("0->1;"))
                .isInstanceOf(SemanticProperties.InvalidSemanticAnnotationException.class);
    }

    // --------------------------------------------------------------------------------------------

    private static class NoAnnotationMapper<T> implements MapFunction<T, T> {

        @Override
        public T map(T value) {
            return value;
        }
    }

    @ForwardedFields("*")
    private static class WildcardForwardedMapper<T> implements MapFunction<T, T> {

        @Override
        public T map(T value) {
            return value;
        }
    }

    @ForwardedFields("0;2")
    private static class IndividualForwardedMapper<X, Y, Z>
            implements MapFunction<Tuple3<X, Y, Z>, Tuple3<X, Y, Z>> {

        @Override
        public Tuple3<X, Y, Z> map(Tuple3<X, Y, Z> value) {
            return value;
        }
    }

    @ForwardedFields("0->2;1->0;2->1")
    private static class ShufflingMapper<X>
            implements MapFunction<Tuple3<X, X, X>, Tuple3<X, X, X>> {

        @Override
        public Tuple3<X, X, X> map(Tuple3<X, X, X> value) {
            return value;
        }
    }

    @FunctionAnnotation.NonForwardedFields({"1"})
    private static class AllForwardedExceptMapper<T> implements MapFunction<T, T> {

        @Override
        public T map(T value) {
            return value;
        }
    }

    @FunctionAnnotation.ReadFields({"0;2"})
    private static class ReadSetMapper<T> implements MapFunction<T, T> {

        @Override
        public T map(T value) {
            return value;
        }
    }

    private static class NoAnnotationJoin<X>
            implements JoinFunction<Tuple2<X, X>, Tuple2<X, X>, Tuple3<X, X, X>> {

        @Override
        public Tuple3<X, X, X> join(Tuple2<X, X> first, Tuple2<X, X> second) throws Exception {
            return null;
        }
    }

    @ForwardedFieldsFirst("0->2")
    private static class ForwardedFirstAnnotationJoin<X>
            implements JoinFunction<Tuple2<X, X>, Tuple2<X, X>, Tuple3<X, X, X>> {

        @Override
        public Tuple3<X, X, X> join(Tuple2<X, X> first, Tuple2<X, X> second) throws Exception {
            return null;
        }
    }

    @ForwardedFieldsSecond("1->2")
    private static class ForwardedSecondAnnotationJoin<X>
            implements JoinFunction<Tuple2<X, X>, Tuple2<X, X>, Tuple3<X, X, X>> {

        @Override
        public Tuple3<X, X, X> join(Tuple2<X, X> first, Tuple2<X, X> second) throws Exception {
            return null;
        }
    }

    @ForwardedFieldsFirst("1 -> 0")
    @ForwardedFieldsSecond("1 -> 1")
    private static class ForwardedBothAnnotationJoin<A, B, C, D>
            implements JoinFunction<Tuple2<A, B>, Tuple2<C, D>, Tuple2<B, D>> {

        @Override
        public Tuple2<B, D> join(Tuple2<A, B> first, Tuple2<C, D> second) {
            return new Tuple2<B, D>(first.f1, second.f1);
        }
    }

    @FunctionAnnotation.NonForwardedFieldsFirst("0;2")
    @FunctionAnnotation.NonForwardedFieldsSecond("0;1")
    private static class AllForwardedExceptJoin<X>
            implements JoinFunction<Tuple3<X, X, X>, Tuple3<X, X, X>, Tuple3<X, X, X>> {

        @Override
        public Tuple3<X, X, X> join(Tuple3<X, X, X> first, Tuple3<X, X, X> second)
                throws Exception {
            return null;
        }
    }

    @FunctionAnnotation.ReadFieldsFirst("1")
    @FunctionAnnotation.ReadFieldsSecond("0")
    private static class ReadSetJoin<X>
            implements JoinFunction<Tuple2<X, X>, Tuple2<X, X>, Tuple3<X, X, X>> {

        @Override
        public Tuple3<X, X, X> join(Tuple2<X, X> first, Tuple2<X, X> second) throws Exception {
            return null;
        }
    }
}
