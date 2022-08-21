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
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.GenericDataSinkBase;
import org.apache.flink.api.common.operators.SingleInputSemanticProperties;
import org.apache.flink.api.common.operators.base.MapOperatorBase;
import org.apache.flink.api.common.operators.util.FieldSet;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.tuple.Tuple3;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests the precedence of semantic properties: annotation > API. */
class SemanticPropertiesPrecedenceTest {

    @Test
    void testFunctionForwardedAnnotationPrecedence() {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        @SuppressWarnings("unchecked")
        DataSet<Tuple3<Long, String, Integer>> input = env.fromElements(Tuple3.of(3L, "test", 42));
        input.map(new WildcardForwardedMapperWithForwardAnnotation<>())
                .output(new DiscardingOutputFormat<>());
        Plan plan = env.createProgramPlan();

        GenericDataSinkBase<?> sink = plan.getDataSinks().iterator().next();
        MapOperatorBase<?, ?, ?> mapper = (MapOperatorBase<?, ?, ?>) sink.getInput();

        SingleInputSemanticProperties semantics = mapper.getSemanticProperties();

        FieldSet fw1 = semantics.getForwardingTargetFields(0, 0);
        FieldSet fw2 = semantics.getForwardingTargetFields(0, 1);
        FieldSet fw3 = semantics.getForwardingTargetFields(0, 2);
        assertThat(fw1).contains(0);
        assertThat(fw2).doesNotContain(1);
        assertThat(fw3).doesNotContain(2);
    }

    @Test
    void testFunctionApiPrecedence() {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        @SuppressWarnings("unchecked")
        DataSet<Tuple3<Long, String, Integer>> input = env.fromElements(Tuple3.of(3L, "test", 42));
        input.map(new WildcardForwardedMapper<>())
                .withForwardedFields("f0")
                .output(new DiscardingOutputFormat<>());
        Plan plan = env.createProgramPlan();

        GenericDataSinkBase<?> sink = plan.getDataSinks().iterator().next();
        MapOperatorBase<?, ?, ?> mapper = (MapOperatorBase<?, ?, ?>) sink.getInput();

        SingleInputSemanticProperties semantics = mapper.getSemanticProperties();

        FieldSet fw1 = semantics.getForwardingTargetFields(0, 0);
        FieldSet fw2 = semantics.getForwardingTargetFields(0, 1);
        FieldSet fw3 = semantics.getForwardingTargetFields(0, 2);
        assertThat(fw1).contains(0);
        assertThat(fw2).doesNotContain(1);
        assertThat(fw3).doesNotContain(2);
    }

    // --------------------------------------------------------------------------------------------

    @FunctionAnnotation.ForwardedFields("f0")
    private static class WildcardForwardedMapperWithForwardAnnotation<T>
            implements MapFunction<T, T> {

        @Override
        public T map(T value) {
            return value;
        }
    }

    private static class WildcardForwardedMapper<T> implements MapFunction<T, T> {

        @Override
        public T map(T value) {
            return value;
        }
    }
}
