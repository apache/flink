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

package org.apache.flink.api.java.operators.translation;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.operators.GenericDataSinkBase;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.SingleInputOperator;
import org.apache.flink.api.common.operators.Union;
import org.apache.flink.api.common.operators.base.MapOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.StringValue;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Tests for translation of union operation. */
@SuppressWarnings("serial")
class UnionTranslationTest {

    @Test
    void translateUnion2Group() {
        try {
            final int parallelism = 4;
            ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment(parallelism);

            DataSet<Tuple3<Double, StringValue, LongValue>> dataset1 = getSourceDataSet(env, 3);

            DataSet<Tuple3<Double, StringValue, LongValue>> dataset2 = getSourceDataSet(env, 2);

            dataset1.union(dataset2)
                    .groupBy(
                            (KeySelector<Tuple3<Double, StringValue, LongValue>, String>)
                                    value -> "")
                    .reduceGroup(
                            (GroupReduceFunction<Tuple3<Double, StringValue, LongValue>, String>)
                                    (values, out) -> {})
                    .returns(String.class)
                    .output(new DiscardingOutputFormat<>());

            Plan p = env.createProgramPlan();

            // The plan should look like the following one.
            //
            // DataSet1(3) - MapOperator(3)-+
            //	                            |- Union(-1) - SingleInputOperator - Sink
            // DataSet2(2) - MapOperator(2)-+

            GenericDataSinkBase<?> sink = p.getDataSinks().iterator().next();
            Union unionOperator = (Union) ((SingleInputOperator) sink.getInput()).getInput();

            // The key mappers should be added to both of the two input streams for union.
            assertThat(unionOperator.getFirstInput()).isInstanceOf(MapOperatorBase.class);
            assertThat(unionOperator.getSecondInput()).isInstanceOf(MapOperatorBase.class);

            // The parallelisms of the key mappers should be equal to those of their inputs.
            assertThat(unionOperator.getFirstInput().getParallelism()).isEqualTo(3);
            assertThat(unionOperator.getSecondInput().getParallelism()).isEqualTo(2);

            // The union should always have the default parallelism.
            assertThat(unionOperator.getParallelism())
                    .isEqualTo(ExecutionConfig.PARALLELISM_DEFAULT);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            fail("Test caused an error: " + e.getMessage());
        }
    }

    @Test
    void translateUnion3SortedGroup() {
        try {
            final int parallelism = 4;
            ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment(parallelism);

            DataSet<Tuple3<Double, StringValue, LongValue>> dataset1 = getSourceDataSet(env, 2);

            DataSet<Tuple3<Double, StringValue, LongValue>> dataset2 = getSourceDataSet(env, 3);

            DataSet<Tuple3<Double, StringValue, LongValue>> dataset3 = getSourceDataSet(env, -1);

            dataset1.union(dataset2)
                    .union(dataset3)
                    .groupBy(
                            (KeySelector<Tuple3<Double, StringValue, LongValue>, String>)
                                    value -> "")
                    .sortGroup(
                            (KeySelector<Tuple3<Double, StringValue, LongValue>, String>)
                                    value -> "",
                            Order.ASCENDING)
                    .reduceGroup(
                            (GroupReduceFunction<Tuple3<Double, StringValue, LongValue>, String>)
                                    (values, out) -> {})
                    .returns(String.class)
                    .output(new DiscardingOutputFormat<>());

            Plan p = env.createProgramPlan();

            // The plan should look like the following one.
            //
            // DataSet1(2) - MapOperator(2)-+
            //	                            |- Union(-1) -+
            // DataSet2(3) - MapOperator(3)-+             |- Union(-1) - SingleInputOperator - Sink
            //                                            |
            //             DataSet3(-1) - MapOperator(-1)-+

            GenericDataSinkBase<?> sink = p.getDataSinks().iterator().next();
            Union secondUnionOperator = (Union) ((SingleInputOperator) sink.getInput()).getInput();

            // The first input of the second union should be the first union.
            Union firstUnionOperator = (Union) secondUnionOperator.getFirstInput();

            // The key mapper should be added to the second input stream of the second union.
            assertThat(secondUnionOperator.getSecondInput()).isInstanceOf(MapOperatorBase.class);

            // The key mappers should be added to both of the two input streams for the first union.
            assertThat(firstUnionOperator.getFirstInput()).isInstanceOf(MapOperatorBase.class);
            assertThat(firstUnionOperator.getSecondInput()).isInstanceOf(MapOperatorBase.class);

            // The parallelisms of the key mappers should be equal to those of their inputs.
            assertThat(firstUnionOperator.getFirstInput().getParallelism()).isEqualTo(2);
            assertThat(firstUnionOperator.getSecondInput().getParallelism()).isEqualTo(3);
            assertThat(secondUnionOperator.getSecondInput().getParallelism()).isEqualTo(-1);

            // The union should always have the default parallelism.
            assertThat(secondUnionOperator.getParallelism())
                    .isEqualTo(ExecutionConfig.PARALLELISM_DEFAULT);
            assertThat(firstUnionOperator.getParallelism())
                    .isEqualTo(ExecutionConfig.PARALLELISM_DEFAULT);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            fail("Test caused an error: " + e.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    private static DataSet<Tuple3<Double, StringValue, LongValue>> getSourceDataSet(
            ExecutionEnvironment env, int parallelism) {
        return env.fromElements(new Tuple3<>(0.0, new StringValue(""), new LongValue(1L)))
                .setParallelism(parallelism);
    }
}
