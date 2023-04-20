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

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.operators.GenericDataSinkBase;
import org.apache.flink.api.common.operators.GenericDataSourceBase;
import org.apache.flink.api.common.operators.base.GroupReduceOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.types.StringValue;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Tests for translation of aggregations. */
class AggregateTranslationTest {

    @Test
    void translateAggregate() {
        try {
            final int parallelism = 8;
            ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment(parallelism);

            @SuppressWarnings("unchecked")
            DataSet<Tuple3<Double, StringValue, Long>> initialData =
                    env.fromElements(new Tuple3<>(3.141592, new StringValue("foobar"), 77L));

            initialData
                    .groupBy(0)
                    .aggregate(Aggregations.MIN, 1)
                    .and(Aggregations.SUM, 2)
                    .output(new DiscardingOutputFormat<>());

            Plan p = env.createProgramPlan();

            GenericDataSinkBase<?> sink = p.getDataSinks().iterator().next();

            GroupReduceOperatorBase<?, ?, ?> reducer =
                    (GroupReduceOperatorBase<?, ?, ?>) sink.getInput();

            // check keys
            assertThat(reducer.getKeyColumns(0)).containsExactly(0);

            assertThat(reducer.getParallelism()).isEqualTo(-1);
            assertThat(reducer.isCombinable()).isTrue();

            assertThat(reducer.getInput()).isInstanceOf(GenericDataSourceBase.class);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            fail("Test caused an error: " + e.getMessage());
        }
    }
}
