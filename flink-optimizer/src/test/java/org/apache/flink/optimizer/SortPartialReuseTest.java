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

package org.apache.flink.optimizer;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plan.SingleInputPlanNode;
import org.apache.flink.optimizer.plan.SinkPlanNode;
import org.apache.flink.optimizer.testfunctions.IdentityGroupReducerCombinable;
import org.apache.flink.optimizer.testfunctions.IdentityMapper;
import org.apache.flink.optimizer.util.CompilerTestBase;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.runtime.operators.util.LocalStrategy;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

@SuppressWarnings("serial")
public class SortPartialReuseTest extends CompilerTestBase {

    @Test
    void testPartialPartitioningReuse() {
        try {
            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

            @SuppressWarnings("unchecked")
            DataSet<Tuple3<Long, Long, Long>> input =
                    env.fromElements(new Tuple3<Long, Long, Long>(0L, 0L, 0L));

            input.partitionByHash(0)
                    .map(new IdentityMapper<Tuple3<Long, Long, Long>>())
                    .withForwardedFields("0", "1", "2")
                    .groupBy(0, 1)
                    .reduceGroup(new IdentityGroupReducerCombinable<Tuple3<Long, Long, Long>>())
                    .withForwardedFields("0", "1", "2")
                    .groupBy(0)
                    .reduceGroup(new IdentityGroupReducerCombinable<Tuple3<Long, Long, Long>>())
                    .output(new DiscardingOutputFormat<Tuple3<Long, Long, Long>>());

            Plan p = env.createProgramPlan();
            OptimizedPlan op = compileNoStats(p);

            SinkPlanNode sink = op.getDataSinks().iterator().next();
            SingleInputPlanNode reducer2 = (SingleInputPlanNode) sink.getInput().getSource();
            SingleInputPlanNode reducer1 = (SingleInputPlanNode) reducer2.getInput().getSource();

            assertThat(sink.getInput().getShipStrategy()).isEqualTo(ShipStrategyType.FORWARD);

            // should be locally forwarding, reusing sort and partitioning
            assertThat(reducer2.getInput().getShipStrategy()).isEqualTo(ShipStrategyType.FORWARD);
            assertThat(reducer2.getInput().getLocalStrategy()).isEqualTo(LocalStrategy.NONE);

            assertThat(reducer1.getInput().getShipStrategy()).isEqualTo(ShipStrategyType.FORWARD);
            assertThat(reducer1.getInput().getLocalStrategy())
                    .isEqualTo(LocalStrategy.COMBININGSORT);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    void testCustomPartitioningNotReused() {
        try {
            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

            @SuppressWarnings("unchecked")
            DataSet<Tuple3<Long, Long, Long>> input =
                    env.fromElements(new Tuple3<Long, Long, Long>(0L, 0L, 0L));

            input.partitionCustom(
                            new Partitioner<Long>() {
                                @Override
                                public int partition(Long key, int numPartitions) {
                                    return 0;
                                }
                            },
                            0)
                    .map(new IdentityMapper<Tuple3<Long, Long, Long>>())
                    .withForwardedFields("0", "1", "2")
                    .groupBy(0, 1)
                    .reduceGroup(new IdentityGroupReducerCombinable<Tuple3<Long, Long, Long>>())
                    .withForwardedFields("0", "1", "2")
                    .groupBy(1)
                    .reduceGroup(new IdentityGroupReducerCombinable<Tuple3<Long, Long, Long>>())
                    .output(new DiscardingOutputFormat<Tuple3<Long, Long, Long>>());

            Plan p = env.createProgramPlan();
            OptimizedPlan op = compileNoStats(p);

            SinkPlanNode sink = op.getDataSinks().iterator().next();
            SingleInputPlanNode reducer2 = (SingleInputPlanNode) sink.getInput().getSource();
            SingleInputPlanNode combiner = (SingleInputPlanNode) reducer2.getInput().getSource();
            SingleInputPlanNode reducer1 = (SingleInputPlanNode) combiner.getInput().getSource();

            assertThat(sink.getInput().getShipStrategy()).isEqualTo(ShipStrategyType.FORWARD);

            // should be locally forwarding, reusing sort and partitioning
            assertThat(reducer2.getInput().getShipStrategy())
                    .isEqualTo(ShipStrategyType.PARTITION_HASH);
            assertThat(reducer2.getInput().getLocalStrategy())
                    .isEqualTo(LocalStrategy.COMBININGSORT);

            assertThat(combiner.getInput().getShipStrategy()).isEqualTo(ShipStrategyType.FORWARD);
            assertThat(combiner.getInput().getLocalStrategy()).isEqualTo(LocalStrategy.NONE);

            assertThat(reducer1.getInput().getShipStrategy()).isEqualTo(ShipStrategyType.FORWARD);
            assertThat(reducer1.getInput().getLocalStrategy())
                    .isEqualTo(LocalStrategy.COMBININGSORT);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }
}
