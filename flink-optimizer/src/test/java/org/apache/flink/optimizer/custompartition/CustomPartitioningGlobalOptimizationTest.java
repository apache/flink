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

package org.apache.flink.optimizer.custompartition;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.optimizer.plan.DualInputPlanNode;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plan.SingleInputPlanNode;
import org.apache.flink.optimizer.plan.SinkPlanNode;
import org.apache.flink.optimizer.testfunctions.IdentityGroupReducerCombinable;
import org.apache.flink.optimizer.util.CompilerTestBase;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@SuppressWarnings({"serial", "unchecked"})
public class CustomPartitioningGlobalOptimizationTest extends CompilerTestBase {

    @Test
    public void testJoinReduceCombination() {
        try {
            final Partitioner<Long> partitioner = new TestPartitionerLong();

            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

            DataSet<Tuple2<Long, Long>> input1 = env.fromElements(new Tuple2<Long, Long>(0L, 0L));
            DataSet<Tuple3<Long, Long, Long>> input2 =
                    env.fromElements(new Tuple3<Long, Long, Long>(0L, 0L, 0L));

            DataSet<Tuple3<Long, Long, Long>> joined =
                    input1.join(input2)
                            .where(1)
                            .equalTo(0)
                            .projectFirst(0, 1)
                            .<Tuple3<Long, Long, Long>>projectSecond(2)
                            .withPartitioner(partitioner);

            joined.groupBy(1)
                    .withPartitioner(partitioner)
                    .reduceGroup(new IdentityGroupReducerCombinable<Tuple3<Long, Long, Long>>())
                    .output(new DiscardingOutputFormat<Tuple3<Long, Long, Long>>());

            Plan p = env.createProgramPlan();
            OptimizedPlan op = compileNoStats(p);

            SinkPlanNode sink = op.getDataSinks().iterator().next();
            SingleInputPlanNode reducer = (SingleInputPlanNode) sink.getInput().getSource();

            assertTrue(
                    "Reduce is not chained, property reuse does not happen",
                    reducer.getInput().getSource() instanceof DualInputPlanNode);

            DualInputPlanNode join = (DualInputPlanNode) reducer.getInput().getSource();

            assertEquals(ShipStrategyType.PARTITION_CUSTOM, join.getInput1().getShipStrategy());
            assertEquals(ShipStrategyType.PARTITION_CUSTOM, join.getInput2().getShipStrategy());
            assertEquals(partitioner, join.getInput1().getPartitioner());
            assertEquals(partitioner, join.getInput2().getPartitioner());

            assertEquals(ShipStrategyType.FORWARD, reducer.getInput().getShipStrategy());
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    // --------------------------------------------------------------------------------------------

    private static class TestPartitionerLong implements Partitioner<Long> {
        @Override
        public int partition(Long key, int numPartitions) {
            return 0;
        }
    }
}
