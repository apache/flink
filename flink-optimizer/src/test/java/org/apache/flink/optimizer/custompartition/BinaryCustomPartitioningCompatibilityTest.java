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
import org.apache.flink.optimizer.plantranslate.JobGraphGenerator;
import org.apache.flink.optimizer.testfunctions.DummyCoGroupFunction;
import org.apache.flink.optimizer.util.CompilerTestBase;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;

import org.junit.Test;

import static org.junit.Assert.*;

@SuppressWarnings({"serial", "unchecked"})
public class BinaryCustomPartitioningCompatibilityTest extends CompilerTestBase {

    @Test
    public void testCompatiblePartitioningJoin() {
        try {
            final Partitioner<Long> partitioner =
                    new Partitioner<Long>() {
                        @Override
                        public int partition(Long key, int numPartitions) {
                            return 0;
                        }
                    };

            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

            DataSet<Tuple2<Long, Long>> input1 = env.fromElements(new Tuple2<Long, Long>(0L, 0L));
            DataSet<Tuple3<Long, Long, Long>> input2 =
                    env.fromElements(new Tuple3<Long, Long, Long>(0L, 0L, 0L));

            input1.partitionCustom(partitioner, 1)
                    .join(input2.partitionCustom(partitioner, 0))
                    .where(1)
                    .equalTo(0)
                    .output(
                            new DiscardingOutputFormat<
                                    Tuple2<Tuple2<Long, Long>, Tuple3<Long, Long, Long>>>());

            Plan p = env.createProgramPlan();
            OptimizedPlan op = compileNoStats(p);

            SinkPlanNode sink = op.getDataSinks().iterator().next();
            DualInputPlanNode join = (DualInputPlanNode) sink.getInput().getSource();
            SingleInputPlanNode partitioner1 = (SingleInputPlanNode) join.getInput1().getSource();
            SingleInputPlanNode partitioner2 = (SingleInputPlanNode) join.getInput2().getSource();

            assertEquals(ShipStrategyType.FORWARD, join.getInput1().getShipStrategy());
            assertEquals(ShipStrategyType.FORWARD, join.getInput2().getShipStrategy());

            assertEquals(
                    ShipStrategyType.PARTITION_CUSTOM, partitioner1.getInput().getShipStrategy());
            assertEquals(
                    ShipStrategyType.PARTITION_CUSTOM, partitioner2.getInput().getShipStrategy());
            assertEquals(partitioner, partitioner1.getInput().getPartitioner());
            assertEquals(partitioner, partitioner2.getInput().getPartitioner());

            new JobGraphGenerator().compileJobGraph(op);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testCompatiblePartitioningCoGroup() {
        try {
            final Partitioner<Long> partitioner =
                    new Partitioner<Long>() {
                        @Override
                        public int partition(Long key, int numPartitions) {
                            return 0;
                        }
                    };

            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

            DataSet<Tuple2<Long, Long>> input1 = env.fromElements(new Tuple2<Long, Long>(0L, 0L));
            DataSet<Tuple3<Long, Long, Long>> input2 =
                    env.fromElements(new Tuple3<Long, Long, Long>(0L, 0L, 0L));

            input1.partitionCustom(partitioner, 1)
                    .coGroup(input2.partitionCustom(partitioner, 0))
                    .where(1)
                    .equalTo(0)
                    .with(new DummyCoGroupFunction<Tuple2<Long, Long>, Tuple3<Long, Long, Long>>())
                    .output(
                            new DiscardingOutputFormat<
                                    Tuple2<Tuple2<Long, Long>, Tuple3<Long, Long, Long>>>());

            Plan p = env.createProgramPlan();
            OptimizedPlan op = compileNoStats(p);

            SinkPlanNode sink = op.getDataSinks().iterator().next();
            DualInputPlanNode coGroup = (DualInputPlanNode) sink.getInput().getSource();
            SingleInputPlanNode partitioner1 =
                    (SingleInputPlanNode) coGroup.getInput1().getSource();
            SingleInputPlanNode partitioner2 =
                    (SingleInputPlanNode) coGroup.getInput2().getSource();

            assertEquals(ShipStrategyType.FORWARD, coGroup.getInput1().getShipStrategy());
            assertEquals(ShipStrategyType.FORWARD, coGroup.getInput2().getShipStrategy());

            assertEquals(
                    ShipStrategyType.PARTITION_CUSTOM, partitioner1.getInput().getShipStrategy());
            assertEquals(
                    ShipStrategyType.PARTITION_CUSTOM, partitioner2.getInput().getShipStrategy());
            assertEquals(partitioner, partitioner1.getInput().getPartitioner());
            assertEquals(partitioner, partitioner2.getInput().getPartitioner());

            new JobGraphGenerator().compileJobGraph(op);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }
}
