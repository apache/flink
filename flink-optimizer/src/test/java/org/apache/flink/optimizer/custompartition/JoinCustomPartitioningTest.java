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

import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.optimizer.plan.DualInputPlanNode;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plan.SinkPlanNode;
import org.apache.flink.optimizer.testfunctions.DummyFlatJoinFunction;
import org.apache.flink.optimizer.testfunctions.IdentityGroupReducerCombinable;
import org.apache.flink.optimizer.testfunctions.IdentityMapper;
import org.apache.flink.optimizer.util.CompilerTestBase;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;

import org.junit.Test;

import static org.junit.Assert.*;

@SuppressWarnings({"serial", "unchecked"})
public class JoinCustomPartitioningTest extends CompilerTestBase {

    @Test
    public void testJoinWithTuples() {
        try {
            final Partitioner<Long> partitioner = new TestPartitionerLong();

            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

            DataSet<Tuple2<Long, Long>> input1 = env.fromElements(new Tuple2<Long, Long>(0L, 0L));
            DataSet<Tuple3<Long, Long, Long>> input2 =
                    env.fromElements(new Tuple3<Long, Long, Long>(0L, 0L, 0L));

            input1.join(input2, JoinHint.REPARTITION_HASH_FIRST)
                    .where(1)
                    .equalTo(0)
                    .withPartitioner(partitioner)
                    .output(
                            new DiscardingOutputFormat<
                                    Tuple2<Tuple2<Long, Long>, Tuple3<Long, Long, Long>>>());

            Plan p = env.createProgramPlan();
            OptimizedPlan op = compileNoStats(p);

            SinkPlanNode sink = op.getDataSinks().iterator().next();
            DualInputPlanNode join = (DualInputPlanNode) sink.getInput().getSource();

            assertEquals(ShipStrategyType.PARTITION_CUSTOM, join.getInput1().getShipStrategy());
            assertEquals(ShipStrategyType.PARTITION_CUSTOM, join.getInput2().getShipStrategy());
            assertEquals(partitioner, join.getInput1().getPartitioner());
            assertEquals(partitioner, join.getInput2().getPartitioner());
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testJoinWithTuplesWrongType() {
        try {
            final Partitioner<Integer> partitioner = new TestPartitionerInt();

            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

            DataSet<Tuple2<Long, Long>> input1 = env.fromElements(new Tuple2<Long, Long>(0L, 0L));
            DataSet<Tuple3<Long, Long, Long>> input2 =
                    env.fromElements(new Tuple3<Long, Long, Long>(0L, 0L, 0L));

            try {
                input1.join(input2, JoinHint.REPARTITION_HASH_FIRST)
                        .where(1)
                        .equalTo(0)
                        .withPartitioner(partitioner);

                fail("should throw an exception");
            } catch (InvalidProgramException e) {
                // expected
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testJoinWithPojos() {
        try {
            final Partitioner<Integer> partitioner = new TestPartitionerInt();

            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

            DataSet<Pojo2> input1 = env.fromElements(new Pojo2());
            DataSet<Pojo3> input2 = env.fromElements(new Pojo3());

            input1.join(input2, JoinHint.REPARTITION_HASH_FIRST)
                    .where("b")
                    .equalTo("a")
                    .withPartitioner(partitioner)
                    .output(new DiscardingOutputFormat<Tuple2<Pojo2, Pojo3>>());

            Plan p = env.createProgramPlan();
            OptimizedPlan op = compileNoStats(p);

            SinkPlanNode sink = op.getDataSinks().iterator().next();
            DualInputPlanNode join = (DualInputPlanNode) sink.getInput().getSource();

            assertEquals(ShipStrategyType.PARTITION_CUSTOM, join.getInput1().getShipStrategy());
            assertEquals(ShipStrategyType.PARTITION_CUSTOM, join.getInput2().getShipStrategy());
            assertEquals(partitioner, join.getInput1().getPartitioner());
            assertEquals(partitioner, join.getInput2().getPartitioner());
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testJoinWithPojosWrongType() {
        try {
            final Partitioner<Long> partitioner = new TestPartitionerLong();

            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

            DataSet<Pojo2> input1 = env.fromElements(new Pojo2());
            DataSet<Pojo3> input2 = env.fromElements(new Pojo3());

            try {
                input1.join(input2, JoinHint.REPARTITION_HASH_FIRST)
                        .where("a")
                        .equalTo("b")
                        .withPartitioner(partitioner);

                fail("should throw an exception");
            } catch (InvalidProgramException e) {
                // expected
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testJoinWithKeySelectors() {
        try {
            final Partitioner<Integer> partitioner = new TestPartitionerInt();

            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

            DataSet<Pojo2> input1 = env.fromElements(new Pojo2());
            DataSet<Pojo3> input2 = env.fromElements(new Pojo3());

            input1.join(input2, JoinHint.REPARTITION_HASH_FIRST)
                    .where(new Pojo2KeySelector())
                    .equalTo(new Pojo3KeySelector())
                    .withPartitioner(partitioner)
                    .output(new DiscardingOutputFormat<Tuple2<Pojo2, Pojo3>>());

            Plan p = env.createProgramPlan();
            OptimizedPlan op = compileNoStats(p);

            SinkPlanNode sink = op.getDataSinks().iterator().next();
            DualInputPlanNode join = (DualInputPlanNode) sink.getInput().getSource();

            assertEquals(ShipStrategyType.PARTITION_CUSTOM, join.getInput1().getShipStrategy());
            assertEquals(ShipStrategyType.PARTITION_CUSTOM, join.getInput2().getShipStrategy());
            assertEquals(partitioner, join.getInput1().getPartitioner());
            assertEquals(partitioner, join.getInput2().getPartitioner());
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testJoinWithKeySelectorsWrongType() {
        try {
            final Partitioner<Long> partitioner = new TestPartitionerLong();

            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

            DataSet<Pojo2> input1 = env.fromElements(new Pojo2());
            DataSet<Pojo3> input2 = env.fromElements(new Pojo3());

            try {
                input1.join(input2, JoinHint.REPARTITION_HASH_FIRST)
                        .where(new Pojo2KeySelector())
                        .equalTo(new Pojo3KeySelector())
                        .withPartitioner(partitioner);

                fail("should throw an exception");
            } catch (InvalidProgramException e) {
                // expected
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testIncompatibleHashAndCustomPartitioning() {
        try {
            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

            DataSet<Tuple3<Long, Long, Long>> input =
                    env.fromElements(new Tuple3<Long, Long, Long>(0L, 0L, 0L));

            DataSet<Tuple3<Long, Long, Long>> partitioned =
                    input.partitionCustom(
                                    new Partitioner<Long>() {
                                        @Override
                                        public int partition(Long key, int numPartitions) {
                                            return 0;
                                        }
                                    },
                                    0)
                            .map(new IdentityMapper<Tuple3<Long, Long, Long>>())
                            .withForwardedFields("0", "1", "2");

            DataSet<Tuple3<Long, Long, Long>> grouped =
                    partitioned
                            .distinct(0, 1)
                            .groupBy(1)
                            .sortGroup(0, Order.ASCENDING)
                            .reduceGroup(
                                    new IdentityGroupReducerCombinable<Tuple3<Long, Long, Long>>())
                            .withForwardedFields("0", "1");

            grouped.join(partitioned, JoinHint.REPARTITION_HASH_FIRST)
                    .where(0)
                    .equalTo(0)
                    .with(new DummyFlatJoinFunction<Tuple3<Long, Long, Long>>())
                    .output(new DiscardingOutputFormat<Tuple3<Long, Long, Long>>());

            Plan p = env.createProgramPlan();
            OptimizedPlan op = compileNoStats(p);

            SinkPlanNode sink = op.getDataSinks().iterator().next();
            DualInputPlanNode coGroup = (DualInputPlanNode) sink.getInput().getSource();

            assertEquals(ShipStrategyType.PARTITION_HASH, coGroup.getInput1().getShipStrategy());
            assertTrue(
                    coGroup.getInput2().getShipStrategy() == ShipStrategyType.PARTITION_HASH
                            || coGroup.getInput2().getShipStrategy() == ShipStrategyType.FORWARD);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    // --------------------------------------------------------------------------------------------

    private static class TestPartitionerInt implements Partitioner<Integer> {
        @Override
        public int partition(Integer key, int numPartitions) {
            return 0;
        }
    }

    private static class TestPartitionerLong implements Partitioner<Long> {
        @Override
        public int partition(Long key, int numPartitions) {
            return 0;
        }
    }

    public static class Pojo2 {
        public int a;
        public int b;
    }

    public static class Pojo3 {
        public int a;
        public int b;
        public int c;
    }

    private static class Pojo2KeySelector implements KeySelector<Pojo2, Integer> {
        @Override
        public Integer getKey(Pojo2 value) {
            return value.a;
        }
    }

    private static class Pojo3KeySelector implements KeySelector<Pojo3, Integer> {
        @Override
        public Integer getKey(Pojo3 value) {
            return value.b;
        }
    }
}
