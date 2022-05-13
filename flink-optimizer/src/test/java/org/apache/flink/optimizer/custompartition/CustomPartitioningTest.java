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
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plan.SingleInputPlanNode;
import org.apache.flink.optimizer.plan.SinkPlanNode;
import org.apache.flink.optimizer.testfunctions.IdentityPartitionerMapper;
import org.apache.flink.optimizer.util.CompilerTestBase;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;

import org.junit.Test;

import static org.junit.Assert.*;

@SuppressWarnings({"serial", "unchecked"})
public class CustomPartitioningTest extends CompilerTestBase {

    @Test
    public void testPartitionTuples() {
        try {
            final Partitioner<Integer> part = new TestPartitionerInt();
            final int parallelism = 4;

            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(parallelism);

            DataSet<Tuple2<Integer, Integer>> data =
                    env.fromElements(new Tuple2<Integer, Integer>(0, 0)).rebalance();

            data.partitionCustom(part, 0)
                    .mapPartition(new IdentityPartitionerMapper<Tuple2<Integer, Integer>>())
                    .output(new DiscardingOutputFormat<Tuple2<Integer, Integer>>());

            Plan p = env.createProgramPlan();
            OptimizedPlan op = compileNoStats(p);

            SinkPlanNode sink = op.getDataSinks().iterator().next();
            SingleInputPlanNode mapper = (SingleInputPlanNode) sink.getInput().getSource();
            SingleInputPlanNode partitioner = (SingleInputPlanNode) mapper.getInput().getSource();
            SingleInputPlanNode balancer = (SingleInputPlanNode) partitioner.getInput().getSource();

            assertEquals(ShipStrategyType.FORWARD, sink.getInput().getShipStrategy());
            assertEquals(parallelism, sink.getParallelism());

            assertEquals(ShipStrategyType.FORWARD, mapper.getInput().getShipStrategy());
            assertEquals(parallelism, mapper.getParallelism());

            assertEquals(
                    ShipStrategyType.PARTITION_CUSTOM, partitioner.getInput().getShipStrategy());
            assertEquals(part, partitioner.getInput().getPartitioner());
            assertEquals(parallelism, partitioner.getParallelism());

            assertEquals(
                    ShipStrategyType.PARTITION_FORCED_REBALANCE,
                    balancer.getInput().getShipStrategy());
            assertEquals(parallelism, balancer.getParallelism());
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testPartitionTuplesInvalidType() {
        try {
            final int parallelism = 4;

            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(parallelism);

            DataSet<Tuple2<Integer, Integer>> data =
                    env.fromElements(new Tuple2<Integer, Integer>(0, 0)).rebalance();

            try {
                data.partitionCustom(new TestPartitionerLong(), 0);
                fail("Should throw an exception");
            } catch (InvalidProgramException e) {
                // expected
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testPartitionPojo() {
        try {
            final Partitioner<Integer> part = new TestPartitionerInt();
            final int parallelism = 4;

            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(parallelism);

            DataSet<Pojo> data = env.fromElements(new Pojo()).rebalance();

            data.partitionCustom(part, "a")
                    .mapPartition(new IdentityPartitionerMapper<Pojo>())
                    .output(new DiscardingOutputFormat<Pojo>());

            Plan p = env.createProgramPlan();
            OptimizedPlan op = compileNoStats(p);

            SinkPlanNode sink = op.getDataSinks().iterator().next();
            SingleInputPlanNode mapper = (SingleInputPlanNode) sink.getInput().getSource();
            SingleInputPlanNode partitioner = (SingleInputPlanNode) mapper.getInput().getSource();
            SingleInputPlanNode balancer = (SingleInputPlanNode) partitioner.getInput().getSource();

            assertEquals(ShipStrategyType.FORWARD, sink.getInput().getShipStrategy());
            assertEquals(parallelism, sink.getParallelism());

            assertEquals(ShipStrategyType.FORWARD, mapper.getInput().getShipStrategy());
            assertEquals(parallelism, mapper.getParallelism());

            assertEquals(
                    ShipStrategyType.PARTITION_CUSTOM, partitioner.getInput().getShipStrategy());
            assertEquals(part, partitioner.getInput().getPartitioner());
            assertEquals(parallelism, partitioner.getParallelism());

            assertEquals(
                    ShipStrategyType.PARTITION_FORCED_REBALANCE,
                    balancer.getInput().getShipStrategy());
            assertEquals(parallelism, balancer.getParallelism());
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testPartitionPojoInvalidType() {
        try {
            final int parallelism = 4;

            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(parallelism);

            DataSet<Pojo> data = env.fromElements(new Pojo()).rebalance();

            try {
                data.partitionCustom(new TestPartitionerLong(), "a");
                fail("Should throw an exception");
            } catch (InvalidProgramException e) {
                // expected
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testPartitionKeySelector() {
        try {
            final Partitioner<Integer> part = new TestPartitionerInt();
            final int parallelism = 4;

            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(parallelism);

            DataSet<Pojo> data = env.fromElements(new Pojo()).rebalance();

            data.partitionCustom(part, new TestKeySelectorInt<Pojo>())
                    .mapPartition(new IdentityPartitionerMapper<Pojo>())
                    .output(new DiscardingOutputFormat<Pojo>());

            Plan p = env.createProgramPlan();
            OptimizedPlan op = compileNoStats(p);

            SinkPlanNode sink = op.getDataSinks().iterator().next();
            SingleInputPlanNode mapper = (SingleInputPlanNode) sink.getInput().getSource();
            SingleInputPlanNode keyRemover = (SingleInputPlanNode) mapper.getInput().getSource();
            SingleInputPlanNode partitioner =
                    (SingleInputPlanNode) keyRemover.getInput().getSource();
            SingleInputPlanNode keyExtractor =
                    (SingleInputPlanNode) partitioner.getInput().getSource();
            SingleInputPlanNode balancer =
                    (SingleInputPlanNode) keyExtractor.getInput().getSource();

            assertEquals(ShipStrategyType.FORWARD, sink.getInput().getShipStrategy());
            assertEquals(parallelism, sink.getParallelism());

            assertEquals(ShipStrategyType.FORWARD, mapper.getInput().getShipStrategy());
            assertEquals(parallelism, mapper.getParallelism());

            assertEquals(ShipStrategyType.FORWARD, keyRemover.getInput().getShipStrategy());
            assertEquals(parallelism, keyRemover.getParallelism());

            assertEquals(
                    ShipStrategyType.PARTITION_CUSTOM, partitioner.getInput().getShipStrategy());
            assertEquals(part, partitioner.getInput().getPartitioner());
            assertEquals(parallelism, partitioner.getParallelism());

            assertEquals(ShipStrategyType.FORWARD, keyExtractor.getInput().getShipStrategy());
            assertEquals(parallelism, keyExtractor.getParallelism());

            assertEquals(
                    ShipStrategyType.PARTITION_FORCED_REBALANCE,
                    balancer.getInput().getShipStrategy());
            assertEquals(parallelism, balancer.getParallelism());
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testPartitionKeySelectorInvalidType() {
        try {
            final Partitioner<Integer> part =
                    (Partitioner<Integer>) (Partitioner<?>) new TestPartitionerLong();
            final int parallelism = 4;

            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(parallelism);

            DataSet<Pojo> data = env.fromElements(new Pojo()).rebalance();

            try {
                data.partitionCustom(part, new TestKeySelectorInt<Pojo>());
                fail("Should throw an exception");
            } catch (InvalidProgramException e) {
                // expected
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    // --------------------------------------------------------------------------------------------

    public static class Pojo {
        public int a;
        public int b;
    }

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

    private static class TestKeySelectorInt<T> implements KeySelector<T, Integer> {
        @Override
        public Integer getKey(T value) {
            return null;
        }
    }
}
