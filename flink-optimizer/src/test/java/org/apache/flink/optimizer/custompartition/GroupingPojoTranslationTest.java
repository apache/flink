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
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plan.SingleInputPlanNode;
import org.apache.flink.optimizer.plan.SinkPlanNode;
import org.apache.flink.optimizer.testfunctions.IdentityGroupReducerCombinable;
import org.apache.flink.optimizer.testfunctions.SelectOneReducer;
import org.apache.flink.optimizer.util.CompilerTestBase;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;

import org.junit.Test;

import static org.junit.Assert.*;

@SuppressWarnings("serial")
public class GroupingPojoTranslationTest extends CompilerTestBase {

    @Test
    public void testCustomPartitioningTupleReduce() {
        try {
            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

            DataSet<Pojo2> data = env.fromElements(new Pojo2()).rebalance().setParallelism(4);

            data.groupBy("a")
                    .withPartitioner(new TestPartitionerInt())
                    .reduce(new SelectOneReducer<Pojo2>())
                    .output(new DiscardingOutputFormat<Pojo2>());

            Plan p = env.createProgramPlan();
            OptimizedPlan op = compileNoStats(p);

            SinkPlanNode sink = op.getDataSinks().iterator().next();
            SingleInputPlanNode reducer = (SingleInputPlanNode) sink.getInput().getSource();
            SingleInputPlanNode combiner = (SingleInputPlanNode) reducer.getInput().getSource();

            assertEquals(ShipStrategyType.FORWARD, sink.getInput().getShipStrategy());
            assertEquals(ShipStrategyType.PARTITION_CUSTOM, reducer.getInput().getShipStrategy());
            assertEquals(ShipStrategyType.FORWARD, combiner.getInput().getShipStrategy());
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testCustomPartitioningTupleGroupReduce() {
        try {
            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

            DataSet<Pojo2> data = env.fromElements(new Pojo2()).rebalance().setParallelism(4);

            data.groupBy("a")
                    .withPartitioner(new TestPartitionerInt())
                    .reduceGroup(new IdentityGroupReducerCombinable<Pojo2>())
                    .output(new DiscardingOutputFormat<Pojo2>());

            Plan p = env.createProgramPlan();
            OptimizedPlan op = compileNoStats(p);

            SinkPlanNode sink = op.getDataSinks().iterator().next();
            SingleInputPlanNode reducer = (SingleInputPlanNode) sink.getInput().getSource();
            SingleInputPlanNode combiner = (SingleInputPlanNode) reducer.getInput().getSource();

            assertEquals(ShipStrategyType.FORWARD, sink.getInput().getShipStrategy());
            assertEquals(ShipStrategyType.PARTITION_CUSTOM, reducer.getInput().getShipStrategy());
            assertEquals(ShipStrategyType.FORWARD, combiner.getInput().getShipStrategy());
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testCustomPartitioningTupleGroupReduceSorted() {
        try {
            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

            DataSet<Pojo3> data = env.fromElements(new Pojo3()).rebalance().setParallelism(4);

            data.groupBy("a")
                    .withPartitioner(new TestPartitionerInt())
                    .sortGroup("b", Order.ASCENDING)
                    .reduceGroup(new IdentityGroupReducerCombinable<Pojo3>())
                    .output(new DiscardingOutputFormat<Pojo3>());

            Plan p = env.createProgramPlan();
            OptimizedPlan op = compileNoStats(p);

            SinkPlanNode sink = op.getDataSinks().iterator().next();
            SingleInputPlanNode reducer = (SingleInputPlanNode) sink.getInput().getSource();
            SingleInputPlanNode combiner = (SingleInputPlanNode) reducer.getInput().getSource();

            assertEquals(ShipStrategyType.FORWARD, sink.getInput().getShipStrategy());
            assertEquals(ShipStrategyType.PARTITION_CUSTOM, reducer.getInput().getShipStrategy());
            assertEquals(ShipStrategyType.FORWARD, combiner.getInput().getShipStrategy());
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testCustomPartitioningTupleGroupReduceSorted2() {
        try {
            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

            DataSet<Pojo4> data = env.fromElements(new Pojo4()).rebalance().setParallelism(4);

            data.groupBy("a")
                    .withPartitioner(new TestPartitionerInt())
                    .sortGroup("b", Order.ASCENDING)
                    .sortGroup("c", Order.DESCENDING)
                    .reduceGroup(new IdentityGroupReducerCombinable<Pojo4>())
                    .output(new DiscardingOutputFormat<Pojo4>());

            Plan p = env.createProgramPlan();
            OptimizedPlan op = compileNoStats(p);

            SinkPlanNode sink = op.getDataSinks().iterator().next();
            SingleInputPlanNode reducer = (SingleInputPlanNode) sink.getInput().getSource();
            SingleInputPlanNode combiner = (SingleInputPlanNode) reducer.getInput().getSource();

            assertEquals(ShipStrategyType.FORWARD, sink.getInput().getShipStrategy());
            assertEquals(ShipStrategyType.PARTITION_CUSTOM, reducer.getInput().getShipStrategy());
            assertEquals(ShipStrategyType.FORWARD, combiner.getInput().getShipStrategy());
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testCustomPartitioningTupleInvalidType() {
        try {
            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

            DataSet<Pojo2> data = env.fromElements(new Pojo2()).rebalance().setParallelism(4);

            try {
                data.groupBy("a").withPartitioner(new TestPartitionerLong());
                fail("Should throw an exception");
            } catch (InvalidProgramException e) {
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testCustomPartitioningTupleInvalidTypeSorted() {
        try {
            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

            DataSet<Pojo3> data = env.fromElements(new Pojo3()).rebalance().setParallelism(4);

            try {
                data.groupBy("a")
                        .sortGroup("b", Order.ASCENDING)
                        .withPartitioner(new TestPartitionerLong());
                fail("Should throw an exception");
            } catch (InvalidProgramException e) {
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testCustomPartitioningTupleRejectCompositeKey() {
        try {
            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

            DataSet<Pojo2> data = env.fromElements(new Pojo2()).rebalance().setParallelism(4);

            try {
                data.groupBy("a", "b").withPartitioner(new TestPartitionerInt());
                fail("Should throw an exception");
            } catch (InvalidProgramException e) {
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    // --------------------------------------------------------------------------------------------

    public static class Pojo2 {
        public int a;
        public int b;
    }

    public static class Pojo3 {
        public int a;
        public int b;
        public int c;
    }

    public static class Pojo4 {
        public int a;
        public int b;
        public int c;
        public int d;
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
}
