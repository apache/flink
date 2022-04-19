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
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.util.FieldList;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.optimizer.plan.Channel;
import org.apache.flink.optimizer.plan.DualInputPlanNode;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plan.SingleInputPlanNode;
import org.apache.flink.optimizer.plan.SinkPlanNode;
import org.apache.flink.optimizer.testfunctions.IdentityCoGrouper;
import org.apache.flink.optimizer.testfunctions.IdentityGroupReducer;
import org.apache.flink.optimizer.util.CompilerTestBase;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.runtime.operators.util.LocalStrategy;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * This test case has been created to validate that correct strategies are used if orders within
 * groups are requested.
 */
@SuppressWarnings({"serial"})
public class GroupOrderTest extends CompilerTestBase {

    @Test
    void testReduceWithGroupOrder() {
        // construct the plan
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);
        DataSet<Tuple4<Long, Long, Long, Long>> set1 =
                env.readCsvFile("/tmp/fake.csv")
                        .types(Long.class, Long.class, Long.class, Long.class);

        set1.groupBy(1)
                .sortGroup(3, Order.DESCENDING)
                .reduceGroup(new IdentityGroupReducer<Tuple4<Long, Long, Long, Long>>())
                .name("Reduce")
                .output(new DiscardingOutputFormat<Tuple4<Long, Long, Long, Long>>())
                .name("Sink");

        Plan plan = env.createProgramPlan();
        OptimizedPlan oPlan;

        try {
            oPlan = compileNoStats(plan);
        } catch (CompilerException ce) {
            ce.printStackTrace();
            fail("The pact compiler is unable to compile this plan correctly.");
            return; // silence the compiler
        }

        OptimizerPlanNodeResolver resolver = getOptimizerPlanNodeResolver(oPlan);
        SinkPlanNode sinkNode = resolver.getNode("Sink");
        SingleInputPlanNode reducer = resolver.getNode("Reduce");

        // verify the strategies
        assertThat(sinkNode.getInput().getShipStrategy()).isEqualTo(ShipStrategyType.FORWARD);
        assertThat(reducer.getInput().getShipStrategy()).isEqualTo(ShipStrategyType.PARTITION_HASH);

        Channel c = reducer.getInput();
        assertThat(c.getLocalStrategy()).isEqualTo(LocalStrategy.SORT);

        FieldList ship = new FieldList(1);
        FieldList local = new FieldList(1, 3);
        assertThat(c.getShipStrategyKeys()).isEqualTo(ship);
        assertThat(c.getLocalStrategyKeys()).isEqualTo(local);
        assertThat(c.getLocalStrategySortOrder()[0]).isSameAs(reducer.getSortOrders(0)[0]);

        // check that we indeed sort descending
        assertThat(c.getLocalStrategySortOrder()[1]).isFalse();
    }

    @Test
    void testCoGroupWithGroupOrder() {
        // construct the plan
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);
        DataSet<Tuple7<Long, Long, Long, Long, Long, Long, Long>> set1 =
                env.readCsvFile("/tmp/fake1.csv")
                        .types(
                                Long.class,
                                Long.class,
                                Long.class,
                                Long.class,
                                Long.class,
                                Long.class,
                                Long.class);
        DataSet<Tuple7<Long, Long, Long, Long, Long, Long, Long>> set2 =
                env.readCsvFile("/tmp/fake2.csv")
                        .types(
                                Long.class,
                                Long.class,
                                Long.class,
                                Long.class,
                                Long.class,
                                Long.class,
                                Long.class);

        set1.coGroup(set2)
                .where(3, 0)
                .equalTo(6, 0)
                .sortFirstGroup(5, Order.DESCENDING)
                .sortSecondGroup(1, Order.DESCENDING)
                .sortSecondGroup(4, Order.ASCENDING)
                .with(new IdentityCoGrouper<Tuple7<Long, Long, Long, Long, Long, Long, Long>>())
                .name("CoGroup")
                .output(
                        new DiscardingOutputFormat<
                                Tuple7<Long, Long, Long, Long, Long, Long, Long>>())
                .name("Sink");

        Plan plan = env.createProgramPlan();
        OptimizedPlan oPlan;

        try {
            oPlan = compileNoStats(plan);
        } catch (CompilerException ce) {
            ce.printStackTrace();
            fail("The pact compiler is unable to compile this plan correctly.");
            return; // silence the compiler
        }

        OptimizerPlanNodeResolver resolver = getOptimizerPlanNodeResolver(oPlan);
        SinkPlanNode sinkNode = resolver.getNode("Sink");
        DualInputPlanNode coGroupNode = resolver.getNode("CoGroup");

        // verify the strategies
        assertThat(sinkNode.getInput().getShipStrategy()).isEqualTo(ShipStrategyType.FORWARD);
        assertThat(coGroupNode.getInput1().getShipStrategy())
                .isEqualTo(ShipStrategyType.PARTITION_HASH);
        assertThat(coGroupNode.getInput2().getShipStrategy())
                .isEqualTo(ShipStrategyType.PARTITION_HASH);

        Channel c1 = coGroupNode.getInput1();
        Channel c2 = coGroupNode.getInput2();

        assertThat(c1.getLocalStrategy()).isEqualTo(LocalStrategy.SORT);
        assertThat(c2.getLocalStrategy()).isEqualTo(LocalStrategy.SORT);

        FieldList ship1 = new FieldList(3, 0);
        FieldList ship2 = new FieldList(6, 0);

        FieldList local1 = new FieldList(3, 0, 5);
        FieldList local2 = new FieldList(6, 0, 1, 4);

        assertThat(c1.getShipStrategyKeys()).isEqualTo(ship1);
        assertThat(c2.getShipStrategyKeys()).isEqualTo(ship2);
        assertThat(c1.getLocalStrategyKeys()).isEqualTo(local1);
        assertThat(c2.getLocalStrategyKeys()).isEqualTo(local2);

        assertThat(c1.getLocalStrategySortOrder()[0]).isSameAs(coGroupNode.getSortOrders()[0]);
        assertThat(c1.getLocalStrategySortOrder()[1]).isSameAs(coGroupNode.getSortOrders()[1]);
        assertThat(c2.getLocalStrategySortOrder()[0]).isSameAs(coGroupNode.getSortOrders()[0]);
        assertThat(c2.getLocalStrategySortOrder()[1]).isSameAs(coGroupNode.getSortOrders()[1]);

        // check that the local group orderings are correct
        assertThat(c1.getLocalStrategySortOrder()[2]).isFalse();
        assertThat(c2.getLocalStrategySortOrder()[2]).isFalse();
        assertThat(c2.getLocalStrategySortOrder()[3]).isTrue();
    }
}
