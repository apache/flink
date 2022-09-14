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
import org.apache.flink.api.common.operators.util.FieldList;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.optimizer.plan.DualInputPlanNode;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plan.SingleInputPlanNode;
import org.apache.flink.optimizer.plantranslate.JobGraphGenerator;
import org.apache.flink.optimizer.testfunctions.IdentityGroupReducer;
import org.apache.flink.optimizer.testfunctions.IdentityJoiner;
import org.apache.flink.optimizer.testfunctions.IdentityMapper;
import org.apache.flink.optimizer.util.CompilerTestBase;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests that validate optimizer choices when using operators that are requesting certain specific
 * execution strategies.
 */
public class WorksetIterationsRecordApiCompilerTest extends CompilerTestBase {

    private static final long serialVersionUID = 1L;

    private static final String ITERATION_NAME = "Test Workset Iteration";
    private static final String JOIN_WITH_INVARIANT_NAME = "Test Join Invariant";
    private static final String JOIN_WITH_SOLUTION_SET = "Test Join SolutionSet";
    private static final String NEXT_WORKSET_REDUCER_NAME = "Test Reduce Workset";
    private static final String SOLUTION_DELTA_MAPPER_NAME = "Test Map Delta";

    private final FieldList list0 = new FieldList(0);

    @Test
    public void testRecordApiWithDeferredSoltionSetUpdateWithMapper() {
        Plan plan = getTestPlan(false, true);

        OptimizedPlan oPlan;
        try {
            oPlan = compileNoStats(plan);
        } catch (CompilerException ce) {
            ce.printStackTrace();
            fail("The pact compiler is unable to compile this plan correctly.");
            return; // silence the compiler
        }

        OptimizerPlanNodeResolver resolver = getOptimizerPlanNodeResolver(oPlan);
        DualInputPlanNode joinWithInvariantNode = resolver.getNode(JOIN_WITH_INVARIANT_NAME);
        DualInputPlanNode joinWithSolutionSetNode = resolver.getNode(JOIN_WITH_SOLUTION_SET);
        SingleInputPlanNode worksetReducer = resolver.getNode(NEXT_WORKSET_REDUCER_NAME);
        SingleInputPlanNode deltaMapper = resolver.getNode(SOLUTION_DELTA_MAPPER_NAME);

        // iteration preserves partitioning in reducer, so the first partitioning is out of the
        // loop,
        // the in-loop partitioning is before the final reducer

        // verify joinWithInvariant
        assertEquals(ShipStrategyType.FORWARD, joinWithInvariantNode.getInput1().getShipStrategy());
        assertEquals(
                ShipStrategyType.PARTITION_HASH,
                joinWithInvariantNode.getInput2().getShipStrategy());
        assertEquals(list0, joinWithInvariantNode.getKeysForInput1());
        assertEquals(list0, joinWithInvariantNode.getKeysForInput2());

        // verify joinWithSolutionSet
        assertEquals(
                ShipStrategyType.FORWARD, joinWithSolutionSetNode.getInput1().getShipStrategy());
        assertEquals(
                ShipStrategyType.FORWARD, joinWithSolutionSetNode.getInput2().getShipStrategy());

        // verify reducer
        assertEquals(ShipStrategyType.PARTITION_HASH, worksetReducer.getInput().getShipStrategy());
        assertEquals(list0, worksetReducer.getKeys(0));

        // currently, the system may partition before or after the mapper
        ShipStrategyType ss1 = deltaMapper.getInput().getShipStrategy();
        ShipStrategyType ss2 = deltaMapper.getOutgoingChannels().get(0).getShipStrategy();

        assertTrue(
                (ss1 == ShipStrategyType.FORWARD && ss2 == ShipStrategyType.PARTITION_HASH)
                        || (ss2 == ShipStrategyType.FORWARD
                                && ss1 == ShipStrategyType.PARTITION_HASH));

        new JobGraphGenerator().compileJobGraph(oPlan);
    }

    @Test
    public void testRecordApiWithDeferredSoltionSetUpdateWithNonPreservingJoin() {
        Plan plan = getTestPlan(false, false);

        OptimizedPlan oPlan;
        try {
            oPlan = compileNoStats(plan);
        } catch (CompilerException ce) {
            ce.printStackTrace();
            fail("The pact compiler is unable to compile this plan correctly.");
            return; // silence the compiler
        }

        OptimizerPlanNodeResolver resolver = getOptimizerPlanNodeResolver(oPlan);
        DualInputPlanNode joinWithInvariantNode = resolver.getNode(JOIN_WITH_INVARIANT_NAME);
        DualInputPlanNode joinWithSolutionSetNode = resolver.getNode(JOIN_WITH_SOLUTION_SET);
        SingleInputPlanNode worksetReducer = resolver.getNode(NEXT_WORKSET_REDUCER_NAME);

        // iteration preserves partitioning in reducer, so the first partitioning is out of the
        // loop,
        // the in-loop partitioning is before the final reducer

        // verify joinWithInvariant
        assertEquals(ShipStrategyType.FORWARD, joinWithInvariantNode.getInput1().getShipStrategy());
        assertEquals(
                ShipStrategyType.PARTITION_HASH,
                joinWithInvariantNode.getInput2().getShipStrategy());
        assertEquals(list0, joinWithInvariantNode.getKeysForInput1());
        assertEquals(list0, joinWithInvariantNode.getKeysForInput2());

        // verify joinWithSolutionSet
        assertEquals(
                ShipStrategyType.FORWARD, joinWithSolutionSetNode.getInput1().getShipStrategy());
        assertEquals(
                ShipStrategyType.FORWARD, joinWithSolutionSetNode.getInput2().getShipStrategy());

        // verify reducer
        assertEquals(ShipStrategyType.PARTITION_HASH, worksetReducer.getInput().getShipStrategy());
        assertEquals(list0, worksetReducer.getKeys(0));

        // verify solution delta
        assertEquals(2, joinWithSolutionSetNode.getOutgoingChannels().size());
        assertEquals(
                ShipStrategyType.PARTITION_HASH,
                joinWithSolutionSetNode.getOutgoingChannels().get(0).getShipStrategy());
        assertEquals(
                ShipStrategyType.PARTITION_HASH,
                joinWithSolutionSetNode.getOutgoingChannels().get(1).getShipStrategy());

        new JobGraphGenerator().compileJobGraph(oPlan);
    }

    @Test
    public void testRecordApiWithDirectSoltionSetUpdate() {
        Plan plan = getTestPlan(true, false);

        OptimizedPlan oPlan;
        try {
            oPlan = compileNoStats(plan);
        } catch (CompilerException ce) {
            ce.printStackTrace();
            fail("The pact compiler is unable to compile this plan correctly.");
            return; // silence the compiler
        }

        OptimizerPlanNodeResolver resolver = getOptimizerPlanNodeResolver(oPlan);
        DualInputPlanNode joinWithInvariantNode = resolver.getNode(JOIN_WITH_INVARIANT_NAME);
        DualInputPlanNode joinWithSolutionSetNode = resolver.getNode(JOIN_WITH_SOLUTION_SET);
        SingleInputPlanNode worksetReducer = resolver.getNode(NEXT_WORKSET_REDUCER_NAME);

        // iteration preserves partitioning in reducer, so the first partitioning is out of the
        // loop,
        // the in-loop partitioning is before the final reducer

        // verify joinWithInvariant
        assertEquals(ShipStrategyType.FORWARD, joinWithInvariantNode.getInput1().getShipStrategy());
        assertEquals(
                ShipStrategyType.PARTITION_HASH,
                joinWithInvariantNode.getInput2().getShipStrategy());
        assertEquals(list0, joinWithInvariantNode.getKeysForInput1());
        assertEquals(list0, joinWithInvariantNode.getKeysForInput2());

        // verify joinWithSolutionSet
        assertEquals(
                ShipStrategyType.FORWARD, joinWithSolutionSetNode.getInput1().getShipStrategy());
        assertEquals(
                ShipStrategyType.FORWARD, joinWithSolutionSetNode.getInput2().getShipStrategy());

        // verify reducer
        assertEquals(ShipStrategyType.FORWARD, worksetReducer.getInput().getShipStrategy());
        assertEquals(list0, worksetReducer.getKeys(0));

        // verify solution delta
        assertEquals(1, joinWithSolutionSetNode.getOutgoingChannels().size());
        assertEquals(
                ShipStrategyType.FORWARD,
                joinWithSolutionSetNode.getOutgoingChannels().get(0).getShipStrategy());

        new JobGraphGenerator().compileJobGraph(oPlan);
    }

    private Plan getTestPlan(boolean joinPreservesSolutionSet, boolean mapBeforeSolutionDelta) {

        // construct the plan
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);
        DataSet<Tuple2<Long, Long>> solSetInput =
                env.readCsvFile("/tmp/sol.csv").types(Long.class, Long.class).name("Solution Set");
        DataSet<Tuple2<Long, Long>> workSetInput =
                env.readCsvFile("/tmp/sol.csv").types(Long.class, Long.class).name("Workset");
        DataSet<Tuple2<Long, Long>> invariantInput =
                env.readCsvFile("/tmp/sol.csv")
                        .types(Long.class, Long.class)
                        .name("Invariant Input");

        DeltaIteration<Tuple2<Long, Long>, Tuple2<Long, Long>> deltaIt =
                solSetInput.iterateDelta(workSetInput, 100, 0).name(ITERATION_NAME);

        DataSet<Tuple2<Long, Long>> join1 =
                deltaIt.getWorkset()
                        .join(invariantInput)
                        .where(0)
                        .equalTo(0)
                        .with(new IdentityJoiner<Tuple2<Long, Long>>())
                        .withForwardedFieldsFirst("*")
                        .name(JOIN_WITH_INVARIANT_NAME);

        DataSet<Tuple2<Long, Long>> join2 =
                deltaIt.getSolutionSet()
                        .join(join1)
                        .where(0)
                        .equalTo(0)
                        .with(new IdentityJoiner<Tuple2<Long, Long>>())
                        .name(JOIN_WITH_SOLUTION_SET);
        if (joinPreservesSolutionSet) {
            ((JoinOperator<?, ?, ?>) join2).withForwardedFieldsFirst("*");
        }

        DataSet<Tuple2<Long, Long>> nextWorkset =
                join2.groupBy(0)
                        .reduceGroup(new IdentityGroupReducer<Tuple2<Long, Long>>())
                        .withForwardedFields("*")
                        .name(NEXT_WORKSET_REDUCER_NAME);

        if (mapBeforeSolutionDelta) {

            DataSet<Tuple2<Long, Long>> mapper =
                    join2.map(new IdentityMapper<Tuple2<Long, Long>>())
                            .withForwardedFields("*")
                            .name(SOLUTION_DELTA_MAPPER_NAME);

            deltaIt.closeWith(mapper, nextWorkset)
                    .output(new DiscardingOutputFormat<Tuple2<Long, Long>>());
        } else {
            deltaIt.closeWith(join2, nextWorkset)
                    .output(new DiscardingOutputFormat<Tuple2<Long, Long>>());
        }

        return env.createProgramPlan();
    }
}
