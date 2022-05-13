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
import org.apache.flink.api.common.functions.RichJoinFunction;
import org.apache.flink.api.common.operators.GenericDataSourceBase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.optimizer.dag.TempMode;
import org.apache.flink.optimizer.plan.DualInputPlanNode;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plantranslate.JobGraphGenerator;
import org.apache.flink.optimizer.util.CompilerTestBase;
import org.apache.flink.runtime.operators.DriverStrategy;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/** Tests that validate optimizer choice when using hash joins inside of iterations */
@SuppressWarnings("serial")
public class CachedMatchStrategyCompilerTest extends CompilerTestBase {

    /**
     * This tests whether a HYBRIDHASH_BUILD_SECOND is correctly transformed to a
     * HYBRIDHASH_BUILD_SECOND_CACHED when inside of an iteration an on the static path
     */
    @Test
    public void testRightSide() {
        try {

            Plan plan = getTestPlanRightStatic(Optimizer.HINT_LOCAL_STRATEGY_HASH_BUILD_SECOND);

            OptimizedPlan oPlan = compileNoStats(plan);

            OptimizerPlanNodeResolver resolver = getOptimizerPlanNodeResolver(oPlan);
            DualInputPlanNode innerJoin = resolver.getNode("DummyJoiner");

            // verify correct join strategy
            assertEquals(
                    DriverStrategy.HYBRIDHASH_BUILD_SECOND_CACHED, innerJoin.getDriverStrategy());
            assertEquals(TempMode.NONE, innerJoin.getInput1().getTempMode());
            assertEquals(TempMode.NONE, innerJoin.getInput2().getTempMode());

            new JobGraphGenerator().compileJobGraph(oPlan);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            fail("Test errored: " + e.getMessage());
        }
    }

    /**
     * This test makes sure that only a HYBRIDHASH on the static path is transformed to the cached
     * variant
     */
    @Test
    public void testRightSideCountercheck() {
        try {

            Plan plan = getTestPlanRightStatic(Optimizer.HINT_LOCAL_STRATEGY_HASH_BUILD_FIRST);

            OptimizedPlan oPlan = compileNoStats(plan);

            OptimizerPlanNodeResolver resolver = getOptimizerPlanNodeResolver(oPlan);
            DualInputPlanNode innerJoin = resolver.getNode("DummyJoiner");

            // verify correct join strategy
            assertEquals(DriverStrategy.HYBRIDHASH_BUILD_FIRST, innerJoin.getDriverStrategy());
            assertEquals(TempMode.NONE, innerJoin.getInput1().getTempMode());
            assertEquals(TempMode.CACHED, innerJoin.getInput2().getTempMode());

            new JobGraphGenerator().compileJobGraph(oPlan);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            fail("Test errored: " + e.getMessage());
        }
    }

    /**
     * This tests whether a HYBRIDHASH_BUILD_FIRST is correctly transformed to a
     * HYBRIDHASH_BUILD_FIRST_CACHED when inside of an iteration an on the static path
     */
    @Test
    public void testLeftSide() {
        try {

            Plan plan = getTestPlanLeftStatic(Optimizer.HINT_LOCAL_STRATEGY_HASH_BUILD_FIRST);

            OptimizedPlan oPlan = compileNoStats(plan);

            OptimizerPlanNodeResolver resolver = getOptimizerPlanNodeResolver(oPlan);
            DualInputPlanNode innerJoin = resolver.getNode("DummyJoiner");

            // verify correct join strategy
            assertEquals(
                    DriverStrategy.HYBRIDHASH_BUILD_FIRST_CACHED, innerJoin.getDriverStrategy());
            assertEquals(TempMode.NONE, innerJoin.getInput1().getTempMode());
            assertEquals(TempMode.NONE, innerJoin.getInput2().getTempMode());

            new JobGraphGenerator().compileJobGraph(oPlan);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            fail("Test errored: " + e.getMessage());
        }
    }

    /**
     * This test makes sure that only a HYBRIDHASH on the static path is transformed to the cached
     * variant
     */
    @Test
    public void testLeftSideCountercheck() {
        try {

            Plan plan = getTestPlanLeftStatic(Optimizer.HINT_LOCAL_STRATEGY_HASH_BUILD_SECOND);

            OptimizedPlan oPlan = compileNoStats(plan);

            OptimizerPlanNodeResolver resolver = getOptimizerPlanNodeResolver(oPlan);
            DualInputPlanNode innerJoin = resolver.getNode("DummyJoiner");

            // verify correct join strategy
            assertEquals(DriverStrategy.HYBRIDHASH_BUILD_SECOND, innerJoin.getDriverStrategy());
            assertEquals(TempMode.CACHED, innerJoin.getInput1().getTempMode());
            assertEquals(TempMode.NONE, innerJoin.getInput2().getTempMode());

            new JobGraphGenerator().compileJobGraph(oPlan);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            fail("Test errored: " + e.getMessage());
        }
    }

    /**
     * This test simulates a join of a big left side with a small right side inside of an iteration,
     * where the small side is on a static path. Currently the best execution plan is a
     * HYBRIDHASH_BUILD_SECOND_CACHED, where the small side is hashed and cached. This test also
     * makes sure that all relevant plans are correctly enumerated by the optimizer.
     */
    @Test
    public void testCorrectChoosing() {
        try {

            Plan plan = getTestPlanRightStatic("");

            SourceCollectorVisitor sourceCollector = new SourceCollectorVisitor();
            plan.accept(sourceCollector);

            for (GenericDataSourceBase<?, ?> s : sourceCollector.getSources()) {
                if (s.getName().equals("bigFile")) {
                    this.setSourceStatistics(s, 10000000, 1000);
                } else if (s.getName().equals("smallFile")) {
                    this.setSourceStatistics(s, 100, 100);
                }
            }

            OptimizedPlan oPlan = compileNoStats(plan);

            OptimizerPlanNodeResolver resolver = getOptimizerPlanNodeResolver(oPlan);
            DualInputPlanNode innerJoin = resolver.getNode("DummyJoiner");

            // verify correct join strategy
            assertEquals(
                    DriverStrategy.HYBRIDHASH_BUILD_SECOND_CACHED, innerJoin.getDriverStrategy());
            assertEquals(TempMode.NONE, innerJoin.getInput1().getTempMode());
            assertEquals(TempMode.NONE, innerJoin.getInput2().getTempMode());

            new JobGraphGenerator().compileJobGraph(oPlan);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            fail("Test errored: " + e.getMessage());
        }
    }

    private Plan getTestPlanRightStatic(String strategy) {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);

        DataSet<Tuple3<Long, Long, Long>> bigInput =
                env.readCsvFile("file://bigFile")
                        .types(Long.class, Long.class, Long.class)
                        .name("bigFile");

        DataSet<Tuple3<Long, Long, Long>> smallInput =
                env.readCsvFile("file://smallFile")
                        .types(Long.class, Long.class, Long.class)
                        .name("smallFile");

        IterativeDataSet<Tuple3<Long, Long, Long>> iteration = bigInput.iterate(10);

        Configuration joinStrategy = new Configuration();
        joinStrategy.setString(
                Optimizer.HINT_SHIP_STRATEGY, Optimizer.HINT_SHIP_STRATEGY_REPARTITION_HASH);

        if (!strategy.equals("")) {
            joinStrategy.setString(Optimizer.HINT_LOCAL_STRATEGY, strategy);
        }

        DataSet<Tuple3<Long, Long, Long>> inner =
                iteration
                        .join(smallInput)
                        .where(0)
                        .equalTo(0)
                        .with(new DummyJoiner())
                        .name("DummyJoiner")
                        .withParameters(joinStrategy);

        DataSet<Tuple3<Long, Long, Long>> output = iteration.closeWith(inner);

        output.output(new DiscardingOutputFormat<Tuple3<Long, Long, Long>>());

        return env.createProgramPlan();
    }

    private Plan getTestPlanLeftStatic(String strategy) {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);

        @SuppressWarnings("unchecked")
        DataSet<Tuple3<Long, Long, Long>> bigInput =
                env.fromElements(
                                new Tuple3<Long, Long, Long>(1L, 2L, 3L),
                                new Tuple3<Long, Long, Long>(1L, 2L, 3L),
                                new Tuple3<Long, Long, Long>(1L, 2L, 3L))
                        .name("Big");

        @SuppressWarnings("unchecked")
        DataSet<Tuple3<Long, Long, Long>> smallInput =
                env.fromElements(new Tuple3<Long, Long, Long>(1L, 2L, 3L)).name("Small");

        IterativeDataSet<Tuple3<Long, Long, Long>> iteration = bigInput.iterate(10);

        Configuration joinStrategy = new Configuration();
        joinStrategy.setString(Optimizer.HINT_LOCAL_STRATEGY, strategy);

        DataSet<Tuple3<Long, Long, Long>> inner =
                smallInput
                        .join(iteration)
                        .where(0)
                        .equalTo(0)
                        .with(new DummyJoiner())
                        .name("DummyJoiner")
                        .withParameters(joinStrategy);

        DataSet<Tuple3<Long, Long, Long>> output = iteration.closeWith(inner);

        output.output(new DiscardingOutputFormat<Tuple3<Long, Long, Long>>());

        return env.createProgramPlan();
    }

    private static class DummyJoiner
            extends RichJoinFunction<
                    Tuple3<Long, Long, Long>, Tuple3<Long, Long, Long>, Tuple3<Long, Long, Long>> {

        @Override
        public Tuple3<Long, Long, Long> join(
                Tuple3<Long, Long, Long> first, Tuple3<Long, Long, Long> second) throws Exception {

            return first;
        }
    }
}
