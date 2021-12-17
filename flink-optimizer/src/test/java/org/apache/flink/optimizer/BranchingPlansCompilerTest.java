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
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichJoinFunction;
import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plan.SinkPlanNode;
import org.apache.flink.optimizer.plantranslate.JobGraphGenerator;
import org.apache.flink.optimizer.testfunctions.DummyCoGroupFunction;
import org.apache.flink.optimizer.testfunctions.DummyFlatJoinFunction;
import org.apache.flink.optimizer.testfunctions.IdentityCoGrouper;
import org.apache.flink.optimizer.testfunctions.IdentityCrosser;
import org.apache.flink.optimizer.testfunctions.IdentityGroupReducer;
import org.apache.flink.optimizer.testfunctions.IdentityJoiner;
import org.apache.flink.optimizer.testfunctions.IdentityKeyExtractor;
import org.apache.flink.optimizer.testfunctions.IdentityMapper;
import org.apache.flink.optimizer.testfunctions.SelectOneReducer;
import org.apache.flink.optimizer.testfunctions.Top1GroupReducer;
import org.apache.flink.optimizer.util.CompilerTestBase;
import org.apache.flink.util.Collector;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;

@SuppressWarnings({"serial"})
public class BranchingPlansCompilerTest extends CompilerTestBase {

    @Test
    public void testCostComputationWithMultipleDataSinks() {
        final int SINKS = 5;

        try {
            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(DEFAULT_PARALLELISM);

            DataSet<Long> source = env.generateSequence(1, 10000);

            DataSet<Long> mappedA = source.map(new IdentityMapper<Long>());
            DataSet<Long> mappedC = source.map(new IdentityMapper<Long>());

            for (int sink = 0; sink < SINKS; sink++) {
                mappedA.output(new DiscardingOutputFormat<Long>());
                mappedC.output(new DiscardingOutputFormat<Long>());
            }

            Plan plan = env.createProgramPlan("Plans With Multiple Data Sinks");
            OptimizedPlan oPlan = compileNoStats(plan);

            new JobGraphGenerator().compileJobGraph(oPlan);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    /**
     *
     *
     * <pre>
     *                (SRC A)
     *                   |
     *                (MAP A)
     *             /         \
     *          (MAP B)      (MAP C)
     *           /           /     \
     *        (SINK A)    (SINK B)  (SINK C)
     * </pre>
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testBranchingWithMultipleDataSinks2() {
        try {
            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(DEFAULT_PARALLELISM);

            DataSet<Long> source = env.generateSequence(1, 10000);

            DataSet<Long> mappedA = source.map(new IdentityMapper<Long>());
            DataSet<Long> mappedB = mappedA.map(new IdentityMapper<Long>());
            DataSet<Long> mappedC = mappedA.map(new IdentityMapper<Long>());

            mappedB.output(new DiscardingOutputFormat<Long>());
            mappedC.output(new DiscardingOutputFormat<Long>());
            mappedC.output(new DiscardingOutputFormat<Long>());

            Plan plan = env.createProgramPlan();
            Set<Operator<?>> sinks = new HashSet<Operator<?>>(plan.getDataSinks());

            OptimizedPlan oPlan = compileNoStats(plan);

            // ---------- check the optimizer plan ----------

            // number of sinks
            assertEquals("Wrong number of data sinks.", 3, oPlan.getDataSinks().size());

            // remove matching sinks to check relation
            for (SinkPlanNode sink : oPlan.getDataSinks()) {
                assertTrue(sinks.remove(sink.getProgramOperator()));
            }
            assertTrue(sinks.isEmpty());

            new JobGraphGenerator().compileJobGraph(oPlan);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    /**
     *
     *
     * <pre>
     *                              SINK
     *                               |
     *                            COGROUP
     *                        +---/    \----+
     *                       /               \
     *                      /             MATCH10
     *                     /               |    \
     *                    /                |  MATCH9
     *                MATCH5               |  |   \
     *                |   \                |  | MATCH8
     *                | MATCH4             |  |  |   \
     *                |  |   \             |  |  | MATCH7
     *                |  | MATCH3          |  |  |  |   \
     *                |  |  |   \          |  |  |  | MATCH6
     *                |  |  | MATCH2       |  |  |  |  |  |
     *                |  |  |  |   \       +--+--+--+--+--+
     *                |  |  |  | MATCH1            MAP
     *                \  |  |  |  |  | /-----------/
     *                (DATA SOURCE ONE)
     * </pre>
     */
    @Test
    public void testBranchingSourceMultipleTimes() {
        try {
            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(DEFAULT_PARALLELISM);

            DataSet<Tuple2<Long, Long>> source =
                    env.generateSequence(1, 10000000).map(new Duplicator<Long>());

            DataSet<Tuple2<Long, Long>> joined1 =
                    source.join(source)
                            .where(0)
                            .equalTo(0)
                            .with(new DummyFlatJoinFunction<Tuple2<Long, Long>>());

            DataSet<Tuple2<Long, Long>> joined2 =
                    source.join(joined1)
                            .where(0)
                            .equalTo(0)
                            .with(new DummyFlatJoinFunction<Tuple2<Long, Long>>());

            DataSet<Tuple2<Long, Long>> joined3 =
                    source.join(joined2)
                            .where(0)
                            .equalTo(0)
                            .with(new DummyFlatJoinFunction<Tuple2<Long, Long>>());

            DataSet<Tuple2<Long, Long>> joined4 =
                    source.join(joined3)
                            .where(0)
                            .equalTo(0)
                            .with(new DummyFlatJoinFunction<Tuple2<Long, Long>>());

            DataSet<Tuple2<Long, Long>> joined5 =
                    source.join(joined4)
                            .where(0)
                            .equalTo(0)
                            .with(new DummyFlatJoinFunction<Tuple2<Long, Long>>());

            DataSet<Tuple2<Long, Long>> mapped =
                    source.map(
                            new MapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>>() {
                                @Override
                                public Tuple2<Long, Long> map(Tuple2<Long, Long> value) {
                                    return null;
                                }
                            });

            DataSet<Tuple2<Long, Long>> joined6 =
                    mapped.join(mapped)
                            .where(0)
                            .equalTo(0)
                            .with(new DummyFlatJoinFunction<Tuple2<Long, Long>>());

            DataSet<Tuple2<Long, Long>> joined7 =
                    mapped.join(joined6)
                            .where(0)
                            .equalTo(0)
                            .with(new DummyFlatJoinFunction<Tuple2<Long, Long>>());

            DataSet<Tuple2<Long, Long>> joined8 =
                    mapped.join(joined7)
                            .where(0)
                            .equalTo(0)
                            .with(new DummyFlatJoinFunction<Tuple2<Long, Long>>());

            DataSet<Tuple2<Long, Long>> joined9 =
                    mapped.join(joined8)
                            .where(0)
                            .equalTo(0)
                            .with(new DummyFlatJoinFunction<Tuple2<Long, Long>>());

            DataSet<Tuple2<Long, Long>> joined10 =
                    mapped.join(joined9)
                            .where(0)
                            .equalTo(0)
                            .with(new DummyFlatJoinFunction<Tuple2<Long, Long>>());

            joined5.coGroup(joined10)
                    .where(1)
                    .equalTo(1)
                    .with(new DummyCoGroupFunction<Tuple2<Long, Long>, Tuple2<Long, Long>>())
                    .output(
                            new DiscardingOutputFormat<
                                    Tuple2<Tuple2<Long, Long>, Tuple2<Long, Long>>>());

            Plan plan = env.createProgramPlan();
            OptimizedPlan oPlan = compileNoStats(plan);
            new JobGraphGenerator().compileJobGraph(oPlan);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    /**
     *
     *
     * <pre>
     *
     *              (SINK A)
     *                  |    (SINK B)    (SINK C)
     *                CROSS    /          /
     *               /     \   |  +------+
     *              /       \  | /
     *          REDUCE      MATCH2
     *             |    +---/    \
     *              \  /          |
     *               MAP          |
     *                |           |
     *             COGROUP      MATCH1
     *             /     \     /     \
     *        (SRC A)    (SRC B)    (SRC C)
     * </pre>
     */
    @Test
    public void testBranchingWithMultipleDataSinks() {
        try {
            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(DEFAULT_PARALLELISM);

            DataSet<Tuple2<Long, Long>> sourceA =
                    env.generateSequence(1, 10000000).map(new Duplicator<Long>());

            DataSet<Tuple2<Long, Long>> sourceB =
                    env.generateSequence(1, 10000000).map(new Duplicator<Long>());

            DataSet<Tuple2<Long, Long>> sourceC =
                    env.generateSequence(1, 10000000).map(new Duplicator<Long>());

            DataSet<Tuple2<Long, Long>> mapped =
                    sourceA.coGroup(sourceB)
                            .where(0)
                            .equalTo(1)
                            .with(
                                    new CoGroupFunction<
                                            Tuple2<Long, Long>,
                                            Tuple2<Long, Long>,
                                            Tuple2<Long, Long>>() {
                                        @Override
                                        public void coGroup(
                                                Iterable<Tuple2<Long, Long>> first,
                                                Iterable<Tuple2<Long, Long>> second,
                                                Collector<Tuple2<Long, Long>> out) {}
                                    })
                            .map(new IdentityMapper<Tuple2<Long, Long>>());

            DataSet<Tuple2<Long, Long>> joined =
                    sourceB.join(sourceC)
                            .where(0)
                            .equalTo(1)
                            .with(new DummyFlatJoinFunction<Tuple2<Long, Long>>());

            DataSet<Tuple2<Long, Long>> joined2 =
                    mapped.join(joined)
                            .where(1)
                            .equalTo(1)
                            .with(new DummyFlatJoinFunction<Tuple2<Long, Long>>());

            DataSet<Tuple2<Long, Long>> reduced =
                    mapped.groupBy(1).reduceGroup(new Top1GroupReducer<Tuple2<Long, Long>>());

            reduced.cross(joined2)
                    .output(
                            new DiscardingOutputFormat<
                                    Tuple2<Tuple2<Long, Long>, Tuple2<Long, Long>>>());

            joined2.output(new DiscardingOutputFormat<Tuple2<Long, Long>>());
            joined2.output(new DiscardingOutputFormat<Tuple2<Long, Long>>());

            Plan plan = env.createProgramPlan();
            OptimizedPlan oPlan = compileNoStats(plan);
            new JobGraphGenerator().compileJobGraph(oPlan);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testBranchEachContractType() {
        try {
            // construct the plan
            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(DEFAULT_PARALLELISM);
            DataSet<Long> sourceA = env.generateSequence(0, 1);
            DataSet<Long> sourceB = env.generateSequence(0, 1);
            DataSet<Long> sourceC = env.generateSequence(0, 1);

            DataSet<Long> map1 = sourceA.map(new IdentityMapper<Long>()).name("Map 1");

            DataSet<Long> reduce1 =
                    map1.groupBy("*")
                            .reduceGroup(new IdentityGroupReducer<Long>())
                            .name("Reduce 1");

            DataSet<Long> join1 =
                    sourceB.union(sourceB)
                            .union(sourceC)
                            .join(sourceC)
                            .where("*")
                            .equalTo("*")
                            .with(new IdentityJoiner<Long>())
                            .name("Join 1");

            DataSet<Long> coGroup1 =
                    sourceA.coGroup(sourceB)
                            .where("*")
                            .equalTo("*")
                            .with(new IdentityCoGrouper<Long>())
                            .name("CoGroup 1");

            DataSet<Long> cross1 =
                    reduce1.cross(coGroup1).with(new IdentityCrosser<Long>()).name("Cross 1");

            DataSet<Long> coGroup2 =
                    cross1.coGroup(cross1)
                            .where("*")
                            .equalTo("*")
                            .with(new IdentityCoGrouper<Long>())
                            .name("CoGroup 2");

            DataSet<Long> coGroup3 =
                    map1.coGroup(join1)
                            .where("*")
                            .equalTo("*")
                            .with(new IdentityCoGrouper<Long>())
                            .name("CoGroup 3");

            DataSet<Long> map2 = coGroup3.map(new IdentityMapper<Long>()).name("Map 2");

            DataSet<Long> coGroup4 =
                    map2.coGroup(join1)
                            .where("*")
                            .equalTo("*")
                            .with(new IdentityCoGrouper<Long>())
                            .name("CoGroup 4");

            DataSet<Long> coGroup5 =
                    coGroup2.coGroup(coGroup1)
                            .where("*")
                            .equalTo("*")
                            .with(new IdentityCoGrouper<Long>())
                            .name("CoGroup 5");

            DataSet<Long> coGroup6 =
                    reduce1.coGroup(coGroup4)
                            .where("*")
                            .equalTo("*")
                            .with(new IdentityCoGrouper<Long>())
                            .name("CoGroup 6");

            DataSet<Long> coGroup7 =
                    coGroup5.coGroup(coGroup6)
                            .where("*")
                            .equalTo("*")
                            .with(new IdentityCoGrouper<Long>())
                            .name("CoGroup 7");

            coGroup7.union(sourceA)
                    .union(coGroup3)
                    .union(coGroup4)
                    .union(coGroup1)
                    .output(new DiscardingOutputFormat<Long>());

            Plan plan = env.createProgramPlan();
            OptimizedPlan oPlan = compileNoStats(plan);

            JobGraphGenerator jobGen = new JobGraphGenerator();

            // Compile plan to verify that no error is thrown
            jobGen.compileJobGraph(oPlan);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testBranchingUnion() {
        try {
            // construct the plan
            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(DEFAULT_PARALLELISM);
            DataSet<Long> source1 = env.generateSequence(0, 1);
            DataSet<Long> source2 = env.generateSequence(0, 1);

            DataSet<Long> join1 =
                    source1.join(source2)
                            .where("*")
                            .equalTo("*")
                            .with(new IdentityJoiner<Long>())
                            .name("Join 1");

            DataSet<Long> map1 = join1.map(new IdentityMapper<Long>()).name("Map 1");

            DataSet<Long> reduce1 =
                    map1.groupBy("*")
                            .reduceGroup(new IdentityGroupReducer<Long>())
                            .name("Reduce 1");

            DataSet<Long> reduce2 =
                    join1.groupBy("*")
                            .reduceGroup(new IdentityGroupReducer<Long>())
                            .name("Reduce 2");

            DataSet<Long> map2 = join1.map(new IdentityMapper<Long>()).name("Map 2");

            DataSet<Long> map3 = map2.map(new IdentityMapper<Long>()).name("Map 3");

            DataSet<Long> join2 =
                    reduce1.union(reduce2)
                            .union(map2)
                            .union(map3)
                            .join(map2, JoinHint.REPARTITION_SORT_MERGE)
                            .where("*")
                            .equalTo("*")
                            .with(new IdentityJoiner<Long>())
                            .name("Join 2");

            join2.output(new DiscardingOutputFormat<Long>());

            Plan plan = env.createProgramPlan();
            OptimizedPlan oPlan = compileNoStats(plan);

            JobGraphGenerator jobGen = new JobGraphGenerator();

            // Compile plan to verify that no error is thrown
            jobGen.compileJobGraph(oPlan);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    /**
     *
     *
     * <pre>
     *             (SRC A)
     *             /     \
     *        (SINK A)    (SINK B)
     * </pre>
     */
    @Test
    public void testBranchingWithMultipleDataSinksSmall() {
        try {
            String outPath1 = "/tmp/out1";
            String outPath2 = "/tmp/out2";

            // construct the plan
            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(DEFAULT_PARALLELISM);
            DataSet<Long> source1 = env.generateSequence(0, 1);

            source1.writeAsText(outPath1);
            source1.writeAsText(outPath2);

            Plan plan = env.createProgramPlan();
            OptimizedPlan oPlan = compileNoStats(plan);

            // ---------- check the optimizer plan ----------

            // number of sinks
            Assert.assertEquals("Wrong number of data sinks.", 2, oPlan.getDataSinks().size());

            // sinks contain all sink paths
            Set<String> allSinks = new HashSet<String>();
            allSinks.add(outPath1);
            allSinks.add(outPath2);

            for (SinkPlanNode n : oPlan.getDataSinks()) {
                String path =
                        ((TextOutputFormat<String>)
                                        n.getSinkNode()
                                                .getOperator()
                                                .getFormatWrapper()
                                                .getUserCodeObject())
                                .getOutputFilePath()
                                .toString();
                Assert.assertTrue("Invalid data sink.", allSinks.remove(path));
            }

            // ---------- compile plan to job graph to verify that no error is thrown ----------

            JobGraphGenerator jobGen = new JobGraphGenerator();
            jobGen.compileJobGraph(oPlan);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    /**
     *
     *
     * <pre>
     *     (SINK 3) (SINK 1)   (SINK 2) (SINK 4)
     *         \     /             \     /
     *         (SRC A)             (SRC B)
     * </pre>
     *
     * NOTE: this case is currently not caught by the compiler. we should enable the test once it is
     * caught.
     */
    @Test
    public void testBranchingDisjointPlan() {
        // construct the plan
        final String out1Path = "file:///test/1";
        final String out2Path = "file:///test/2";
        final String out3Path = "file:///test/3";
        final String out4Path = "file:///test/4";

        // construct the plan
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);
        DataSet<Long> sourceA = env.generateSequence(0, 1);
        DataSet<Long> sourceB = env.generateSequence(0, 1);

        sourceA.writeAsText(out1Path);
        sourceB.writeAsText(out2Path);
        sourceA.writeAsText(out3Path);
        sourceB.writeAsText(out4Path);

        Plan plan = env.createProgramPlan();
        compileNoStats(plan);
    }

    @Test
    public void testBranchAfterIteration() {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);
        DataSet<Long> sourceA = env.generateSequence(0, 1);

        IterativeDataSet<Long> loopHead = sourceA.iterate(10);
        DataSet<Long> loopTail = loopHead.map(new IdentityMapper<Long>()).name("Mapper");
        DataSet<Long> loopRes = loopHead.closeWith(loopTail);

        loopRes.output(new DiscardingOutputFormat<Long>());
        loopRes.map(new IdentityMapper<Long>()).output(new DiscardingOutputFormat<Long>());

        Plan plan = env.createProgramPlan();

        try {
            compileNoStats(plan);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testBranchBeforeIteration() {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);
        DataSet<Long> source1 = env.generateSequence(0, 1);
        DataSet<Long> source2 = env.generateSequence(0, 1);

        IterativeDataSet<Long> loopHead = source2.iterate(10).name("Loop");
        DataSet<Long> loopTail =
                source1.map(new IdentityMapper<Long>())
                        .withBroadcastSet(loopHead, "BC")
                        .name("In-Loop Mapper");
        DataSet<Long> loopRes = loopHead.closeWith(loopTail);

        DataSet<Long> map =
                source1.map(new IdentityMapper<Long>())
                        .withBroadcastSet(loopRes, "BC")
                        .name("Post-Loop Mapper");
        map.output(new DiscardingOutputFormat<Long>());

        Plan plan = env.createProgramPlan();

        try {
            compileNoStats(plan);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    /**
     * Test to ensure that sourceA is inside as well as outside of the iteration the same node.
     *
     * <pre>
     *       (SRC A)               (SRC B)
     *      /       \             /       \
     *  (SINK 1)   (ITERATION)    |     (SINK 2)
     *             /        \     /
     *         (SINK 3)     (CROSS => NEXT PARTIAL SOLUTION)
     * </pre>
     */
    @Test
    public void testClosure() {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);
        DataSet<Long> sourceA = env.generateSequence(0, 1);
        DataSet<Long> sourceB = env.generateSequence(0, 1);

        sourceA.output(new DiscardingOutputFormat<Long>());
        sourceB.output(new DiscardingOutputFormat<Long>());

        IterativeDataSet<Long> loopHead = sourceA.iterate(10).name("Loop");

        DataSet<Long> loopTail = loopHead.cross(sourceB).with(new IdentityCrosser<Long>());
        DataSet<Long> loopRes = loopHead.closeWith(loopTail);

        loopRes.output(new DiscardingOutputFormat<Long>());

        Plan plan = env.createProgramPlan();

        try {
            compileNoStats(plan);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    /**
     *
     *
     * <pre>
     *       (SRC A)         (SRC B)          (SRC C)
     *      /       \       /                /       \
     *  (SINK 1) (DELTA ITERATION)          |     (SINK 2)
     *             /    |   \               /
     *         (SINK 3) |   (CROSS => NEXT WORKSET)
     *                  |             |
     *                (JOIN => SOLUTION SET DELTA)
     * </pre>
     */
    @Test
    public void testClosureDeltaIteration() {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);
        DataSet<Tuple2<Long, Long>> sourceA =
                env.generateSequence(0, 1).map(new Duplicator<Long>());
        DataSet<Tuple2<Long, Long>> sourceB =
                env.generateSequence(0, 1).map(new Duplicator<Long>());
        DataSet<Tuple2<Long, Long>> sourceC =
                env.generateSequence(0, 1).map(new Duplicator<Long>());

        sourceA.output(new DiscardingOutputFormat<Tuple2<Long, Long>>());
        sourceC.output(new DiscardingOutputFormat<Tuple2<Long, Long>>());

        DeltaIteration<Tuple2<Long, Long>, Tuple2<Long, Long>> loop =
                sourceA.iterateDelta(sourceB, 10, 0);

        DataSet<Tuple2<Long, Long>> workset =
                loop.getWorkset()
                        .cross(sourceB)
                        .with(new IdentityCrosser<Tuple2<Long, Long>>())
                        .name("Next work set");
        DataSet<Tuple2<Long, Long>> delta =
                workset.join(loop.getSolutionSet())
                        .where(0)
                        .equalTo(0)
                        .with(new IdentityJoiner<Tuple2<Long, Long>>())
                        .name("Solution set delta");

        DataSet<Tuple2<Long, Long>> result = loop.closeWith(delta, workset);
        result.output(new DiscardingOutputFormat<Tuple2<Long, Long>>());

        Plan plan = env.createProgramPlan();

        try {
            compileNoStats(plan);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    /**
     *
     *
     * <pre>
     *                  +----Iteration-------+
     *                  |                    |
     *       /---------< >---------join-----< >---sink
     *      / (Solution)|           /        |
     *     /            |          /         |
     *    /--map-------< >----\   /       /--|
     *   /     (Workset)|      \ /       /   |
     * src-map          |     join------/    |
     *   \              |      /             |
     *    \             +-----/--------------+
     *     \                 /
     *      \--reduce-------/
     * </pre>
     */
    @Test
    public void testDeltaIterationWithStaticInput() {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);
        DataSet<Tuple2<Long, Long>> source = env.generateSequence(0, 1).map(new Duplicator<Long>());

        DataSet<Tuple2<Long, Long>> map = source.map(new IdentityMapper<Tuple2<Long, Long>>());
        DataSet<Tuple2<Long, Long>> reduce =
                source.reduceGroup(new IdentityGroupReducer<Tuple2<Long, Long>>());

        DeltaIteration<Tuple2<Long, Long>, Tuple2<Long, Long>> loop =
                source.iterateDelta(map, 10, 0);

        DataSet<Tuple2<Long, Long>> workset =
                loop.getWorkset()
                        .join(reduce)
                        .where(0)
                        .equalTo(0)
                        .with(new IdentityJoiner<Tuple2<Long, Long>>())
                        .name("Next work set");
        DataSet<Tuple2<Long, Long>> delta =
                loop.getSolutionSet()
                        .join(workset)
                        .where(0)
                        .equalTo(0)
                        .with(new IdentityJoiner<Tuple2<Long, Long>>())
                        .name("Solution set delta");

        DataSet<Tuple2<Long, Long>> result = loop.closeWith(delta, workset);
        result.output(new DiscardingOutputFormat<Tuple2<Long, Long>>());

        Plan plan = env.createProgramPlan();

        try {
            compileNoStats(plan);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    /**
     *
     *
     * <pre>
     *             +---------Iteration-------+
     *             |                         |
     *    /--map--< >----\                   |
     *   /         |      \         /-------< >---sink
     * src-map     |     join------/         |
     *   \         |      /                  |
     *    \        +-----/-------------------+
     *     \            /
     *      \--reduce--/
     * </pre>
     */
    @Test
    public void testIterationWithStaticInput() {
        try {
            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(100);

            DataSet<Long> source = env.generateSequence(1, 1000000);

            DataSet<Long> mapped = source.map(new IdentityMapper<Long>());

            DataSet<Long> reduced =
                    source.groupBy(new IdentityKeyExtractor<Long>())
                            .reduce(new SelectOneReducer<Long>());

            IterativeDataSet<Long> iteration = mapped.iterate(10);
            iteration
                    .closeWith(
                            iteration
                                    .join(reduced)
                                    .where(new IdentityKeyExtractor<Long>())
                                    .equalTo(new IdentityKeyExtractor<Long>())
                                    .with(new DummyFlatJoinFunction<Long>()))
                    .output(new DiscardingOutputFormat<Long>());

            compileNoStats(env.createProgramPlan());
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testBranchingBroadcastVariable() {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(100);

        DataSet<String> input1 = env.readTextFile(IN_FILE).name("source1");
        DataSet<String> input2 = env.readTextFile(IN_FILE).name("source2");
        DataSet<String> input3 = env.readTextFile(IN_FILE).name("source3");

        DataSet<String> result1 =
                input1.map(new IdentityMapper<String>())
                        .reduceGroup(new Top1GroupReducer<String>())
                        .withBroadcastSet(input3, "bc");

        DataSet<String> result2 =
                input2.map(new IdentityMapper<String>())
                        .reduceGroup(new Top1GroupReducer<String>())
                        .withBroadcastSet(input3, "bc");

        result1.join(result2)
                .where(new IdentityKeyExtractor<String>())
                .equalTo(new IdentityKeyExtractor<String>())
                .with(
                        new RichJoinFunction<String, String, String>() {
                            @Override
                            public String join(String first, String second) {
                                return null;
                            }
                        })
                .withBroadcastSet(input3, "bc1")
                .withBroadcastSet(input1, "bc2")
                .withBroadcastSet(result1, "bc3")
                .output(new DiscardingOutputFormat<String>());

        Plan plan = env.createProgramPlan();

        try {
            compileNoStats(plan);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testBCVariableClosure() {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> input = env.readTextFile(IN_FILE).name("source1");

        DataSet<String> reduced =
                input.map(new IdentityMapper<String>()).reduceGroup(new Top1GroupReducer<String>());

        DataSet<String> initialSolution =
                input.map(new IdentityMapper<String>()).withBroadcastSet(reduced, "bc");

        IterativeDataSet<String> iteration = initialSolution.iterate(100);

        iteration
                .closeWith(
                        iteration
                                .map(new IdentityMapper<String>())
                                .withBroadcastSet(reduced, "red"))
                .output(new DiscardingOutputFormat<String>());

        Plan plan = env.createProgramPlan();

        try {
            compileNoStats(plan);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testMultipleIterations() {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(100);

        DataSet<String> input = env.readTextFile(IN_FILE).name("source1");

        DataSet<String> reduced =
                input.map(new IdentityMapper<String>()).reduceGroup(new Top1GroupReducer<String>());

        IterativeDataSet<String> iteration1 = input.iterate(100);
        IterativeDataSet<String> iteration2 = input.iterate(20);
        IterativeDataSet<String> iteration3 = input.iterate(17);

        iteration1
                .closeWith(
                        iteration1
                                .map(new IdentityMapper<String>())
                                .withBroadcastSet(reduced, "bc1"))
                .output(new DiscardingOutputFormat<String>());
        iteration2
                .closeWith(
                        iteration2
                                .reduceGroup(new Top1GroupReducer<String>())
                                .withBroadcastSet(reduced, "bc2"))
                .output(new DiscardingOutputFormat<String>());
        iteration3
                .closeWith(
                        iteration3
                                .reduceGroup(new IdentityGroupReducer<String>())
                                .withBroadcastSet(reduced, "bc3"))
                .output(new DiscardingOutputFormat<String>());

        Plan plan = env.createProgramPlan();

        try {
            compileNoStats(plan);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testMultipleIterationsWithClosueBCVars() {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(100);

        DataSet<String> input = env.readTextFile(IN_FILE).name("source1");

        IterativeDataSet<String> iteration1 = input.iterate(100);
        IterativeDataSet<String> iteration2 = input.iterate(20);
        IterativeDataSet<String> iteration3 = input.iterate(17);

        iteration1
                .closeWith(iteration1.map(new IdentityMapper<String>()))
                .output(new DiscardingOutputFormat<String>());
        iteration2
                .closeWith(iteration2.reduceGroup(new Top1GroupReducer<String>()))
                .output(new DiscardingOutputFormat<String>());
        iteration3
                .closeWith(iteration3.reduceGroup(new IdentityGroupReducer<String>()))
                .output(new DiscardingOutputFormat<String>());

        Plan plan = env.createProgramPlan();

        try {
            compileNoStats(plan);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testBranchesOnlyInBCVariables1() {
        try {
            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(100);

            DataSet<Long> input = env.generateSequence(1, 10);
            DataSet<Long> bc_input = env.generateSequence(1, 10);

            input.map(new IdentityMapper<Long>())
                    .withBroadcastSet(bc_input, "name1")
                    .map(new IdentityMapper<Long>())
                    .withBroadcastSet(bc_input, "name2")
                    .output(new DiscardingOutputFormat<Long>());

            Plan plan = env.createProgramPlan();
            compileNoStats(plan);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testBranchesOnlyInBCVariables2() {
        try {
            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(100);

            DataSet<Tuple2<Long, Long>> input =
                    env.generateSequence(1, 10).map(new Duplicator<Long>()).name("proper input");

            DataSet<Long> bc_input1 = env.generateSequence(1, 10).name("BC input 1");
            DataSet<Long> bc_input2 = env.generateSequence(1, 10).name("BC input 1");

            DataSet<Tuple2<Long, Long>> joinInput1 =
                    input.map(new IdentityMapper<Tuple2<Long, Long>>())
                            .withBroadcastSet(bc_input1.map(new IdentityMapper<Long>()), "bc1")
                            .withBroadcastSet(bc_input2, "bc2");

            DataSet<Tuple2<Long, Long>> joinInput2 =
                    input.map(new IdentityMapper<Tuple2<Long, Long>>())
                            .withBroadcastSet(bc_input1, "bc1")
                            .withBroadcastSet(bc_input2, "bc2");

            DataSet<Tuple2<Long, Long>> joinResult =
                    joinInput1
                            .join(joinInput2, JoinHint.REPARTITION_HASH_FIRST)
                            .where(0)
                            .equalTo(1)
                            .with(new DummyFlatJoinFunction<Tuple2<Long, Long>>());

            input.map(new IdentityMapper<Tuple2<Long, Long>>())
                    .withBroadcastSet(bc_input1, "bc1")
                    .union(joinResult)
                    .output(new DiscardingOutputFormat<Tuple2<Long, Long>>());

            Plan plan = env.createProgramPlan();
            compileNoStats(plan);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    private static final class Duplicator<T> implements MapFunction<T, Tuple2<T, T>> {

        @Override
        public Tuple2<T, T> map(T value) {
            return new Tuple2<T, T>(value, value);
        }
    }
}
