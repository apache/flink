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

package org.apache.flink.test.optimizer.iterations;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.operators.util.FieldList;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsFirst;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsSecond;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.optimizer.dag.TempMode;
import org.apache.flink.optimizer.plan.*;
import org.apache.flink.optimizer.plandump.PlanJSONDumpGenerator;
import org.apache.flink.optimizer.plantranslate.JobGraphGenerator;
import org.apache.flink.optimizer.util.CompilerTestBase;
import org.apache.flink.runtime.operators.DriverStrategy;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.runtime.operators.util.LocalStrategy;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Timeout;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.hamcrest.MatcherAssert;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** */
@SuppressWarnings("serial")
public class ConnectedComponentsCoGroupTest extends CompilerTestBase {

    private static final String VERTEX_SOURCE = "Vertices";

    private static final String ITERATION_NAME = "Connected Components Iteration";

    private static final String EDGES_SOURCE = "Edges";
    private static final String JOIN_NEIGHBORS_MATCH = "Join Candidate Id With Neighbor";
    private static final String MIN_ID_AND_UPDATE = "Min Id and Update";

    private static final String SINK = "Result";

    private static final boolean PRINT_PLAN = false;

    private final FieldList set0 = new FieldList(0);

    @Test
    public void testWorksetConnectedComponents() throws Exception {
        Plan plan = getConnectedComponentsCoGroupPlan();
        plan.setExecutionConfig(new ExecutionConfig());
        OptimizedPlan optPlan = compileNoStats(plan);
        OptimizerPlanNodeResolver or = getOptimizerPlanNodeResolver(optPlan);

        if (PRINT_PLAN) {
            PlanJSONDumpGenerator dumper = new PlanJSONDumpGenerator();
            String json = dumper.getOptimizerPlanAsJSON(optPlan);
            System.out.println(json);
        }

        SourcePlanNode vertexSource = or.getNode(VERTEX_SOURCE);
        SourcePlanNode edgesSource = or.getNode(EDGES_SOURCE);
        SinkPlanNode sink = or.getNode(SINK);
        WorksetIterationPlanNode iter = or.getNode(ITERATION_NAME);

        DualInputPlanNode neighborsJoin = or.getNode(JOIN_NEIGHBORS_MATCH);
        DualInputPlanNode cogroup = or.getNode(MIN_ID_AND_UPDATE);

        // --------------------------------------------------------------------
        // Plan validation:
        //
        // We expect the plan to go with a sort-merge join, because the CoGroup
        // sorts and the join in the successive iteration can re-exploit the sorting.
        // --------------------------------------------------------------------

        // test all drivers
        Assertions.assertEquals(DriverStrategy.NONE, sink.getDriverStrategy());
        Assertions.assertEquals(DriverStrategy.NONE, vertexSource.getDriverStrategy());
        Assertions.assertEquals(DriverStrategy.NONE, edgesSource.getDriverStrategy());

        Assertions.assertEquals(DriverStrategy.INNER_MERGE, neighborsJoin.getDriverStrategy());
        Assertions.assertEquals(set0, neighborsJoin.getKeysForInput1());
        Assertions.assertEquals(set0, neighborsJoin.getKeysForInput2());

        Assertions.assertEquals(DriverStrategy.CO_GROUP, cogroup.getDriverStrategy());
        Assertions.assertEquals(set0, cogroup.getKeysForInput1());
        Assertions.assertEquals(set0, cogroup.getKeysForInput2());

        // test all the shipping strategies
        Assertions.assertEquals(ShipStrategyType.FORWARD, sink.getInput().getShipStrategy());
        Assertions.assertEquals(
                ShipStrategyType.PARTITION_HASH,
                iter.getInitialSolutionSetInput().getShipStrategy());
        Assertions.assertEquals(set0, iter.getInitialSolutionSetInput().getShipStrategyKeys());
        Assertions.assertEquals(
                ShipStrategyType.PARTITION_HASH, iter.getInitialWorksetInput().getShipStrategy());
        Assertions.assertEquals(set0, iter.getInitialWorksetInput().getShipStrategyKeys());

        Assertions.assertEquals(
                ShipStrategyType.FORWARD, neighborsJoin.getInput1().getShipStrategy()); // workset
        Assertions.assertEquals(
                ShipStrategyType.PARTITION_HASH,
                neighborsJoin.getInput2().getShipStrategy()); // edges
        Assertions.assertEquals(set0, neighborsJoin.getInput2().getShipStrategyKeys());
        Assertions.assertTrue(neighborsJoin.getInput2().getTempMode().isCached());

        Assertions.assertEquals(
                ShipStrategyType.PARTITION_HASH, cogroup.getInput1().getShipStrategy()); // min id
        Assertions.assertEquals(
                ShipStrategyType.FORWARD, cogroup.getInput2().getShipStrategy()); // solution set

        // test all the local strategies
        Assertions.assertEquals(LocalStrategy.NONE, sink.getInput().getLocalStrategy());
        Assertions.assertEquals(
                LocalStrategy.NONE, iter.getInitialSolutionSetInput().getLocalStrategy());

        // the sort for the neighbor join in the first iteration is pushed out of the loop
        Assertions.assertEquals(
                LocalStrategy.SORT, iter.getInitialWorksetInput().getLocalStrategy());
        Assertions.assertEquals(
                LocalStrategy.NONE, neighborsJoin.getInput1().getLocalStrategy()); // workset
        Assertions.assertEquals(
                LocalStrategy.SORT, neighborsJoin.getInput2().getLocalStrategy()); // edges

        Assertions.assertEquals(LocalStrategy.SORT, cogroup.getInput1().getLocalStrategy());
        Assertions.assertEquals(
                LocalStrategy.NONE, cogroup.getInput2().getLocalStrategy()); // solution set

        // check the caches
        Assertions.assertTrue(TempMode.CACHED == neighborsJoin.getInput2().getTempMode());

        JobGraphGenerator jgg = new JobGraphGenerator();
        jgg.compileJobGraph(optPlan);
    }

    public static Plan getConnectedComponentsCoGroupPlan() throws Exception {
        return connectedComponentsWithCoGroup(
                new String[] {DEFAULT_PARALLELISM_STRING, IN_FILE, IN_FILE, OUT_FILE, "100"});
    }

    public static Plan connectedComponentsWithCoGroup(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(Integer.parseInt(args[0]));

        DataSet<Tuple1<Long>> initialVertices =
                env.readCsvFile(args[1]).types(Long.class).name(VERTEX_SOURCE);

        DataSet<Tuple2<Long, Long>> edges =
                env.readCsvFile(args[2]).types(Long.class, Long.class).name(EDGES_SOURCE);

        DataSet<Tuple2<Long, Long>> verticesWithId =
                initialVertices.flatMap(new DummyMapFunction());

        DeltaIteration<Tuple2<Long, Long>, Tuple2<Long, Long>> iteration =
                verticesWithId
                        .iterateDelta(verticesWithId, Integer.parseInt(args[4]), 0)
                        .name(ITERATION_NAME);

        DataSet<Tuple2<Long, Long>> joinWithNeighbors =
                iteration
                        .getWorkset()
                        .join(edges)
                        .where(0)
                        .equalTo(0)
                        .with(new DummyJoinFunction())
                        .name(JOIN_NEIGHBORS_MATCH);

        DataSet<Tuple2<Long, Long>> minAndUpdate =
                joinWithNeighbors
                        .coGroup(iteration.getSolutionSet())
                        .where(0)
                        .equalTo(0)
                        .with(new DummyCoGroupFunction())
                        .name(MIN_ID_AND_UPDATE);

        iteration.closeWith(minAndUpdate, minAndUpdate).writeAsCsv(args[3]).name(SINK);

        return env.createProgramPlan();
    }

    private static class DummyMapFunction
            implements FlatMapFunction<Tuple1<Long>, Tuple2<Long, Long>> {
        @Override
        public void flatMap(Tuple1<Long> value, Collector<Tuple2<Long, Long>> out)
                throws Exception {
            // won't be executed
        }
    }

    private static class DummyJoinFunction
            implements FlatJoinFunction<
                    Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>> {
        @Override
        public void join(
                Tuple2<Long, Long> first,
                Tuple2<Long, Long> second,
                Collector<Tuple2<Long, Long>> out)
                throws Exception {
            // won't be executed
        }
    }

    @ForwardedFieldsFirst("f0->f0")
    @ForwardedFieldsSecond("f0->f0")
    private static class DummyCoGroupFunction
            implements CoGroupFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>> {
        @Override
        public void coGroup(
                Iterable<Tuple2<Long, Long>> first,
                Iterable<Tuple2<Long, Long>> second,
                Collector<Tuple2<Long, Long>> out)
                throws Exception {
            // won't be executed
        }
    }
}
