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
import org.apache.flink.optimizer.plan.DualInputPlanNode;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plan.SinkPlanNode;
import org.apache.flink.optimizer.plan.SourcePlanNode;
import org.apache.flink.optimizer.plan.WorksetIterationPlanNode;
import org.apache.flink.optimizer.plandump.PlanJSONDumpGenerator;
import org.apache.flink.optimizer.plantranslate.JobGraphGenerator;
import org.apache.flink.optimizer.util.CompilerTestBase;
import org.apache.flink.runtime.operators.DriverStrategy;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.runtime.operators.util.LocalStrategy;
import org.apache.flink.util.Collector;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

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
        assertThat(sink.getDriverStrategy()).isEqualTo(DriverStrategy.NONE);
        assertThat(vertexSource.getDriverStrategy()).isEqualTo(DriverStrategy.NONE);
        assertThat(edgesSource.getDriverStrategy()).isEqualTo(DriverStrategy.NONE);

        assertThat(neighborsJoin.getDriverStrategy()).isEqualTo(DriverStrategy.INNER_MERGE);
        assertThat(neighborsJoin.getKeysForInput1()).isEqualTo(set0);
        assertThat(neighborsJoin.getKeysForInput2()).isEqualTo(set0);

        assertThat(cogroup.getDriverStrategy()).isEqualTo(DriverStrategy.CO_GROUP);
        assertThat(cogroup.getKeysForInput1()).isEqualTo(set0);
        assertThat(cogroup.getKeysForInput2()).isEqualTo(set0);

        // test all the shipping strategies
        assertThat(sink.getInput().getShipStrategy()).isEqualTo(ShipStrategyType.FORWARD);
        assertThat(iter.getInitialSolutionSetInput().getShipStrategy())
                .isEqualTo(ShipStrategyType.PARTITION_HASH);
        assertThat(iter.getInitialSolutionSetInput().getShipStrategyKeys()).isEqualTo(set0);
        assertThat(iter.getInitialWorksetInput().getShipStrategy())
                .isEqualTo(ShipStrategyType.PARTITION_HASH);
        assertThat(iter.getInitialWorksetInput().getShipStrategyKeys()).isEqualTo(set0);

        assertThat(neighborsJoin.getInput1().getShipStrategy())
                .isEqualTo(ShipStrategyType.FORWARD); // workset
        assertThat(neighborsJoin.getInput2().getShipStrategy())
                .isEqualTo(ShipStrategyType.PARTITION_HASH); // edges
        assertThat(neighborsJoin.getInput2().getShipStrategyKeys()).isEqualTo(set0);
        assertThat(neighborsJoin.getInput2().getTempMode().isCached()).isTrue();

        assertThat(cogroup.getInput1().getShipStrategy())
                .isEqualTo(ShipStrategyType.PARTITION_HASH); // min id
        assertThat(cogroup.getInput2().getShipStrategy())
                .isEqualTo(ShipStrategyType.FORWARD); // solution set

        // test all the local strategies
        assertThat(sink.getInput().getLocalStrategy()).isEqualTo(LocalStrategy.NONE);
        assertThat(iter.getInitialSolutionSetInput().getLocalStrategy())
                .isEqualTo(LocalStrategy.NONE);

        // the sort for the neighbor join in the first iteration is pushed out of the loop
        assertThat(iter.getInitialWorksetInput().getLocalStrategy()).isEqualTo(LocalStrategy.SORT);
        assertThat(neighborsJoin.getInput1().getLocalStrategy())
                .isEqualTo(LocalStrategy.NONE); // workset
        assertThat(neighborsJoin.getInput2().getLocalStrategy())
                .isEqualTo(LocalStrategy.SORT); // edges

        assertThat(cogroup.getInput1().getLocalStrategy()).isEqualTo(LocalStrategy.SORT);
        assertThat(cogroup.getInput2().getLocalStrategy())
                .isEqualTo(LocalStrategy.NONE); // solution set

        // check the caches
        assertThat(neighborsJoin.getInput2().getTempMode()).isSameAs(TempMode.CACHED);

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
