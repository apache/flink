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

package org.apache.flink.graph.pregel;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.util.FieldList;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.utils.Tuple2ToVertexMap;
import org.apache.flink.optimizer.plan.DualInputPlanNode;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plan.PlanNode;
import org.apache.flink.optimizer.plan.SingleInputPlanNode;
import org.apache.flink.optimizer.plan.SinkPlanNode;
import org.apache.flink.optimizer.plan.WorksetIterationPlanNode;
import org.apache.flink.optimizer.util.CompilerTestBase;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.types.NullValue;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Validate compiled {@link VertexCentricIteration} programs. */
public class PregelCompilerTest extends CompilerTestBase {

    private static final long serialVersionUID = 1L;

    @SuppressWarnings("serial")
    @Test
    public void testPregelCompiler() {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);
        // compose test program
        {
            DataSet<Vertex<Long, Long>> initialVertices =
                    env.fromElements(new Tuple2<>(1L, 1L), new Tuple2<>(2L, 2L))
                            .map(new Tuple2ToVertexMap<>());

            DataSet<Edge<Long, NullValue>> edges =
                    env.fromElements(new Tuple2<>(1L, 2L))
                            .map(
                                    new MapFunction<Tuple2<Long, Long>, Edge<Long, NullValue>>() {

                                        public Edge<Long, NullValue> map(Tuple2<Long, Long> edge) {
                                            return new Edge<>(
                                                    edge.f0, edge.f1, NullValue.getInstance());
                                        }
                                    });

            Graph<Long, Long, NullValue> graph = Graph.fromDataSet(initialVertices, edges, env);

            DataSet<Vertex<Long, Long>> result =
                    graph.runVertexCentricIteration(new CCCompute(), null, 100).getVertices();

            result.output(new DiscardingOutputFormat<>());
        }

        Plan p = env.createProgramPlan("Pregel Connected Components");
        OptimizedPlan op = compileNoStats(p);

        // check the sink
        SinkPlanNode sink = op.getDataSinks().iterator().next();
        assertThat(sink.getInput().getShipStrategy()).isEqualTo(ShipStrategyType.FORWARD);
        assertThat(sink.getParallelism()).isEqualTo(DEFAULT_PARALLELISM);

        // check the iteration
        WorksetIterationPlanNode iteration = (WorksetIterationPlanNode) sink.getInput().getSource();
        assertThat(iteration.getParallelism()).isEqualTo(DEFAULT_PARALLELISM);

        // check the solution set delta
        PlanNode ssDelta = iteration.getSolutionSetDeltaPlanNode();
        assertThat(ssDelta).isInstanceOf(SingleInputPlanNode.class);

        SingleInputPlanNode ssFlatMap =
                (SingleInputPlanNode) ((SingleInputPlanNode) (ssDelta)).getInput().getSource();
        assertThat(ssFlatMap.getParallelism()).isEqualTo(DEFAULT_PARALLELISM);
        assertThat(ssFlatMap.getInput().getShipStrategy()).isEqualTo(ShipStrategyType.FORWARD);

        // check the computation coGroup
        DualInputPlanNode computationCoGroup =
                (DualInputPlanNode) (ssFlatMap.getInput().getSource());
        assertThat(computationCoGroup.getParallelism()).isEqualTo(DEFAULT_PARALLELISM);
        assertThat(computationCoGroup.getInput1().getShipStrategy())
                .isEqualTo(ShipStrategyType.FORWARD);
        assertThat(computationCoGroup.getInput2().getShipStrategy())
                .isEqualTo(ShipStrategyType.PARTITION_HASH);
        assertThat(computationCoGroup.getInput2().getTempMode().isCached()).isTrue();

        assertThat(computationCoGroup.getInput2().getShipStrategyKeys())
                .isEqualTo(new FieldList(0));

        // check that the initial partitioning is pushed out of the loop
        assertThat(iteration.getInput1().getShipStrategy())
                .isEqualTo(ShipStrategyType.PARTITION_HASH);
        assertThat(iteration.getInput1().getShipStrategyKeys()).isEqualTo(new FieldList(0));
    }

    @SuppressWarnings("serial")
    @Test
    public void testPregelCompilerWithBroadcastVariable() {
        final String broadcastSetName = "broadcast";

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);
        // compose test program
        {
            DataSet<Long> bcVar = env.fromElements(1L);

            DataSet<Vertex<Long, Long>> initialVertices =
                    env.fromElements(new Tuple2<>(1L, 1L), new Tuple2<>(2L, 2L))
                            .map(new Tuple2ToVertexMap<>());

            DataSet<Edge<Long, NullValue>> edges =
                    env.fromElements(new Tuple2<>(1L, 2L))
                            .map(
                                    new MapFunction<Tuple2<Long, Long>, Edge<Long, NullValue>>() {

                                        public Edge<Long, NullValue> map(Tuple2<Long, Long> edge) {
                                            return new Edge<>(
                                                    edge.f0, edge.f1, NullValue.getInstance());
                                        }
                                    });

            Graph<Long, Long, NullValue> graph = Graph.fromDataSet(initialVertices, edges, env);

            VertexCentricConfiguration parameters = new VertexCentricConfiguration();
            parameters.addBroadcastSet(broadcastSetName, bcVar);

            DataSet<Vertex<Long, Long>> result =
                    graph.runVertexCentricIteration(new CCCompute(), null, 100, parameters)
                            .getVertices();

            result.output(new DiscardingOutputFormat<>());
        }

        Plan p = env.createProgramPlan("Pregel Connected Components");
        OptimizedPlan op = compileNoStats(p);

        // check the sink
        SinkPlanNode sink = op.getDataSinks().iterator().next();
        assertThat(sink.getInput().getShipStrategy()).isEqualTo(ShipStrategyType.FORWARD);
        assertThat(sink.getParallelism()).isEqualTo(DEFAULT_PARALLELISM);

        // check the iteration
        WorksetIterationPlanNode iteration = (WorksetIterationPlanNode) sink.getInput().getSource();
        assertThat(iteration.getParallelism()).isEqualTo(DEFAULT_PARALLELISM);

        // check the solution set delta
        PlanNode ssDelta = iteration.getSolutionSetDeltaPlanNode();
        assertThat(ssDelta).isInstanceOf(SingleInputPlanNode.class);

        SingleInputPlanNode ssFlatMap =
                (SingleInputPlanNode) ((SingleInputPlanNode) (ssDelta)).getInput().getSource();
        assertThat(ssFlatMap.getParallelism()).isEqualTo(DEFAULT_PARALLELISM);
        assertThat(ssFlatMap.getInput().getShipStrategy()).isEqualTo(ShipStrategyType.FORWARD);

        // check the computation coGroup
        DualInputPlanNode computationCoGroup =
                (DualInputPlanNode) (ssFlatMap.getInput().getSource());
        assertThat(computationCoGroup.getParallelism()).isEqualTo(DEFAULT_PARALLELISM);
        assertThat(computationCoGroup.getInput1().getShipStrategy())
                .isEqualTo(ShipStrategyType.FORWARD);
        assertThat(computationCoGroup.getInput2().getShipStrategy())
                .isEqualTo(ShipStrategyType.PARTITION_HASH);
        assertThat(computationCoGroup.getInput2().getTempMode().isCached()).isTrue();

        assertThat(computationCoGroup.getInput2().getShipStrategyKeys())
                .isEqualTo(new FieldList(0));

        // check that the initial partitioning is pushed out of the loop
        assertThat(iteration.getInput1().getShipStrategy())
                .isEqualTo(ShipStrategyType.PARTITION_HASH);
        assertThat(iteration.getInput1().getShipStrategyKeys()).isEqualTo(new FieldList(0));
    }

    @SuppressWarnings("serial")
    @Test
    public void testPregelWithCombiner() {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);
        // compose test program
        {
            DataSet<Vertex<Long, Long>> initialVertices =
                    env.fromElements(new Tuple2<>(1L, 1L), new Tuple2<>(2L, 2L))
                            .map(new Tuple2ToVertexMap<>());

            DataSet<Edge<Long, NullValue>> edges =
                    env.fromElements(new Tuple2<>(1L, 2L))
                            .map(
                                    new MapFunction<Tuple2<Long, Long>, Edge<Long, NullValue>>() {

                                        public Edge<Long, NullValue> map(Tuple2<Long, Long> edge) {
                                            return new Edge<>(
                                                    edge.f0, edge.f1, NullValue.getInstance());
                                        }
                                    });

            Graph<Long, Long, NullValue> graph = Graph.fromDataSet(initialVertices, edges, env);

            DataSet<Vertex<Long, Long>> result =
                    graph.runVertexCentricIteration(new CCCompute(), new CCCombiner(), 100)
                            .getVertices();

            result.output(new DiscardingOutputFormat<>());
        }

        Plan p = env.createProgramPlan("Pregel Connected Components");
        OptimizedPlan op = compileNoStats(p);

        // check the sink
        SinkPlanNode sink = op.getDataSinks().iterator().next();
        assertThat(sink.getInput().getShipStrategy()).isEqualTo(ShipStrategyType.FORWARD);
        assertThat(sink.getParallelism()).isEqualTo(DEFAULT_PARALLELISM);

        // check the iteration
        WorksetIterationPlanNode iteration = (WorksetIterationPlanNode) sink.getInput().getSource();
        assertThat(iteration.getParallelism()).isEqualTo(DEFAULT_PARALLELISM);

        // check the combiner
        SingleInputPlanNode combiner = (SingleInputPlanNode) iteration.getInput2().getSource();
        assertThat(combiner.getInput().getShipStrategy()).isEqualTo(ShipStrategyType.FORWARD);

        // check the solution set delta
        PlanNode ssDelta = iteration.getSolutionSetDeltaPlanNode();
        assertThat(ssDelta).isInstanceOf(SingleInputPlanNode.class);

        SingleInputPlanNode ssFlatMap =
                (SingleInputPlanNode) ((SingleInputPlanNode) (ssDelta)).getInput().getSource();
        assertThat(ssFlatMap.getParallelism()).isEqualTo(DEFAULT_PARALLELISM);
        assertThat(ssFlatMap.getInput().getShipStrategy()).isEqualTo(ShipStrategyType.FORWARD);

        // check the computation coGroup
        DualInputPlanNode computationCoGroup =
                (DualInputPlanNode) (ssFlatMap.getInput().getSource());
        assertThat(computationCoGroup.getParallelism()).isEqualTo(DEFAULT_PARALLELISM);
        assertThat(computationCoGroup.getInput1().getShipStrategy())
                .isEqualTo(ShipStrategyType.FORWARD);
        assertThat(computationCoGroup.getInput2().getShipStrategy())
                .isEqualTo(ShipStrategyType.PARTITION_HASH);
        assertThat(computationCoGroup.getInput2().getTempMode().isCached()).isTrue();

        assertThat(computationCoGroup.getInput2().getShipStrategyKeys())
                .isEqualTo(new FieldList(0));

        // check that the initial partitioning is pushed out of the loop
        assertThat(iteration.getInput1().getShipStrategy())
                .isEqualTo(ShipStrategyType.PARTITION_HASH);
        assertThat(iteration.getInput1().getShipStrategyKeys()).isEqualTo(new FieldList(0));
    }

    @SuppressWarnings("serial")
    private static final class CCCompute extends ComputeFunction<Long, Long, NullValue, Long> {

        @Override
        public void compute(Vertex<Long, Long> vertex, MessageIterator<Long> messages)
                throws Exception {
            long currentComponent = vertex.getValue();

            for (Long msg : messages) {
                currentComponent = Math.min(currentComponent, msg);
            }

            if ((getSuperstepNumber() == 1) || (currentComponent < vertex.getValue())) {
                setNewVertexValue(currentComponent);
                for (Edge<Long, NullValue> edge : getEdges()) {
                    sendMessageTo(edge.getTarget(), currentComponent);
                }
            }
        }
    }

    @SuppressWarnings("serial")
    private static final class CCCombiner extends MessageCombiner<Long, Long> {

        public void combineMessages(MessageIterator<Long> messages) {

            long minMessage = Long.MAX_VALUE;
            for (Long msg : messages) {
                minMessage = Math.min(minMessage, msg);
            }
            sendCombinedMessage(minMessage);
        }
    }
}
