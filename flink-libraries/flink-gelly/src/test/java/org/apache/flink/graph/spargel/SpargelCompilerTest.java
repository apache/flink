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

package org.apache.flink.graph.spargel;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.util.FieldList;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.library.ConnectedComponents;
import org.apache.flink.graph.utils.Tuple2ToVertexMap;
import org.apache.flink.optimizer.plan.DualInputPlanNode;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plan.PlanNode;
import org.apache.flink.optimizer.plan.SinkPlanNode;
import org.apache.flink.optimizer.plan.WorksetIterationPlanNode;
import org.apache.flink.optimizer.util.CompilerTestBase;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.runtime.operators.util.LocalStrategy;
import org.apache.flink.types.NullValue;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Validate compiled {@link ScatterGatherIteration} programs.
 */
public class SpargelCompilerTest extends CompilerTestBase {

	private static final long serialVersionUID = 1L;

	@SuppressWarnings("serial")
	@Test
	public void testSpargelCompiler() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(DEFAULT_PARALLELISM);

		// compose test program
		DataSet<Vertex<Long, Long>> initialVertices = env.fromElements(
			new Tuple2<>(1L, 1L), new Tuple2<>(2L, 2L))
			.map(new Tuple2ToVertexMap<>());

		DataSet<Edge<Long, NullValue>> edges = env.fromElements(new Tuple2<>(1L, 2L))
			.map(new MapFunction<Tuple2<Long, Long>, Edge<Long, NullValue>>() {

				public Edge<Long, NullValue> map(Tuple2<Long, Long> edge) {
					return new Edge<>(edge.f0, edge.f1, NullValue.getInstance());
				}
			});

		Graph<Long, Long, NullValue> graph = Graph.fromDataSet(initialVertices, edges, env);

		DataSet<Vertex<Long, Long>> result = graph.runScatterGatherIteration(
			new ConnectedComponents.CCMessenger<>(BasicTypeInfo.LONG_TYPE_INFO),
			new ConnectedComponents.CCUpdater<>(), 100)
			.getVertices();

		result.output(new DiscardingOutputFormat<>());

		Plan p = env.createProgramPlan("Spargel Connected Components");
		OptimizedPlan op = compileNoStats(p);

		// check the sink
		SinkPlanNode sink = op.getDataSinks().iterator().next();
		assertEquals(ShipStrategyType.FORWARD, sink.getInput().getShipStrategy());
		assertEquals(DEFAULT_PARALLELISM, sink.getParallelism());

		// check the iteration
		WorksetIterationPlanNode iteration = (WorksetIterationPlanNode) sink.getInput().getSource();
		assertEquals(DEFAULT_PARALLELISM, iteration.getParallelism());

		// check the solution set join and the delta
		PlanNode ssDelta = iteration.getSolutionSetDeltaPlanNode();
		assertTrue(ssDelta instanceof DualInputPlanNode); // this is only true if the update functions preserves the partitioning

		DualInputPlanNode ssJoin = (DualInputPlanNode) ssDelta;
		assertEquals(DEFAULT_PARALLELISM, ssJoin.getParallelism());
		assertEquals(ShipStrategyType.PARTITION_HASH, ssJoin.getInput1().getShipStrategy());
		assertEquals(new FieldList(0), ssJoin.getInput1().getShipStrategyKeys());

		// check the workset set join
		DualInputPlanNode edgeJoin = (DualInputPlanNode) ssJoin.getInput1().getSource();
		assertEquals(DEFAULT_PARALLELISM, edgeJoin.getParallelism());
		assertEquals(ShipStrategyType.PARTITION_HASH, edgeJoin.getInput1().getShipStrategy());
		assertEquals(ShipStrategyType.FORWARD, edgeJoin.getInput2().getShipStrategy());
		assertTrue(edgeJoin.getInput1().getTempMode().isCached());

		assertEquals(new FieldList(0), edgeJoin.getInput1().getShipStrategyKeys());

		// check that the initial partitioning is pushed out of the loop
		assertEquals(ShipStrategyType.PARTITION_HASH, iteration.getInput1().getShipStrategy());
		assertEquals(ShipStrategyType.PARTITION_HASH, iteration.getInput2().getShipStrategy());
		assertEquals(new FieldList(0), iteration.getInput1().getShipStrategyKeys());
		assertEquals(new FieldList(0), iteration.getInput2().getShipStrategyKeys());

		// check that the initial workset sort is outside the loop
		assertEquals(LocalStrategy.SORT, iteration.getInput2().getLocalStrategy());
		assertEquals(new FieldList(0), iteration.getInput2().getLocalStrategyKeys());
	}

	@SuppressWarnings("serial")
	@Test
	public void testSpargelCompilerWithBroadcastVariable() {
		final String broadcastVariableName = "broadcast variable";

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(DEFAULT_PARALLELISM);

		// compose test program

		DataSet<Long> bcVar = env.fromElements(1L);

		DataSet<Vertex<Long, Long>> initialVertices = env.fromElements(
			new Tuple2<>(1L, 1L), new Tuple2<>(2L, 2L))
			.map(new Tuple2ToVertexMap<>());

		DataSet<Edge<Long, NullValue>> edges = env.fromElements(new Tuple2<>(1L, 2L))
			.map(new MapFunction<Tuple2<Long, Long>, Edge<Long, NullValue>>() {

				public Edge<Long, NullValue> map(Tuple2<Long, Long> edge) {
					return new Edge<>(edge.f0, edge.f1, NullValue.getInstance());
				}
			});

		Graph<Long, Long, NullValue> graph = Graph.fromDataSet(initialVertices, edges, env);

		ScatterGatherConfiguration parameters = new ScatterGatherConfiguration();
		parameters.addBroadcastSetForScatterFunction(broadcastVariableName, bcVar);
		parameters.addBroadcastSetForGatherFunction(broadcastVariableName, bcVar);

		DataSet<Vertex<Long, Long>> result = graph.runScatterGatherIteration(
			new ConnectedComponents.CCMessenger<>(BasicTypeInfo.LONG_TYPE_INFO),
			new ConnectedComponents.CCUpdater<>(), 100)
			.getVertices();

		result.output(new DiscardingOutputFormat<>());

		Plan p = env.createProgramPlan("Spargel Connected Components");
		OptimizedPlan op = compileNoStats(p);

		// check the sink
		SinkPlanNode sink = op.getDataSinks().iterator().next();
		assertEquals(ShipStrategyType.FORWARD, sink.getInput().getShipStrategy());
		assertEquals(DEFAULT_PARALLELISM, sink.getParallelism());

		// check the iteration
		WorksetIterationPlanNode iteration = (WorksetIterationPlanNode) sink.getInput().getSource();
		assertEquals(DEFAULT_PARALLELISM, iteration.getParallelism());

		// check the solution set join and the delta
		PlanNode ssDelta = iteration.getSolutionSetDeltaPlanNode();
		assertTrue(ssDelta instanceof DualInputPlanNode); // this is only true if the update functions preserves the partitioning

		DualInputPlanNode ssJoin = (DualInputPlanNode) ssDelta;
		assertEquals(DEFAULT_PARALLELISM, ssJoin.getParallelism());
		assertEquals(ShipStrategyType.PARTITION_HASH, ssJoin.getInput1().getShipStrategy());
		assertEquals(new FieldList(0), ssJoin.getInput1().getShipStrategyKeys());

		// check the workset set join
		DualInputPlanNode edgeJoin = (DualInputPlanNode) ssJoin.getInput1().getSource();
		assertEquals(DEFAULT_PARALLELISM, edgeJoin.getParallelism());
		assertEquals(ShipStrategyType.PARTITION_HASH, edgeJoin.getInput1().getShipStrategy());
		assertEquals(ShipStrategyType.FORWARD, edgeJoin.getInput2().getShipStrategy());
		assertTrue(edgeJoin.getInput1().getTempMode().isCached());

		assertEquals(new FieldList(0), edgeJoin.getInput1().getShipStrategyKeys());

		// check that the initial partitioning is pushed out of the loop
		assertEquals(ShipStrategyType.PARTITION_HASH, iteration.getInput1().getShipStrategy());
		assertEquals(ShipStrategyType.PARTITION_HASH, iteration.getInput2().getShipStrategy());
		assertEquals(new FieldList(0), iteration.getInput1().getShipStrategyKeys());
		assertEquals(new FieldList(0), iteration.getInput2().getShipStrategyKeys());
	}
}
