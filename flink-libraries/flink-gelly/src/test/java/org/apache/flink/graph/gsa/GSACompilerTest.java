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

package org.apache.flink.graph.gsa;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.util.FieldList;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.utils.Tuple3ToEdgeMap;
import org.apache.flink.optimizer.dataproperties.PartitioningProperty;
import org.apache.flink.optimizer.plan.DualInputPlanNode;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plan.PlanNode;
import org.apache.flink.optimizer.plan.SingleInputPlanNode;
import org.apache.flink.optimizer.plan.SinkPlanNode;
import org.apache.flink.optimizer.plan.WorksetIterationPlanNode;
import org.apache.flink.optimizer.util.CompilerTestBase;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.types.NullValue;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Validate compiled {@link GatherSumApplyIteration} programs.
 */
public class GSACompilerTest extends CompilerTestBase {

	private static final long serialVersionUID = 1L;

	@Test
	public void testGSACompiler() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(DEFAULT_PARALLELISM);

		// compose test program

		DataSet<Edge<Long, NullValue>> edges = env.fromElements(new Tuple3<>(
			1L, 2L, NullValue.getInstance())).map(new Tuple3ToEdgeMap<>());

		Graph<Long, Long, NullValue> graph = Graph.fromDataSet(edges, new InitVertices(), env);

		DataSet<Vertex<Long, Long>> result = graph.runGatherSumApplyIteration(
			new GatherNeighborIds(), new SelectMinId(),
			new UpdateComponentId(), 100).getVertices();

		result.output(new DiscardingOutputFormat<>());

		Plan p = env.createProgramPlan("GSA Connected Components");
		OptimizedPlan op = compileNoStats(p);

		// check the sink
		SinkPlanNode sink = op.getDataSinks().iterator().next();
		assertEquals(ShipStrategyType.FORWARD, sink.getInput().getShipStrategy());
		assertEquals(DEFAULT_PARALLELISM, sink.getParallelism());
		assertEquals(PartitioningProperty.HASH_PARTITIONED, sink.getGlobalProperties().getPartitioning());

		// check the iteration
		WorksetIterationPlanNode iteration = (WorksetIterationPlanNode) sink.getInput().getSource();
		assertEquals(DEFAULT_PARALLELISM, iteration.getParallelism());

		// check the solution set join and the delta
		PlanNode ssDelta = iteration.getSolutionSetDeltaPlanNode();
		assertTrue(ssDelta instanceof DualInputPlanNode); // this is only true if the update function preserves the partitioning

		DualInputPlanNode ssJoin = (DualInputPlanNode) ssDelta;
		assertEquals(DEFAULT_PARALLELISM, ssJoin.getParallelism());
		assertEquals(ShipStrategyType.PARTITION_HASH, ssJoin.getInput1().getShipStrategy());
		assertEquals(new FieldList(0), ssJoin.getInput1().getShipStrategyKeys());

		// check the workset set join
		SingleInputPlanNode sumReducer = (SingleInputPlanNode) ssJoin.getInput1().getSource();
		SingleInputPlanNode gatherMapper = (SingleInputPlanNode) sumReducer.getInput().getSource();
		DualInputPlanNode edgeJoin = (DualInputPlanNode) gatherMapper.getInput().getSource();
		assertEquals(DEFAULT_PARALLELISM, edgeJoin.getParallelism());
		// input1 is the workset
		assertEquals(ShipStrategyType.FORWARD, edgeJoin.getInput1().getShipStrategy());
		// input2 is the edges
		assertEquals(ShipStrategyType.PARTITION_HASH, edgeJoin.getInput2().getShipStrategy());
		assertTrue(edgeJoin.getInput2().getTempMode().isCached());

		assertEquals(new FieldList(0), edgeJoin.getInput2().getShipStrategyKeys());
	}

	@SuppressWarnings("serial")
	private static final class InitVertices implements MapFunction<Long, Long> {

		public Long map(Long vertexId) {
			return vertexId;
		}
	}

	@SuppressWarnings("serial")
	private static final class GatherNeighborIds extends GatherFunction<Long, NullValue, Long> {

		public Long gather(Neighbor<Long, NullValue> neighbor) {
			return neighbor.getNeighborValue();
		}
	}

	@SuppressWarnings("serial")
	private static final class SelectMinId extends SumFunction<Long, NullValue, Long> {

		public Long sum(Long newValue, Long currentValue) {
			return Math.min(newValue, currentValue);
		}
	}

	@SuppressWarnings("serial")
	private static final class UpdateComponentId extends ApplyFunction<Long, Long, Long> {

		public void apply(Long summedValue, Long origValue) {
			if (summedValue < origValue) {
				setResult(summedValue);
			}
		}
	}
}
