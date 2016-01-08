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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.flink.api.common.aggregators.LongSumAggregator;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.operators.DeltaIterationResultSet;
import org.apache.flink.api.java.operators.SingleInputUdfOperator;
import org.apache.flink.api.java.operators.TwoInputUdfOperator;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.utils.Tuple3ToEdgeMap;
import org.apache.flink.types.NullValue;
import org.junit.Test;

public class GSATranslationTest {

	@Test
	public void testTranslation() {
		try {
			final String ITERATION_NAME = "Test Name";
			
			final String AGGREGATOR_NAME = "AggregatorName";
			
			final String BC_SET_GATHER_NAME = "gather messages";
			
			final String BC_SET_SUM_NAME = "sum updates";

			final String BC_SET_APLLY_NAME = "apply updates";

			final int NUM_ITERATIONS = 13;
			
			final int ITERATION_parallelism = 77;
			
			
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			
			DataSet<Long> bcGather = env.fromElements(1L);
			DataSet<Long> bcSum = env.fromElements(1L);
			DataSet<Long> bcApply = env.fromElements(1L);

			DataSet<Vertex<Long, Long>> result;

			// ------------ construct the test program ------------------
			{

				@SuppressWarnings("unchecked")
				DataSet<Edge<Long, NullValue>> edges = env.fromElements(new Tuple3<Long, Long, NullValue>(
						1L, 2L, NullValue.getInstance())).map(new Tuple3ToEdgeMap<Long, NullValue>());

				Graph<Long, Long, NullValue> graph = Graph.fromDataSet(edges, new InitVertices(), env);

				GSAConfiguration parameters = new GSAConfiguration();

				parameters.registerAggregator(AGGREGATOR_NAME, new LongSumAggregator());
				parameters.setName(ITERATION_NAME);
				parameters.setParallelism(ITERATION_parallelism);
				parameters.addBroadcastSetForGatherFunction(BC_SET_GATHER_NAME, bcGather);
				parameters.addBroadcastSetForSumFunction(BC_SET_SUM_NAME, bcSum);
				parameters.addBroadcastSetForApplyFunction(BC_SET_APLLY_NAME, bcApply);

				result = graph.runGatherSumApplyIteration(
						new GatherNeighborIds(), new SelectMinId(),
						new UpdateComponentId(), NUM_ITERATIONS, parameters).getVertices();
				
				result.output(new DiscardingOutputFormat<Vertex<Long, Long>>());
			}
			
			
			// ------------- validate the java program ----------------
			
			assertTrue(result instanceof DeltaIterationResultSet);
			
			DeltaIterationResultSet<?, ?> resultSet = (DeltaIterationResultSet<?, ?>) result;
			DeltaIteration<?, ?> iteration = (DeltaIteration<?, ?>) resultSet.getIterationHead();
			
			// check the basic iteration properties
			assertEquals(NUM_ITERATIONS, resultSet.getMaxIterations());
			assertArrayEquals(new int[] {0}, resultSet.getKeyPositions());
			assertEquals(ITERATION_parallelism, iteration.getParallelism());
			assertEquals(ITERATION_NAME, iteration.getName());
			
			assertEquals(AGGREGATOR_NAME, iteration.getAggregators().getAllRegisteredAggregators().iterator().next().getName());
			
			// validate that the semantic properties are set as they should
			TwoInputUdfOperator<?, ?, ?, ?> solutionSetJoin = (TwoInputUdfOperator<?, ?, ?, ?>) resultSet.getNextWorkset();
			assertTrue(solutionSetJoin.getSemanticProperties().getForwardingTargetFields(0, 0).contains(0));
			assertTrue(solutionSetJoin.getSemanticProperties().getForwardingTargetFields(1, 0).contains(0));

			SingleInputUdfOperator<?, ?, ?> sumReduce = (SingleInputUdfOperator<?, ?, ?>) solutionSetJoin.getInput1();
			SingleInputUdfOperator<?, ?, ?> gatherMap = (SingleInputUdfOperator<?, ?, ?>) sumReduce.getInput();

			// validate that the broadcast sets are forwarded
			assertEquals(bcGather, gatherMap.getBroadcastSets().get(BC_SET_GATHER_NAME));
			assertEquals(bcSum, sumReduce.getBroadcastSets().get(BC_SET_SUM_NAME));
			assertEquals(bcApply, solutionSetJoin.getBroadcastSets().get(BC_SET_APLLY_NAME));
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@SuppressWarnings("serial")
	private static final class InitVertices	implements MapFunction<Long, Long> {

		public Long map(Long vertexId) {
			return vertexId;
		}
	}

	@SuppressWarnings("serial")
	private static final class GatherNeighborIds extends GatherFunction<Long, NullValue, Long> {

		public Long gather(Neighbor<Long, NullValue> neighbor) {
			return neighbor.getNeighborValue();
		}
	};

	@SuppressWarnings("serial")
	private static final class SelectMinId extends SumFunction<Long, NullValue, Long> {

		public Long sum(Long newValue, Long currentValue) {
			return Math.min(newValue, currentValue);
		}
	};

	@SuppressWarnings("serial")
	private static final class UpdateComponentId extends ApplyFunction<Long, Long, Long> {

		public void apply(Long summedValue, Long origValue) {
			if (summedValue < origValue) {
				setResult(summedValue);
			}
		}
	}
}
